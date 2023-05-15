"""Workflow (DAG) model, graph validation, and dependency resolution.

A :class:`Workflow` is a directed acyclic graph of :class:`Task`
objects. Edges are expressed as *upstream dependencies*: a task runs
only after every task it depends on has completed successfully
(success semantics are enforced by the execution engine in later
milestones; this module owns structure and structure validation only).
"""

from __future__ import annotations

import heapq
import itertools
import re
from enum import IntEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from etl_orchestrator.exceptions import (
    DependencyCycleError,
    DuplicateTaskError,
    SelfDependencyError,
    UnknownTaskError,
    WorkflowValidationError,
)

#: Identifiers must start with a letter and contain only letters,
#: digits, underscores, and hyphens. This keeps ids valid as YAML keys,
#: CLI arguments, and database columns.
_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")

#: Validation message appended when a workflow defines no tasks.
_EMPTY_WORKFLOW_MESSAGE = "workflow must define at least one task"


def _validate_id(value: str, kind: str) -> str:
    """Validate an identifier, returning it unchanged when valid."""
    if not value or not _ID_PATTERN.match(value):
        raise ValueError(
            f"invalid {kind} '{value}': must start with a letter and "
            "contain only letters, digits, underscores, and hyphens"
        )
    return value


class _Color(IntEnum):
    """DFS visit colors for cycle detection."""

    WHITE = 0  # unvisited
    GRAY = 1  # visiting (on the current DFS path)
    BLACK = 2  # fully explored


class Task(BaseModel):
    """A single unit of work inside a workflow.

    Task ids are immutable and unique within a workflow. The actual
    work (the callable executed by the engine) is attached in the
    execution milestones; this model carries identity and metadata.
    """

    model_config = ConfigDict(frozen=True)

    task_id: str
    description: str = ""

    # Upstream dependencies. When a task is added to a workflow via
    # :meth:`Workflow.add_task`, each entry in this list is wired into the
    # graph as a directed edge ``dependency -> task``. Equivalent to
    # calling :meth:`Workflow.add_dependency` for every entry.
    depends_on: list[str] = Field(default_factory=list)

    # Retry policy (enforced by the execution engine, Milestone 5).
    max_attempts: int = Field(default=1, ge=1)
    backoff_seconds: float = Field(default=0.0, ge=0.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)

    @field_validator("task_id")
    @classmethod
    def _check_task_id(cls, value: str) -> str:
        return _validate_id(value, "task id")

    def backoff_delay(self, failed_attempt: int) -> float:
        """Return the delay before re-running after ``failed_attempt``.

        Attempt 1 failure waits ``backoff_seconds``, attempt 2 failure
        waits ``backoff_seconds * backoff_multiplier``, and so on.
        """
        return self.backoff_seconds * (self.backoff_multiplier ** (failed_attempt - 1))


class Workflow(BaseModel):
    """A directed acyclic graph of tasks.

    Dependencies are stored as ``task_id -> frozenset(upstream task
    ids)``. Only task ids that exist in :attr:`tasks` may appear as
    keys or as upstream members; :meth:`add_task` and
    :meth:`add_dependency` enforce this fail-fast, while
    :meth:`validate_graph` re-checks the full graph (including cycle
    and emptiness rules) for workflows built or loaded in bulk.
    """

    workflow_id: str
    description: str = ""
    tasks: dict[str, Task] = Field(default_factory=dict)
    dependencies: dict[str, frozenset[str]] = Field(default_factory=dict)

    @field_validator("workflow_id")
    @classmethod
    def _check_workflow_id(cls, value: str) -> str:
        return _validate_id(value, "workflow id")

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    def add_task(self, task: Task) -> None:
        """Register a task and, if it declares upstream dependencies via
        ``depends_on``, wire those into the dependency graph.

        Raises:
            DuplicateTaskError: If a task with the same id exists.
        """
        if task.task_id in self.tasks:
            raise DuplicateTaskError(task.task_id)
        self.tasks[task.task_id] = task
        for upstream_id in task.depends_on:
            self.add_dependency(task.task_id, upstream_id)

    def add_dependency(self, task_id: str, upstream_id: str) -> None:
        """Declare that ``task_id`` must run after ``upstream_id``.

        Raises:
            UnknownTaskError: If either task id is not registered.
            SelfDependencyError: If both ids are identical.
        """
        if upstream_id not in self.tasks:
            raise UnknownTaskError(upstream_id)
        if task_id not in self.tasks:
            raise UnknownTaskError(task_id)
        if task_id == upstream_id:
            raise SelfDependencyError(task_id)
        upstream = set(self.dependencies.get(task_id, frozenset()))
        upstream.add(upstream_id)
        self.dependencies[task_id] = frozenset(upstream)

    # ------------------------------------------------------------------
    # Graph queries
    # ------------------------------------------------------------------
    def upstream(self, task_id: str) -> frozenset[str]:
        """Return the direct upstream dependencies of a task."""
        return self.dependencies.get(task_id, frozenset())

    def downstream(self, task_id: str) -> frozenset[str]:
        """Return the direct downstream dependents of a task."""
        return frozenset(
            tid for tid, ups in self.dependencies.items() if task_id in ups
        )

    def roots(self) -> frozenset[str]:
        """Return tasks with no upstream dependencies (entry points)."""
        return frozenset(tid for tid in self.tasks if not self.dependencies.get(tid))

    def leaves(self) -> frozenset[str]:
        """Return tasks with no downstream dependents (exit points)."""
        depended = {up for ups in self.dependencies.values() for up in ups}
        return frozenset(tid for tid in self.tasks if tid not in depended)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate_graph(self) -> list[str]:
        """Validate the whole graph, collecting every problem found.

        Returns a list of human-readable error messages; an empty list
        means the graph is a valid non-empty DAG.
        """
        errors: list[str] = []
        if not self.tasks:
            errors.append(_EMPTY_WORKFLOW_MESSAGE)
        for task_id, upstream in self.dependencies.items():
            if task_id not in self.tasks:
                errors.append(f"unknown task id: '{task_id}'")
            for up_id in sorted(upstream):
                if up_id == task_id:
                    errors.append(f"task '{task_id}' cannot depend on itself")
                elif up_id not in self.tasks:
                    errors.append(f"unknown task id: '{up_id}'")
        for cycle in self._find_cycles():
            cycle_path = " -> ".join([*cycle, cycle[0]])
            errors.append(f"dependency cycle: {cycle_path}")
        return errors

    def validate_workflow(self) -> None:
        """Validate the graph, raising if any problem exists.

        Named ``validate_workflow`` (not ``validate``) to avoid
        clashing with the pydantic ``BaseModel.validate`` classmethod.

        Raises:
            WorkflowValidationError: If any validation problem exists.
        """
        errors = self.validate_graph()
        if errors:
            raise WorkflowValidationError(errors)

    def _find_cycles(self) -> list[list[str]]:
        """Find dependency cycles via depth-first search.

        Returns each distinct cycle once, as a list of task ids in
        traversal order (without the closing repetition).
        """
        color = dict.fromkeys(self.tasks, _Color.WHITE)
        path: list[str] = []
        seen: set[frozenset[str]] = set()
        cycles: list[list[str]] = []

        def visit(node: str) -> None:
            color[node] = _Color.GRAY
            path.append(node)
            for neighbor in sorted(self.dependencies.get(node, frozenset())):
                if neighbor not in self.tasks:
                    continue
                if color[neighbor] == _Color.GRAY:
                    cycle = path[path.index(neighbor) :]
                    key = frozenset(cycle)
                    if key not in seen:
                        seen.add(key)
                        cycles.append(cycle)
                elif color[neighbor] == _Color.WHITE:
                    visit(neighbor)
            path.pop()
            color[node] = _Color.BLACK

        for task_id in sorted(self.tasks):
            if color[task_id] == _Color.WHITE:
                visit(task_id)
        return cycles

    # ------------------------------------------------------------------
    # Dependency resolution
    # ------------------------------------------------------------------
    def topological_order(self) -> list[str]:
        """Return a deterministic topological order of all tasks.

        Uses Kahn's algorithm with a min-heap so that independent tasks
        are emitted in ascending task-id order, making the result
        reproducible across runs and machines.

        Raises:
            DependencyCycleError: If the graph contains a cycle.
        """
        indegree = dict.fromkeys(self.tasks, 0)
        for task_id, upstream in self.dependencies.items():
            indegree[task_id] = len(upstream & self.tasks.keys())

        ready: list[str] = [
            task_id for task_id, degree in indegree.items() if degree == 0
        ]
        heapq.heapify(ready)
        order: list[str] = []

        while ready:
            node = heapq.heappop(ready)
            order.append(node)
            for child in sorted(self.downstream(node)):
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(ready, child)

        if len(order) != len(self.tasks):
            cycle = self._find_cycles()
            if cycle:
                raise DependencyCycleError([*cycle[0], cycle[0][0]])
            remaining = sorted(set(self.tasks) - set(order))
            raise DependencyCycleError(remaining)

        return order

    # ------------------------------------------------------------------
    # Convenience builders
    # ------------------------------------------------------------------
    @classmethod
    def from_chain(cls, workflow_id: str, task_ids: list[str]) -> Workflow:
        """Build a linear workflow ``t1 -> t2 -> ... -> tn``."""
        workflow = cls(workflow_id=workflow_id)
        for task_id in task_ids:
            workflow.add_task(Task(task_id=task_id))
        for upstream, downstream in itertools.pairwise(task_ids):
            workflow.add_dependency(downstream, upstream)
        return workflow
