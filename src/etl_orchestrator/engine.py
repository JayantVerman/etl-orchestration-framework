"""Workflow execution engine.

Executes a validated :class:`~etl_orchestrator.dag.Workflow` in
deterministic topological order, one task at a time (bounded parallel
execution of independent tasks arrives in the concurrency milestone).

Every state change goes through the transition tables in
:mod:`etl_orchestrator.states`, so an illegal transition can never
occur silently — the engine either performs a declared move or the
framework raises.

Failure semantics (before retries exist, Milestone 5): when a task
fails, all of its transitive downstream tasks that are still
``pending`` are marked ``upstream_failed`` and never executed. Tasks
in *independent* branches are unaffected.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone

from etl_orchestrator.dag import Workflow
from etl_orchestrator.exceptions import EngineConfigurationError
from etl_orchestrator.states import (
    TaskState,
    WorkflowState,
    aggregate_workflow_state,
    transition_task,
    transition_workflow,
)

#: Signature every task callable must implement.
TaskCallable = Callable[["TaskContext"], None]

UtcNow = Callable[[], datetime]


@dataclass
class TaskContext:
    """Information handed to a task callable when it runs."""

    run_id: str
    task_id: str
    workflow_id: str
    attempt: int = 1
    params: dict[str, object] = field(default_factory=dict)


@dataclass
class TaskRunRecord:
    """Execution record for one task within one workflow run."""

    task_id: str
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass
class WorkflowRun:
    """The full record of one execution of a workflow."""

    workflow_id: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    workflow_state: WorkflowState = WorkflowState.PENDING
    records: dict[str, TaskRunRecord] = field(default_factory=dict)
    log: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def record(self, task_id: str) -> TaskRunRecord:
        """Return (creating if needed) the record for a task."""
        if task_id not in self.records:
            self.records[task_id] = TaskRunRecord(task_id=task_id)
        return self.records[task_id]

    def task_states(self) -> list[TaskState]:
        """Return the current state of every task record."""
        return [record.state for record in self.records.values()]

    def note(self, message: str) -> None:
        """Append an entry to the run's transition log."""
        self.log.append(message)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkflowEngine:
    """Executes a workflow's tasks respecting dependency order."""

    def __init__(
        self,
        workflow: Workflow,
        tasks: Mapping[str, TaskCallable],
        *,
        clock: UtcNow = _utcnow,
    ) -> None:
        """Create an engine for a workflow and its task callables.

        Args:
            workflow: The workflow definition; validated here so an
                invalid graph can never be executed.
            tasks: One callable per task id. Each callable receives a
                :class:`TaskContext`.
            clock: Injectable clock for deterministic tests.

        Raises:
            EngineConfigurationError: If the workflow is invalid, a
                task has no callable, or a callable has no task.
        """
        try:
            workflow.validate_workflow()
        except Exception as exc:
            raise EngineConfigurationError(
                f"workflow '{workflow.workflow_id}' is not executable: {exc}"
            ) from exc

        missing = sorted(set(workflow.tasks) - set(tasks))
        if missing:
            raise EngineConfigurationError(
                "no callable registered for task(s): " + ", ".join(missing)
            )
        extra = sorted(set(tasks) - set(workflow.tasks))
        if extra:
            raise EngineConfigurationError(
                "callable(s) registered for unknown task(s): " + ", ".join(extra)
            )

        self._workflow = workflow
        self._tasks = dict(tasks)
        self._clock = clock

    @property
    def workflow(self) -> Workflow:
        """The workflow definition this engine executes."""
        return self._workflow

    def run(self, params: Mapping[str, object] | None = None) -> WorkflowRun:
        """Execute the workflow once, sequentially, in topological order.

        Args:
            params: Optional parameters copied into every task context.

        Returns:
            The completed :class:`WorkflowRun` with per-task records,
            a transition log, and the final workflow state.
        """
        run = WorkflowRun(workflow_id=self._workflow.workflow_id)
        run_params = dict(params or {})
        run.started_at = self._clock()
        transition_workflow(run.workflow_state, WorkflowState.RUNNING)
        run.workflow_state = WorkflowState.RUNNING
        run.note("workflow: pending -> running")

        for task_id in self._workflow.topological_order():
            self._execute_task(run, task_id, run_params)

        final = aggregate_workflow_state(run.task_states())
        assert final is not None, "validated workflows have >= 1 task"
        transition_workflow(run.workflow_state, final)
        run.workflow_state = final
        run.finished_at = self._clock()
        run.note(f"workflow: running -> {final.value}")
        return run

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _execute_task(
        self,
        run: WorkflowRun,
        task_id: str,
        run_params: dict[str, object],
    ) -> None:
        """Run one task unless its state forbids execution.

        Only ``pending`` tasks execute; ``upstream_failed`` and other
        non-pending states are skipped (re-running a blocked or
        finished task is recovery territory, Milestone 12).
        """
        record = run.record(task_id)
        if record.state is not TaskState.PENDING:
            run.note(f"{task_id}: skipped execution (state={record.state.value})")
            return

        ctx = TaskContext(
            run_id=run.run_id,
            task_id=task_id,
            workflow_id=run.workflow_id,
            attempt=record.attempts + 1,
            params=dict(run_params),
        )

        transition_task(record.state, TaskState.RUNNING)
        record.state = TaskState.RUNNING
        record.attempts = ctx.attempt
        if record.started_at is None:
            record.started_at = self._clock()
        run.note(f"{task_id}: {TaskState.PENDING.value} -> running")

        try:
            self._tasks[task_id](ctx)
        except Exception as exc:
            transition_task(record.state, TaskState.FAILED)
            record.state = TaskState.FAILED
            record.error = f"{type(exc).__name__}: {exc}"
            record.finished_at = self._clock()
            run.note(f"{task_id}: running -> failed ({record.error})")
            self._block_downstream(run, task_id)
            return

        transition_task(record.state, TaskState.SUCCESS)
        record.state = TaskState.SUCCESS
        record.finished_at = self._clock()
        run.note(f"{task_id}: running -> success")

    def _block_downstream(self, run: WorkflowRun, failed_task_id: str) -> None:
        """Mark still-pending transitive dependents upstream_failed."""
        pending_visit = sorted(self._workflow.downstream(failed_task_id))
        visited: set[str] = set()
        while pending_visit:
            task_id = pending_visit.pop(0)
            if task_id in visited:
                continue
            visited.add(task_id)
            record = run.record(task_id)
            if record.state == TaskState.PENDING:
                transition_task(record.state, TaskState.UPSTREAM_FAILED)
                record.state = TaskState.UPSTREAM_FAILED
                record.error = f"upstream task '{failed_task_id}' failed"
                record.finished_at = self._clock()
                run.note(
                    f"{task_id}: pending -> upstream_failed "
                    f"(upstream '{failed_task_id}' failed)"
                )
            pending_visit.extend(sorted(self._workflow.downstream(task_id)))
