"""YAML workflow configuration (Milestone 7).

Workflow definitions can be declared in YAML instead of Python code::

    workflow_id: daily_etl
    description: Daily customers/orders ETL.
    tasks:
      - task_id: extract_customers
        callable: echo          # name in the task registry
        params:
          message: extracting customers
      - task_id: validate_customers
        callable: echo
        depends_on: [extract_customers]

Loading never produces a half-valid workflow: either the file parses
and the resulting :class:`~etl_orchestrator.dag.Workflow` validates, or
a :class:`WorkflowConfigError` (or another framework error) is raised.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from etl_orchestrator.dag import Task, Workflow
from etl_orchestrator.engine import TaskCallable
from etl_orchestrator.exceptions import (
    UnknownTaskError,
    WorkflowConfigError,
)


class TaskConfig(BaseModel):
    """One task entry from a workflow configuration file."""

    task_id: str
    callable: str
    description: str = ""
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=1, ge=1)
    backoff_seconds: float = Field(default=0.0, ge=0.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)
    params: dict[str, object] = Field(default_factory=dict)


class WorkflowConfig(BaseModel):
    """A workflow configuration file's content."""

    workflow_id: str
    description: str = ""
    tasks: list[TaskConfig] = Field(min_length=1)


def load_workflow_config(path: str | Path) -> WorkflowConfig:
    """Read and validate a workflow configuration file.

    Raises:
        WorkflowConfigError: If the file is missing or is not valid
            YAML / does not match the configuration schema.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise WorkflowConfigError(f"workflow file not found: {file_path}")
    try:
        raw = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise WorkflowConfigError(f"invalid YAML in {file_path}: {exc}") from exc
    try:
        return WorkflowConfig.model_validate(raw)
    except Exception as exc:
        raise WorkflowConfigError(f"invalid workflow configuration in {file_path}: {exc}") from exc


def build_workflow(config: WorkflowConfig) -> Workflow:
    """Build a validated :class:`Workflow` from its configuration.

    Raises:
        WorkflowConfigError: If a task depends on an unknown task.
        DuplicateTaskError: If a task id is declared twice.
    """
    workflow = Workflow(workflow_id=config.workflow_id, description=config.description)
    for task_config in config.tasks:
        workflow.add_task(
            Task(
                task_id=task_config.task_id,
                description=task_config.description,
                max_attempts=task_config.max_attempts,
                backoff_seconds=task_config.backoff_seconds,
                backoff_multiplier=task_config.backoff_multiplier,
            )
        )
    for task_config in config.tasks:
        for upstream in task_config.depends_on:
            try:
                workflow.add_dependency(task_config.task_id, upstream)
            except UnknownTaskError as exc:
                raise WorkflowConfigError(
                    f"task '{task_config.task_id}' depends on unknown task '{upstream}'"
                ) from exc
    return workflow


def load_workflow(path: str | Path) -> Workflow:
    """Load a workflow definition from a YAML file."""
    return build_workflow(load_workflow_config(path))


def load_task_params(path: str | Path) -> dict[str, dict[str, object]]:
    """Return per-task parameter dicts from a workflow config file."""
    return {
        task.task_id: dict(task.params) for task in load_workflow_config(path).tasks if task.params
    }


def resolve_callables(
    config: WorkflowConfig,
    registry: Mapping[str, TaskCallable],
) -> dict[str, TaskCallable]:
    """Map each configured task id to its registry callable.

    Raises:
        WorkflowConfigError: If a task references an unknown callable
            name.
    """
    missing = sorted({t.task_id for t in config.tasks if t.callable not in registry})
    if missing:
        known = ", ".join(sorted(registry))
        raise WorkflowConfigError(
            "unknown callable(s) for task(s): "
            + ", ".join(missing)
            + f" (known callables: {known})"
        )
    return {t.task_id: registry[t.callable] for t in config.tasks}
