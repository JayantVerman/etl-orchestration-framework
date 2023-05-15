"""Engine hooks (Milestone 11).

Optional callbacks the execution engine fires at well-defined points.
All hooks are optional; passing nothing keeps the engine's behavior
identical. Hooks are invoked synchronously, *after* the corresponding
state change, and must not raise (an exception in a hook propagates and
fails the run — treat hooks as trusted instrumentation code).

Typical uses: notifications (email/Slack), metrics emission, audit logs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etl_orchestrator.engine import TaskRunRecord, WorkflowRun

WorkflowHook = Callable[["WorkflowRun"], None]
TaskHook = Callable[["WorkflowRun", "TaskRunRecord"], None]
RetryHook = Callable[["WorkflowRun", "TaskRunRecord", float], None]


@dataclass
class EngineHooks:
    """Optional lifecycle callbacks for the execution engine."""

    on_workflow_start: WorkflowHook | None = None
    on_task_start: TaskHook | None = None
    on_task_success: TaskHook | None = None
    on_task_retry: RetryHook | None = None
    on_task_failure: TaskHook | None = None
    on_workflow_end: WorkflowHook | None = None
