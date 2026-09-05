"""Task and workflow state machines.

States model the lifecycle of tasks and workflow runs. Every state
change must be declared in the transition tables below, which keeps
execution-engine behavior explicit and testable.

Task lifecycle (success path)::

    pending -> running -> success

Failure path::

    pending -> running -> failed -> retrying -> running ...
    pending -> upstream_failed   (a dependency failed terminally)
    any active state -> cancelled

``success``, ``skipped``, and ``cancelled`` are terminal for a task
*run*; recovery (resetting a terminal task so it can run again) is
owned by the recovery milestone and modeled there as an explicit
transition.
"""

from __future__ import annotations

from enum import Enum

from etl_orchestrator.exceptions import StateTransitionError


class TaskState(str, Enum):
    """Lifecycle states of a single task run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    RETRYING = "retrying"
    FAILED = "failed"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    def __str__(self) -> str:
        return self.value


class WorkflowState(str, Enum):
    """Lifecycle states of a workflow run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

    def __str__(self) -> str:
        return self.value


#: States meaning the task (or its dependencies) did not succeed.
FAILURE_STATES: frozenset[TaskState] = frozenset({TaskState.FAILED, TaskState.UPSTREAM_FAILED})

#: States from which a task will never move again (for a given run).
TERMINAL_TASK_STATES: frozenset[TaskState] = frozenset(
    {TaskState.SUCCESS, TaskState.SKIPPED, TaskState.CANCELLED}
)

#: States from which a workflow run will never move again.
TERMINAL_WORKFLOW_STATES: frozenset[WorkflowState] = frozenset(
    {WorkflowState.SUCCESS, WorkflowState.FAILED, WorkflowState.CANCELLED}
)

#: Legal task-state transitions.
TASK_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset(
        {
            TaskState.RUNNING,
            TaskState.RETRYING,  # scheduled for an immediate retry
            TaskState.UPSTREAM_FAILED,
            TaskState.SKIPPED,
            TaskState.CANCELLED,
        }
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCESS,
            TaskState.FAILED,
            TaskState.CANCELLED,
        }
    ),
    TaskState.RETRYING: frozenset(
        {
            TaskState.RUNNING,  # backoff elapsed, picked up again
            TaskState.CANCELLED,
        }
    ),
    TaskState.FAILED: frozenset(
        {
            TaskState.RETRYING,  # attempts remain: schedule a retry
            TaskState.UPSTREAM_FAILED,  # no attempts left: block downstream
        }
    ),
    TaskState.UPSTREAM_FAILED: frozenset(
        {
            TaskState.RETRYING,  # upstream recovered, requeue
            TaskState.SKIPPED,  # workflow decided not to requeue
            TaskState.CANCELLED,
        }
    ),
    TaskState.SUCCESS: frozenset(),
    TaskState.SKIPPED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}

#: Legal workflow-state transitions.
WORKFLOW_TRANSITIONS: dict[WorkflowState, frozenset[WorkflowState]] = {
    WorkflowState.PENDING: frozenset({WorkflowState.RUNNING, WorkflowState.CANCELLED}),
    WorkflowState.RUNNING: frozenset(
        {
            WorkflowState.SUCCESS,
            WorkflowState.FAILED,
            WorkflowState.CANCELLED,
        }
    ),
    WorkflowState.SUCCESS: frozenset(),
    WorkflowState.FAILED: frozenset(),
    WorkflowState.CANCELLED: frozenset(),
}


def can_transition(current: TaskState, target: TaskState) -> bool:
    """Return whether a task-state change is legal."""
    return target in TASK_TRANSITIONS[current]


def transition_task(current: TaskState, target: TaskState) -> None:
    """Validate a task-state change, raising when it is illegal.

    Raises:
        StateTransitionError: If the transition is not declared.
    """
    if not can_transition(current, target):
        raise StateTransitionError(current.value, target.value, "task")


def can_transition_workflow(current: WorkflowState, target: WorkflowState) -> bool:
    """Return whether a workflow-state change is legal."""
    return target in WORKFLOW_TRANSITIONS[current]


def transition_workflow(current: WorkflowState, target: WorkflowState) -> None:
    """Validate a workflow-state change, raising when it is illegal.

    Raises:
        StateTransitionError: If the transition is not declared.
    """
    if not can_transition_workflow(current, target):
        raise StateTransitionError(current.value, target.value, "workflow")


def is_terminal_task_state(state: TaskState) -> bool:
    """Return whether a task run can no longer change state."""
    return state in TERMINAL_TASK_STATES


def is_terminal_workflow_state(state: WorkflowState) -> bool:
    """Return whether a workflow run can no longer change state."""
    return state in TERMINAL_WORKFLOW_STATES


def aggregate_workflow_state(task_states: list[TaskState]) -> WorkflowState | None:
    """Derive a workflow state from the states of its tasks.

    Rules, applied in order:

    1. No tasks -> ``None`` (caller decides what an empty run means).
    2. Any task ``running`` or ``retrying`` -> ``RUNNING``.
    3. Any task ``failed`` -> ``FAILED`` (more informative than a
       cancellation that may have followed the failure).
    4. Any task ``cancelled`` -> ``CANCELLED``.
    5. Remaining tasks are only ``success`` / ``skipped`` /
       ``upstream_failed`` -> ``SUCCESS``.

    ``upstream_failed`` surviving to rule 5 occurs when a workflow was
    short-circuited before those tasks were queued; rules 3-4 take
    precedence in ordinary runs.
    """
    if not task_states:
        return None
    states = set(task_states)
    if states & {TaskState.RUNNING, TaskState.RETRYING}:
        return WorkflowState.RUNNING
    if TaskState.FAILED in states:
        return WorkflowState.FAILED
    if TaskState.CANCELLED in states:
        return WorkflowState.CANCELLED
    return WorkflowState.SUCCESS
