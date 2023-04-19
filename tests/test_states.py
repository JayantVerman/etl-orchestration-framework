"""Tests for task and workflow state machines."""

from __future__ import annotations

import pytest

from etl_orchestrator.exceptions import StateTransitionError
from etl_orchestrator.states import (
    FAILURE_STATES,
    TASK_TRANSITIONS,
    TERMINAL_TASK_STATES,
    TERMINAL_WORKFLOW_STATES,
    WORKFLOW_TRANSITIONS,
    TaskState,
    WorkflowState,
    aggregate_workflow_state,
    can_transition,
    can_transition_workflow,
    is_terminal_task_state,
    is_terminal_workflow_state,
    transition_task,
    transition_workflow,
)


class TestTaskTransitions:
    def test_happy_path(self) -> None:
        assert can_transition(TaskState.PENDING, TaskState.RUNNING)
        assert can_transition(TaskState.RUNNING, TaskState.SUCCESS)

    def test_running_to_failed(self) -> None:
        assert can_transition(TaskState.RUNNING, TaskState.FAILED)

    def test_retry_path(self) -> None:
        assert can_transition(TaskState.FAILED, TaskState.RETRYING)
        assert can_transition(TaskState.RETRYING, TaskState.RUNNING)
        assert can_transition(TaskState.RUNNING, TaskState.SUCCESS)

    def test_upstream_failure_path(self) -> None:
        assert can_transition(TaskState.PENDING, TaskState.UPSTREAM_FAILED)
        assert can_transition(TaskState.UPSTREAM_FAILED, TaskState.RETRYING)
        assert can_transition(TaskState.UPSTREAM_FAILED, TaskState.SKIPPED)

    def test_cancellation_paths(self) -> None:
        assert can_transition(TaskState.PENDING, TaskState.CANCELLED)
        assert can_transition(TaskState.RUNNING, TaskState.CANCELLED)
        assert can_transition(TaskState.RETRYING, TaskState.CANCELLED)
        assert can_transition(TaskState.UPSTREAM_FAILED, TaskState.CANCELLED)

    @pytest.mark.parametrize(
        ("current", "target"),
        [
            (TaskState.SUCCESS, TaskState.RUNNING),
            (TaskState.SUCCESS, TaskState.FAILED),
            (TaskState.SKIPPED, TaskState.RUNNING),
            (TaskState.CANCELLED, TaskState.RUNNING),
            (TaskState.PENDING, TaskState.SUCCESS),  # must run first
            (TaskState.PENDING, TaskState.FAILED),
            (TaskState.FAILED, TaskState.SUCCESS),
            (TaskState.RETRYING, TaskState.SUCCESS),  # must re-run first
        ],
    )
    def test_illegal_transitions_rejected(
        self, current: TaskState, target: TaskState
    ) -> None:
        assert not can_transition(current, target)
        with pytest.raises(StateTransitionError) as exc:
            transition_task(current, target)
        assert exc.value.current == current.value
        assert exc.value.target == target.value
        assert "task" in str(exc.value)

    def test_every_state_has_a_transition_entry(self) -> None:
        assert set(TASK_TRANSITIONS) == set(TaskState)

    def test_terminal_task_states_cannot_leave(self) -> None:
        for state in TERMINAL_TASK_STATES:
            assert TASK_TRANSITIONS[state] == frozenset()
            assert is_terminal_task_state(state)

    def test_non_terminal_states_can_leave(self) -> None:
        for state in set(TaskState) - TERMINAL_TASK_STATES:
            assert TASK_TRANSITIONS[state], state
            assert not is_terminal_task_state(state)

    def test_failure_states(self) -> None:
        assert FAILURE_STATES == frozenset(
            {TaskState.FAILED, TaskState.UPSTREAM_FAILED}
        )


class TestWorkflowTransitions:
    def test_happy_path(self) -> None:
        assert can_transition_workflow(WorkflowState.PENDING, WorkflowState.RUNNING)
        assert can_transition_workflow(WorkflowState.RUNNING, WorkflowState.SUCCESS)

    def test_failure_path(self) -> None:
        assert can_transition_workflow(WorkflowState.RUNNING, WorkflowState.FAILED)

    def test_cancellation_paths(self) -> None:
        assert can_transition_workflow(WorkflowState.PENDING, WorkflowState.CANCELLED)
        assert can_transition_workflow(WorkflowState.RUNNING, WorkflowState.CANCELLED)

    def test_cannot_start_from_terminal(self) -> None:
        for state in TERMINAL_WORKFLOW_STATES:
            assert not can_transition_workflow(state, WorkflowState.RUNNING)
            assert is_terminal_workflow_state(state)

    def test_illegal_transition_raises(self) -> None:
        with pytest.raises(StateTransitionError) as exc:
            transition_workflow(WorkflowState.SUCCESS, WorkflowState.FAILED)
        assert exc.value.current == "success"
        assert exc.value.target == "failed"
        assert "workflow" in str(exc.value)

    def test_every_state_has_a_transition_entry(self) -> None:
        assert set(WORKFLOW_TRANSITIONS) == set(WorkflowState)


class TestWorkflowStateAggregation:
    def test_empty_returns_none(self) -> None:
        assert aggregate_workflow_state([]) is None

    def test_all_success(self) -> None:
        states = [TaskState.SUCCESS, TaskState.SUCCESS]
        assert aggregate_workflow_state(states) == WorkflowState.SUCCESS

    def test_success_with_skipped(self) -> None:
        states = [TaskState.SUCCESS, TaskState.SKIPPED]
        assert aggregate_workflow_state(states) == WorkflowState.SUCCESS

    def test_any_running(self) -> None:
        states = [TaskState.SUCCESS, TaskState.RUNNING, TaskState.PENDING]
        assert aggregate_workflow_state(states) == WorkflowState.RUNNING

    def test_retrying_counts_as_running(self) -> None:
        states = [TaskState.RETRYING, TaskState.SUCCESS]
        assert aggregate_workflow_state(states) == WorkflowState.RUNNING

    def test_any_failed(self) -> None:
        states = [TaskState.SUCCESS, TaskState.FAILED]
        assert aggregate_workflow_state(states) == WorkflowState.FAILED

    def test_failed_outranks_cancelled(self) -> None:
        # Both present: FAILED is the more informative terminal outcome.
        states = [TaskState.FAILED, TaskState.CANCELLED]
        assert aggregate_workflow_state(states) == WorkflowState.FAILED

    def test_cancelled_only(self) -> None:
        states = [TaskState.CANCELLED, TaskState.SUCCESS]
        assert aggregate_workflow_state(states) == WorkflowState.CANCELLED


class TestStateEnumValues:
    def test_states_are_lowercase_strings(self) -> None:
        for task_state in TaskState:
            assert task_state.value == str(task_state)
            assert task_state.value == task_state.value.lower()
        for workflow_state in WorkflowState:
            assert workflow_state.value == str(workflow_state)
