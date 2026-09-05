"""Tests for retry and failure handling (Milestone 5)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from etl_orchestrator import (
    Task,
    TaskContext,
    TaskState,
    Workflow,
    WorkflowEngine,
    WorkflowState,
)


class FlakyRunner:
    """Task callables that fail a fixed number of times before succeeding."""

    def __init__(self, failures_by_task: dict[str, int]) -> None:
        self.failures_by_task = dict(failures_by_task)
        self.calls: list[tuple[str, int]] = []

    def callables(self, workflow: Workflow) -> dict[str, Any]:
        def make(task_id: str) -> Any:
            def task(ctx: TaskContext) -> None:
                remaining = self.failures_by_task.get(task_id, 0)
                self.calls.append((task_id, ctx.attempt))
                if remaining > 0:
                    self.failures_by_task[task_id] = remaining - 1
                    raise RuntimeError(f"flaky failure #{ctx.attempt}")

            return task

        return {tid: make(tid) for tid in workflow.tasks}


class SleepRecorder:
    """Replaces time.sleep; records requested delays."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def chain_with_retries(
    max_attempts: int,
    backoff_seconds: float = 0.0,
    backoff_multiplier: float = 2.0,
) -> Workflow:
    wf = Workflow.from_chain("retry_chain", ["a", "b"])
    # from_chain rebuilt tasks with defaults; re-register retry fields.
    wf.tasks["a"] = Task(
        task_id="a",
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        backoff_multiplier=backoff_multiplier,
    )
    return wf


# ----------------------------------------------------------------------
# Transient failures recover
# ----------------------------------------------------------------------
class TestTransientFailures:
    def test_succeeds_on_second_attempt(self) -> None:
        wf = chain_with_retries(max_attempts=3)
        runner = FlakyRunner({"a": 1})
        sleeps = SleepRecorder()
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=sleeps).run()
        assert run.records["a"].state == TaskState.SUCCESS
        assert run.records["a"].attempts == 2
        assert run.records["b"].state == TaskState.SUCCESS
        assert run.workflow_state == WorkflowState.SUCCESS

    def test_succeeds_on_last_allowed_attempt(self) -> None:
        wf = chain_with_retries(max_attempts=3)
        runner = FlakyRunner({"a": 2})
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=SleepRecorder()).run()
        assert run.records["a"].state == TaskState.SUCCESS
        assert run.records["a"].attempts == 3
        assert run.workflow_state == WorkflowState.SUCCESS

    def test_downstream_runs_after_recovered_upstream(self) -> None:
        wf = chain_with_retries(max_attempts=3)
        runner = FlakyRunner({"a": 1})
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=SleepRecorder()).run()
        assert [t for t, _ in runner.calls] == ["a", "a", "b"]
        assert run.records["b"].attempts == 1

    def test_attempt_numbers_passed_to_context(self) -> None:
        seen: list[int] = []

        def flaky(ctx: TaskContext) -> None:
            seen.append(ctx.attempt)
            if ctx.attempt < 3:
                raise RuntimeError("again")

        wf = chain_with_retries(max_attempts=5)
        engine = WorkflowEngine(
            wf,
            {"a": flaky, "b": lambda ctx: None},
            sleeper=SleepRecorder(),
        )
        run = engine.run()
        assert seen == [1, 2, 3]
        assert run.records["a"].attempts == 3


# ----------------------------------------------------------------------
# Exhaustion is a permanent failure
# ----------------------------------------------------------------------
class TestRetryExhaustion:
    def test_exhaustion_fails_task_and_blocks_downstream(self) -> None:
        wf = chain_with_retries(max_attempts=2)
        runner = FlakyRunner({"a": 99})  # never succeeds
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=SleepRecorder()).run()
        assert run.records["a"].state == TaskState.FAILED
        assert run.records["a"].attempts == 2
        assert run.records["b"].state == TaskState.UPSTREAM_FAILED
        assert [t for t, _ in runner.calls] == ["a", "a"]
        assert run.workflow_state == WorkflowState.FAILED

    def test_default_max_attempts_is_one(self) -> None:
        wf = Workflow.from_chain("plain", ["a", "b"])
        runner = FlakyRunner({"a": 5})
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=SleepRecorder()).run()
        assert run.records["a"].attempts == 1
        assert run.records["b"].state == TaskState.UPSTREAM_FAILED

    def test_exhaustion_log_records_permanent_failure(self) -> None:
        wf = chain_with_retries(max_attempts=2)
        runner = FlakyRunner({"a": 99})
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=SleepRecorder()).run()
        assert any("permanent failure after 2 attempt(s)" in entry for entry in run.log)

    def test_retry_log_entries_present(self) -> None:
        wf = chain_with_retries(max_attempts=3)
        runner = FlakyRunner({"a": 1})
        sleeps = SleepRecorder()
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=sleeps).run()
        assert "a: failed -> retrying" in "\n".join(run.log)
        assert "a: running -> success" in "\n".join(run.log)


# ----------------------------------------------------------------------
# Exponential backoff
# ----------------------------------------------------------------------
class TestExponentialBackoff:
    def test_delays_follow_exponential_schedule(self) -> None:
        wf = chain_with_retries(max_attempts=4, backoff_seconds=0.5, backoff_multiplier=3.0)
        runner = FlakyRunner({"a": 99})
        sleeps = SleepRecorder()
        WorkflowEngine(wf, runner.callables(wf), sleeper=sleeps).run()
        assert sleeps.delays == [0.5, 1.5, 4.5]  # 3 retries for 4 attempts

    def test_zero_backoff_never_sleeps(self) -> None:
        wf = chain_with_retries(max_attempts=3)
        runner = FlakyRunner({"a": 2})
        sleeps = SleepRecorder()
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=sleeps).run()
        assert sleeps.delays == []
        assert run.records["a"].state == TaskState.SUCCESS

    def test_next_retry_at_set_while_retrying(self) -> None:
        wf = chain_with_retries(max_attempts=2, backoff_seconds=10.0)
        runner = FlakyRunner({"a": 99})
        run = WorkflowEngine(wf, runner.callables(wf), sleeper=SleepRecorder()).run()
        # After exhaustion the retry timestamp is meaningless; but during
        # the single retry it was computed. Verify via log instead.
        assert any("next attempt in 10" in entry for entry in run.log)

    def test_backoff_delay_math(self) -> None:
        task = Task(task_id="t", backoff_seconds=2.0, backoff_multiplier=2.0)
        assert task.backoff_delay(1) == 2.0
        assert task.backoff_delay(2) == 4.0
        assert task.backoff_delay(3) == 8.0


# ----------------------------------------------------------------------
# Task validation
# ----------------------------------------------------------------------
class TestRetryPolicyValidation:
    def test_max_attempts_must_be_positive(self) -> None:
        with pytest.raises(PydanticValidationError):
            Task(task_id="t", max_attempts=0)

    def test_backoff_must_be_non_negative(self) -> None:
        with pytest.raises(PydanticValidationError):
            Task(task_id="t", backoff_seconds=-1.0)

    def test_multiplier_must_be_at_least_one(self) -> None:
        with pytest.raises(PydanticValidationError):
            Task(task_id="t", backoff_multiplier=0.5)
