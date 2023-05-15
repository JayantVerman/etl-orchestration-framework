"""Tests for the engine hook system (Milestone 11)."""

from __future__ import annotations

import pytest

from etl_orchestrator import TaskState, WorkflowEngine, WorkflowState
from etl_orchestrator.dag import Task, Workflow
from etl_orchestrator.hooks import EngineHooks


def _two_step_workflow() -> tuple[WorkflowEngine, Workflow]:
    wf = Workflow(workflow_id="hook_wf")
    wf.add_task(Task(task_id="a"))
    wf.add_task(Task(task_id="b", depends_on=["a"]))

    def _run_a(ctx: object) -> None:
        return None

    def _run_b(ctx: object) -> None:
        return None

    engine = WorkflowEngine(workflow=wf, tasks={"a": _run_a, "b": _run_b})
    return engine, wf


def test_default_hooks_are_all_none() -> None:
    hooks = EngineHooks()
    for field_name in (
        "on_workflow_start",
        "on_task_start",
        "on_task_success",
        "on_task_retry",
        "on_task_failure",
        "on_workflow_end",
    ):
        assert getattr(hooks, field_name) is None


def test_engine_runs_without_hooks() -> None:
    engine, _ = _two_step_workflow()
    run = engine.run()
    assert run.workflow_state == WorkflowState.SUCCESS


def test_workflow_start_and_end_fire_once() -> None:
    engine, _ = _two_step_workflow()
    starts: list[object] = []
    ends: list[object] = []
    hooks = EngineHooks(on_workflow_start=starts.append, on_workflow_end=ends.append)
    engine_with = WorkflowEngine(
        workflow=engine._workflow,  # type: ignore[attr-defined]
        tasks=engine._tasks,  # type: ignore[attr-defined]
        hooks=hooks,
    )
    run = engine_with.run()
    assert starts == [run]
    assert ends == [run]


def test_task_start_and_success_fire_per_task() -> None:
    engine, _ = _two_step_workflow()
    starts: list[tuple[str, int]] = []
    successes: list[tuple[str, int]] = []

    def on_start(run, record) -> None:
        starts.append((record.task_id, record.attempts))

    def on_success(run, record) -> None:
        successes.append((record.task_id, record.attempts))

    hooks = EngineHooks(on_task_start=on_start, on_task_success=on_success)
    engine_with = WorkflowEngine(
        workflow=engine._workflow,  # type: ignore[attr-defined]
        tasks=engine._tasks,  # type: ignore[attr-defined]
        hooks=hooks,
    )
    engine_with.run()
    assert sorted(starts) == [("a", 1), ("b", 1)]
    assert sorted(successes) == [("a", 1), ("b", 1)]


def test_failure_hook_fires_on_permanent_failure() -> None:
    wf = Workflow(workflow_id="hook_fail")
    wf.add_task(Task(task_id="boom", max_attempts=1))

    def _boom(ctx: object) -> None:
        raise RuntimeError("kaboom")

    failures: list[tuple[str, str | None]] = []

    def on_failure(run, record) -> None:
        failures.append((record.task_id, record.error))

    engine = WorkflowEngine(
        workflow=wf,
        tasks={"boom": _boom},
        hooks=EngineHooks(on_task_failure=on_failure),
    )
    run = engine.run()
    assert run.workflow_state == WorkflowState.FAILED
    assert failures == [("boom", "RuntimeError: kaboom")]


def test_retry_hook_fires_with_delay() -> None:
    wf = Workflow(workflow_id="hook_retry")
    wf.add_task(Task(task_id="flaky", max_attempts=3, backoff_seconds=0.01))

    counter = {"n": 0}

    def _flaky(ctx: object) -> None:
        counter["n"] += 1
        if counter["n"] < 3:
            raise RuntimeError("not yet")

    retries: list[tuple[str, int, float]] = []

    def on_retry(run, record, delay) -> None:
        retries.append((record.task_id, record.attempts, delay))

    def sleeper(_delay: float) -> None:
        pass

    engine = WorkflowEngine(
        workflow=wf,
        tasks={"flaky": _flaky},
        hooks=EngineHooks(on_task_retry=on_retry),
        sleeper=sleeper,
    )
    run = engine.run()
    assert run.workflow_state == WorkflowState.SUCCESS
    # Two failures -> two retries (one before attempt 2, one before attempt 3).
    assert len(retries) == 2
    assert all(task_id == "flaky" for task_id, _, _ in retries)
    assert [delay for _, _, delay in retries] == [0.01, 0.02]


def test_hook_exception_propagates_and_fails_run() -> None:
    """A buggy hook raises and the engine does not silently swallow it."""
    wf = Workflow(workflow_id="hook_bug")
    wf.add_task(Task(task_id="ok"))

    def _ok(ctx: object) -> None:
        return None

    def buggy_hook(run) -> None:
        raise RuntimeError("hook broke")

    engine = WorkflowEngine(
        workflow=wf,
        tasks={"ok": _ok},
        hooks=EngineHooks(on_workflow_start=buggy_hook),
    )
    with pytest.raises(RuntimeError, match="hook broke"):
        engine.run()


def test_success_hook_does_not_fire_for_skipped_or_upstream_failed() -> None:
    """``upstream_failed`` tasks never run, so they never start or succeed."""
    wf = Workflow(workflow_id="hook_skip")
    wf.add_task(Task(task_id="bad", max_attempts=1))
    wf.add_task(Task(task_id="down", depends_on=["bad"]))

    def _bad(ctx: object) -> None:
        raise RuntimeError("nope")

    def _down(ctx: object) -> None:
        pytest.fail("downstream should not run when upstream is permanently failed")

    successes: list[str] = []
    engine = WorkflowEngine(
        workflow=wf,
        tasks={"bad": _bad, "down": _down},
        hooks=EngineHooks(on_task_success=lambda r, rec: successes.append(rec.task_id)),
    )
    run = engine.run()
    assert run.records["down"].state == TaskState.UPSTREAM_FAILED
    assert "down" not in successes
    assert "bad" not in successes  # bad failed, not succeeded
