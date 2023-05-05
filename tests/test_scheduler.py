"""Tests for the local scheduler (Milestone 9)."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from etl_orchestrator import WorkflowState
from etl_orchestrator.persistence import RunStore
from etl_orchestrator.scheduler import Scheduler

SIMPLE_YAML = """
workflow_id: scheduled_demo
tasks:
  - task_id: only
    callable: echo
    params:
      message: scheduled run
"""

BROKEN_YAML = """
workflow_id: broken_schedule
tasks:
  - task_id: only
    callable: no_such_callable
"""


@pytest.fixture()
def env(tmp_path: Path) -> tuple[RunStore, Path, Path]:
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    good = workflows / "good.yaml"
    good.write_text(SIMPLE_YAML, encoding="utf-8")
    bad = workflows / "bad.yaml"
    bad.write_text(BROKEN_YAML, encoding="utf-8")
    store = RunStore(tmp_path / "runs.db")
    return store, good, bad


class TestDueLogic:
    def test_never_run_is_due(self, env: tuple) -> None:
        store, good, _ = env
        sched = Scheduler(store)
        sched.register("good", good, interval_seconds=60)
        assert sched.due() == ["good"]
        store.close()

    def test_not_due_within_interval(self, env: tuple) -> None:
        store, good, _ = env
        base = datetime(2026, 9, 5, tzinfo=timezone.utc)
        clock = {"now": base}
        sched = Scheduler(store, clock=lambda: clock["now"])
        sched.register("good", good, interval_seconds=60)
        first = sched.run_once()
        assert len(first) == 1
        clock["now"] = base + timedelta(seconds=59)
        assert sched.due() == []
        clock["now"] = base + timedelta(seconds=60)
        assert sched.due() == ["good"]
        store.close()

    def test_manual_clock_advancement_triggers_rerun(self, env: tuple) -> None:
        store, good, _ = env
        base = datetime(2026, 9, 5, tzinfo=timezone.utc)
        clock = {"now": base}
        sched = Scheduler(store, clock=lambda: clock["now"])
        sched.register("good", good, interval_seconds=30)
        assert len(sched.run_once()) == 1
        clock["now"] += timedelta(seconds=30)
        assert len(sched.run_once()) == 1
        assert len(store.list_runs("scheduled_demo")) == 2
        store.close()

    def test_interval_must_be_positive(self, env: tuple) -> None:
        store, good, _ = env
        sched = Scheduler(store)
        with pytest.raises(ValueError):
            sched.register("good", good, interval_seconds=0)
        store.close()


class TestRunOnce:
    def test_runs_are_persisted(self, env: tuple) -> None:
        store, good, _ = env
        sched = Scheduler(store)
        sched.register("good", good, interval_seconds=60)
        sched.run_once()
        summaries = store.list_runs("scheduled_demo")
        assert len(summaries) == 1
        assert summaries[0].state == WorkflowState.SUCCESS
        store.close()

    def test_broken_config_becomes_failed_run(self, env: tuple) -> None:
        store, _, bad = env
        sched = Scheduler(store)
        sched.register("broken_schedule", bad, interval_seconds=60)
        runs = sched.run_once()
        assert len(runs) == 1
        assert runs[0].workflow_state == WorkflowState.FAILED
        assert any("cannot load workflow" in entry for entry in runs[0].log)
        # The scheduler itself is still usable.
        assert sched.due() == []
        store.close()


class TestBackgroundLoop:
    def test_thread_runs_and_stops(self, env: tuple) -> None:
        store, good, _ = env
        sched = Scheduler(store, sleeper=lambda _s: None)
        sched.register("good", good, interval_seconds=0.01)
        sched.start(poll_interval=0.01)
        try:
            deadline = time.monotonic() + 5.0
            while len(store.list_runs("scheduled_demo")) < 2:
                assert time.monotonic() < deadline, "scheduler did not run"
                time.sleep(0.01)
        finally:
            sched.stop()
        assert sched.running is False
        assert sched.stop is not None
        store.close()

    def test_double_start_rejected(self, env: tuple) -> None:
        store, good, _ = env
        sched = Scheduler(store, sleeper=lambda _s: None)
        sched.register("good", good, interval_seconds=60)
        sched.start(poll_interval=0.01)
        try:
            with pytest.raises(RuntimeError, match="already running"):
                sched.start()
        finally:
            sched.stop()
        store.close()
