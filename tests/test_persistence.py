"""Tests for SQLite run persistence (Milestone 6)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from etl_orchestrator import (
    Task,
    TaskContext,
    TaskState,
    Workflow,
    WorkflowEngine,
    WorkflowState,
)
from etl_orchestrator.engine import WorkflowRun
from etl_orchestrator.persistence import RunStore


def run_a_workflow(tmp_path: Path, fail: bool) -> WorkflowRun:
    """Execute a small workflow and persist it, returning the run."""
    wf = Workflow.from_chain("orders", ["extract", "load"])
    wf.tasks["extract"] = Task(task_id="extract", max_attempts=2)

    attempts = {"n": 0}

    def extract(ctx: TaskContext) -> None:
        attempts["n"] += 1
        if fail and attempts["n"] < 2:
            raise RuntimeError("transient")

    engine = WorkflowEngine(
        wf,
        {"extract": extract, "load": lambda ctx: None},
    )
    run = engine.run(params={"date": "2026-09-05"})
    store = RunStore(tmp_path / "runs.db")
    try:
        store.save_run(run)
    finally:
        store.close()
    return run


class TestSaveAndLoad:
    def test_roundtrip_preserves_everything(self, tmp_path: Path) -> None:
        run = run_a_workflow(tmp_path, fail=True)
        store = RunStore(tmp_path / "runs.db")
        loaded = store.load_run(run.run_id)
        store.close()
        assert loaded is not None
        assert loaded.run_id == run.run_id
        assert loaded.workflow_id == "orders"
        assert loaded.workflow_state == WorkflowState.SUCCESS
        assert loaded.params == {"date": "2026-09-05"}
        assert loaded.log == run.log
        assert loaded.started_at == run.started_at
        assert loaded.finished_at == run.finished_at
        for task_id in ("extract", "load"):
            original = run.records[task_id]
            restored = loaded.records[task_id]
            assert restored.state == original.state
            assert restored.attempts == original.attempts
            assert restored.error == original.error
            assert restored.started_at == original.started_at
            assert restored.finished_at == original.finished_at

    def test_restart_survives_process_recreation(self, tmp_path: Path) -> None:
        """Simulate an app restart: new store object, same database file."""
        run = run_a_workflow(tmp_path, fail=False)
        # New process: brand-new RunStore over the same file.
        fresh_store = RunStore(tmp_path / "runs.db")
        summary = fresh_store.latest_run("orders")
        loaded = fresh_store.load_run(run.run_id)
        fresh_store.close()
        assert summary is not None
        assert summary.run_id == run.run_id
        assert summary.state == WorkflowState.SUCCESS
        assert loaded is not None
        assert loaded.records["extract"].state == TaskState.SUCCESS
        assert loaded.workflow_state == WorkflowState.SUCCESS

    def test_load_missing_returns_none(self, tmp_path: Path) -> None:
        store = RunStore(tmp_path / "runs.db")
        assert store.load_run("nope") is None
        store.close()


class TestHistory:
    def test_list_runs_filters_and_orders(self, tmp_path: Path) -> None:
        wf = Workflow.from_chain("w", ["only"])
        engine = WorkflowEngine(wf, {"only": lambda ctx: None})
        wf2 = Workflow.from_chain("v", ["only"])
        engine2 = WorkflowEngine(wf2, {"only": lambda ctx: None})
        store = RunStore(tmp_path / "runs.db")
        try:
            first = engine.run()
            store.save_run(first)
            second = engine2.run()
            store.save_run(second)
            assert len(store.list_runs()) == 2
            assert len(store.list_runs("w")) == 1
            assert store.list_runs("w")[0].run_id == first.run_id
            assert store.latest_run("missing") is None
            assert store.latest_run("w") is not None
        finally:
            store.close()

    def test_resave_does_not_duplicate_task_rows(self, tmp_path: Path) -> None:
        wf = Workflow.from_chain("w", ["only"])
        run = WorkflowEngine(wf, {"only": lambda ctx: None}).run()
        store = RunStore(tmp_path / "runs.db")
        try:
            store.save_run(run)
            store.save_run(run)
            loaded = store.load_run(run.run_id)
            assert loaded is not None
            assert list(loaded.records) == ["only"]
        finally:
            store.close()


class TestCancel:
    def test_cancel_non_terminal_run(self, tmp_path: Path) -> None:
        wf = Workflow.from_chain("w", ["only"])
        run: WorkflowRun = WorkflowEngine(wf, {"only": lambda ctx: None}).run()
        run.workflow_state = WorkflowState.RUNNING  # simulate in-flight
        store = RunStore(tmp_path / "runs.db")
        try:
            store.save_run(run)
            assert store.cancel_run(run.run_id) is True
            loaded = store.load_run(run.run_id)
            assert loaded is not None
            assert loaded.workflow_state == WorkflowState.CANCELLED
        finally:
            store.close()

    def test_cancel_terminal_run_returns_false(self, tmp_path: Path) -> None:
        wf = Workflow.from_chain("w", ["only"])
        run = WorkflowEngine(wf, {"only": lambda ctx: None}).run()
        store = RunStore(tmp_path / "runs.db")
        try:
            store.save_run(run)
            assert store.cancel_run(run.run_id) is False
            assert store.cancel_run("missing") is False
        finally:
            store.close()


class TestTimestampRoundtrip:
    def test_datetimes_survive_roundtrip(self, tmp_path: Path) -> None:
        wf = Workflow.from_chain("w", ["only"])
        run = WorkflowEngine(wf, {"only": lambda ctx: None}).run()
        store = RunStore(tmp_path / "runs.db")
        try:
            store.save_run(run)
            loaded = store.load_run(run.run_id)
        finally:
            store.close()
        assert loaded is not None
        assert isinstance(loaded.started_at, datetime)
        assert loaded.finished_at is not None
        assert loaded.finished_at >= loaded.started_at
