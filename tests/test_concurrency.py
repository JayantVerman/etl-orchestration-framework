"""Tests for bounded parallel execution (Milestone 10)."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from etl_orchestrator import (
    Task,
    TaskContext,
    TaskState,
    Workflow,
    WorkflowEngine,
    WorkflowState,
)


def diamond_workflow() -> Workflow:
    """Two independent branches joining at the end."""
    wf = Workflow(workflow_id="parallel")
    for tid in ("a1", "a2", "join"):
        wf.add_task(Task(task_id=tid))
    wf.add_dependency("join", "a1")
    wf.add_dependency("join", "a2")
    return wf


class TestBoundedConcurrency:
    def test_independent_tasks_actually_overlap(self) -> None:
        """A shared Barrier proves both tasks ran at the same time."""
        wf = diamond_workflow()
        barrier = threading.Barrier(2, timeout=10)

        def make_waiter(ctx: TaskContext) -> None:
            barrier.wait()

        engine = WorkflowEngine(
            wf,
            {"a1": make_waiter, "a2": make_waiter, "join": lambda ctx: None},
            max_workers=2,
        )
        run = engine.run()
        assert run.workflow_state == WorkflowState.SUCCESS

    def test_concurrency_never_exceeds_max_workers(self) -> None:
        wf = Workflow(workflow_id="wide")
        for tid in ("t1", "t2", "t3", "t4"):
            wf.add_task(Task(task_id=tid))

        lock = threading.Lock()
        active = {"n": 0, "peak": 0}

        def busy(ctx: TaskContext) -> None:
            with lock:
                active["n"] += 1
                active["peak"] = max(active["peak"], active["n"])
            time.sleep(0.05)
            with lock:
                active["n"] -= 1

        engine = WorkflowEngine(wf, dict.fromkeys(wf.tasks, busy), max_workers=2)
        run = engine.run()
        assert run.workflow_state == WorkflowState.SUCCESS
        assert active["peak"] <= 2
        assert active["peak"] >= 2  # parallelism actually happened

    def test_dependents_never_overlap_upstream(self) -> None:
        wf = Workflow(workflow_id="chain")
        for tid in ("first", "second"):
            wf.add_task(Task(task_id=tid))
        wf.add_dependency("second", "first")

        intervals: dict[str, tuple[float, float]] = {}
        lock = threading.Lock()

        def make(task_id: str) -> Any:
            def task(ctx: TaskContext) -> None:
                start = time.monotonic()
                time.sleep(0.05)
                end = time.monotonic()
                with lock:
                    intervals[task_id] = (start, end)

            return task

        engine = WorkflowEngine(
            wf,
            {"first": make("first"), "second": make("second")},
            max_workers=4,
        )
        run = engine.run()
        assert run.workflow_state == WorkflowState.SUCCESS
        first_end = intervals["first"][1]
        second_start = intervals["second"][0]
        assert second_start >= first_end

    def test_parallel_failure_blocks_only_downstream(self) -> None:
        wf = Workflow(workflow_id="mixed")
        for tid in ("good1", "good2", "bad", "after_bad", "join"):
            wf.add_task(Task(task_id=tid))
        wf.add_dependency("join", "good1")
        wf.add_dependency("join", "good2")
        wf.add_dependency("after_bad", "bad")
        wf.add_dependency("join", "after_bad")

        def ok(ctx: TaskContext) -> None:
            time.sleep(0.01)

        def bad(ctx: TaskContext) -> None:
            raise RuntimeError("parallel boom")

        engine = WorkflowEngine(
            wf,
            {
                "good1": ok,
                "good2": ok,
                "bad": bad,
                "after_bad": ok,
                "join": ok,
            },
            max_workers=4,
        )
        run = engine.run()
        assert run.records["bad"].state == TaskState.FAILED
        assert run.records["after_bad"].state == TaskState.UPSTREAM_FAILED
        assert run.records["join"].state == TaskState.UPSTREAM_FAILED
        assert run.records["good1"].state == TaskState.SUCCESS
        assert run.records["good2"].state == TaskState.SUCCESS
        assert run.workflow_state == WorkflowState.FAILED

    def test_parallel_results_match_sequential(self) -> None:
        tasks_ids = [f"task_{i}" for i in range(6)]
        results: dict[str, list[TaskState]] = {}

        for workers in (1, 4):
            wf = Workflow(workflow_id=f"w{workers}")
            for tid in tasks_ids:
                wf.add_task(Task(task_id=tid))
            # Fan in: last task depends on all others.
            wf.add_task(Task(task_id="final"))
            for tid in tasks_ids:
                wf.add_dependency("final", tid)
            run = WorkflowEngine(
                wf,
                {tid: (lambda ctx: None) for tid in [*tasks_ids, "final"]},
                max_workers=workers,
            ).run()
            results[str(workers)] = run.task_states()
        assert results["1"] == results["4"]
        assert all(s == TaskState.SUCCESS for s in results["4"])

    def test_invalid_max_workers_rejected(self) -> None:
        wf = Workflow.from_chain("w", ["a"])
        with pytest.raises(Exception, match="max_workers"):
            WorkflowEngine(wf, {"a": lambda ctx: None}, max_workers=0)

    def test_dependency_levels_grouping(self) -> None:
        wf = diamond_workflow()
        engine = WorkflowEngine(
            wf,
            {"a1": lambda ctx: None, "a2": lambda ctx: None, "join": lambda ctx: None},
        )
        assert engine._dependency_levels() == [
            ["a1", "a2"],
            ["join"],
        ]
