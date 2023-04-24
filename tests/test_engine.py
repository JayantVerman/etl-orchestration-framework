"""Tests for the workflow execution engine."""

from __future__ import annotations

from typing import Any

import pytest

from etl_orchestrator import (
    EngineConfigurationError,
    Task,
    TaskContext,
    TaskState,
    Workflow,
    WorkflowEngine,
    WorkflowState,
)
from tests.test_dag import build_customers_orders_workflow


class Recorder:
    """Records task execution order and optionally fails for task ids."""

    def __init__(self, fail_on: set[str] | None = None) -> None:
        self.calls: list[str] = []
        self.contexts: dict[str, TaskContext] = {}
        self.fail_on = fail_on or set()

    def callable_for(self, task_id: str) -> Any:
        def task(ctx: TaskContext) -> None:
            self.calls.append(task_id)
            self.contexts[task_id] = ctx
            if task_id in self.fail_on:
                raise RuntimeError(f"boom in {task_id}")

        return task

    def callables(self, workflow: Workflow) -> dict[str, Any]:
        return {tid: self.callable_for(tid) for tid in workflow.tasks}


def linear_workflow() -> Workflow:
    return Workflow.from_chain("linear", ["a", "b", "c"])


# ----------------------------------------------------------------------
# Successful execution
# ----------------------------------------------------------------------
class TestSuccessfulRuns:
    def test_all_tasks_succeed_in_order(self) -> None:
        wf = linear_workflow()
        rec = Recorder()
        run = WorkflowEngine(wf, rec.callables(wf)).run()
        assert rec.calls == ["a", "b", "c"]
        assert run.workflow_state == WorkflowState.SUCCESS
        assert all(s == TaskState.SUCCESS for s in run.task_states())

    def test_run_metadata(self) -> None:
        wf = linear_workflow()
        run = WorkflowEngine(wf, Recorder().callables(wf)).run()
        assert run.workflow_id == "linear"
        assert run.run_id
        assert run.started_at is not None
        assert run.finished_at is not None

    def test_each_task_attempted_once(self) -> None:
        wf = build_customers_orders_workflow()
        rec = Recorder()
        run = WorkflowEngine(wf, rec.callables(wf)).run()
        assert run.workflow_state == WorkflowState.SUCCESS
        for task_id in wf.tasks:
            assert run.records[task_id].attempts == 1
            assert run.records[task_id].error is None

    def test_context_passed_to_tasks(self) -> None:
        wf = linear_workflow()
        rec = Recorder()
        engine = WorkflowEngine(wf, rec.callables(wf))
        run = engine.run(params={"date": "2026-09-05"})
        for task_id, ctx in rec.contexts.items():
            assert ctx.task_id == task_id
            assert ctx.workflow_id == "linear"
            assert ctx.run_id == run.run_id
            assert ctx.attempt == 1
            assert ctx.params == {"date": "2026-09-05"}

    def test_transition_log_records_states(self) -> None:
        wf = linear_workflow()
        run = WorkflowEngine(wf, Recorder().callables(wf)).run()
        assert "workflow: pending -> running" in run.log
        assert "a: pending -> running" in run.log
        assert "a: running -> success" in run.log
        assert "workflow: running -> success" in run.log

    def test_run_ids_are_unique(self) -> None:
        wf = linear_workflow()
        engine = WorkflowEngine(wf, Recorder().callables(wf))
        assert engine.run().run_id != engine.run().run_id

    def test_base_exception_propagates(self) -> None:
        wf = linear_workflow()

        def interrupted(ctx: TaskContext) -> None:
            raise KeyboardInterrupt

        engine = WorkflowEngine(
            wf, {"a": interrupted, "b": interrupted, "c": interrupted}
        )
        with pytest.raises(KeyboardInterrupt):
            engine.run()


# ----------------------------------------------------------------------
# Failure handling
# ----------------------------------------------------------------------
class TestFailureHandling:
    def test_failed_task_and_downstream_blocked(self) -> None:
        wf = Workflow.from_chain("linear", ["a", "b", "c"])
        rec = Recorder(fail_on={"a"})
        run = WorkflowEngine(wf, rec.callables(wf)).run()
        assert run.records["a"].state == TaskState.FAILED
        assert "boom in a" in (run.records["a"].error or "")
        assert run.records["b"].state == TaskState.UPSTREAM_FAILED
        assert run.records["c"].state == TaskState.UPSTREAM_FAILED
        assert rec.calls == ["a"]  # b and c never executed
        assert run.workflow_state == WorkflowState.FAILED

    def test_independent_branch_unaffected(self) -> None:
        wf = build_customers_orders_workflow()
        rec = Recorder(fail_on={"transform_customers"})
        run = WorkflowEngine(wf, rec.callables(wf)).run()
        # Customers branch: failed, then blocked downstream.
        assert run.records["transform_customers"].state == TaskState.FAILED
        assert run.records["load_customers"].state == TaskState.UPSTREAM_FAILED
        assert run.records["build_customer_summary"].state == TaskState.UPSTREAM_FAILED
        # Orders branch completed normally.
        orders_chain = [
            "extract_orders",
            "validate_orders",
            "transform_orders",
            "load_orders",
        ]
        for task_id in orders_chain:
            assert run.records[task_id].state == TaskState.SUCCESS
        orders_positions = [rec.calls.index(t) for t in orders_chain]
        assert orders_positions == sorted(orders_positions)
        # Join and notification never ran.
        assert run.records["daily_business_summary"].state == TaskState.UPSTREAM_FAILED
        assert run.records["send_notification"].state == TaskState.UPSTREAM_FAILED
        assert run.workflow_state == WorkflowState.FAILED

    def test_blocked_tasks_have_error_reason(self) -> None:
        wf = Workflow.from_chain("linear", ["a", "b"])
        run = WorkflowEngine(wf, Recorder(fail_on={"a"}).callables(wf)).run()
        assert "upstream task 'a' failed" in (run.records["b"].error or "")


# ----------------------------------------------------------------------
# Engine configuration
# ----------------------------------------------------------------------
class TestEngineConfiguration:
    def test_missing_callable_rejected(self) -> None:
        wf = linear_workflow()
        with pytest.raises(EngineConfigurationError) as exc:
            WorkflowEngine(wf, {"a": lambda ctx: None, "b": lambda ctx: None})
        assert "c" in str(exc.value)

    def test_extra_callable_rejected(self) -> None:
        wf = linear_workflow()

        def noop(ctx: TaskContext) -> None: ...

        with pytest.raises(EngineConfigurationError) as exc:
            WorkflowEngine(
                wf,
                {
                    "a": noop,
                    "b": noop,
                    "c": noop,
                    "ghost": noop,
                },
            )
        assert "ghost" in str(exc.value)

    def test_cyclic_workflow_rejected(self) -> None:
        wf = Workflow(workflow_id="wf")
        for tid in ("a", "b"):
            wf.add_task(Task(task_id=tid))
        wf.add_dependency("b", "a")
        wf.dependencies["a"] = frozenset({"b"})
        with pytest.raises(EngineConfigurationError):
            WorkflowEngine(wf, {"a": lambda ctx: None, "b": lambda ctx: None})
