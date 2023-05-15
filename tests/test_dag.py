"""Tests for the DAG model: validation and dependency resolution."""

from __future__ import annotations

import pytest
from pydantic import ValidationError as PydanticValidationError

from etl_orchestrator.dag import Task, Workflow
from etl_orchestrator.exceptions import (
    DependencyCycleError,
    DuplicateTaskError,
    SelfDependencyError,
    UnknownTaskError,
    WorkflowValidationError,
)


def build_customers_orders_workflow() -> Workflow:
    """Build the spec's daily pipeline: two branches joining into a summary."""
    wf = Workflow(workflow_id="daily_etl")
    task_ids = [
        "extract_customers",
        "validate_customers",
        "transform_customers",
        "load_customers",
        "build_customer_summary",
        "extract_orders",
        "validate_orders",
        "transform_orders",
        "load_orders",
        "build_order_summary",
        "daily_business_summary",
        "send_notification",
    ]
    for task_id in task_ids:
        wf.add_task(Task(task_id=task_id))

    edges = {
        "validate_customers": "extract_customers",
        "transform_customers": "validate_customers",
        "load_customers": "transform_customers",
        "build_customer_summary": "load_customers",
        "validate_orders": "extract_orders",
        "transform_orders": "validate_orders",
        "load_orders": "transform_orders",
        "build_order_summary": "load_orders",
        "daily_business_summary": "build_customer_summary",
        "send_notification": "daily_business_summary",
    }
    for task_id, upstream_id in edges.items():
        wf.add_dependency(task_id, upstream_id)
    # daily_business_summary also needs the order branch.
    wf.add_dependency("daily_business_summary", "build_order_summary")
    return wf


# ----------------------------------------------------------------------
# Construction and basic rules
# ----------------------------------------------------------------------
class TestTaskAndWorkflowConstruction:
    def test_task_and_workflow_validate_ids(self) -> None:
        wf = Workflow(workflow_id="daily_etl")
        wf.add_task(Task(task_id="extract_customers", description="pull"))
        assert "extract_customers" in wf.tasks

    @pytest.mark.parametrize("bad_id", ["", "1bad", "has space", "x.y"])
    def test_invalid_task_id_rejected(self, bad_id: str) -> None:
        with pytest.raises(PydanticValidationError):
            Task(task_id=bad_id)

    def test_invalid_workflow_id_rejected(self) -> None:
        with pytest.raises(PydanticValidationError):
            Workflow(workflow_id="bad id")

    def test_duplicate_task_rejected(self) -> None:
        wf = Workflow(workflow_id="wf")
        wf.add_task(Task(task_id="a"))
        with pytest.raises(DuplicateTaskError) as exc:
            wf.add_task(Task(task_id="a"))
        assert exc.value.task_id == "a"

    def test_unknown_upstream_rejected(self) -> None:
        wf = Workflow(workflow_id="wf")
        wf.add_task(Task(task_id="a"))
        with pytest.raises(UnknownTaskError) as exc:
            wf.add_dependency("a", "ghost")
        assert exc.value.task_id == "ghost"

    def test_unknown_downstream_rejected(self) -> None:
        wf = Workflow(workflow_id="wf")
        wf.add_task(Task(task_id="a"))
        with pytest.raises(UnknownTaskError):
            wf.add_dependency("ghost", "a")

    def test_self_dependency_rejected(self) -> None:
        wf = Workflow(workflow_id="wf")
        wf.add_task(Task(task_id="a"))
        with pytest.raises(SelfDependencyError) as exc:
            wf.add_dependency("a", "a")
        assert exc.value.task_id == "a"


# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------
class TestWorkflowValidation:
    def test_valid_workflow_passes(self) -> None:
        wf = build_customers_orders_workflow()
        wf.validate_workflow()  # must not raise
        assert wf.validate_graph() == []

    def test_empty_workflow_reports_error(self) -> None:
        wf = Workflow(workflow_id="empty")
        errors = wf.validate_graph()
        assert errors == ["workflow must define at least one task"]
        with pytest.raises(WorkflowValidationError):
            wf.validate_workflow()

    def test_validation_collects_multiple_problems(self) -> None:
        wf = Workflow(workflow_id="wf")
        wf.add_task(Task(task_id="a"))
        wf.tasks["ghost"] = Task(task_id="ghost")  # simulate bulk load
        wf.add_dependency("ghost", "a")
        wf.dependencies["a"] = frozenset({"a", "missing"})
        errors = wf.validate_graph()
        assert any("cannot depend on itself" in e for e in errors)
        assert any("'missing'" in e for e in errors)

    def test_simple_cycle_detected(self) -> None:
        wf = Workflow(workflow_id="wf")
        for tid in ("a", "b", "c"):
            wf.add_task(Task(task_id=tid))
        wf.add_dependency("b", "a")
        wf.add_dependency("c", "b")
        wf.dependencies["a"] = frozenset({"c"})  # close the loop
        # validate_workflow() collects all problems and raises the family base.
        with pytest.raises(WorkflowValidationError) as exc:
            wf.validate_workflow()
        assert "dependency cycle: a -> c -> b -> a" in str(exc.value)
        cycles = wf._find_cycles()
        assert len(cycles) == 1
        assert set(cycles[0]) == {"a", "b", "c"}

    def test_two_node_cycle_detected(self) -> None:
        wf = Workflow(workflow_id="wf")
        wf.add_task(Task(task_id="a"))
        wf.add_task(Task(task_id="b"))
        wf.add_dependency("b", "a")
        wf.dependencies["a"] = frozenset({"b"})
        errors = wf.validate_graph()
        assert any("dependency cycle" in e for e in errors)


# ----------------------------------------------------------------------
# Dependency resolution
# ----------------------------------------------------------------------
class TestDependencyResolution:
    def test_linear_chain_order(self) -> None:
        wf = Workflow.from_chain("chain", ["a", "b", "c"])
        assert wf.topological_order() == ["a", "b", "c"]

    def test_diamond_order_respects_edges(self) -> None:
        wf = Workflow(workflow_id="diamond")
        for tid in ("a", "b", "c", "d"):
            wf.add_task(Task(task_id=tid))
        wf.add_dependency("b", "a")
        wf.add_dependency("c", "a")
        wf.add_dependency("d", "b")
        wf.add_dependency("d", "c")
        order = wf.topological_order()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_order_is_deterministic(self) -> None:
        wf = build_customers_orders_workflow()
        assert wf.topological_order() == wf.topological_order()

    def test_independent_tasks_emit_in_id_order(self) -> None:
        wf = Workflow(workflow_id="wf")
        for tid in ("z_root", "a_root"):
            wf.add_task(Task(task_id=tid))
        assert wf.topological_order() == ["a_root", "z_root"]

    def test_cycle_raises_from_topological_order(self) -> None:
        wf = Workflow(workflow_id="wf")
        for tid in ("a", "b"):
            wf.add_task(Task(task_id=tid))
        wf.add_dependency("b", "a")
        wf.dependencies["a"] = frozenset({"b"})
        with pytest.raises(DependencyCycleError):
            wf.topological_order()

    def test_customers_orders_scenario_resolution(self) -> None:
        wf = build_customers_orders_workflow()
        order = wf.topological_order()
        assert len(order) == len(wf.tasks)
        # Every edge must be respected in the resolved order.
        for task_id, upstream in wf.dependencies.items():
            for up_id in upstream:
                assert order.index(up_id) < order.index(task_id)
        # Entry points come first, notification is last.
        assert set(order[:2]) == {"extract_customers", "extract_orders"}
        assert order[-1] == "send_notification"


# ----------------------------------------------------------------------
# Graph queries
# ----------------------------------------------------------------------
class TestGraphQueries:
    def test_upstream_and_downstream(self) -> None:
        wf = Workflow.from_chain("chain", ["a", "b", "c"])
        assert wf.upstream("b") == frozenset({"a"})
        assert wf.downstream("b") == frozenset({"c"})
        assert wf.upstream("a") == frozenset()
        assert wf.downstream("c") == frozenset()

    def test_roots_and_leaves(self) -> None:
        wf = build_customers_orders_workflow()
        assert wf.roots() == frozenset({"extract_customers", "extract_orders"})
        assert wf.leaves() == frozenset({"send_notification"})

    def test_from_chain_helper(self) -> None:
        wf = Workflow.from_chain("daily", ["extract", "transform", "load"])
        assert wf.validate_graph() == []
        assert wf.topological_order() == ["extract", "transform", "load"]

    def test_task_is_immutable(self) -> None:
        task = Task(task_id="a")
        with pytest.raises(PydanticValidationError):
            task.task_id = "b"

    def test_add_task_wires_depends_on_into_graph(self) -> None:
        """``Task.depends_on`` must be wired into the graph automatically
        by :meth:`Workflow.add_task` — equivalent to calling
        :meth:`Workflow.add_dependency` for every entry."""
        wf = Workflow(workflow_id="auto")
        wf.add_task(Task(task_id="a"))
        wf.add_task(Task(task_id="b", depends_on=["a"]))
        wf.add_task(Task(task_id="c", depends_on=["a", "b"]))
        assert wf.upstream("b") == frozenset({"a"})
        assert wf.upstream("c") == frozenset({"a", "b"})
        assert wf.topological_order() == ["a", "b", "c"]

    def test_add_task_with_unknown_dependency_raises(self) -> None:
        """If ``depends_on`` references a missing task, ``add_task`` must
        raise ``UnknownTaskError`` (not silently drop the dependency)."""
        wf = Workflow(workflow_id="auto_bad")
        wf.add_task(Task(task_id="a"))
        with pytest.raises(UnknownTaskError):
            wf.add_task(Task(task_id="b", depends_on=["ghost"]))
