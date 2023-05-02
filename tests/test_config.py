"""Tests for YAML workflow configuration (Milestone 7)."""

from __future__ import annotations

from pathlib import Path

import pytest

from etl_orchestrator import WorkflowEngine, WorkflowState
from etl_orchestrator.builtins import DEFAULT_TASK_REGISTRY
from etl_orchestrator.config import (
    build_workflow,
    load_task_params,
    load_workflow,
    load_workflow_config,
    resolve_callables,
)
from etl_orchestrator.exceptions import WorkflowConfigError

EXAMPLE = (
    Path(__file__).resolve().parents[1] / "configs" / "workflows" / "daily_etl.yaml"
)


class TestLoading:
    def test_example_workflow_loads(self) -> None:
        config = load_workflow_config(EXAMPLE)
        assert config.workflow_id == "daily_etl"
        assert len(config.tasks) == 12

    def test_build_validates_and_resolves(self) -> None:
        wf = load_workflow(EXAMPLE)
        wf.validate_workflow()
        order = wf.topological_order()
        assert order.index("extract_customers") < order.index("load_customers")
        assert order[-1] == "send_notification"

    def test_retry_policy_from_yaml(self) -> None:
        wf = load_workflow(EXAMPLE)
        assert wf.tasks["transform_customers"].max_attempts == 3
        assert wf.tasks["extract_customers"].max_attempts == 1

    def test_task_params_from_yaml(self) -> None:
        params = load_task_params(EXAMPLE)
        assert params["transform_customers"]["fail_times"] == 1
        assert params["extract_orders"]["message"] == "extracting orders"

    def test_resolve_callables_maps_task_ids(self) -> None:
        config = load_workflow_config(EXAMPLE)
        callables = resolve_callables(config, DEFAULT_TASK_REGISTRY)
        assert set(callables) == {t.task_id for t in config.tasks}
        assert callables["transform_customers"] is (DEFAULT_TASK_REGISTRY["flaky"])

    def test_resolve_callables_unknown_name(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "workflow_id: w\ntasks:\n  - task_id: a\n    callable: does_not_exist\n",
            encoding="utf-8",
        )
        with pytest.raises(WorkflowConfigError) as exc:
            resolve_callables(load_workflow_config(bad), DEFAULT_TASK_REGISTRY)
        assert "unknown callable" in str(exc.value)

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(WorkflowConfigError):
            load_workflow(tmp_path / "missing.yaml")

    def test_invalid_yaml(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("workflow_id: [unclosed", encoding="utf-8")
        with pytest.raises(WorkflowConfigError):
            load_workflow(bad)

    def test_schema_violation(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("workflow_id: w\ntasks: []\n", encoding="utf-8")
        with pytest.raises(WorkflowConfigError):
            load_workflow(bad)

    def test_unknown_dependency(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "workflow_id: w\n"
            "tasks:\n"
            "  - task_id: a\n"
            "    callable: echo\n"
            "    depends_on: [ghost]\n",
            encoding="utf-8",
        )
        with pytest.raises(WorkflowConfigError) as exc:
            load_workflow(bad)
        assert "unknown task 'ghost'" in str(exc.value)

    def test_duplicate_task_ids(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "workflow_id: w\n"
            "tasks:\n"
            "  - task_id: a\n"
            "    callable: echo\n"
            "  - task_id: a\n"
            "    callable: echo\n",
            encoding="utf-8",
        )
        with pytest.raises(Exception, match="duplicate"):
            build_workflow(load_workflow_config(bad))

    def test_build_directly_from_config_object(self) -> None:
        from etl_orchestrator.config import TaskConfig, WorkflowConfig

        config = WorkflowConfig(
            workflow_id="mini",
            tasks=[TaskConfig(task_id="only", callable="echo")],
        )
        wf = build_workflow(config)
        assert wf.topological_order() == ["only"]


class TestConfigDrivenExecution:
    def test_example_workflow_executes_end_to_end(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config = load_workflow_config(EXAMPLE)
        wf = build_workflow(config)
        engine = WorkflowEngine(
            wf,
            resolve_callables(config, DEFAULT_TASK_REGISTRY),
            task_params=load_task_params(EXAMPLE),
        )
        run = engine.run()
        assert run.workflow_state == WorkflowState.SUCCESS
        output = capsys.readouterr().out
        assert "[extract_customers] extracting customers" in output
        # flaky transform succeeded on its second attempt.
        assert run.records["transform_customers"].attempts == 2

    def test_failing_builtin_marks_run_failed(self) -> None:
        config = load_workflow_config(EXAMPLE)
        wf = build_workflow(config)
        callables = resolve_callables(config, DEFAULT_TASK_REGISTRY)
        # Replace the flaky task with the always-failing builtin.
        callables["transform_customers"] = DEFAULT_TASK_REGISTRY["fail"]
        engine = WorkflowEngine(
            wf,
            callables,
            task_params=load_task_params(EXAMPLE),
        )
        run = engine.run()
        assert run.workflow_state == WorkflowState.FAILED
        assert run.records["transform_customers"].state.value == "failed"
        assert run.records["send_notification"].state.value == "upstream_failed"
