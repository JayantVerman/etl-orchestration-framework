"""Tests for the CLI (Milestone 8)."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from etl_orchestrator.cli import main

OK_YAML = """
workflow_id: demo
tasks:
  - task_id: extract
    callable: echo
    params:
      message: pulling data
  - task_id: load
    callable: echo
    depends_on: [extract]
    params:
      message: done
"""

FAILING_YAML = """
workflow_id: broken
tasks:
  - task_id: always_fails
    callable: fail
    params:
      message: nope
"""

BAD_YAML = "workflow_id: [broken"


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    """A workflows dir + db path for CLI invocations."""
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    (workflows / "demo.yaml").write_text(OK_YAML, encoding="utf-8")
    (workflows / "broken.yaml").write_text(FAILING_YAML, encoding="utf-8")
    (workflows / "invalid.yaml").write_text(BAD_YAML, encoding="utf-8")
    return tmp_path


def invoke(workspace: Path, *args: str) -> Result:
    runner = CliRunner()
    return runner.invoke(
        main,
        [
            "--db",
            str(workspace / "runs.db"),
            "--workflows-dir",
            str(workspace / "workflows"),
            *args,
        ],
    )


class TestBasicCommands:
    def test_version(self, workspace: Path) -> None:
        result = invoke(workspace, "version")
        assert result.exit_code == 0
        assert "orchestrator" in result.output

    def test_workflow_list(self, workspace: Path) -> None:
        result = invoke(workspace, "workflow", "list")
        assert result.exit_code == 0
        assert "demo (demo.yaml) - 2 tasks" in result.output

    def test_workflow_validate(self, workspace: Path) -> None:
        result = invoke(workspace, "workflow", "validate", "demo")
        assert result.exit_code == 0
        assert "valid (2 tasks)" in result.output
        assert "extract" in result.output


class TestWorkflowRun:
    def test_successful_run(self, workspace: Path) -> None:
        result = invoke(workspace, "workflow", "run", "demo")
        assert result.exit_code == 0
        assert "success" in result.output
        assert "extract: success" in result.output

    def test_run_params_reach_tasks(self, workspace: Path) -> None:
        result = invoke(
            workspace, "workflow", "run", "demo", "--param", "date=2026-09-05"
        )
        assert result.exit_code == 0

    def test_failed_run_exits_nonzero(self, workspace: Path) -> None:
        result = invoke(workspace, "workflow", "run", "broken")
        assert result.exit_code == 1
        assert "failed" in result.output
        assert "nope" in result.output

    def test_unknown_workflow(self, workspace: Path) -> None:
        result = invoke(workspace, "workflow", "run", "ghost")
        assert result.exit_code != 0
        assert "not found" in result.output


class TestHistoryCommands:
    def test_status_history_show(self, workspace: Path) -> None:
        invoke(workspace, "workflow", "run", "demo")
        status = invoke(workspace, "workflow", "status", "demo")
        assert status.exit_code == 0
        assert "success" in status.output
        assert "extract: success" in status.output

        history = invoke(workspace, "workflow", "history", "demo")
        assert history.exit_code == 0
        assert "success" in history.output

        run_id = status.output.split("run ")[1].split(":")[0].strip()
        shown = invoke(workspace, "run", "show", run_id)
        assert shown.exit_code == 0
        assert "log:" in shown.output
        assert "workflow: demo" in shown.output

    def test_status_without_runs(self, workspace: Path) -> None:
        result = invoke(workspace, "workflow", "status", "demo")
        assert result.exit_code == 0
        assert "no runs recorded" in result.output

    def test_run_show_missing(self, workspace: Path) -> None:
        result = invoke(workspace, "run", "show", "missing")
        assert result.exit_code != 0

    def test_cancel_terminal_run_fails(self, workspace: Path) -> None:
        invoke(workspace, "workflow", "run", "demo")
        status = invoke(workspace, "workflow", "status", "demo")
        run_id = status.output.split("run ")[1].split(":")[0].strip()
        result = invoke(workspace, "run", "cancel", run_id)
        assert result.exit_code != 0
        assert "already terminal" in result.output
