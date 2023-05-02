"""Command-line interface for the ETL Orchestration Framework.

Commands (Milestone 8)::

    orchestrator workflow list
    orchestrator workflow validate <name>
    orchestrator workflow run <name> [--param key=value ...]
    orchestrator workflow status <name>
    orchestrator workflow history <name>
    orchestrator run show <run_id>
    orchestrator run cancel <run_id>
    orchestrator version
"""

from __future__ import annotations

from pathlib import Path

import click

from etl_orchestrator import __version__
from etl_orchestrator.builtins import DEFAULT_TASK_REGISTRY
from etl_orchestrator.config import (
    build_workflow,
    load_task_params,
    load_workflow_config,
    resolve_callables,
)
from etl_orchestrator.dag import Workflow
from etl_orchestrator.engine import TaskCallable, WorkflowEngine
from etl_orchestrator.exceptions import WorkflowConfigError
from etl_orchestrator.persistence import RunStore
from etl_orchestrator.states import WorkflowState

_WORKFLOW_EXTENSIONS = (".yaml", ".yml")


def _find_workflow_file(workflows_dir: Path, name: str) -> Path:
    """Resolve a workflow name (or explicit path) to a YAML file."""
    candidate = Path(name)
    if candidate.suffix in _WORKFLOW_EXTENSIONS:
        return (
            candidate
            if candidate.is_file()
            else _fail(f"workflow file not found: {candidate}")
        )
    for extension in _WORKFLOW_EXTENSIONS:
        path = workflows_dir / f"{name}{extension}"
        if path.is_file():
            return path
    return _fail(f"workflow '{name}' not found in {workflows_dir}")


def _fail(message: str) -> Path:
    raise click.ClickException(message)


def _build_engine(path: Path) -> tuple[WorkflowEngine, Workflow]:
    """Load, resolve, and wire up an engine for a workflow file."""
    config = load_workflow_config(path)
    built = build_workflow(config)
    callables: dict[str, TaskCallable] = resolve_callables(
        config, DEFAULT_TASK_REGISTRY
    )
    engine = WorkflowEngine(
        built,
        callables,
        task_params=load_task_params(path),
    )
    return engine, built


@click.group()
@click.version_option(version=__version__, prog_name="orchestrator")
@click.option(
    "--db",
    "db_path",
    default="data/orchestrator.db",
    show_default=True,
    type=click.Path(path_type=Path),
    help="Path to the SQLite runs database.",
)
@click.option(
    "--workflows-dir",
    "workflows_dir",
    default="configs/workflows",
    show_default=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory containing workflow YAML files.",
)
@click.pass_context
def main(ctx: click.Context, db_path: Path, workflows_dir: Path) -> None:
    """ETL Orchestration Framework command-line interface."""
    ctx.obj = {"db_path": db_path, "workflows_dir": workflows_dir}


@main.command()
def version() -> None:
    """Print the framework version."""
    click.echo(f"orchestrator {__version__}")


@main.group()
def workflow() -> None:
    """Manage workflow definitions."""


@main.group()
def run() -> None:
    """Inspect and manage stored workflow runs."""


@workflow.command("list")
@click.pass_obj
def workflow_list(obj: dict[str, Path]) -> None:
    """List workflow files in the workflows directory."""
    workflows_dir: Path = obj["workflows_dir"]
    if not workflows_dir.is_dir():
        raise click.ClickException(f"workflows directory not found: {workflows_dir}")
    files = sorted(
        p for p in workflows_dir.iterdir() if p.suffix in _WORKFLOW_EXTENSIONS
    )
    if not files:
        click.echo(f"no workflow files in {workflows_dir}")
        return
    for path in files:
        try:
            config = load_workflow_config(path)
            click.echo(
                f"{config.workflow_id} ({path.name}) - {len(config.tasks)} tasks"
            )
        except WorkflowConfigError as exc:
            click.echo(f"{path.name}: INVALID ({exc})")


@workflow.command("validate")
@click.argument("name")
@click.pass_obj
def workflow_validate(obj: dict[str, Path], name: str) -> None:
    """Validate a workflow definition and print its resolved order."""
    path = _find_workflow_file(obj["workflows_dir"], name)
    try:
        _engine, built = _build_engine(path)
    except WorkflowConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"{built.workflow_id}: valid ({len(built.tasks)} tasks)")
    click.echo("execution order:")
    for task_id in built.topological_order():
        click.echo(f"  {task_id}")


def _parse_params(pairs: tuple[str, ...]) -> dict[str, str]:
    params: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.BadParameter(f"--param expects key=value, got '{pair}'")
        key, value = pair.split("=", 1)
        params[key] = value
    return params


@workflow.command("run")
@click.argument("name")
@click.option(
    "--param",
    "params",
    multiple=True,
    help="Run parameter as key=value (repeatable).",
)
@click.pass_obj
def workflow_run(obj: dict[str, Path], name: str, params: tuple[str, ...]) -> None:
    """Execute a workflow and persist the run."""
    path = _find_workflow_file(obj["workflows_dir"], name)
    try:
        engine, built = _build_engine(path)
    except WorkflowConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    run = engine.run(params=_parse_params(params))
    with RunStore(obj["db_path"]) as store:
        store.save_run(run)

    click.echo(f"run {run.run_id}: {run.workflow_state.value}")
    for task_id in built.topological_order():
        record = run.records[task_id]
        attempts = f" (attempts: {record.attempts})" if record.attempts else ""
        click.echo(f"  {task_id}: {record.state.value}{attempts}")
        if record.error:
            click.echo(f"    error: {record.error}")
    if run.workflow_state is not WorkflowState.SUCCESS:
        raise SystemExit(1)


@workflow.command("status")
@click.argument("name")
@click.pass_obj
def workflow_status(obj: dict[str, Path], name: str) -> None:
    """Show the latest stored run of a workflow."""
    with RunStore(obj["db_path"]) as store:
        summary = store.latest_run(name)
        loaded = store.load_run(summary.run_id) if summary else None
    if summary is None or loaded is None:
        click.echo(f"no runs recorded for workflow '{name}'")
        return
    click.echo(
        f"run {summary.run_id}: {summary.state.value}"
        f" (started {summary.started_at.isoformat() if summary.started_at else '?'})"
    )
    for task_id in sorted(loaded.records):
        record = loaded.records[task_id]
        click.echo(f"  {task_id}: {record.state.value}")


@workflow.command("history")
@click.argument("name")
@click.pass_obj
def workflow_history(obj: dict[str, Path], name: str) -> None:
    """List stored runs of a workflow, newest first."""
    with RunStore(obj["db_path"]) as store:
        summaries = store.list_runs(name)
    if not summaries:
        click.echo(f"no runs recorded for workflow '{name}'")
        return
    for summary in summaries:
        started = summary.started_at.isoformat() if summary.started_at else "?"
        click.echo(f"{summary.run_id}  {summary.state.value}  {started}")


@run.command("show")
@click.argument("run_id")
@click.pass_obj
def run_show(obj: dict[str, Path], run_id: str) -> None:
    """Print the full record of a stored run."""
    with RunStore(obj["db_path"]) as store:
        loaded = store.load_run(run_id)
    if loaded is None:
        raise click.ClickException(f"run not found: {run_id}")
    click.echo(f"workflow: {loaded.workflow_id}")
    click.echo(f"state: {loaded.workflow_state.value}")
    click.echo("tasks:")
    for task_id in sorted(loaded.records):
        record = loaded.records[task_id]
        click.echo(f"  {task_id}: {record.state.value} (attempts: {record.attempts})")
    click.echo("log:")
    for entry in loaded.log:
        click.echo(f"  {entry}")


@run.command("cancel")
@click.argument("run_id")
@click.pass_obj
def run_cancel(obj: dict[str, Path], run_id: str) -> None:
    """Mark a non-terminal stored run as cancelled."""
    with RunStore(obj["db_path"]) as store:
        if not store.cancel_run(run_id):
            raise click.ClickException(f"run '{run_id}' not found or already terminal")
    click.echo(f"run {run_id} cancelled")


if __name__ == "__main__":  # pragma: no cover
    main()
