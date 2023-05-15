"""Tests for observability utilities: logging and run metrics (Milestone 11)."""

from __future__ import annotations

import io
import json
import logging

from etl_orchestrator import TaskState, WorkflowEngine, WorkflowState
from etl_orchestrator.dag import Task, Workflow
from etl_orchestrator.observability import (
    LOGGER_NAME,
    JsonLogFormatter,
    RunMetrics,
    compute_run_metrics,
    setup_logging,
)


def _ok(_ctx: object) -> None:
    return None


def _build_engine(workflow: Workflow) -> WorkflowEngine:
    callables = dict.fromkeys(workflow.tasks, _ok)
    return WorkflowEngine(workflow=workflow, tasks=callables)


def test_json_formatter_emits_valid_json() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="etl",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    output = formatter.format(record)
    payload = json.loads(output)
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "etl"
    assert "timestamp" in payload


def test_json_formatter_includes_known_extras() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name=LOGGER_NAME,
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="task event",
        args=(),
        exc_info=None,
    )
    record.task_id = "t1"  # type: ignore[attr-defined]
    record.workflow_id = "wf1"  # type: ignore[attr-defined]
    record.run_id = "r1"  # type: ignore[attr-defined]
    record.attempt = 2  # type: ignore[attr-defined]
    record.state = TaskState.SUCCESS.value  # type: ignore[attr-defined]
    payload = json.loads(formatter.format(record))
    assert payload["task_id"] == "t1"
    assert payload["workflow_id"] == "wf1"
    assert payload["run_id"] == "r1"
    assert payload["attempt"] == 2
    assert payload["state"] == "success"


def test_json_formatter_ignores_unknown_extras() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name=LOGGER_NAME,
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="clean",
        args=(),
        exc_info=None,
    )
    record.whatever = "noise"  # type: ignore[attr-defined]
    payload = json.loads(formatter.format(record))
    assert "whatever" not in payload
    assert payload["message"] == "clean"


def test_setup_logging_writes_json_to_stream() -> None:
    stream = io.StringIO()
    setup_logging(level=logging.INFO, stream=stream)
    logging.getLogger(LOGGER_NAME).info("hi %s", "there", extra={"task_id": "x"})
    output = stream.getvalue().strip()
    assert output, "expected at least one log line"
    payload = json.loads(output)
    assert payload["message"] == "hi there"
    assert payload["task_id"] == "x"
    setup_logging(level=logging.WARNING, stream=stream)


def test_setup_logging_is_idempotent_and_isolated() -> None:
    setup_logging()
    framework_logger = logging.getLogger(LOGGER_NAME)
    other_logger = logging.getLogger("etl_orchestrator_test_other")
    other_handlers_before = list(other_logger.handlers)
    setup_logging()
    assert len(framework_logger.handlers) == 1
    assert framework_logger.propagate is False
    assert other_logger.handlers == other_handlers_before


def test_compute_run_metrics_for_success() -> None:
    wf = Workflow(workflow_id="metrics_ok")
    wf.add_task(Task(task_id="a"))
    wf.add_task(Task(task_id="b", depends_on=["a"]))
    engine = _build_engine(wf)
    run = engine.run()
    metrics = compute_run_metrics(run)
    assert isinstance(metrics, RunMetrics)
    assert metrics.run_id == run.run_id
    assert metrics.workflow_id == "metrics_ok"
    assert metrics.workflow_state == WorkflowState.SUCCESS
    assert metrics.total_tasks == 2
    assert metrics.succeeded == 2
    assert metrics.failed == 0
    assert metrics.upstream_failed == 0
    assert metrics.skipped == 0
    assert metrics.retries == 0
    assert metrics.duration_seconds is not None and metrics.duration_seconds >= 0
    assert metrics.task_states == {"a": "success", "b": "success"}


def test_compute_run_metrics_counts_retries() -> None:
    wf = Workflow(workflow_id="metrics_retry")
    wf.add_task(Task(task_id="flaky", max_attempts=3, backoff_seconds=0.0))
    counter = {"n": 0}

    def _flaky(_ctx: object) -> None:
        counter["n"] += 1
        if counter["n"] < 2:
            raise RuntimeError("again")

    engine = WorkflowEngine(
        workflow=wf, tasks={"flaky": _flaky}, sleeper=lambda _d: None
    )
    run = engine.run()
    metrics = compute_run_metrics(run)
    assert metrics.succeeded == 1
    assert metrics.retries == 1


def test_compute_run_metrics_counts_failures_and_upstream_block() -> None:
    wf = Workflow(workflow_id="metrics_fail")
    wf.add_task(Task(task_id="boom", max_attempts=1))
    wf.add_task(Task(task_id="down", depends_on=["boom"]))

    def _boom(_ctx: object) -> None:
        raise RuntimeError("bad")

    def _down(_ctx: object) -> None:
        raise AssertionError("down should not run")

    engine = WorkflowEngine(workflow=wf, tasks={"boom": _boom, "down": _down})
    run = engine.run()
    metrics = compute_run_metrics(run)
    assert metrics.workflow_state == WorkflowState.FAILED
    assert metrics.failed == 1
    assert metrics.upstream_failed == 1
    assert metrics.succeeded == 0
    assert metrics.task_states["boom"] == "failed"
    assert metrics.task_states["down"] == "upstream_failed"


def test_compute_run_metrics_duration_is_none_without_timestamps() -> None:
    from etl_orchestrator.engine import TaskRunRecord, WorkflowRun

    run = WorkflowRun(workflow_id="empty", records={"a": TaskRunRecord(task_id="a")})
    metrics = compute_run_metrics(run)
    assert metrics.total_tasks == 1
    assert metrics.duration_seconds is None


def test_compute_run_metrics_duration_uses_timestamps() -> None:
    from datetime import datetime, timedelta, timezone

    from etl_orchestrator.engine import TaskRunRecord, WorkflowRun

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=42)
    run = WorkflowRun(
        workflow_id="timed",
        records={"a": TaskRunRecord(task_id="a")},
        started_at=start,
        finished_at=end,
    )
    metrics = compute_run_metrics(run)
    assert metrics.duration_seconds == 42.0
