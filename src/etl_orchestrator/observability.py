"""Observability utilities: structured logging and run metrics (Milestone 11).

The engine logs lifecycle events to the ``etl_orchestrator`` logger
tree; :func:`setup_logging` configures it with a JSON formatter so
logs can be shipped to any structured-log pipeline. Metrics are
computed from a finished :class:`WorkflowRun` — the run record is the
single source of truth, so metrics require no extra bookkeeping.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TextIO

from etl_orchestrator.states import TaskState, WorkflowState

if TYPE_CHECKING:
    from etl_orchestrator.engine import WorkflowRun

LOGGER_NAME = "etl_orchestrator"

_JSON_FIELDS = (
    "name",
    "levelname",
    "message",
    "task_id",
    "workflow_id",
    "run_id",
    "attempt",
    "state",
)


class JsonLogFormatter(logging.Formatter):
    """Formats log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": record.created,
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for field_name in ("task_id", "workflow_id", "run_id", "attempt", "state"):
            if hasattr(record, field_name):
                payload[field_name] = getattr(record, field_name)
        return json.dumps(payload, default=str)


def setup_logging(
    level: str | int = logging.INFO, stream: TextIO | None = None
) -> None:
    """Configure structured JSON logging for the framework logger.

    Safe to call multiple times: existing framework handlers are
    replaced, other loggers are untouched.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False


@dataclass
class RunMetrics:
    """Aggregated outcome counts for one finished workflow run."""

    run_id: str
    workflow_id: str
    workflow_state: WorkflowState
    total_tasks: int = 0
    succeeded: int = 0
    failed: int = 0
    upstream_failed: int = 0
    skipped: int = 0
    cancelled: int = 0
    pending: int = 0
    retries: int = 0
    duration_seconds: float | None = None
    task_states: dict[str, str] = field(default_factory=dict)


def compute_run_metrics(run: WorkflowRun) -> RunMetrics:
    """Compute :class:`RunMetrics` from a finished run's records."""
    counts = {
        TaskState.SUCCESS: "succeeded",
        TaskState.FAILED: "failed",
        TaskState.UPSTREAM_FAILED: "upstream_failed",
        TaskState.SKIPPED: "skipped",
        TaskState.CANCELLED: "cancelled",
        TaskState.PENDING: "pending",
    }
    metrics = RunMetrics(
        run_id=run.run_id,
        workflow_id=run.workflow_id,
        workflow_state=run.workflow_state,
        total_tasks=len(run.records),
    )
    for record in run.records.values():
        setattr(
            metrics, counts[record.state], getattr(metrics, counts[record.state]) + 1
        )
        metrics.retries += max(record.attempts - 1, 0)
        metrics.task_states[record.task_id] = record.state.value
    if run.started_at is not None and run.finished_at is not None:
        metrics.duration_seconds = (run.finished_at - run.started_at).total_seconds()
    return metrics
