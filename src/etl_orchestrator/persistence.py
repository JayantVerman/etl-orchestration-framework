"""SQLite persistence for workflow runs (Milestone 6).

Uses the standard library ``sqlite3`` module — no extra dependencies.
A :class:`RunStore` persists :class:`~etl_orchestrator.engine.WorkflowRun`
objects with their task records, transition log, and params, and reloads
them fully reconstructed. Re-opening the same database file (as happens
after a process restart) yields identical state; that is the restart
guarantee this module is tested against.

Limitations: runs are saved when the caller saves them (the CLI saves
on completion); a hard crash mid-run loses the in-memory portion of
that run — see docs/limitations.md (Milestone 14).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from etl_orchestrator.engine import TaskRunRecord, WorkflowRun
from etl_orchestrator.states import TaskState, WorkflowState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    state TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    params TEXT NOT NULL DEFAULT '{}',
    log TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_workflow
    ON workflow_runs (workflow_id);
CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT NOT NULL REFERENCES workflow_runs (run_id),
    task_id TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    started_at TEXT,
    finished_at TEXT,
    next_retry_at TEXT,
    PRIMARY KEY (run_id, task_id)
);
"""


@dataclass
class RunSummary:
    """Lightweight row from execution history."""

    run_id: str
    workflow_id: str
    state: WorkflowState
    started_at: datetime | None
    finished_at: datetime | None


def _dump_dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _load_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class RunStore:
    """Persists workflow runs to a SQLite database file."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False + _lock: the scheduler writes from a
        # background thread while the main thread reads history.
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()

    def __enter__(self) -> RunStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------
    def save_run(self, run: WorkflowRun) -> None:
        """Insert or replace a run and its task records."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO workflow_runs "
                "(run_id, workflow_id, state, started_at, finished_at,"
                " params, log) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run.run_id,
                    run.workflow_id,
                    run.workflow_state.value,
                    _dump_dt(run.started_at),
                    _dump_dt(run.finished_at),
                    json.dumps(run.params),
                    json.dumps(run.log),
                ),
            )
            self._conn.execute("DELETE FROM task_runs WHERE run_id = ?", (run.run_id,))
            for record in run.records.values():
                self._conn.execute(
                    "INSERT INTO task_runs (run_id, task_id, state,"
                    " attempts, error, started_at, finished_at,"
                    " next_retry_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run.run_id,
                        record.task_id,
                        record.state.value,
                        record.attempts,
                        record.error,
                        _dump_dt(record.started_at),
                        _dump_dt(record.finished_at),
                        _dump_dt(record.next_retry_at),
                    ),
                )

    def cancel_run(self, run_id: str) -> bool:
        """Mark a non-terminal run cancelled.

        Returns True when the run existed and was transitioned; False
        when it does not exist or was already terminal.
        """
        run = self.load_run(run_id)
        terminal = (
            WorkflowState.SUCCESS,
            WorkflowState.FAILED,
            WorkflowState.CANCELLED,
        )
        if run is None or run.workflow_state in terminal:
            return False
        run.note(f"workflow: {run.workflow_state.value} -> cancelled")
        run.workflow_state = WorkflowState.CANCELLED
        run.note("workflow: cancelled (manual)")
        self.save_run(run)
        return True

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def load_run(self, run_id: str) -> WorkflowRun | None:
        """Fully reconstruct a run, or return None if unknown."""
        with self._lock:
            row = self._conn.execute(
                "SELECT workflow_id, state, started_at, finished_at, params,"
                " log FROM workflow_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            task_rows = self._conn.execute(
                "SELECT task_id, state, attempts, error, started_at,"
                " finished_at, next_retry_at FROM task_runs WHERE run_id = ?"
                " ORDER BY task_id",
                (run_id,),
            ).fetchall()
        workflow_id, state, started_at, finished_at, params, log = row
        run = WorkflowRun(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_state=WorkflowState(state),
            started_at=_load_dt(started_at),
            finished_at=_load_dt(finished_at),
            params=json.loads(params),
            log=json.loads(log),
        )
        for task_row in task_rows:
            (
                task_id,
                task_state,
                attempts,
                error,
                task_started,
                task_finished,
                next_retry,
            ) = task_row
            run.records[task_id] = TaskRunRecord(
                task_id=task_id,
                state=TaskState(task_state),
                attempts=attempts,
                error=error,
                started_at=_load_dt(task_started),
                finished_at=_load_dt(task_finished),
                next_retry_at=_load_dt(next_retry),
            )
        return run

    def list_runs(self, workflow_id: str | None = None) -> list[RunSummary]:
        """Return execution history, newest first, optionally filtered."""
        query = "SELECT run_id, workflow_id, state, started_at, finished_at FROM workflow_runs"
        params: tuple[str, ...] = ()
        if workflow_id is not None:
            query += " WHERE workflow_id = ?"
            params = (workflow_id,)
        query += " ORDER BY COALESCE(started_at, '') DESC, run_id DESC"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [
            RunSummary(
                run_id=row[0],
                workflow_id=row[1],
                state=WorkflowState(row[2]),
                started_at=_load_dt(row[3]),
                finished_at=_load_dt(row[4]),
            )
            for row in rows
        ]

    def latest_run(self, workflow_id: str) -> RunSummary | None:
        """Return the most recent run summary for a workflow, if any."""
        summaries = self.list_runs(workflow_id)
        return summaries[0] if summaries else None
