"""Local interval-based scheduling (Milestone 9).

The :class:`Scheduler` executes registered workflow files on fixed
intervals. It supports two usage styles:

* **Manual/verifiable**: :meth:`Scheduler.run_once` runs every workflow
  whose interval has elapsed — this is what tests and the CLI use.
* **Background loop**: :meth:`Scheduler.start` spawns one thread that
  polls and calls :meth:`run_once` until :meth:`Scheduler.stop`.

This is deliberately *not* a distributed cron replacement: schedules
live in a local JSON file, intervals are fixed, and missed intervals
collapse into a single run (no backfill). See docs/limitations.md.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from etl_orchestrator.builtins import DEFAULT_TASK_REGISTRY
from etl_orchestrator.config import (
    build_workflow,
    load_task_params,
    load_workflow_config,
    resolve_callables,
)
from etl_orchestrator.engine import TaskCallable, WorkflowEngine, WorkflowRun
from etl_orchestrator.exceptions import OrchestrationError
from etl_orchestrator.persistence import RunStore
from etl_orchestrator.states import WorkflowState

UtcNow = Callable[[], datetime]


@dataclass
class Schedule:
    """One registered workflow and its interval."""

    name: str
    path: Path
    interval_seconds: float
    last_run_at: datetime | None = None


class Scheduler:
    """Runs registered workflows when their intervals elapse."""

    def __init__(
        self,
        store: RunStore,
        *,
        registry: dict[str, TaskCallable] | None = None,
        clock: UtcNow | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._store = store
        self._registry = (
            dict(DEFAULT_TASK_REGISTRY) if registry is None else dict(registry)
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper
        self._schedules: dict[str, Schedule] = {}
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------
    def register(self, name: str, path: str | Path, interval_seconds: float) -> None:
        """Register a workflow file under ``name`` with a fixed interval."""
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._schedules[name] = Schedule(
            name=name,
            path=Path(path),
            interval_seconds=interval_seconds,
        )

    def unregister(self, name: str) -> None:
        """Remove a registration (no-op when unknown)."""
        self._schedules.pop(name, None)

    @property
    def names(self) -> list[str]:
        """Registered schedule names, sorted."""
        return sorted(self._schedules)

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------
    def due(self, now: datetime | None = None) -> list[str]:
        """Names of schedules whose intervals have elapsed."""
        current = now or self._clock()
        return sorted(
            name
            for name, schedule in self._schedules.items()
            if schedule.last_run_at is None
            or (current - schedule.last_run_at).total_seconds()
            >= schedule.interval_seconds
        )

    def run_once(self) -> list[WorkflowRun]:
        """Run every due schedule once; return the completed runs.

        Failures to *load or configure* a workflow are captured as a
        FAILED run entry with an explanatory note (the scheduler must
        survive a broken workflow file); failures during execution are
        ordinary FAILED runs.
        """
        completed: list[WorkflowRun] = []
        for name in self.due():
            schedule = self._schedules[name]
            run = self._run_schedule(schedule)
            completed.append(run)
            self._store.save_run(run)
            schedule.last_run_at = self._clock()
        return completed

    def _run_schedule(self, schedule: Schedule) -> WorkflowRun:
        try:
            config = load_workflow_config(schedule.path)
            workflow = build_workflow(config)
            callables = resolve_callables(config, self._registry)
            engine = WorkflowEngine(
                workflow,
                callables,
                task_params=load_task_params(schedule.path),
            )
        except OrchestrationError as exc:
            # Unrunnable config: persist a FAILED run for visibility.
            return WorkflowRun(
                workflow_id=schedule.name,
                workflow_state=WorkflowState.FAILED,
                log=[f"scheduler: cannot load workflow: {exc}"],
            )
        return engine.run()

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------
    def start(self, poll_interval: float = 0.05) -> None:
        """Start the background polling thread."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("scheduler already running")
        self._stop_event.clear()

        def loop() -> None:
            while not self._stop_event.is_set():
                self.run_once()
                self._sleeper(poll_interval)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    @property
    def running(self) -> bool:
        """Whether the background thread is alive."""
        return self._thread is not None and self._thread.is_alive()
