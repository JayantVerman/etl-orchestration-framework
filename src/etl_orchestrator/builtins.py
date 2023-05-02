"""Built-in task callables for configuration-driven workflows.

These simple tasks exist so YAML-defined workflows can be executed
without writing Python per workflow — they demonstrate the framework's
own mechanics (echo output, failures, retries, waits). Real ETL tasks
call the same :class:`~etl_orchestrator.engine.TaskContext` protocol.
"""

from __future__ import annotations

import sys
import time

from etl_orchestrator.engine import TaskCallable, TaskContext


def echo(ctx: TaskContext) -> None:
    """Print a message (``params.message``) for the task."""
    message = str(ctx.params.get("message", ""))
    print(f"[{ctx.task_id}] {message}", file=sys.stdout)


def fail(ctx: TaskContext) -> None:
    """Always fail (``params.message`` describes why)."""
    raise RuntimeError(str(ctx.params.get("message", "task failed")))


def flaky(ctx: TaskContext) -> None:
    """Fail while ``attempt <= params.fail_times``, then succeed."""
    fail_times = int(str(ctx.params.get("fail_times", 1)))
    if ctx.attempt <= fail_times:
        raise RuntimeError(
            str(ctx.params.get("message", f"flaky on attempt {ctx.attempt}"))
        )


def wait(ctx: TaskContext) -> None:
    """Sleep for ``params.seconds`` (simulates slow work)."""
    time.sleep(float(str(ctx.params.get("seconds", 0.0))))


#: Registry used by the CLI and config loader by default.
DEFAULT_TASK_REGISTRY: dict[str, TaskCallable] = {
    "echo": echo,
    "fail": fail,
    "flaky": flaky,
    "wait": wait,
}
