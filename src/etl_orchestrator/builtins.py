"""Built-in task callables.

Provides a small library of reusable task implementations that can be
referenced from YAML workflows via the ``type`` field:

- :func:`noop` — a task that does nothing (useful for stubs / joins)
- :func:`flaky` — a task that fails randomly (useful for testing retries)
- :func:`log_message` — a task that prints a message

All callables accept a single :class:`TaskContext` argument as required
by the workflow engine.
"""

import random
from collections.abc import Callable

from etl_orchestrator.engine import TaskContext


def noop(ctx: TaskContext) -> None:
    """A task that does nothing.

    Useful as a placeholder or as a join node that waits for upstream
    tasks to complete without performing any work.
    """
    _ = ctx  # explicitly unused


def flaky(ctx: TaskContext) -> None:
    """A task that fails a configurable number of times.

    The ``fail_times`` parameter controls how many times this task will
    fail before succeeding. This is useful for testing retry logic.

    If ``fail_times`` is not provided, the task fails randomly ~30% of the time.
    """
    fail_times_raw = ctx.params.get("fail_times")
    if fail_times_raw is not None:
        attempt = ctx.attempt
        fail_times = int(fail_times_raw)  # type: ignore
        if attempt <= fail_times:
            raise RuntimeError(f"flaky task failed (attempt {attempt}/{fail_times})")
        return
    # Legacy random behavior
    if random.random() < 0.3:
        raise RuntimeError("flaky task failed (random)")


def log_message(ctx: TaskContext) -> None:
    """A task that prints a message to stdout.

    The message can be provided via ``ctx.params["message"]``.
    """
    message = ctx.params.get("message", "hello from log_message")
    print(f"[{ctx.workflow_id}:{ctx.task_id}] {message}")


def fail(ctx: TaskContext) -> None:
    """A task that always fails.

    Useful for testing failure handling and downstream blocking.
    The error message defaults to a generic message but can be overridden
    via ``ctx.params["message"]``.
    """
    message = ctx.params.get("message", f"task {ctx.task_id} failed (always_fails)")
    raise RuntimeError(message)


def always_fails(ctx: TaskContext) -> None:
    """Alias for :func:`fail`."""
    fail(ctx)


def echo(ctx: TaskContext) -> None:
    """A task that prints a message from ``ctx.params['message']``.

    This is the default callable used in the example YAML workflows.
    If ``message`` is not provided, it prints a generic greeting.
    """
    message = ctx.params.get("message", "hello from echo")
    print(f"[{ctx.task_id}] {message}")


DEFAULT_TASK_REGISTRY: dict[str, Callable[[TaskContext], None]] = {
    "noop": noop,
    "flaky": flaky,
    "log_message": log_message,
    "fail": fail,
    "always_fails": always_fails,
    "echo": echo,
}
