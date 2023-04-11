"""Exception hierarchy for the ETL Orchestration Framework.

All framework errors derive from :class:`OrchestrationError` so callers
can catch every framework failure with a single ``except`` clause.
Validation-specific errors derive from :class:`WorkflowValidationError`
so graph problems can be handled as one family.
"""

from __future__ import annotations


class OrchestrationError(Exception):
    """Base class for all ETL Orchestration Framework errors."""


class WorkflowValidationError(OrchestrationError):
    """Raised when a workflow definition fails graph validation.

    Attributes:
        errors: Every human-readable validation problem found. Graph
            validation collects all problems rather than stopping at
            the first one, so a caller can fix everything in one pass.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = list(errors)
        super().__init__("workflow validation failed: " + "; ".join(self.errors))


class DuplicateTaskError(WorkflowValidationError):
    """Raised when a task id is registered more than once in a workflow."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__([f"duplicate task id: '{task_id}'"])


class UnknownTaskError(WorkflowValidationError):
    """Raised when a dependency references a task that is not defined."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__([f"unknown task id: '{task_id}'"])


class SelfDependencyError(WorkflowValidationError):
    """Raised when a task is made dependent on itself."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__([f"task '{task_id}' cannot depend on itself"])


class DependencyCycleError(WorkflowValidationError):
    """Raised when the dependency graph contains a cycle.

    Attributes:
        cycle: The task ids forming the cycle, in traversal order, with
            the first task repeated at the end (e.g. ``a -> b -> a``).
    """

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = list(cycle)
        super().__init__([f"dependency cycle: {' -> '.join(self.cycle)}"])
