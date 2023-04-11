"""ETL Orchestration Framework.

A reusable Python-based ETL workflow orchestration framework.

The framework allows a data engineer to define workflows consisting of
tasks, dependencies, execution order, retries, scheduling, task and
workflow states, execution history, bounded concurrency, failure
handling, parameters, logging, metrics, and hooks/notifications.

Core pipeline:

    Workflow Definition
        -> DAG Validation
        -> Scheduling
        -> Task Dependency Resolution
        -> Task Execution
        -> Retry / Failure Handling
        -> State Persistence
        -> Execution Metadata
        -> Logging / Metrics
        -> Workflow Completion
"""

from etl_orchestrator.dag import Task, Workflow
from etl_orchestrator.exceptions import (
    DependencyCycleError,
    DuplicateTaskError,
    OrchestrationError,
    SelfDependencyError,
    UnknownTaskError,
    WorkflowValidationError,
)

__version__ = "0.1.0"

__all__ = [
    "DependencyCycleError",
    "DuplicateTaskError",
    "OrchestrationError",
    "SelfDependencyError",
    "Task",
    "UnknownTaskError",
    "Workflow",
    "WorkflowValidationError",
    "__version__",
]
