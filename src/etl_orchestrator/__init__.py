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
from etl_orchestrator.engine import (
    TaskCallable,
    TaskContext,
    TaskRunRecord,
    WorkflowEngine,
    WorkflowRun,
)
from etl_orchestrator.exceptions import (
    DependencyCycleError,
    DuplicateTaskError,
    EngineConfigurationError,
    OrchestrationError,
    SelfDependencyError,
    StateTransitionError,
    UnknownTaskError,
    WorkflowValidationError,
)
from etl_orchestrator.states import (
    FAILURE_STATES,
    TASK_TRANSITIONS,
    TERMINAL_TASK_STATES,
    TERMINAL_WORKFLOW_STATES,
    WORKFLOW_TRANSITIONS,
    TaskState,
    WorkflowState,
    aggregate_workflow_state,
    can_transition,
    can_transition_workflow,
    is_terminal_task_state,
    is_terminal_workflow_state,
    transition_task,
    transition_workflow,
)

__version__ = "0.1.0"

__all__ = [
    "FAILURE_STATES",
    "TASK_TRANSITIONS",
    "TERMINAL_TASK_STATES",
    "TERMINAL_WORKFLOW_STATES",
    "WORKFLOW_TRANSITIONS",
    "DependencyCycleError",
    "DuplicateTaskError",
    "EngineConfigurationError",
    "OrchestrationError",
    "SelfDependencyError",
    "StateTransitionError",
    "Task",
    "TaskCallable",
    "TaskContext",
    "TaskRunRecord",
    "TaskState",
    "UnknownTaskError",
    "Workflow",
    "WorkflowEngine",
    "WorkflowRun",
    "WorkflowState",
    "WorkflowValidationError",
    "__version__",
    "aggregate_workflow_state",
    "can_transition",
    "can_transition_workflow",
    "is_terminal_task_state",
    "is_terminal_workflow_state",
    "transition_task",
    "transition_workflow",
]
