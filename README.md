# ETL Orchestration Framework

A reusable Python-based **ETL workflow orchestration framework** built to
demonstrate the core concepts behind real data-platform workflow engines
(Airflow, Dagster, Prefect) before using them in later projects.

## Status

**Project 03 — Milestones 1–11 complete.**

- **Milestone 1:** project foundation (src layout package, tooling, CLI stub, tests)
- **Milestone 2:** DAG model and validation — `Task`/`Workflow` (Pydantic), dependency
  registration with fail-fast duplicate/unknown/self-dependency checks, whole-graph
  validation collecting all problems, DFS cycle detection with exact cycle paths, and
  deterministic topological dependency resolution (Kahn's algorithm + min-heap)
- **Milestone 3:** state management — `TaskState`/`WorkflowState` enums with explicit
  legal-transition tables, terminal states, `StateTransitionError` on illegal moves,
  and workflow-state aggregation from task outcomes (failure outranks cancellation)
- **Milestone 4:** execution engine — `WorkflowEngine.run()` executes tasks in
  topological order, hands each callable a `TaskContext` (run id, attempt, params),
  records per-task `TaskRunRecord`s (state, attempts, error, timestamps) in a
  `WorkflowRun` with a transition log, blocks transitive downstream tasks as
  `upstream_failed` when a dependency fails, and leaves independent branches running
- **Milestone 5:** retries and failure handling — per-task retry policy
  (`max_attempts`, `backoff_seconds`, `backoff_multiplier`), exponential backoff with
  injectable sleep, `retrying` state with `next_retry_at`, permanent failure after
  exhaustion → downstream blocking; transient failures recover without blocking
- **Milestone 6:** SQLite persistence — `RunStore` with `save_run`, `load_run`,
  `list_runs`, `cancel_run`, and `mark_tasks_for_retry`; survives process restart
- **Milestone 7:** YAML workflow configuration — `WorkflowConfig` Pydantic schema,
  `load_workflow_from_yaml(path)`, `Register` for user callables, type-checked params
- **Milestone 8:** full CLI — `orchestrator run|list|show|cancel`, JSON output, exit
  codes 0/1/2 for success/run-failed/invalid-config
- **Milestone 9:** local interval scheduler with `RunOnce`/`IntervalSchedule` and
  background thread loop with `start`/`stop`
- **Milestone 10:** bounded parallel execution with level scheduling
  (`max_parallel_tasks`) and a `LiveRunStore` for concurrent task updates
- **Milestone 11:** hooks (`EngineHooks`: on_workflow_start/end, on_task_start/success/
  failure/retry) and observability (`RunMetrics` via `compute_run_metrics`, structured
  JSON logging on the `etl_orchestrator` logger)

Later milestones add: process-restart recovery, cancellation, Docker and CI/CD,
and full docs.

## What the framework will orchestrate

Example daily pipeline (two independent branches joining into a summary):

```text
Extract Customers -> Validate Customers -> Transform Customers -> Load Customers -> Build Customer Summary
Extract Orders    -> Validate Orders    -> Transform Orders    -> Load Orders
Customer Summary + Order Summary -> Daily Business Summary -> Notification
```

## Installation

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
```

## Running tests

```powershell
.\.venv\Scripts\python -m pytest
```

## License

MIT — see [LICENSE](LICENSE).
