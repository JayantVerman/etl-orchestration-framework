# ETL Orchestration Framework

A reusable Python-based **ETL workflow orchestration framework** built to
demonstrate the core concepts behind real data-platform workflow engines
(Airflow, Dagster, Prefect) before using them in later projects.

## Status

**Project 03 — Milestones 1–3 complete.**

- **Milestone 1:** project foundation (src layout package, tooling, CLI stub, tests)
- **Milestone 2:** DAG model and validation — `Task`/`Workflow` (Pydantic), dependency
  registration with fail-fast duplicate/unknown/self-dependency checks, whole-graph
  validation collecting all problems, DFS cycle detection with exact cycle paths, and
  deterministic topological dependency resolution (Kahn's algorithm + min-heap)
- **Milestone 3:** state management — `TaskState`/`WorkflowState` enums with explicit
  legal-transition tables, terminal states, `StateTransitionError` on illegal moves,
  and workflow-state aggregation from task outcomes (failure outranks cancellation)

Later milestones add: execution engine, retries with exponential backoff, persistence,
YAML workflow configuration, a full CLI, a local scheduler, bounded concurrency, hooks
and observability, recovery/cancellation, Docker and CI/CD, and full docs.

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
