# ETL Orchestration Framework

A reusable Python-based **ETL workflow orchestration framework** built to
demonstrate the core concepts behind real data-platform workflow engines
(Airflow, Dagster, Prefect) before using them in later projects.

## Status

**Project 03 — Milestone 1 (Project Foundation): complete.**

Later milestones add: DAG validation, dependency resolution, task/workflow
state machines, retries with exponential backoff, persistence, YAML workflow
configuration, a CLI, a local scheduler, bounded concurrency, hooks and
observability, recovery/cancellation, Docker and CI/CD, and full docs.

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
