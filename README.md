# ETL Orchestration Framework

![CI](https://github.com/JayantVerman/etl-orchestration-framework/actions/workflows/ci.yml/badge.svg)

A small, self-contained Python framework for defining and running **ETL workflows** — pipelines of `Extract → Transform → Load` jobs and the **control logic** that surrounds them: dependencies, retries, failure handling, scheduling, persistence, concurrency, logging, and metrics.

Written **from scratch** (no Airflow / Dagster / Prefect dependency) to demonstrate the internals of what a workflow engine actually does. Once you understand this codebase, the production-grade tools stop feeling magical.

---

## Milestones

| # | Milestone | One-line summary |
|---|---|---|
| 1 | Foundation | `src/` layout package, tests, CLI stub |
| 2 | DAG model | `Task` / `Workflow`, validation, cycle detection, topological order |
| 3 | State machine | `TaskState` / `WorkflowState` enums with explicit transition tables |
| 4 | Execution engine | Sequential topological execution, run records, downstream blocking |
| 5 | Retries | Per-task retry policy with exponential backoff and permanent-failure blocking |
| 6 | Persistence | SQLite-backed `RunStore`, survives process restart |
| 7 | YAML workflows | Declarative workflow definitions loaded from `.yaml` |
| 8 | Full CLI | `orchestrator run / list / show / cancel` |
| 9 | Local scheduler | `RunOnce` and `IntervalSchedule` with a thread-safe background loop |
| 10 | Bounded concurrency | Level-scheduled parallel execution with `max_parallel_tasks` |
| 11 | Hooks & observability | `EngineHooks` callbacks + structured JSON logging + `RunMetrics` |

---

## Quick Start

```powershell
# Clone and set up
git clone https://github.com/JayantVerman/etl-orchestration-framework
cd etl-orchestration-framework
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

# Run tests
pytest

# Run the example workflow
orchestrator run --workflow configs/workflows/daily_etl.yaml
```

---

## Project Structure

```
etl-orchestration-framework/
├── src/etl_orchestrator/     # Main package
│   ├── __init__.py           # Package init, public API
│   ├── dag.py                # Task, Workflow, graph validation, topological order
│   ├── states.py             # TaskState, WorkflowState, transition tables
│   ├── engine.py             # WorkflowEngine — execution, retries, downstream blocking
│   ├── persistence.py        # SQLite RunStore for crash recovery
│   ├── config.py             # YAML workflow loader
│   ├── cli.py                # Click CLI (run/list/show/cancel)
│   ├── scheduler.py          # RunOnce and IntervalSchedule
│   ├── concurrency.py        # Level-scheduled parallel execution
│   ├── hooks.py              # EngineHooks callbacks
│   ├── observability.py      # Structured logging + RunMetrics
│   ├── builtins.py           # Built-in task callables
│   └── exceptions.py         # Exception hierarchy
├── tests/                    # Pytest test suite
├── configs/workflows/        # Example YAML workflow definitions
├── docs/                     # Design documents
├── examples/                 # Usage examples
├── data/                     # Runtime data (SQLite DBs)
├── pyproject.toml            # Build, deps, tool config
├── EXPLANATION.md            # Detailed project explanation + interview Q&A
└── README.md                 # This file
```

---

## How a Workflow Runs

```
┌────────────────────┐
│  Workflow (DAG)    │   dag.py        — Task, Workflow, validation
└─────────┬──────────┘
          │ add_task / add_dependency / from_yaml
          ▼
┌────────────────────┐
│  YAML or Python    │   config.py     — WorkflowConfig loader
└─────────┬──────────┘
          │ build WorkflowEngine(workflow, tasks)
          ▼
┌────────────────────┐
│  WorkflowEngine    │   engine.py     — topo-order execution, retries, blocking
└─────────┬──────────┘
          │ persist each run
          ▼
┌────────────────────┐
│  RunStore (SQLite) │   persistence.py — survives process restart
└────────────────────┘
```

---

## Key Design Decisions

1. **State machine in its own module** — every state change is declared in a transition table; illegal moves are structurally impossible.
2. **Deterministic topological order** — Kahn's algorithm with a min-heap; independent tasks always emit in task-id order.
3. **Collect-all validation** — `validate_graph()` reports every problem (cycles, unknown refs, self-deps) at once instead of failing on the first.
4. **Level-scheduled concurrency** — tasks at the same topological level run together; `max_parallel_tasks` caps the worker pool.
5. **Injectable sleeper** — retry backoff uses a `time.sleep` replacement so tests run instantly.
6. **Single exception family** — `OrchestrationError` is the root; every specific error inherits from it.

---

## Documentation

- **[EXPLANATION.md](EXPLANATION.md)** — detailed walkthrough of every file, design decisions, and 12 interview Q&As
- **[docs/](docs/)** — design documents
- **[CHANGELOG.md](CHANGELOG.md)** — version history

---

## License

MIT
