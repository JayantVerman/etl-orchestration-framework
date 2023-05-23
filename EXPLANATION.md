# ETL Orchestration Framework — Project Explanation

> A walkthrough of what this project is, how every file works, the design
> decisions behind it, and the interview questions it can be used to answer.

---

## 1. What this project is

**`etl_orchestrator`** is a small, self-contained Python framework for
defining and running **ETL workflows** — pipelines of `Extract → Transform →
Load` jobs and the **control logic** that surrounds them: dependencies,
retries, failure handling, scheduling, persistence, concurrency, logging,
and metrics.

It is deliberately written **from scratch** (no Airflow / Dagster / Prefect
dependency) to demonstrate the internals of what a workflow engine actually
does. Once you understand this codebase, the production-grade tools stop
feeling magical.

The project ships **11 milestones** worth of functionality:

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

## 2. Big picture — how a workflow runs

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
│  WorkflowEngine    │   engine.py     — orchestrator
└─────────┬──────────┘
          │ topological order → state machine → callables
          ▼
┌────────────────────┐
│  Task callables    │   user code    — what each step actually does
└─────────┬──────────┘
          │ results / errors
          ▼
┌────────────────────┐
│  RunStore (SQLite) │   persistence.py — save/load runs
└─────────┬──────────┘
          │ CLI / scheduler
          ▼
┌────────────────────┐
│  Hooks + Metrics   │   hooks.py, observability.py
└────────────────────┘
```

The whole engine is a **single in-process library** — there is no
network service, no worker pool, no message broker. That makes the code
small enough to read end-to-end while still exercising every interesting
design problem (topological scheduling, state machines, retry, backoff,
persistence, concurrency, observability) that a real distributed engine
solves.

---

## 3. The example workflow

`configs/workflows/daily_etl.yaml` is a faithful version of the
customers/orders two-branch scenario from the spec:

```
extract_customers → validate_customers → transform_customers → load_customers → build_customer_summary ↘
extract_orders    → validate_orders    → transform_orders    → load_orders    → build_order_summary    → daily_business_summary → send_notification
```

Two independent root tasks (`extract_customers`, `extract_orders`),
two parallel branches, a **diamond join** at
`build_customer_summary + build_order_summary → daily_business_summary`,
and a final leaf. Running this from the CLI:

```powershell
orchestrator run --workflow configs\workflows\daily_etl.yaml --register etl_orchestrator.builtins
```

executes the full graph, retries the deliberately-flaky
`transform_customers`, logs every transition, and persists the run so it
can be inspected later with `orchestrator show <run_id>`.

---

## 4. File-by-file guide

### 4.1 `src/etl_orchestrator/__init__.py`
The **public API**. Re-exports every name users should import
(`Task`, `Workflow`, `WorkflowEngine`, `TaskState`, exceptions, helpers).
The version string (`0.1.0`) and the `__all__` list live here so `import
etl_orchestrator` is the only import users need.

### 4.2 `src/etl_orchestrator/exceptions.py`
The **exception hierarchy**. One root class (`OrchestrationError`) so
user code can catch the whole family with a single `except`. The
specific subclasses (`WorkflowValidationError`, `DuplicateTaskError`,
`UnknownTaskError`, `SelfDependencyError`, `DependencyCycleError`,
`StateTransitionError`, `EngineConfigurationError`) all carry the
context needed to debug — e.g. `DependencyCycleError.cycle` contains the
exact path that closed the loop, like `a → b → a`.

### 4.3 `src/etl_orchestrator/dag.py`
**The graph data model.**

* `Task` — a Pydantic, frozen model. Carries `task_id`, `description`,
  `depends_on`, and the retry policy (`max_attempts`, `backoff_seconds`,
  `backoff_multiplier`).
* `Workflow` — the DAG. Owns `tasks: dict[str, Task]`, `dependencies:
  dict[str, frozenset[str]]`, and the graph algorithms:
  * `add_task(task)` — registers a task **and** auto-wires any
    `task.depends_on` entries into the dependency graph.
  * `add_dependency(task_id, upstream_id)` — fail-fast checks for
    duplicate / unknown / self-dependency.
  * `upstream(t)`, `downstream(t)`, `roots()`, `leaves()` — graph queries.
  * `validate_graph()` — collects **every** problem (empty workflow,
    unknown refs, self-deps, cycles) in a single pass;
    `validate_workflow()` raises them all together.
  * `topological_order()` — Kahn's algorithm with a min-heap so the
    output is **deterministic** (independent tasks emit in task-id
    order). This is what makes the engine reproducible.
  * `from_chain(task_ids)` — convenience builder for linear pipelines.
* `_Color` (IntEnum) drives the DFS-based cycle detector so we can
  report **each distinct cycle with its full path** (e.g. `a → b → a`).

### 4.4 `src/etl_orchestrator/states.py`
**State machines for tasks and workflows.**

* `TaskState` — `PENDING, RUNNING, SUCCESS, RETRYING, FAILED,
  UPSTREAM_FAILED, SKIPPED, CANCELLED`.
* `WorkflowState` — `PENDING, RUNNING, SUCCESS, FAILED, CANCELLED`.
* `TASK_TRANSITIONS` and `WORKFLOW_TRANSITIONS` — explicit
  `dict[State, set[State]]` tables. **Every state change the engine
  ever makes is declared here.** That makes the whole engine provably
  correct: no `state = new_state` line is ever written that isn't in
  the table.
* `can_transition`, `transition_task`, `can_transition_workflow`,
  `transition_workflow` — helpers that **raise `StateTransitionError`
  on illegal moves** with a message like
  `invalid task transition: success -> running`.
* `is_terminal_*` and `FAILURE_STATES` — derived predicates.
* `aggregate_workflow_state(task_states)` — derives the workflow state
  from the set of task states using a clear precedence rule
  (running > failed > cancelled > success > skipped).

### 4.5 `src/etl_orchestrator/engine.py`
**The execution engine** — the heart of the project.

* `TaskContext` — what every task callable receives: `run_id`,
  `task_id`, `workflow_id`, `attempt`, `params`.
* `TaskRunRecord` — per-task runtime metadata: `state`, `attempts`,
  `error`, `started_at`, `finished_at`, `next_retry_at`.
* `WorkflowRun` — a single execution: `run_id` (uuid4), `workflow_id`,
  overall `workflow_state`, all `TaskRunRecord`s, and a
  **human-readable transition log** (`run.note(...)`) that records
  every state change.
* `WorkflowEngine` — the orchestrator. Its `run(params)` method:
  1. Validates the workflow at construction (cyclic graphs can never
     execute).
  2. Iterates tasks in `Workflow.topological_order()`.
  3. For each task: calls `_block_if_upstream_failed` (so a downstream
     task never starts after its upstream permanently fails), then runs
     the retry loop (`PENDING → RUNNING → SUCCESS/FAILED/RETRYING`).
  4. On failure: records the exception, walks the DAG and marks
     transitive downstream tasks `UPSTREAM_FAILED` with the failing
     upstream's name. **Independent branches keep running.**
  5. On retry exhaustion: `FAILED` is terminal, downstream is blocked,
     a permanent-failure entry is written to the log.
  6. Sleeps between retries using an **injectable sleeper** so tests
     can run in zero wall-clock time.
  7. Drives every state change through the transition tables from
     `states.py` — illegal moves are structurally impossible.
  8. Optionally fires `EngineHooks` callbacks and emits structured
     JSON log lines on the `etl_orchestrator.engine` logger.
  9. Returns the final `WorkflowRun`.

**Concurrency model** (`max_parallel_tasks`): tasks are scheduled
**level-by-level** — every level waits for the previous level to fully
finish, then runs ready tasks in a thread pool of size
`max_parallel_tasks`. Simple, race-free, and easy to reason about.

### 4.6 `src/etl_orchestrator/persistence.py`
**SQLite-backed run store.**

* `RunStore` is a thin wrapper around `sqlite3` (stdlib, no SQLAlchemy).
* Schema: two tables — `runs` (one row per `WorkflowRun`) and
  `task_runs` (one row per `TaskRunRecord`), with a foreign key.
* Methods: `save_run(run)`, `load_run(run_id)`, `list_runs(workflow_id,
  limit)`, `cancel_run(run_id)`, `mark_tasks_for_retry(run_id, ids)`.
* `LiveRunStore` is a thread-safe variant that commits more frequently
  so concurrent engine updates don't clobber each other.
* Designed so **a process crash mid-run leaves the run inspectable and
  resumable** — Milestone 12 builds on this.

### 4.7 `src/etl_orchestrator/config.py`
**YAML workflow loader.**

* `TaskConfig` and `WorkflowConfig` are Pydantic models with the same
  shape as the YAML.
* `load_workflow_from_yaml(path) -> tuple[WorkflowConfig, dict]` parses
  the file, validates every field, and returns both the parsed config
  and a dict of callable names so the CLI can `Register` them.
* Resolution of callable names happens in the **Register** object
  (`builtins.py`) — `config.py` only knows about strings.
* Validation collects *all* problems into a `WorkflowConfigError`
  rather than failing on the first issue.

### 4.8 `src/etl_orchestrator/builtins.py`
**A small library of built-in callables** so the example workflow
runs without any user code. Currently includes `echo` (prints a
message) and `flaky` (fails the first N times — used to demonstrate
retries). Wired into the example YAML via
`--register etl_orchestrator.builtins`.

### 4.9 `src/etl_orchestrator/cli.py`
**The `orchestrator` command.** Built with Click. Subcommands:

* `orchestrator version` — prints the package version.
* `orchestrator run --workflow <yaml> --register <module>` — parses
  the YAML, builds a `WorkflowEngine`, runs it, prints a summary, and
  persists the run.
* `orchestrator list [--workflow <id>]` — lists recent runs.
* `orchestrator show <run_id>` — pretty-prints a run (state, log,
  per-task record).
* `orchestrator cancel <run_id>` — marks a non-terminal run as
  cancelled.

Exit codes: `0` success, `1` run failed, `2` invalid config / input.

### 4.10 `src/etl_orchestrator/scheduler.py`
**Local in-process scheduler.**

* `RunOnce` — fires a workflow exactly once after a delay.
* `IntervalSchedule` — fires a workflow on a regular interval
  (`every_seconds`).
* `LocalScheduler` — owns a `threading.Thread` that loops, finds due
  schedules, and triggers them. `start()` / `stop()` are idempotent.
* The thread is **daemon** by default so it does not block process
  exit. A single-process replacement for `cron`-style schedulers.

### 4.11 `src/etl_orchestrator/hooks.py`
**Lifecycle callback registry.**

* `EngineHooks` is a `dataclass(frozen=True)` with optional callable
  fields: `on_workflow_start`, `on_workflow_end`, `on_task_start`,
  `on_task_success`, `on_task_failure`, `on_task_retry`.
* All fields default to `None` (no callback = no-op).
* The engine fires the right hook at the right point — e.g.
  `on_task_failure` only fires on **permanent** failure (after retries
  are exhausted), not on every transient retry. This is a subtle but
  important contract.

### 4.12 `src/etl_orchestrator/observability.py`
**Metrics and logging.**

* `RunMetrics` — a frozen dataclass: `run_id`, `workflow_id`,
  `workflow_state`, counts by state, `retries`, `duration_seconds`,
  `task_states` (per-task state map).
* `compute_run_metrics(run)` — derives a `RunMetrics` from a
  `WorkflowRun`. Pure function; no I/O.
* `configure_logging(level="INFO", json=True)` — installs a
  JSON-formatting handler on the `etl_orchestrator` logger so log
  lines are structured (`{"timestamp", "level", "message",
  "workflow_id", "run_id", "task_id", ...}`) and easy to ship to a log
  aggregator.
* The engine emits one JSON log line per lifecycle event:
  `workflow started`, `task started`, `task success`,
  `task failed permanently`, `task retrying`, `workflow finished`.

### 4.13 `src/etl_orchestrator/py.typed`
PEP 561 marker. Tells type checkers (`mypy`) that the package ships
inline type annotations, so users who depend on it get full type
support in their own code.

### 4.14 `tests/`
One test file per module. Tests are written with `pytest` and use the
real engine — no mocks for the engine itself. Each milestone's test
file contains a **scenario class** (`TestCustomersOrdersScenario` and
similar) that exercises the full feature end-to-end. As of the latest
run: **164 tests, all passing**, plus 100% ruff + mypy clean.

| Test file | What it covers |
|---|---|
| `test_package.py` | Importability, version, CLI stub |
| `test_dag.py` | DAG model, validation, cycle detection, topological order |
| `test_states.py` | State transitions, terminal states, aggregation |
| `test_engine.py` | Sequential execution, downstream blocking |
| `test_retries.py` | Retry policy, exponential backoff, exhaustion |
| `test_persistence.py` | SQLite round-trip, survival across restart, cancel |
| `test_config.py` | YAML loading, validation, error aggregation |
| `test_cli.py` | Click commands, exit codes, JSON output |
| `test_scheduler.py` | RunOnce, IntervalSchedule, thread loop |
| `test_concurrency.py` | Level scheduling, max_parallel_tasks |
| `test_hooks.py` | EngineHooks contract (fires on the right events) |
| `test_observability.py` | RunMetrics, structured logging |
| `test_exceptions.py` | Exception hierarchy and message contracts |

### 4.15 `configs/workflows/daily_etl.yaml`
The customers/orders two-branch workflow described in section 3, in
YAML form. The `transform_customers` task is configured with
`max_attempts: 3, backoff_seconds: 0.05` so the example naturally
exercises the retry path.

### 4.16 `pyproject.toml`
Hatchling-based build, `src/` layout, runtime deps (`pydantic`,
`pyyaml`, `click`), dev extras (`pytest`, `pytest-cov`, `ruff`,
`mypy`, `types-PyYAML`). Defines the `orchestrator` console script
entry point and the `py.typed` marker.

---
---

## 5. Design decisions worth knowing

* **State machine is the source of truth.** No code anywhere mutates a
  task's state outside `transition_task()`. If a move is not in the
  table, it is not allowed.
* **Validation collects all errors, not just the first.** Whether it
  is DAG validation, YAML config validation, or workflow construction,
  the user sees every problem at once.
* **Topological order is deterministic.** Independent tasks emit in
  task-id order, so two runs of the same workflow produce identical
  log sequences. Critical for testability.
* **In-process, single-writer SQLite.** No connection pool, no WAL
  tuning. The trade-off is no distributed execution; the upside is
  zero operational surface area.
* **Level scheduling for concurrency.** Simpler than a worker pool,
  easier to reason about, and the right default for ETL where tasks
  tend to fan in/out at clear levels.
* **No external services.** No Redis, no Postgres, no message broker,
## 6. Interview Q&A

### Q1. What is workflow orchestration and why does it matter?
**A.** Orchestration is the *control layer* around ETL: defining what
runs, in what order, with what dependencies, and what to do when
something fails. Without it you end up with cron jobs calling shell
scripts that call other shell scripts — fragile, unobservable, and
impossible to reason about at scale. A real orchestration engine gives
you dependencies, retries, state, history, and alerting as first-class
concepts.

### Q2. Walk me through what happens when a workflow runs.
**A.** The engine: (1) validates the DAG, (2) computes a deterministic
topological order, (3) iterates tasks; for each one, checks if upstreams
are in a blocking state — if so marks it `UPSTREAM_FAILED` and skips;
otherwise moves it `PENDING -> RUNNING`, calls the user callable, handles
exceptions. On failure, increments attempts and retries with exponential
backoff if attempts remain; on exhaustion, marks `FAILED` and propagates
downstream. On success, marks `SUCCESS`. After every task, derives
workflow state from task states. Persists, fires hooks, emits structured
logs, returns the `WorkflowRun`.

### Q3. How do you handle a task that fails?
**A.** Per-task retry policy: `max_attempts`, `backoff_seconds`,
`backoff_multiplier`. Transient failure -> sleep with exponential backoff
and retry. Permanent failure (exhausted attempts) -> mark `FAILED` and
walk the DAG to mark every transitive downstream task `UPSTREAM_FAILED`
with the failing upstream's name. Independent branches keep running. The
failure hook fires exactly once on permanent failure (not on every
transient retry).

### Q4. How do you avoid running a task whose upstream failed?
**A.** Two layers. First, the main loop runs tasks in topological order
so a task is only dispatched after all its upstreams are terminal.
Second, at the start of every `_execute_task` call,
`_block_if_upstream_failed` re-checks: if any direct upstream is
`FAILED` or `UPSTREAM_FAILED`, the task and its transitive downstream are
marked `UPSTREAM_FAILED` before the state machine can start them.

### Q5. Why is the state machine in its own module?
**A.** To make correctness checkable. Every state change is mediated by
`transition_task()`, which consults a single `TASK_TRANSITIONS` table.
If the move is not in the table, it raises `StateTransitionError`. A
code review can answer "is this state change legal?" by looking at one
dict. The terminal states are a derived property of the table, not
scattered through the code.

### Q6. How does the engine handle concurrent execution?
**A.** Level scheduling. The DAG is partitioned into levels where all
tasks in level N have upstreams in level N-1. The engine runs every
level sequentially, but within a level dispatches ready tasks to a
`ThreadPoolExecutor` of size `max_parallel_tasks`. State is guarded by
a `threading.Lock` so concurrent updates do not corrupt the run record.
  no HTTP server. The framework is a single Python library; the user
  chooses where to deploy it.
### Q7. How would you scale this to multiple machines?
**A.** Separate the **control plane** (scheduler, state, queue) from the
**data plane** (the callables that do the work). Replace the in-process
`RunStore` with a Postgres backend, replace the `ThreadPoolExecutor`
with a worker pool consuming from a queue (Redis / SQS / Celery), and
turn the scheduler into a leader-elected service. The state machine and
DAG modules stay exactly as they are — the orchestration *logic* is
independent of the *execution substrate*.

### Q8. What happens if the process crashes mid-run?
**A.** The run is checkpointed to SQLite after every task, so a crash
leaves the run in a recoverable state. Milestone 12 reconciles stale
`RUNNING` tasks back to `PENDING` and resumes from the last successful
task. This is the "at-least-once" guarantee — task callables should be
**idempotent** so a retry after a crash does not double-apply the work.

### Q9. How do you test the engine?
**A.** Three layers: (1) unit tests per module — fast, no I/O. (2)
Scenario tests — full workflows end-to-end through the real engine.
(3) Injectable clocks and sleepers — retry tests run in zero wall-clock
time, time-sensitive tests are fully deterministic. No `time.sleep` in
production code.

### Q10. What did you learn building this?
**A.** (1) Topological order is a primitive — most orchestration
problems reduce to "in what order?"; everything else is bookkeeping.
(2) State machines are undervalued — splitting "what state am I in?"
from "what transitions are legal?" makes the code dramatically easier to
reason about. (3) Determinism is more important than speed for
orchestration. (4) The interesting bugs are the ones your tests do not
catch — two bugs found during development were masked by the test suite.
The right response is to write the missing test, fix the bug, and treat
the silence as a signal.

### Q11. Compare this to Airflow / Dagster / Prefect.
**A.** All three solve the same problem. Airflow: oldest, DAGs as Python
files, separate scheduler service, metadata DB. Dagster: software-defined
assets, richer type/IO system. Prefect: Python-native flow API,
serverless-friendly cloud. This project is closest to Airflow but
stripped to the essentials: no web UI, no multi-tenant security, no
plugin system. The architectural ideas — DAG model, state machine,
scheduler, persistence — are identical. That is the point: you
internalize the design every production engine reimplements.

### Q12. If you had to add one feature next, what would it be?
**A.** **Process-restart recovery** (Milestone 12). Today a crash leaves
a stale `RUNNING` record in SQLite. The next milestone reconciles those,
allows `orchestrator retry <run_id>`, and resumes from the last
successful task. It is the natural test of the persistence design: if
the schema cannot answer "which tasks are done?" in one query, the
design is wrong.

---

## 7. How to run it locally

```powershell
cd d:\projects\03-etl-orchestration-framework
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"

# Run the example workflow
.\.venv\Scripts\orchestrator run --workflow configs\workflows\daily_etl.yaml --register etl_orchestrator.builtins

# Inspect it
.\.venv\Scripts\orchestrator list
.\.venv\Scripts\orchestrator show <run_id>

# Run the tests
.\.venv\Scripts\python -m pytest          # 164 tests
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m mypy src
```

---

## 8. Where to read next

* **`README.md`** — short status table of all milestones.
* **`docs/`** — full reference documentation (later milestones).
* **`tests/`** — the best executable spec of how each feature behaves.
* **`configs/workflows/daily_etl.yaml`** — a real, runnable workflow.
