# Workflow Management Service

[![CI](https://github.com/jayakumarvin/workflow-service/actions/workflows/ci.yml/badge.svg)](https://github.com/jayakumarvin/workflow-service/actions/workflows/ci.yml)

A small, dependency-free Python service that runs workflows of dependent tasks (a DAG) concurrently, tracks the state of every task, and handles failures, retries and cancellation.

Single file, standard library only: [`workflow_service.py`](workflow_service.py).

## Features

| Capability | How it works |
|---|---|
| **Dependency graph (DAG)** | Tasks declare `depends_on`; a task starts only when all of its dependencies have succeeded. |
| **Concurrency** | Independent tasks, and independent workflows, run in parallel on a shared `ThreadPoolExecutor`. |
| **Data passing** | A task's return value is passed to dependants as keyword arguments named after the upstream task. |
| **Retries with backoff** | `retries=N` and `retry_delay` (doubled after each failed attempt). |
| **Failure isolation** | A failed task marks its downstream tasks `SKIPPED`; unrelated branches keep running. |
| **Cycle and bad-reference detection** | Validated at submit time with a depth-first search; the error names the cycle path. |
| **Cancellation** | `cancel()` stops scheduling new tasks; tasks already running finish normally. |
| **Status queries** | Per-task status, attempts, duration, result and error, plus workflow status, via `status()` and `list_workflows()`. |

Statuses: tasks are `PENDING, RUNNING, SUCCESS, FAILED, SKIPPED, CANCELLED`; workflows are `PENDING, RUNNING, SUCCESS, FAILED, CANCELLED`.

## Quick start

Requires Python 3.10+. No installation needed for the service itself.

```bash
git clone https://github.com/jayakumarvin/workflow-service.git
cd workflow-service
python3 workflow_service.py          # run the demo
```

Run the tests:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

## Usage

```python
from workflow_service import Task, Workflow, WorkflowService

svc = WorkflowService(max_workers=4)

wf = Workflow("daily-etl")
wf.add_task(Task("extract_a", lambda: 600))
wf.add_task(Task("extract_b", lambda: 400))
wf.add_task(Task("transform", lambda extract_a, extract_b: extract_a + extract_b,
                 depends_on=["extract_a", "extract_b"]))
wf.add_task(Task("load", lambda transform: f"loaded {transform} rows",
                 depends_on=["transform"], retries=3, retry_delay=0.2))

wf_id = svc.submit(wf)       # validates, then runs in the background
svc.wait(wf_id)              # block until finished (optional timeout)
print(svc.status(wf_id))     # full per-task report as a dict
svc.shutdown()
```

### API

| Method | Purpose |
|---|---|
| `Workflow(name)` / `add_task(Task)` | Build a workflow. Duplicate task names raise `ValueError`. |
| `Task(name, func, depends_on=[], retries=0, retry_delay=0.5)` | One unit of work. |
| `svc.submit(wf)` | Validate and start; returns the workflow id. Raises `ValueError` on a cycle or unknown dependency. |
| `svc.status(id)` | Snapshot of the workflow and all tasks. |
| `svc.list_workflows()` | Id, name and status of every submitted workflow. |
| `svc.cancel(id)` | Cancel pending tasks; running tasks finish. |
| `svc.wait(id, timeout=None)` | Wait for completion and return the final status. |
| `svc.shutdown()` | Wait for all workflows, then stop the thread pool. |

## Demo output

`python3 workflow_service.py` runs three scenarios: an ETL pipeline whose two extracts run in parallel and whose flaky load succeeds on the third attempt; a report workflow where a failed validation skips `publish` while `archive` still runs; and a cyclic workflow that is rejected. Timings and ids vary per run.

```text
=== daily-etl (id) -> SUCCESS ===
TASK        STATUS     TRIES  SECS   RESULT / ERROR
extract_a   SUCCESS    1      1.002  600
extract_b   SUCCESS    1      1.002  400
transform   SUCCESS    1      0.5    1000
load        SUCCESS    3      1.502  loaded 1000 rows
notify      SUCCESS    1      0.1    notify ok

=== weekly-report (id) -> FAILED ===
TASK        STATUS     TRIES  SECS   RESULT / ERROR
fetch       SUCCESS    1      0.502  fetch ok
validate    FAILED     1      0.001  ValueError: null values in column 'customer_id'
publish     SKIPPED    0      -      None
archive     SUCCESS    1      0.302  archive ok

All workflows: [{'id': 'efcb0cec', 'name': 'daily-etl', 'status': 'SUCCESS'}, {'id': '5096b56b', 'name': 'weekly-report', 'status': 'FAILED'}]
Total wall time: 3.11s
```

## Design notes

- **One scheduler thread per workflow** resolves task readiness; **worker threads** execute tasks. A single `RLock` guards all shared state.
- A task is marked `RUNNING` by the scheduler before it is submitted to the pool, so it can never be started twice.
- Task code is user-supplied, so every exception is caught, recorded as `ExceptionType: message`, and never crashes the service.
- Threads suit I/O-bound tasks. CPU-bound tasks would need a process pool, because of the GIL.

## Known limits

- State is in memory only: it is lost when the process exits, and there is no persistence or distributed execution.
- Cancellation is cooperative: it cannot interrupt a task that is already running.

## Project layout

```text
workflow_service.py            service and demo (standard library only)
tests/test_workflow_service.py 11 pytest tests
.github/workflows/ci.yml       runs tests and demo on Python 3.10, 3.12, 3.13
```

## License

[MIT](LICENSE)
