"""
Simple Workflow Management Service
----------------------------------
- Workflows are made of tasks with optional dependencies (a DAG).
- Independent tasks run concurrently on a shared thread pool.
- Every task and workflow has a tracked status, timestamps, result and error.
- Supports retries with backoff, cycle detection, cancellation and status queries.

Run:  python workflow_service.py
"""

from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("workflow")


# --------------------------------------------------------------------------- #
# Status enums
# --------------------------------------------------------------------------- #
class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"      # an upstream dependency failed
    CANCELLED = "CANCELLED"


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_TASK = {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED}


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
@dataclass
class Task:
    name: str
    func: Callable[..., Any]
    depends_on: List[str] = field(default_factory=list)
    retries: int = 0
    retry_delay: float = 0.5          # seconds, doubled on each retry

    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    @property
    def duration(self) -> Optional[float]:
        if self.started_at is not None and self.finished_at is not None:
            return round(self.finished_at - self.started_at, 3)
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "depends_on": self.depends_on,
            "attempts": self.attempts,
            "duration_s": self.duration,
            "result": self.result,
            "error": self.error,
        }


@dataclass
class Workflow:
    name: str
    tasks: Dict[str, Task] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    status: WorkflowStatus = WorkflowStatus.PENDING
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None

    def add_task(self, task: Task) -> "Workflow":
        if task.name in self.tasks:
            raise ValueError(f"Duplicate task name: {task.name}")
        self.tasks[task.name] = task
        return self

    def validate(self) -> None:
        """Check every dependency exists and the graph has no cycles."""
        for t in self.tasks.values():
            for dep in t.depends_on:
                if dep not in self.tasks:
                    raise ValueError(f"Task '{t.name}' depends on unknown task '{dep}'")

        WHITE, GREY, BLACK = 0, 1, 2
        color = {n: WHITE for n in self.tasks}

        def visit(n: str, path: List[str]) -> None:
            color[n] = GREY
            for dep in self.tasks[n].depends_on:
                if color[dep] == GREY:
                    raise ValueError("Cycle detected: " + " -> ".join(path + [n, dep]))
                if color[dep] == WHITE:
                    visit(dep, path + [n])
            color[n] = BLACK

        for n in self.tasks:
            if color[n] == WHITE:
                visit(n, [])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value,
            "tasks": [t.to_dict() for t in self.tasks.values()],
        }


# --------------------------------------------------------------------------- #
# Service
# --------------------------------------------------------------------------- #
class WorkflowService:
    """Runs many workflows concurrently on a shared pool of worker threads."""

    def __init__(self, max_workers: int = 4):
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="worker")
        self._workflows: Dict[str, Workflow] = {}
        self._runners: Dict[str, threading.Thread] = {}
        self._cancel: Dict[str, threading.Event] = {}
        self._lock = threading.RLock()

    # ---- public API ------------------------------------------------------- #
    def submit(self, wf: Workflow) -> str:
        """Validate and start a workflow in the background. Returns its id."""
        wf.validate()
        with self._lock:
            self._workflows[wf.id] = wf
            self._cancel[wf.id] = threading.Event()
            runner = threading.Thread(
                target=self._run, args=(wf,), name=f"wf-{wf.id}", daemon=True
            )
            self._runners[wf.id] = runner
        runner.start()
        log.info("Submitted workflow '%s' (%s) with %d tasks", wf.name, wf.id, len(wf.tasks))
        return wf.id

    def status(self, wf_id: str) -> Dict[str, Any]:
        with self._lock:
            return self._get(wf_id).to_dict()

    def list_workflows(self) -> List[Dict[str, str]]:
        with self._lock:
            return [{"id": w.id, "name": w.name, "status": w.status.value}
                    for w in self._workflows.values()]

    def cancel(self, wf_id: str) -> None:
        """Stop scheduling new tasks; tasks already running finish normally."""
        self._get(wf_id)
        self._cancel[wf_id].set()
        log.info("Cancel requested for workflow %s", wf_id)

    def wait(self, wf_id: str, timeout: Optional[float] = None) -> WorkflowStatus:
        self._runners[wf_id].join(timeout)
        return self._get(wf_id).status

    def shutdown(self) -> None:
        for r in list(self._runners.values()):
            r.join()
        self._pool.shutdown(wait=True)

    # ---- internals -------------------------------------------------------- #
    def _get(self, wf_id: str) -> Workflow:
        try:
            return self._workflows[wf_id]
        except KeyError:
            raise KeyError(f"Unknown workflow id: {wf_id}") from None

    def _set(self, task: Task, status: TaskStatus, **fields: Any) -> None:
        with self._lock:
            task.status = status
            for k, v in fields.items():
                setattr(task, k, v)

    def _execute(self, wf: Workflow, task: Task) -> None:
        """Runs on a worker thread. Handles retries and records the outcome."""
        # Pass upstream results to the task as keyword args.
        upstream = {dep: wf.tasks[dep].result for dep in task.depends_on}
        self._set(task, TaskStatus.RUNNING, started_at=time.time())
        delay = task.retry_delay

        while True:
            task.attempts += 1
            try:
                log.info("[%s] %s: attempt %d", wf.id, task.name, task.attempts)
                result = task.func(**upstream) if upstream else task.func()
                self._set(task, TaskStatus.SUCCESS, result=result, error=None,
                          finished_at=time.time())
                log.info("[%s] %s: SUCCESS -> %r", wf.id, task.name, result)
                return
            except Exception as exc:  # noqa: BLE001 - task code is user-supplied
                err = f"{type(exc).__name__}: {exc}"
                if task.attempts <= task.retries and not self._cancel[wf.id].is_set():
                    log.warning("[%s] %s: %s, retrying in %.1fs", wf.id, task.name, err, delay)
                    time.sleep(delay)
                    delay *= 2
                    continue
                self._set(task, TaskStatus.FAILED, error=err, finished_at=time.time())
                log.error("[%s] %s: FAILED (%s)", wf.id, task.name, err)
                return

    def _run(self, wf: Workflow) -> None:
        """Scheduler loop for one workflow (runs on its own thread)."""
        with self._lock:
            wf.status = WorkflowStatus.RUNNING
        cancel = self._cancel[wf.id]
        in_flight: Dict[Future, Task] = {}

        while True:
            with self._lock:
                for task in wf.tasks.values():
                    if task.status != TaskStatus.PENDING:
                        continue
                    if cancel.is_set():
                        task.status = TaskStatus.CANCELLED
                        continue
                    deps = [wf.tasks[d].status for d in task.depends_on]
                    if any(s in (TaskStatus.FAILED, TaskStatus.SKIPPED,
                                 TaskStatus.CANCELLED) for s in deps):
                        task.status = TaskStatus.SKIPPED
                        log.info("[%s] %s: SKIPPED (upstream did not succeed)", wf.id, task.name)
                    elif all(s == TaskStatus.SUCCESS for s in deps):
                        # Mark RUNNING now so it isn't submitted twice.
                        task.status = TaskStatus.RUNNING
                        in_flight[self._pool.submit(self._execute, wf, task)] = task

                all_done = all(t.status in TERMINAL_TASK for t in wf.tasks.values())

            if all_done and not in_flight:
                break
            if not in_flight:
                # Skips/cancels changed state; loop again to resolve dependants.
                continue

            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                in_flight.pop(fut)
                fut.result()  # surfaces bugs in the service itself

        with self._lock:
            statuses = {t.status for t in wf.tasks.values()}
            if cancel.is_set():
                wf.status = WorkflowStatus.CANCELLED
            elif statuses == {TaskStatus.SUCCESS}:
                wf.status = WorkflowStatus.SUCCESS
            else:
                wf.status = WorkflowStatus.FAILED
            wf.finished_at = time.time()
        log.info("Workflow '%s' (%s) finished: %s in %.2fs",
                 wf.name, wf.id, wf.status.value, wf.finished_at - wf.created_at)


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
def print_report(svc: WorkflowService, wf_id: str) -> None:
    s = svc.status(wf_id)
    print(f"\n=== {s['name']} ({s['id']}) -> {s['status']} ===")
    print(f"{'TASK':<12}{'STATUS':<11}{'TRIES':<7}{'SECS':<7}RESULT / ERROR")
    for t in s["tasks"]:
        out = t["error"] or t["result"]
        print(f"{t['name']:<12}{t['status']:<11}{t['attempts']:<7}"
              f"{'-' if t['duration_s'] is None else t['duration_s']!s:<7}{out}")


if __name__ == "__main__":
    random.seed(7)

    def work(name: str, secs: float, value: Any = None):
        def _fn(**upstream):
            time.sleep(secs)
            return value if value is not None else f"{name} ok"
        return _fn

    flaky_calls = {"n": 0}

    def flaky_load(**upstream):
        flaky_calls["n"] += 1
        time.sleep(0.3)
        if flaky_calls["n"] < 3:
            raise ConnectionError("warehouse unavailable")
        return f"loaded {upstream['transform']} rows"

    def bad_validate(**upstream):
        raise ValueError("null values in column 'customer_id'")

    svc = WorkflowService(max_workers=4)

    # 1) ETL pipeline: two extracts run in parallel, then transform,
    #    then a flaky load that succeeds on the third attempt.
    etl = Workflow("daily-etl")
    etl.add_task(Task("extract_a", work("extract_a", 1.0, 600)))
    etl.add_task(Task("extract_b", work("extract_b", 1.0, 400)))
    etl.add_task(Task("transform",
                      lambda extract_a, extract_b: (time.sleep(0.5), extract_a + extract_b)[1],
                      depends_on=["extract_a", "extract_b"]))
    etl.add_task(Task("load", flaky_load, depends_on=["transform"], retries=3, retry_delay=0.2))
    etl.add_task(Task("notify", work("notify", 0.1), depends_on=["load"]))

    # 2) Report workflow with a failing task: its dependant is skipped,
    #    while the independent branch still runs.
    rpt = Workflow("weekly-report")
    rpt.add_task(Task("fetch", work("fetch", 0.5)))
    rpt.add_task(Task("validate", bad_validate, depends_on=["fetch"]))
    rpt.add_task(Task("publish", work("publish", 0.2), depends_on=["validate"]))
    rpt.add_task(Task("archive", work("archive", 0.3), depends_on=["fetch"]))

    # 3) Cycle detection.
    try:
        bad = Workflow("cyclic")
        bad.add_task(Task("a", work("a", 0), depends_on=["b"]))
        bad.add_task(Task("b", work("b", 0), depends_on=["a"]))
        svc.submit(bad)
    except ValueError as e:
        log.info("Rejected workflow: %s", e)

    start = time.time()
    ids = [svc.submit(etl), svc.submit(rpt)]   # both workflows run concurrently
    for wf_id in ids:
        svc.wait(wf_id)

    for wf_id in ids:
        print_report(svc, wf_id)
    print(f"\nAll workflows: {svc.list_workflows()}")
    print(f"Total wall time: {time.time() - start:.2f}s")
    svc.shutdown()
