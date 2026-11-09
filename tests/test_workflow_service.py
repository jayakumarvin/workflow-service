import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workflow_service import (  # noqa: E402
    Task, TaskStatus, Workflow, WorkflowService, WorkflowStatus,
)


@pytest.fixture
def svc():
    s = WorkflowService(max_workers=4)
    yield s
    s.shutdown()


def sleeper(secs, value="ok"):
    def fn(**_):
        time.sleep(secs)
        return value
    return fn


def states(svc, wf_id):
    return {t["name"]: t["status"] for t in svc.status(wf_id)["tasks"]}


def test_cycle_is_rejected(svc):
    wf = Workflow("cyclic")
    wf.add_task(Task("a", sleeper(0), depends_on=["b"]))
    wf.add_task(Task("b", sleeper(0), depends_on=["a"]))
    with pytest.raises(ValueError, match="Cycle detected"):
        svc.submit(wf)


def test_unknown_dependency_is_rejected(svc):
    wf = Workflow("bad").add_task(Task("a", sleeper(0), depends_on=["ghost"]))
    with pytest.raises(ValueError, match="unknown task"):
        svc.submit(wf)


def test_duplicate_task_name_is_rejected():
    wf = Workflow("dup").add_task(Task("a", sleeper(0)))
    with pytest.raises(ValueError, match="Duplicate"):
        wf.add_task(Task("a", sleeper(0)))


def test_unknown_workflow_id(svc):
    with pytest.raises(KeyError):
        svc.status("nope")


def test_upstream_results_are_passed_as_kwargs(svc):
    wf = Workflow("pass")
    wf.add_task(Task("a", lambda: 2))
    wf.add_task(Task("b", lambda: 3))
    wf.add_task(Task("sum", lambda a, b: a + b, depends_on=["a", "b"]))
    wid = svc.submit(wf)
    assert svc.wait(wid, 5) == WorkflowStatus.SUCCESS
    assert wf.tasks["sum"].result == 5


def test_independent_tasks_run_concurrently(svc):
    wf = Workflow("parallel")
    for n in "abc":
        wf.add_task(Task(n, sleeper(0.5)))
    start = time.time()
    wid = svc.submit(wf)
    assert svc.wait(wid, 5) == WorkflowStatus.SUCCESS
    assert time.time() - start < 1.2  # sequential would be 1.5s


def test_retry_with_backoff_then_success(svc):
    calls = {"n": 0}

    def flaky(**_):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("down")
        return "up"

    wf = Workflow("retry").add_task(Task("t", flaky, retries=3, retry_delay=0.01))
    wid = svc.submit(wf)
    assert svc.wait(wid, 5) == WorkflowStatus.SUCCESS
    assert wf.tasks["t"].attempts == 3


def test_retries_exhausted_marks_failed(svc):
    def boom(**_):
        raise ValueError("always")

    wf = Workflow("exhaust").add_task(Task("t", boom, retries=2, retry_delay=0.01))
    wid = svc.submit(wf)
    assert svc.wait(wid, 5) == WorkflowStatus.FAILED
    assert wf.tasks["t"].attempts == 3
    assert wf.tasks["t"].error == "ValueError: always"


def test_failure_skips_dependants_but_not_independent_branch(svc):
    def boom(**_):
        raise RuntimeError("x")

    wf = Workflow("branches")
    wf.add_task(Task("root", sleeper(0)))
    wf.add_task(Task("bad", boom, depends_on=["root"]))
    wf.add_task(Task("after_bad", sleeper(0), depends_on=["bad"]))
    wf.add_task(Task("after_after", sleeper(0), depends_on=["after_bad"]))
    wf.add_task(Task("independent", sleeper(0), depends_on=["root"]))
    wid = svc.submit(wf)
    assert svc.wait(wid, 5) == WorkflowStatus.FAILED
    assert states(svc, wid) == {
        "root": "SUCCESS", "bad": "FAILED", "after_bad": "SKIPPED",
        "after_after": "SKIPPED", "independent": "SUCCESS",
    }


def test_cancel_lets_running_finish_and_cancels_the_rest(svc):
    wf = Workflow("cancel")
    wf.add_task(Task("first", sleeper(0.5)))
    wf.add_task(Task("second", sleeper(0), depends_on=["first"]))
    wid = svc.submit(wf)
    time.sleep(0.15)
    svc.cancel(wid)
    assert svc.wait(wid, 5) == WorkflowStatus.CANCELLED
    assert states(svc, wid) == {"first": "SUCCESS", "second": "CANCELLED"}


def test_list_workflows_and_timings(svc):
    wf = Workflow("listed").add_task(Task("t", sleeper(0.05)))
    wid = svc.submit(wf)
    svc.wait(wid, 5)
    assert {"id": wid, "name": "listed", "status": "SUCCESS"} in svc.list_workflows()
    assert wf.tasks["t"].duration is not None and wf.tasks["t"].status == TaskStatus.SUCCESS
