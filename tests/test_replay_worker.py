"""The replay worker, on kombu's in-memory transport and real child processes"""

import json
import os
import sys
import threading
import time
import uuid

import kombu
import pytest

from maas_engine.replay import (
    DONE,
    FAILED,
    RUNNING,
    Lane,
    LaneTimeout,
    ReplayNotPossible,
    ReplayWorker,
)
from maas_engine.replay.request import ReplayRequest

BODY = {
    "operation_id": "op-1",
    "interface_name": "IF",
    "start_date": "2026-10-01T00:00:00Z",
    "end_date": "2026-10-02T00:00:00Z",
    "requested_by": "bob",
}


class ListRecorder:
    """what the worker recorded, in order"""

    def __init__(self):
        self.calls = []

    def update(self, operation_id, status, exit_code=None, details=None):
        self.calls.append((operation_id, status, exit_code, details))
        return True

    def statuses(self, operation_id="op-1"):
        return [c[1] for c in self.calls if c[0] == operation_id]

    def last(self, operation_id="op-1"):
        return [c for c in self.calls if c[0] == operation_id][-1]


class FakeHandler:
    """runs `code` in a child; records what the worker asked of it"""

    def __init__(self, code="print('hi')", prepare_error=None, env=None):
        self.code = code
        self.prepare_error = prepare_error
        self.env = env if env is not None else dict(os.environ)
        self.prepared = []
        self.commands = []
        self.cleaned = []
        self.started = threading.Event()

    def prepare(self, request, lane, workdir):
        if self.prepare_error:
            raise self.prepare_error
        token = {"request": request, "lane": lane, "workdir": workdir}
        self.prepared.append(token)
        return token

    def command(self, prepared, request, lane):
        self.commands.append((prepared, request, lane))
        self.started.set()
        return [sys.executable, "-u", "-c", self.code], self.env

    def cleanup(self, prepared):
        self.cleaned.append(prepared)


class DescribingHandler(FakeHandler):
    def describe_failure(self, code, tail):
        return f"custom {code}: {tail}"


@pytest.fixture(name="queue_names")
def fixture_queue_names():
    """names no other test shares: the in-memory broker is process-wide"""
    suffix = uuid.uuid4().hex[:8]
    return f"replay-{suffix}", f"replay.{suffix}"


def make_worker(queue_names, handler=None, timeout=None, **kwargs):
    queue, key = queue_names
    recorder = ListRecorder()
    lane = Lane("short", queue, key, timeout or LaneTimeout(base=30))
    worker = ReplayWorker(
        connection_factory=lambda: kombu.Connection("memory://"),
        recorder=recorder,
        handler=handler or FakeHandler(),
        lanes=[lane],
        exchange=kombu.Exchange("replay-test", type="topic", durable=True),
        workdir=kwargs.pop("workdir", "/tmp"),
        poll_interval=0.02,
        **kwargs,
    )
    return worker, recorder, lane


def publish(worker, lane, body):
    with kombu.Connection("memory://") as connection:
        producer = connection.Producer()
        queue = worker.queue_of(lane)
        queue(connection.default_channel).declare()
        producer.publish(
            body,
            exchange=worker.exchange,
            routing_key=lane.routing_key,
            serializer="json" if isinstance(body, dict) else None,
            declare=[queue],
        )


def pending(worker, lane) -> int:
    with kombu.Connection("memory://") as connection:
        return queue_depth(worker.queue_of(lane)(connection.default_channel))


def queue_depth(bound) -> int:
    return bound.queue_declare(passive=True).message_count


def serve(worker, lane):
    thread = threading.Thread(target=worker.run_lane, args=(lane,), daemon=True)
    thread.start()
    return thread


def wait_for(condition, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def finish(worker, thread):
    worker.stop()
    thread.join(15)
    assert not thread.is_alive()


def test_a_request_is_replayed_and_recorded(queue_names):
    handler = FakeHandler("print('hi')")
    worker, recorder, lane = make_worker(queue_names, handler, workdir="/tmp/replay-w")
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: DONE in recorder.statuses())
    finish(worker, thread)

    assert recorder.statuses()[0] == RUNNING
    assert recorder.last() == ("op-1", DONE, 0, "hi")
    [prepared] = handler.prepared
    assert prepared["lane"] == "short"
    assert prepared["workdir"] == "/tmp/replay-w"
    assert isinstance(prepared["request"], ReplayRequest)
    assert handler.cleaned == [prepared]


def test_the_request_is_acknowledged_before_the_replay_runs(queue_names):
    handler = FakeHandler("import time; time.sleep(60)")
    worker, recorder, lane = make_worker(queue_names, handler)
    publish(worker, lane, BODY)
    assert pending(worker, lane) == 1
    thread = serve(worker, lane)
    assert handler.started.wait(15)
    # running, and nothing is left on the queue for a redelivery
    assert pending(worker, lane) == 0
    assert recorder.statuses() == [RUNNING]
    finish(worker, thread)


def test_a_failing_child_is_recorded_with_its_exit_code(queue_names):
    worker, recorder, lane = make_worker(
        queue_names, FakeHandler("print('boom'); raise SystemExit(3)")
    )
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: FAILED in recorder.statuses())
    finish(worker, thread)
    assert recorder.last() == ("op-1", FAILED, 3, "exit code 3\nboom")


def test_a_handler_can_describe_a_failure(queue_names):
    worker, recorder, lane = make_worker(
        queue_names, DescribingHandler("print('x'); raise SystemExit(1)")
    )
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: FAILED in recorder.statuses())
    finish(worker, thread)
    assert recorder.last() == ("op-1", FAILED, 1, "custom 1: x")


def test_a_request_that_cannot_be_replayed_fails_with_the_reason(queue_names):
    handler = FakeHandler(prepare_error=ReplayNotPossible("IF is disabled"))
    worker, recorder, lane = make_worker(queue_names, handler)
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: FAILED in recorder.statuses())
    finish(worker, thread)
    assert recorder.last() == ("op-1", FAILED, None, "IF is disabled")
    assert handler.commands == [] and handler.cleaned == []


def test_an_unexpected_error_fails_the_record_and_keeps_the_lane_alive(queue_names):
    handler = FakeHandler(prepare_error=RuntimeError("boom"))
    worker, recorder, lane = make_worker(queue_names, handler)
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: FAILED in recorder.statuses())
    assert recorder.last()[3] == "RuntimeError: boom"

    handler.prepare_error = None
    publish(worker, lane, {**BODY, "operation_id": "op-2"})
    assert wait_for(lambda: DONE in recorder.statuses("op-2"))
    finish(worker, thread)


def test_a_malformed_request_with_an_id_fails_its_record(queue_names):
    worker, recorder, lane = make_worker(queue_names)
    publish(worker, lane, {"operation_id": "op-bad", "interface_name": "IF"})
    thread = serve(worker, lane)
    assert wait_for(lambda: recorder.calls)
    finish(worker, thread)
    [call] = recorder.calls
    assert call[:2] == ("op-bad", FAILED)
    assert "malformed replay request" in call[3]


def test_a_malformed_request_without_an_id_is_dropped(queue_names):
    worker, recorder, lane = make_worker(queue_names)
    publish(worker, lane, {"interface_name": "IF"})
    publish(worker, lane, {**BODY, "operation_id": "op-ok"})
    thread = serve(worker, lane)
    assert wait_for(lambda: DONE in recorder.statuses("op-ok"))
    finish(worker, thread)
    assert [c[0] for c in recorder.calls if c[0] != "op-ok"] == []


def test_a_replay_past_its_timeout_is_killed_and_recorded(queue_names):
    worker, recorder, lane = make_worker(
        queue_names,
        FakeHandler("import time; time.sleep(60)"),
        timeout=LaneTimeout(base=0.5),
    )
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: FAILED in recorder.statuses())
    finish(worker, thread)
    _, status, code, details = recorder.last()
    assert (status, code) == (FAILED, -9)
    assert "killed after 0.5s" in details


def test_the_progress_is_recorded_while_it_runs(queue_names):
    worker, recorder, lane = make_worker(
        queue_names,
        FakeHandler("import time; print('step', flush=True); time.sleep(1.5)"),
        progress_interval=0.3,
    )
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: DONE in recorder.statuses())
    finish(worker, thread)
    running = [c for c in recorder.calls if c[1] == RUNNING and c[3]]
    assert running and running[0][3] == "step"


def test_stop_terminates_the_running_child(queue_names):
    handler = FakeHandler("import time; print('up', flush=True); time.sleep(60)")
    worker, recorder, lane = make_worker(queue_names, handler)
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert handler.started.wait(15)
    time.sleep(0.3)
    started = time.monotonic()
    finish(worker, thread)
    assert time.monotonic() - started < 10
    assert recorder.last()[:3] == ("op-1", FAILED, -15)
    assert handler.cleaned


def test_a_request_taken_after_the_stop_is_failed_not_run(queue_names):
    handler = FakeHandler()
    worker, recorder, lane = make_worker(queue_names, handler)
    worker.stop()
    worker.on_request(lane, json.dumps(BODY))
    assert recorder.calls == [
        ("op-1", FAILED, None, "the replay worker stopped before the replay started")
    ]
    assert handler.prepared == []


def test_the_environment_of_the_child_is_the_handlers(queue_names):
    handler = FakeHandler(
        "import os; print(os.environ.get('REPLAY_MARK'))",
        env={**os.environ, "REPLAY_MARK": "from-handler"},
    )
    worker, recorder, lane = make_worker(queue_names, handler)
    publish(worker, lane, BODY)
    thread = serve(worker, lane)
    assert wait_for(lambda: DONE in recorder.statuses())
    finish(worker, thread)
    assert recorder.last()[3] == "from-handler"


def test_an_unreachable_broker_is_retried_not_fatal():
    attempts = []

    def broken():
        attempts.append(1)
        raise ConnectionError("broker down")

    recorder = ListRecorder()
    lane = Lane("short", "q", "k")
    worker = ReplayWorker(
        broken,
        recorder,
        FakeHandler(),
        [lane],
        kombu.Exchange("replay-test"),
        "/tmp",
        poll_interval=0.02,
    )
    thread = serve(worker, lane)
    assert wait_for(lambda: len(attempts) >= 3)
    finish(worker, thread)
    assert recorder.calls == []


def test_the_lane_timeout_is_fixed_or_scaled_by_the_window():
    short = LaneTimeout(base=3600)
    long = LaneTimeout(base=3600, per_day=7200)
    one_day = ReplayRequest.from_body(BODY)
    ten_days = ReplayRequest.from_body({**BODY, "end_date": "2026-10-11T00:00:00Z"})
    assert short.seconds(ten_days) == 3600
    assert long.seconds(one_day) == 7200
    assert long.seconds(ten_days) == 72000
    assert LaneTimeout(base=3600, per_day=60).seconds(one_day) == 3600


def test_a_worker_needs_distinct_lanes():
    kwargs = dict(
        connection_factory=lambda: None,
        recorder=ListRecorder(),
        handler=FakeHandler(),
        exchange=kombu.Exchange("x"),
        workdir="/tmp",
    )
    with pytest.raises(ValueError, match="at least one"):
        ReplayWorker(lanes=[], **kwargs)
    lane = Lane("a", "q", "k")
    with pytest.raises(ValueError, match="unique"):
        ReplayWorker(lanes=[lane, lane], **kwargs)


def test_is_alive_follows_the_lane_threads(queue_names):
    worker, _, lane = make_worker(queue_names)
    assert worker.is_alive()  # nothing started yet
    runner = threading.Thread(target=worker.run, daemon=True)
    runner.start()
    assert wait_for(lambda: bool(worker.threads) and worker.is_alive())
    worker.stop()
    runner.join(15)
    assert not runner.is_alive()
    assert not worker.is_alive()
