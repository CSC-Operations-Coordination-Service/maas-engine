"""The replay worker: take replay requests from RabbitMQ and run them

Each lane polls its queue with basic_get, acknowledges the request on receipt,
closes its connection and runs the replay: no connection is held while a replay
runs, so none misses its heartbeats (a push consumer would be dropped by the broker
minutes into a replay of hours, and RabbitMQ closes a channel whose delivery stays
unacknowledged for consumer_timeout, 30 minutes by default). A replay is therefore
never redelivered: a request lost to a crash keeps its record RUNNING, with an
updated_at that stopped moving.
"""

import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import kombu

from maas_engine.replay.handler import ReplayHandler, ReplayNotPossible
from maas_engine.replay.records import DONE, FAILED, RUNNING, OperationRecorder
from maas_engine.replay.request import ReplayRequest, operation_id_of
from maas_engine.replay.runner import ChildRunner

LOGGER = logging.getLogger("maas_engine.replay")

SECONDS_PER_DAY = 86400


@dataclass(frozen=True)
class LaneTimeout:
    """Seconds a replay may run: `base`, or `per_day` per day of its window if more"""

    base: float = 6 * 3600
    per_day: float = 0

    def seconds(self, request: ReplayRequest) -> float:
        """the timeout of this request"""
        days = request.window.total_seconds() / SECONDS_PER_DAY
        return max(self.base, days * self.per_day)


@dataclass(frozen=True)
class Lane:
    """One queue of replay requests, served one request at a time"""

    name: str
    queue: str
    routing_key: str
    timeout: LaneTimeout = LaneTimeout()


class ReplayWorker:
    """One thread per lane"""

    # pylint: disable-next=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        connection_factory: Callable[[], kombu.Connection],
        recorder: OperationRecorder,
        handler: ReplayHandler,
        lanes: list[Lane],
        exchange: kombu.Exchange,
        workdir: str,
        poll_interval: float = 5,
        progress_interval: float = 30,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not lanes:
            raise ValueError("a replay worker needs at least one lane")
        self.connection_factory = connection_factory
        self.recorder = recorder
        self.handler = handler
        self.lanes = {lane.name: lane for lane in lanes}
        if len(self.lanes) != len(lanes):
            raise ValueError("lane names must be unique")
        self.exchange = exchange
        self.workdir = workdir
        self.poll_interval = poll_interval
        self.progress_interval = progress_interval
        self.sleep = sleep
        self.runners = {lane.name: ChildRunner() for lane in lanes}
        self.threads: list[threading.Thread] = []
        self.should_stop = False

    def queue_of(self, lane: Lane) -> kombu.Queue:
        """the kombu queue of a lane"""
        return kombu.Queue(
            lane.queue,
            exchange=self.exchange,
            routing_key=lane.routing_key,
            durable=True,
        )

    def run(self) -> None:
        """Serve the lanes until stop()"""
        self.threads = [
            threading.Thread(
                target=self.run_lane,
                args=(lane,),
                name=f"replay-{lane.name}",
                daemon=True,
            )
            for lane in self.lanes.values()
        ]
        LOGGER.info("Replay worker: lanes %s", ", ".join(self.lanes))
        for thread in self.threads:
            thread.start()
        for thread in self.threads:
            # a join with a timeout lets the main thread run the signal handlers
            while thread.is_alive():
                thread.join(1)

    def is_alive(self) -> bool:
        """whether every lane thread runs (true before run() too)"""
        return all(thread.is_alive() for thread in self.threads)

    def stop(self, *_args) -> None:
        """Stop taking requests, terminate the running children and start no other"""
        self.should_stop = True
        for runner in self.runners.values():
            runner.terminate()

    def run_lane(self, lane: Lane) -> None:
        """Take the requests of a lane, one at a time"""
        queue = self.queue_of(lane)
        while not self.should_stop:
            try:
                body = self.next_body(queue)
            # broker restarting, network blip: try again after a pause
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.warning("Cannot fetch a %s replay request: %s", lane.name, error)
                self.sleep(self.poll_interval)
                continue
            if body is None:
                continue
            try:
                self.on_request(lane, body)
            # a lane that dies takes no more request: keep it alive
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Replay request of lane %s failed", lane.name)

    def next_body(self, queue: kombu.Queue):
        """Wait for a message, acknowledge it and close the connection

        Returns:
            the raw message body, None when stopped while waiting
        """
        with self.connection_factory() as connection:
            bound = queue(connection.default_channel)
            bound.declare()
            while not self.should_stop:
                message = bound.get(no_ack=False)
                if message is not None:
                    message.ack()
                    return message.body
                # also sends the heartbeats and notices a broker gone away
                connection.heartbeat_check()
                self.sleep(self.poll_interval)
        return None

    def on_request(self, lane: Lane, body) -> None:
        """Parse an acknowledged request and run it"""
        try:
            request = ReplayRequest.from_body(body)
        except ValueError as error:
            LOGGER.error(
                "Skipping a malformed replay request of lane %s: %s (%r)",
                lane.name,
                error,
                body,
            )
            operation_id = operation_id_of(body)
            if operation_id:
                self.recorder.update(
                    operation_id, FAILED, details=f"malformed replay request: {error}"
                )
            return

        if self.should_stop:
            # taken between the stop and the end of the poll: already acknowledged
            self.recorder.update(
                request.operation_id,
                FAILED,
                details="the replay worker stopped before the replay started",
            )
            return

        LOGGER.info(
            "Replaying %s from %s to %s in lane %s for %s (operation %s)",
            request.interface_name,
            request.start_date,
            request.end_date,
            lane.name,
            request.requested_by or "?",
            request.operation_id,
        )
        self.handle(lane, request)

    def _failure_details(self, code: int, tail: str) -> str:
        describe = getattr(self.handler, "describe_failure", None)
        if callable(describe):
            return describe(code, tail)
        return f"exit code {code}\n{tail}"

    def handle(self, lane: Lane, request: ReplayRequest) -> None:
        """Replay the request and record its outcome; never raises"""
        prepared = None
        made = False
        try:
            self.recorder.update(request.operation_id, RUNNING)
            os.makedirs(self.workdir, exist_ok=True)
            prepared = self.handler.prepare(request, lane.name, self.workdir)
            made = True
            command, env = self.handler.command(prepared, request, lane.name)
            code, tail = self.runners[lane.name].run(
                command,
                env,
                # the queue the request came from decides, not its label
                lane.timeout.seconds(request),
                lambda lines: self.recorder.update(
                    request.operation_id, RUNNING, details=lines
                ),
                self.progress_interval,
            )
            if code == 0:
                self.recorder.update(
                    request.operation_id, DONE, exit_code=0, details=tail
                )
            else:
                self.recorder.update(
                    request.operation_id,
                    FAILED,
                    exit_code=code,
                    details=self._failure_details(code, tail),
                )
        except ReplayNotPossible as error:
            self.recorder.update(request.operation_id, FAILED, details=str(error))
        except Exception as error:  # pylint: disable=broad-except
            LOGGER.exception("Replay %s failed", request.operation_id)
            self.recorder.update(
                request.operation_id, FAILED, details=f"{type(error).__name__}: {error}"
            )
        finally:
            if made:
                try:
                    self.handler.cleanup(prepared)
                except Exception:  # pylint: disable=broad-except
                    LOGGER.exception("Cleanup of %s failed", request.operation_id)


__all__ = ["Lane", "LaneTimeout", "ReplayWorker"]
