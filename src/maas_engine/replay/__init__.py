"""Replay worker: run a child process over a window, for the operator that asked

A generic worker for the "recollect" pattern: poll RabbitMQ for a replay request,
acknowledge it, run a command as a child process under a deadline, and keep the
outcome in an operation record in OpenSearch. What is specific to a collector (where
its configuration comes from, which command replays an interface) is a
:class:`~maas_engine.replay.handler.ReplayHandler` the caller provides.

See docs/replay.md.
"""

from maas_engine.replay.handler import ReplayHandler, ReplayNotPossible
from maas_engine.replay.records import DONE, FAILED, RUNNING, OperationRecorder
from maas_engine.replay.request import ReplayRequest
from maas_engine.replay.runner import ChildRunner
from maas_engine.replay.worker import Lane, LaneTimeout, ReplayWorker

__all__ = [
    "DONE",
    "FAILED",
    "RUNNING",
    "ChildRunner",
    "Lane",
    "LaneTimeout",
    "OperationRecorder",
    "ReplayHandler",
    "ReplayNotPossible",
    "ReplayRequest",
    "ReplayWorker",
]
