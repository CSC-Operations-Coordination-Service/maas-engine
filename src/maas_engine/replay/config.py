"""The replay worker's configuration file and the worker built from it

    {
      "exchange": {"name": "maas-operator-exchange", "type": "topic"},
      "operation_index": "maas-operation",
      "create_missing_records": false,
      "workdir": "/tmp",
      "poll_interval": 5,
      "progress_interval": 30,
      "handler": {"class": "package.module:Class", "kwargs": {"service": "odata"}},
      "lanes": [
        {"name": "short", "queue": "maas-operator-odata-replay-short",
         "routing_key": "maas.operator.odata.replay.short",
         "timeout": {"base": 21600}},
        {"name": "long", "queue": "maas-operator-odata-replay-long",
         "routing_key": "maas.operator.odata.replay.long",
         "timeout": {"base": 21600, "per_day": 10800}}
      ]
    }

`exchange`, `operation_index`, `handler` and `lanes` are required. A key a comment
could sit in (`_comment...`) is ignored at the top level, nowhere else.
"""

import json
import tempfile
from dataclasses import dataclass

import kombu

from maas_engine.replay.handler import ReplayHandler, load_handler
from maas_engine.replay.records import OperationRecorder
from maas_engine.replay.worker import Lane, LaneTimeout, ReplayWorker

TOP_LEVEL = {
    "exchange",
    "operation_index",
    "create_missing_records",
    "workdir",
    "poll_interval",
    "progress_interval",
    "handler",
    "lanes",
}


@dataclass(frozen=True)
class ReplayConfig:
    """A parsed configuration file"""

    exchange: kombu.Exchange
    operation_index: str
    handler_spec: str
    handler_kwargs: dict
    lanes: list[Lane]
    create_missing_records: bool = False
    workdir: str = tempfile.gettempdir()
    poll_interval: float = 5
    progress_interval: float = 30

    def build_handler(self) -> ReplayHandler:
        """the handler, instantiated"""
        return load_handler(self.handler_spec, self.handler_kwargs)

    def build_worker(self, client, connection_factory, handler=None) -> ReplayWorker:
        """the worker over an OpenSearch client and a kombu connection factory"""
        return ReplayWorker(
            connection_factory=connection_factory,
            recorder=OperationRecorder(
                client, self.operation_index, self.create_missing_records
            ),
            handler=handler or self.build_handler(),
            lanes=self.lanes,
            exchange=self.exchange,
            workdir=self.workdir,
            poll_interval=self.poll_interval,
            progress_interval=self.progress_interval,
        )


def _section(data: dict, name: str, kind: type):
    value = data.get(name)
    if not isinstance(value, kind):
        raise ValueError(f"replay configuration: {name} must be a {kind.__name__}")
    return value


def _number(data: dict, name: str, default: float) -> float:
    value = data.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"replay configuration: {name} must be a number, 0 or more")
    return value


def _lane(entry) -> Lane:
    if not isinstance(entry, dict):
        raise ValueError("replay configuration: a lane must be an object")
    unknown = set(entry) - {"name", "queue", "routing_key", "timeout"}
    if unknown:
        raise ValueError(f"replay configuration: unknown lane keys {sorted(unknown)}")
    for name in ("name", "queue", "routing_key"):
        if not isinstance(entry.get(name), str) or not entry[name]:
            raise ValueError(f"replay configuration: a lane needs a {name}")
    timeout = entry.get("timeout") or {}
    if not isinstance(timeout, dict) or set(timeout) - {"base", "per_day"}:
        raise ValueError(
            "replay configuration: a lane timeout holds base and per_day only"
        )
    return Lane(
        name=entry["name"],
        queue=entry["queue"],
        routing_key=entry["routing_key"],
        timeout=LaneTimeout(
            base=_number(timeout, "base", LaneTimeout.base),
            per_day=_number(timeout, "per_day", 0),
        ),
    )


def parse_config(data: dict) -> ReplayConfig:
    """Validate a configuration

    Raises:
        ValueError: with what is wrong, in the file's own words
    """
    if not isinstance(data, dict):
        raise ValueError("replay configuration must be a JSON object")
    unknown = {k for k in data if not k.startswith("_")} - TOP_LEVEL
    if unknown:
        raise ValueError(f"replay configuration: unknown keys {sorted(unknown)}")

    exchange = _section(data, "exchange", dict)
    if not isinstance(exchange.get("name"), str) or not exchange["name"]:
        raise ValueError("replay configuration: the exchange needs a name")
    handler = _section(data, "handler", dict)
    if not isinstance(handler.get("class"), str):
        raise ValueError("replay configuration: the handler needs a class")
    kwargs = handler.get("kwargs", {})
    if not isinstance(kwargs, dict):
        raise ValueError("replay configuration: handler kwargs must be an object")
    lanes = [_lane(entry) for entry in _section(data, "lanes", list)]
    if not lanes:
        raise ValueError("replay configuration: at least one lane")
    if len({lane.name for lane in lanes}) != len(lanes):
        raise ValueError("replay configuration: lane names must be unique")
    index = data.get("operation_index")
    if not isinstance(index, str) or not index:
        raise ValueError("replay configuration: operation_index is required")

    return ReplayConfig(
        exchange=kombu.Exchange(
            exchange["name"], type=exchange.get("type", "topic"), durable=True
        ),
        operation_index=index,
        handler_spec=handler["class"],
        handler_kwargs=kwargs,
        lanes=lanes,
        create_missing_records=bool(data.get("create_missing_records", False)),
        workdir=data.get("workdir") or tempfile.gettempdir(),
        poll_interval=_number(data, "poll_interval", 5),
        progress_interval=_number(data, "progress_interval", 30),
    )


def load_config(path: str) -> ReplayConfig:
    """Read and validate a configuration file"""
    with open(path, encoding="utf-8") as stream:
        try:
            data = json.load(stream)
        except json.JSONDecodeError as error:
            raise ValueError(f"{path} is not JSON: {error}") from error
    return parse_config(data)
