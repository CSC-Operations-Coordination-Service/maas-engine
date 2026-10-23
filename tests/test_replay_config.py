"""The configuration file, the handler loader and the health check"""

import json
import sys
import types

import kombu
import pytest

from maas_engine.replay import ReplayNotPossible
from maas_engine.replay.config import load_config, parse_config
from maas_engine.replay.handler import load_handler
from maas_engine.replay.health import build_app

CONFIG = {
    "_comment": "comments are fine at the top level",
    "exchange": {"name": "ops-exchange"},
    "operation_index": "ops",
    "handler": {"class": "fake_handler_module:Handler", "kwargs": {"service": "odata"}},
    "lanes": [
        {
            "name": "short",
            "queue": "q-short",
            "routing_key": "k.short",
            "timeout": {"base": 100},
        },
        {
            "name": "long",
            "queue": "q-long",
            "routing_key": "k.long",
            "timeout": {"base": 100, "per_day": 50},
        },
    ],
}


class Handler:
    def __init__(self, service):
        self.service = service

    def prepare(self, request, lane, workdir):
        raise ReplayNotPossible("no")

    def command(self, prepared, request, lane):
        return ["true"], {}

    def cleanup(self, prepared):
        pass


@pytest.fixture(autouse=True)
def fake_module(monkeypatch):
    module = types.ModuleType("fake_handler_module")
    module.Handler = Handler
    module.NotAHandler = object
    monkeypatch.setitem(sys.modules, "fake_handler_module", module)


def test_a_configuration_is_parsed():
    config = parse_config(CONFIG)
    assert config.exchange.name == "ops-exchange"
    assert config.exchange.type == "topic" and config.exchange.durable
    assert config.operation_index == "ops"
    assert [lane.name for lane in config.lanes] == ["short", "long"]
    assert config.lanes[1].timeout.per_day == 50
    assert config.lanes[0].timeout.per_day == 0
    assert config.poll_interval == 5 and config.progress_interval == 30
    assert config.create_missing_records is False


def test_the_handler_is_built_with_its_arguments():
    handler = parse_config(CONFIG).build_handler()
    assert isinstance(handler, Handler) and handler.service == "odata"


def test_the_worker_is_built_from_the_configuration():
    worker = parse_config(CONFIG).build_worker(
        client=object(), connection_factory=lambda: kombu.Connection("memory://")
    )
    assert list(worker.lanes) == ["short", "long"]
    assert worker.recorder.index == "ops"
    assert isinstance(worker.handler, Handler)


def test_a_file_is_loaded(tmp_path):
    path = tmp_path / "replay.json"
    path.write_text(json.dumps(CONFIG))
    assert load_config(str(path)).operation_index == "ops"
    path.write_text("{nope")
    with pytest.raises(ValueError, match="not JSON"):
        load_config(str(path))


@pytest.mark.parametrize(
    "change, message",
    [
        ({"surprise": 1}, "unknown keys"),
        ({"exchange": {}}, "exchange needs a name"),
        ({"exchange": "x"}, "exchange must be a dict"),
        ({"operation_index": ""}, "operation_index"),
        ({"handler": {}}, "handler needs a class"),
        ({"handler": {"class": "a:B", "kwargs": 3}}, "kwargs"),
        ({"lanes": []}, "at least one lane"),
        ({"lanes": "x"}, "lanes must be a list"),
        ({"lanes": [{"name": "a", "queue": "q"}]}, "routing_key"),
        (
            {"lanes": [{"name": "a", "queue": "q", "routing_key": "k", "x": 1}]},
            "unknown lane keys",
        ),
        (
            {
                "lanes": [
                    {
                        "name": "a",
                        "queue": "q",
                        "routing_key": "k",
                        "timeout": {"max": 1},
                    }
                ]
            },
            "base and per_day",
        ),
        (
            {
                "lanes": [
                    {
                        "name": "a",
                        "queue": "q",
                        "routing_key": "k",
                        "timeout": {"base": -1},
                    }
                ]
            },
            "base must be a number",
        ),
        ({"poll_interval": "5"}, "poll_interval"),
    ],
)
def test_a_wrong_configuration_says_what_is_wrong(change, message):
    with pytest.raises(ValueError, match=message):
        parse_config({**CONFIG, **change})


def test_two_lanes_cannot_share_a_name():
    lane = CONFIG["lanes"][0]
    with pytest.raises(ValueError, match="unique"):
        parse_config({**CONFIG, "lanes": [lane, lane]})


def test_a_configuration_must_be_an_object():
    with pytest.raises(ValueError, match="JSON object"):
        parse_config([])


@pytest.mark.parametrize(
    "spec, message",
    [
        ("nocolon", "package.module:Class"),
        ("fake_handler_module:Missing", "has no Missing"),
        ("no_such_module_here:X", "cannot import"),
        ("fake_handler_module:NotAHandler", "no prepare"),
    ],
)
def test_a_bad_handler_spec_is_refused(spec, message):
    with pytest.raises(ValueError, match=message):
        load_handler(spec)


def test_the_health_check_follows_the_worker():
    class FakeWorker:
        alive = True

        def is_alive(self):
            return self.alive

    worker = FakeWorker()
    client = build_app(worker).test_client()
    assert client.get("/").status_code == 200
    worker.alive = False
    assert client.get("/").status_code == 500
