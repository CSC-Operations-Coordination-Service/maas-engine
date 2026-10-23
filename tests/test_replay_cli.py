"""The command line of the replay worker"""

import json
import signal

import pytest

from maas_engine.replay import cli
from maas_engine.replay.config import parse_config

CONFIG = {
    "exchange": {"name": "ops-exchange"},
    "operation_index": "ops",
    "handler": {"class": "builtins:object"},
    "lanes": [{"name": "short", "queue": "q", "routing_key": "k"}],
}


class StubWorker:
    def __init__(self):
        self.ran = False

    def run(self):
        self.ran = True

    def stop(self, *_):
        pass

    def is_alive(self):
        return True


@pytest.fixture(name="wiring")
def fixture_wiring(monkeypatch):
    seen = {"signals": [], "connections": [], "built": []}
    worker = StubWorker()

    class StubConfig:
        def build_handler(self):
            seen["built"].append("handler")
            return "handler"

        def build_worker(self, client, connection_factory, handler):
            seen["built"].append(("worker", client, handler))
            seen["factory"] = connection_factory
            return worker

    monkeypatch.setattr(cli, "load_config", lambda path: StubConfig())
    monkeypatch.setattr(
        cli.db_connections,
        "create_connection",
        lambda **kwargs: seen["connections"].append(kwargs),
    )
    monkeypatch.setattr(cli.db_connections, "get_connection", lambda: "client")
    monkeypatch.setattr(
        cli.signal, "signal", lambda number, handler: seen["signals"].append(number)
    )
    seen["worker"] = worker
    return seen


ARGS = [
    "--replay-config",
    "replay.json",
    "--es-url",
    "http://es:9200",
    "--amqp-url",
    "amqp://broker:5672",
    "--healthcheck-port",
    "0",
]


def test_the_worker_runs_with_the_engine_connections(wiring):
    cli.main(ARGS)
    assert wiring["worker"].ran
    assert wiring["connections"][0]["hosts"] == ["http://es:9200"]
    assert wiring["signals"] == [signal.SIGINT, signal.SIGTERM]
    assert wiring["built"] == ["handler", ("worker", "client", "handler")]
    # a connection per poll, with the heartbeat the engine's AMQP options give
    connection = wiring["factory"]()
    assert connection.as_uri().startswith("amqp://")


def test_the_configuration_comes_from_the_environment_too(monkeypatch, wiring):
    monkeypatch.setenv("REPLAY_CONFIG", "from-env.json")
    namespace = cli.replay_parser().parse_args(
        ["--es-url", "http://es:9200", "--amqp-url", "amqp://b:5672"]
    )
    assert namespace.replay_config == "from-env.json"


def test_a_configuration_is_required(monkeypatch):
    monkeypatch.delenv("REPLAY_CONFIG", raising=False)
    with pytest.raises(SystemExit):
        cli.replay_parser().parse_args(["--es-url", "http://es:9200"])


def test_a_bad_configuration_stops_before_any_connection(tmp_path, monkeypatch):
    bad = tmp_path / "replay.json"
    bad.write_text(json.dumps({**CONFIG, "lanes": []}))
    opened = []
    monkeypatch.setattr(
        cli.db_connections, "create_connection", lambda **kw: opened.append(kw)
    )
    with pytest.raises(ValueError, match="lane"):
        cli.main(["--replay-config", str(bad), "--es-url", "http://es:9200"])
    assert opened == []


def test_a_handler_that_cannot_be_loaded_stops_the_start(tmp_path, monkeypatch):
    config = tmp_path / "replay.json"
    config.write_text(json.dumps({**CONFIG, "handler": {"class": "builtins:object"}}))
    assert (
        parse_config(json.loads(config.read_text())).handler_spec == "builtins:object"
    )
    opened = []
    monkeypatch.setattr(
        cli.db_connections, "create_connection", lambda **kw: opened.append(kw)
    )
    # builtins:object has no prepare(): refused before any connection is opened
    with pytest.raises(ValueError, match="no prepare"):
        cli.main(["--replay-config", str(config), "--es-url", "http://es:9200"])
    assert opened == []
