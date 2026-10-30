"""An engine service following the configuration index"""

import pytest

from maas_engine.config.runtime import ConfigRuntime
from maas_engine.config.store import EngineConfigStore
from maas_engine.engine.base import Engine

from fakes import FakeOpenSearch

DEFAULTS = {"modules": ["maas_engine.engine.query"], "model": "maas_model"}


def query(routing_key="query.cds", engine="QUERY_ENGINE"):
    """the configuration of a query engine"""
    return {
        "amqp": [
            {
                "name": "etl-exchange",
                "queues": [
                    {
                        "name": "etl-query",
                        "routing_key": routing_key,
                        "events": [engine],
                    }
                ],
            }
        ]
    }


@pytest.fixture(autouse=True)
def fresh_engine_config(monkeypatch):
    """every test loads into an empty engine configuration"""
    monkeypatch.setattr(Engine, "CONFIG_DICT", {"amqp": []})


@pytest.fixture(name="store")
def fixture_store():
    return EngineConfigStore(FakeOpenSearch())


def runtime_of(store, sleeps=None):
    """a runtime of service query-01 whose sleeps run the given callables"""
    pending = list(sleeps or [])

    def sleep(_seconds):
        if not pending:
            raise AssertionError("waited too long")
        pending.pop(0)()

    return ConfigRuntime(
        store.client,
        "query-01",
        "maas-engine-config",
        "maas-engine-catalogue",
        300,
        sleep=sleep,
    )


def test_start_loads_the_current_versions(store):
    store.put("default/default.json", DEFAULTS, ["*"], "alice")
    store.put("specific/cds-query.json", query(), ["query-01"], "alice")

    runtime = runtime_of(store)
    runtime.start()

    assert Engine.CONFIG_DICT["amqp"][0]["queues"][0]["routing_key"] == "query.cds"
    entry = runtime.catalogue.read("query-01")
    assert entry["state"] == "running"
    assert entry["loaded"] == {"default/default.json": 1, "specific/cds-query.json": 1}
    assert entry["current"] == entry["loaded"]
    assert entry["rejected"] is None
    assert "QUERY_ENGINE" in [engine["id"] for engine in entry["engines"]]


def test_start_waits_for_a_configuration(store):
    def configure():
        store.put("specific/cds-query.json", query(), ["query-01"], "alice")

    runtime = runtime_of(store, sleeps=[lambda: None, configure])
    states = []
    original = runtime.catalogue.write
    runtime.catalogue.write = lambda entry: states.append(entry["state"]) or original(
        entry
    )

    runtime.start()

    assert states == ["waiting", "waiting", "running"]
    assert runtime.entry["loaded"] == {"specific/cds-query.json": 1}


def test_start_falls_back_to_the_versions_last_loaded(store):
    store.put("specific/cds-query.json", query(), ["query-01"], "alice")
    runtime_of(store).start()

    # a bad version saved while the engine was down
    store.put(
        "specific/cds-query.json",
        query(engine="NO_SUCH_ENGINE"),
        ["query-01"],
        "bob",
        base_version=1,
    )
    runtime = runtime_of(store)
    runtime.start()

    entry = runtime.catalogue.read("query-01")
    assert entry["state"] == "running"
    assert entry["loaded"] == {"specific/cds-query.json": 1}
    assert entry["current"] == {"specific/cds-query.json": 2}
    assert entry["rejected"]["versions"] == {"specific/cds-query.json": 2}
    assert "unknown engine ids NO_SUCH_ENGINE" in entry["rejected"]["reason"]
    # the watcher does not reject the same versions again
    assert not runtime.check_once()


def test_start_waits_when_nothing_can_be_loaded(store):
    store.put(
        "specific/cds-query.json",
        {"modules": ["maas_engine.nothing_here"]},
        ["query-01"],
        "alice",
    )

    def fix():
        store.put(
            "specific/cds-query.json", query(), ["query-01"], "bob", base_version=1
        )

    runtime = runtime_of(store, sleeps=[fix])
    runtime.start()

    assert runtime.entry["loaded"] == {"specific/cds-query.json": 2}
    assert runtime.entry["rejected"] is None


def test_check_once_accepts_a_change_it_can_load(store):
    store.put("specific/cds-query.json", query(), ["query-01"], "alice")
    runtime = runtime_of(store)
    runtime.start()

    assert not runtime.check_once()

    store.put(
        "specific/cds-query.json",
        query("query.cds2"),
        ["query-01"],
        "bob",
        base_version=1,
    )

    assert runtime.check_once()


def test_check_once_rejects_then_forgets_an_undone_change(store):
    store.put("specific/cds-query.json", query(), ["query-01"], "alice")
    runtime = runtime_of(store)
    runtime.start()

    store.put(
        "specific/bad.json",
        {"modules": ["maas_engine.nothing_here"]},
        ["query-01"],
        "bob",
    )

    assert not runtime.check_once()
    entry = runtime.catalogue.read("query-01")
    assert entry["rejected"]["versions"] == {
        "specific/bad.json": 1,
        "specific/cds-query.json": 1,
    }
    assert entry["current"] == entry["rejected"]["versions"]
    assert not runtime.check_once()

    store.delete("specific/bad.json", "bob", base_version=1)

    assert not runtime.check_once()
    assert runtime.catalogue.read("query-01")["rejected"] is None


def test_removing_every_file_restarts_the_engine_to_wait(store):
    store.put("specific/cds-query.json", query(), ["query-01"], "alice")
    runtime = runtime_of(store)
    runtime.start()

    store.delete("specific/cds-query.json", "bob", base_version=1)

    assert runtime.check_once()


def test_watch_calls_on_change_once(store):
    store.put("specific/cds-query.json", query(), ["query-01"], "alice")
    runtime = runtime_of(store)
    runtime.refresh_period = 0.01
    runtime.start()
    store.put(
        "specific/cds-query.json",
        query("query.cds2"),
        ["query-01"],
        "bob",
        base_version=1,
    )

    calls = []
    runtime.watch(lambda: calls.append(1))
    runtime._thread.join(timeout=5)  # pylint: disable=protected-access
    runtime.stop()

    assert calls == [1]
