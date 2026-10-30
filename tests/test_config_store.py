"""Versioned engine configurations and what a service loads of them"""

import pytest

from maas_engine.config.provider import (
    ConfigSource,
    OpenSearchConfigProvider,
    fingerprint,
    serves,
)
from maas_engine.config.store import (
    CONFIG_INDEX,
    ConfigConflictError,
    ConfigInvalidError,
    ConfigNotFoundError,
    EngineConfigStore,
    check_path,
)

from fakes import FakeOpenSearch

QUERY = {
    "amqp": [
        {
            "name": "etl-exchange",
            "queues": [
                {"name": "etl-query.cds", "routing_key": "query.cds", "events": ["QUERY_ENGINE"]}
            ],
        }
    ]
}

DEFAULTS = {"model": "maas_model", "logging": {"shapely.geos": "CRITICAL"}}


@pytest.fixture(name="store")
def fixture_store():
    return EngineConfigStore(FakeOpenSearch())


def test_put_creates_the_index_and_a_first_version(store):
    doc = store.put("specific/cds-query.json", QUERY, ["query-01"], "alice", "first")

    assert store.client.indices.exists(index=CONFIG_INDEX)
    assert doc["version"] == 1 and doc["latest"] and not doc["deleted"]
    assert doc["based_on"] is None
    assert doc["engine_ids"] == ["QUERY_ENGINE"]
    assert doc["routing_keys"] == ["query.cds"]
    assert store.client.data[CONFIG_INDEX]["specific/cds-query.json@1"] == doc


def test_put_needs_the_current_version(store):
    store.put("specific/cds-query.json", QUERY, ["query-01"], "alice")

    with pytest.raises(ConfigConflictError):
        store.put("specific/cds-query.json", QUERY, ["query-01"], "bob", base_version=0)

    doc = store.put(
        "specific/cds-query.json", QUERY, ["query-01", "query-02"], "bob", base_version=1
    )

    assert doc["version"] == 2 and doc["based_on"] == 1
    assert [d["latest"] for d in store.history("specific/cds-query.json")] == [False, True]
    assert store.get("specific/cds-query.json")["services"] == ["query-01", "query-02"]


def test_put_refuses_bad_content_paths_and_services(store):
    with pytest.raises(ConfigInvalidError) as error:
        store.put("specific/x.json", {"amqp": {}}, ["query-01"], "alice")
    assert error.value.problems == ["amqp must be a list of exchanges"]

    for path in ("", "/abs.json", "a/../b.json", "x.txt"):
        with pytest.raises(ValueError):
            check_path(path)

    with pytest.raises(ValueError):
        store.put("specific/x.json", QUERY, [], "alice")
    with pytest.raises(ValueError):
        store.put("specific/x.json", QUERY, ["a b"], "alice")
    assert store.put("default/default.json", DEFAULTS, ["*"], "alice")["services"] == ["*"]


def test_delete_publishes_a_tombstone_and_rollback_restores(store):
    store.put("specific/cds-query.json", QUERY, ["query-01"], "alice")
    tombstone = store.delete("specific/cds-query.json", "bob", base_version=1)

    assert tombstone["deleted"] and tombstone["version"] == 2
    assert tombstone["content"] == QUERY
    assert store.current() == [tombstone]
    assert not store.current(include_deleted=False)

    with pytest.raises(ConfigNotFoundError):
        store.delete("specific/cds-query.json", "bob", base_version=2)

    restored = store.rollback("specific/cds-query.json", 1, "carol", base_version=2)

    assert not restored["deleted"] and restored["version"] == 3
    assert restored["based_on"] == 1 and restored["comment"] == "rollback to version 1"

    # putting over a tombstone brings the path back too
    store.delete("specific/cds-query.json", "bob", base_version=3)
    assert not store.put(
        "specific/cds-query.json", QUERY, ["query-01"], "dan", base_version=4
    )["deleted"]


def test_reads(store):
    with pytest.raises(ConfigNotFoundError):
        store.history("nothing.json")
    assert store.get("nothing.json") is None
    assert store.current() == []

    store.put("specific/b.json", QUERY, ["query-01"], "alice")
    store.put("default/a.json", DEFAULTS, ["*"], "alice")

    assert [doc["path"] for doc in store.current()] == ["default/a.json", "specific/b.json"]
    assert store.version("specific/b.json", 1)["author"] == "alice"
    with pytest.raises(ConfigNotFoundError):
        store.version("specific/b.json", 2)


def test_current_prefers_the_higher_of_two_latest(store):
    """a publish that crashed before clearing the older flag"""
    store.put("specific/b.json", QUERY, ["query-01"], "alice")
    store.put("specific/b.json", QUERY, ["query-02"], "alice", base_version=1)
    store.client.data[CONFIG_INDEX]["specific/b.json@1"]["latest"] = True

    assert [doc["version"] for doc in store.current()] == [2]


def test_concurrent_publish_is_a_conflict(store):
    store.put("specific/b.json", QUERY, ["query-01"], "alice")
    # another writer took version 2 after this one read version 1
    store.client.data[CONFIG_INDEX]["specific/b.json@2"] = {"path": "elsewhere"}

    with pytest.raises(ConfigConflictError):
        store.put("specific/b.json", QUERY, ["query-01"], "bob", base_version=1)


def test_provider_loads_what_the_service_is_served(store):
    store.put("default/default.json", DEFAULTS, ["*"], "alice")
    store.put("specific/cds-query.json", QUERY, ["query-01"], "alice")
    store.put("specific/other.json", QUERY, ["other"], "alice")
    store.put("specific/gone.json", QUERY, ["query-01"], "alice")
    store.delete("specific/gone.json", "alice", base_version=1)

    provider = OpenSearchConfigProvider(store.client, CONFIG_INDEX, "query-01")

    assert provider.load() == [
        ConfigSource("default/default.json", 1, DEFAULTS),
        ConfigSource("specific/cds-query.json", 1, QUERY),
    ]
    assert provider.current_versions() == {
        "default/default.json": 1,
        "specific/cds-query.json": 1,
    }
    assert provider.load_versions({"specific/gone.json": 1}) == [
        ConfigSource("specific/gone.json", 1, QUERY)
    ]
    with pytest.raises(ConfigNotFoundError):
        provider.load_versions({"specific/gone.json": 9})


def test_serves_and_fingerprint():
    assert serves(["query-01"], "query-01") and serves(["*"], "query-01")
    assert not serves(["other"], "query-01")

    a = [ConfigSource("a.json", 1, {}), ConfigSource("b.json", 2, {})]
    assert fingerprint(a) == fingerprint(list(reversed(a)))
    assert fingerprint(a) != fingerprint([ConfigSource("a.json", 2, {}), a[1]])
