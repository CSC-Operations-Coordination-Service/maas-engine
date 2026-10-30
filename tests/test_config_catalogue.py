"""The catalogue the engines publish"""

import types

from maas_model.document import MAASDocument
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError

import maas_engine.engine.query  # noqa: F401 pylint: disable=unused-import
from maas_engine.config.catalogue import (
    CATALOGUE_INDEX,
    Catalogue,
    describe_model,
    document_class_entries,
    engine_entries,
    new_entry,
)

from fakes import FakeOpenSearch


class FakeProduct(MAASDocument):
    """a partitioned product"""

    class Index:
        name = "fake-product"


class FakeProductS1(FakeProduct):
    """an S1 product"""


class NoIndex(MAASDocument):
    """an intermediate class"""


MODEL = types.ModuleType("fake_model")
MODEL.FakeProduct = FakeProduct
MODEL.FakeProductS1 = FakeProductS1
MODEL.NoIndex = NoIndex
MODEL.MAASDocument = MAASDocument
MODEL.helper = len


def test_engine_entries_list_the_registry():
    entries = {entry["id"]: entry for entry in engine_entries()}

    assert entries["QUERY_ENGINE"]["implementation"] == (
        "maas_engine.engine.query.QueryEngine"
    )
    assert entries["QUERY_ENGINE"]["summary"]


def test_document_class_entries():
    assert document_class_entries(MODEL) == [
        {"name": "FakeProduct", "bases": [], "index": "fake-product"},
        {"name": "FakeProductS1", "bases": ["FakeProduct"], "index": "fake-product"},
    ]
    assert document_class_entries(None) == []


def test_entries_round_trip():
    catalogue = Catalogue(FakeOpenSearch())
    assert catalogue.read("query-01") is None
    assert catalogue.entries() == []

    entry = new_entry("query-01", 300)
    describe_model(entry, MODEL)
    entry["state"] = "running"

    assert catalogue.write(entry)
    assert catalogue.client.indices.exists(index=CATALOGUE_INDEX)
    assert catalogue.read("query-01") == entry

    catalogue.write(new_entry("deletion-01", 300))
    assert [e["service"] for e in catalogue.entries()] == ["deletion-01", "query-01"]
    assert entry["document_classes"][0]["name"] == "FakeProduct"
    assert entry["versions"]["maas_engine"]


def test_a_failed_write_is_not_raised():
    client = FakeOpenSearch()
    client.failure = OpenSearchConnectionError("N/A", "down", None)

    assert not Catalogue(client).write(new_entry("query-01", 300))
