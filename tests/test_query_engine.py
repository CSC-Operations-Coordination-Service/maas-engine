from types import SimpleNamespace
from unittest import mock

from maas_model.message import MAASQueryMessage

from maas_engine.engine.query import QueryEngine


class CdsPublication:
    """Fake document: only what the report strategy reads"""

    class Index:
        name = "cds-publication"

    def __init__(self, doc_id, index):
        self.meta = SimpleNamespace(id=doc_id)
        self.partition_index_name = index


class FakeSearch:
    def query(self, *args, **kwargs):
        return self

    def params(self, **kwargs):
        return self

    def scan(self):
        yield CdsPublication("1", "cds-publication-2026-10")
        yield CdsPublication("2", "cds-publication-2026-10")


class FakeModel:
    @staticmethod
    def search():
        return FakeSearch()


def run_query(**payload_args):
    payload = MAASQueryMessage(document_class="CdsPublication", **payload_args)
    with mock.patch.object(QueryEngine, "get_model", new=lambda obj, name: FakeModel):
        engine = QueryEngine()
        return list(engine.run("some.key", payload))


def test_reports_keep_the_scanned_class_and_indices_by_default():
    (report,) = run_query()
    assert report.document_class == "CdsPublication"
    assert report.document_indices == ["cds-publication-2026-10"]
    assert report.exchange == ""


def test_output_document_class_renames_the_class_and_drops_indices():
    (report,) = run_query(
        output_document_class="PripProduct",
        output_routing_key="new.raw.data.prip-product",
    )
    assert report.document_class == "PripProduct"
    assert report.document_indices == []
    assert report.exchange == "collect-exchange"
    assert report.action == "new.raw.data.prip-product"


def test_same_output_class_keeps_indices():
    (report,) = run_query(output_document_class="CdsPublication")
    assert report.document_class == "CdsPublication"
    assert report.document_indices == ["cds-publication-2026-10"]


def test_non_raw_output_routing_key_keeps_default_exchange():
    (report,) = run_query(output_routing_key="update.cds-publication-s1")
    assert report.action == "update.cds-publication-s1"
    assert report.exchange == ""
