from types import SimpleNamespace
from unittest import mock

from maas_model.message import MAASQueryMessage

from maas_engine.engine.query import QueryEngine
from maas_engine.engine.report import EngineReport


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


class ThreeDocsSearch(FakeSearch):
    def scan(self):
        for number in "123":
            yield CdsPublication(number, "cds-publication-2026-10")


class ThreeDocsModel:
    @staticmethod
    def search():
        return ThreeDocsSearch()


def run_three_documents(engine_chunk_size=0, **payload_args):
    payload = MAASQueryMessage(document_class="CdsPublication", **payload_args)
    with mock.patch.object(
        QueryEngine, "get_model", new=lambda obj, name: ThreeDocsModel
    ):
        engine = QueryEngine(chunk_size=engine_chunk_size)
        return list(engine.run("some.key", payload))


def test_the_message_chunk_size_reaches_the_reports():
    (report,) = run_query(chunk_size=1)
    assert report.chunk_size == 1


def test_a_zero_message_chunk_size_keeps_the_engine_chunk_size():
    (report,) = run_query(chunk_size=0)
    assert report.chunk_size == 0
    reports = run_three_documents(engine_chunk_size=2, chunk_size=0)
    assert [report.chunk_size for report in reports] == [2]


def test_chunked_raw_reports_all_keep_the_collect_exchange():
    reports = run_three_documents(
        chunk_size=1,
        output_document_class="PripProduct",
        output_routing_key="new.raw.data.prip-product",
    )
    chunks = list(EngineReport.split_reports(reports))
    assert len(chunks) == 3
    for chunk in chunks:
        assert chunk.exchange == "collect-exchange"
        assert chunk.document_class == "PripProduct"
        assert chunk.document_indices == []
