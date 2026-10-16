import json
import logging
import re
from types import SimpleNamespace
from unittest import mock

import pytest
from maas_model.telemetry import activity, span
from opensearchpy import OpenSearch
from opensearchpy.connection import Urllib3HttpConnection
from opensearchpy.connection.connections import connections as db_connections

from maas_engine.engine.data import DataEngine, DataEngineStatistics


def test_statistics_log_line_still_parsed(caplog):
    caplog.set_level(logging.INFO)
    stats = DataEngineStatistics(logger=logging.getLogger("MyEngine"), node="X")
    stats.created = 2
    stats.updated = 1
    stats.finish()
    line = f"[2026-10-16 00:00:00,000] INFO:MyEngine: {caplog.records[-1].getMessage()}"
    match = re.search(r"INFO:(.+): (\d+) created, (\d+) updated, .* (\d+\.\d+)s", line)
    assert match.groups()[:3] == ("MyEngine", "2", "1")


def test_documents_counted(telemetry_capture):
    stats = DataEngineStatistics(logger=logging.getLogger("MyEngine"), node="X")
    stats.created = 2
    stats.conflicts = 1
    stats.errors = 1
    stats.finish()
    assert (
        telemetry_capture.point(
            "maas.engine.documents", node="X", result="created"
        ).value
        == 2
    )
    assert (
        telemetry_capture.point(
            "maas.engine.documents", node="X", result="conflict"
        ).value
        == 1
    )
    results = {
        p.attributes["result"]
        for p in telemetry_capture.points("maas.engine.documents")
    }
    assert results == {"created", "conflict"}


class FakeDoc:
    def __init__(self, doc_id):
        self.meta = SimpleNamespace(id=doc_id)


class FakeBulk(DataEngine):
    ENGINE_ID = "FAKE_BULK"

    def action_iterator(self):
        for i in range(3):
            yield {
                "_op_type": "index",
                "_index": "fake-index",
                "_id": str(i),
                "_source": {"a": i},
            }


class FakeModel:
    __name__ = "FakeModel"

    @staticmethod
    def mget_by_ids(ids, indices):
        return [FakeDoc("a"), None]


@pytest.fixture
def os_connection(monkeypatch):
    def fake(self, method, url, params=None, body=None, **kwargs):
        if url.endswith("_bulk"):
            n = body.count(b"\n") // 2 if isinstance(body, bytes) else 3
            items = [
                {
                    "index": {
                        "_index": "fake-index",
                        "_id": str(i),
                        "result": "created",
                        "status": 201,
                    }
                }
                for i in range(n)
            ]
            return 200, {}, json.dumps({"items": items})
        return 200, {}, "{}"

    monkeypatch.setattr(Urllib3HttpConnection, "perform_request", fake)
    db_connections.add_connection("default", OpenSearch("http://127.0.0.1:1"))
    yield
    db_connections.remove_connection("default")


def test_opensearch_from_bulk_pool_attributed(telemetry_capture, os_connection):
    engine = FakeBulk(send_reports=False)
    engine.input_documents = [FakeDoc("a")]
    payload = SimpleNamespace(
        document_class="X", document_ids=["a"], document_indices=[]
    )
    with mock.patch.object(FakeBulk, "get_input_model", return_value=FakeModel):
        with span("engine.run FAKE_BULK"), activity("engine", "FAKE_BULK"):
            list(engine.run("rk", payload))
    assert (
        telemetry_capture.point(
            "maas.opensearch.time", node="FAKE_BULK", operation="bulk"
        ).value
        >= 0
    )
    # the parallel_bulk pool threads hang their requests from engine.bulk
    os_spans = [s for s in telemetry_capture.spans() if s.name == "opensearch bulk"]
    assert os_spans
    bulk = telemetry_capture.span("engine.bulk")
    assert {s.parent.span_id for s in os_spans} == {bulk.context.span_id}
    assert (
        telemetry_capture.point(
            "maas.engine.documents", node="FAKE_BULK", result="created"
        ).value
        == 3
    )


def test_missing_input_counted(telemetry_capture):
    engine = FakeBulk(
        args=SimpleNamespace(es_requeue_missing_input=False), send_reports=False
    )
    payload = SimpleNamespace(
        document_class="FakeModel", document_ids=["a", "b"], document_indices=[]
    )
    with mock.patch.object(FakeBulk, "get_input_model", return_value=FakeModel):
        with span("engine.load_input.test"):
            engine._load_input_documents(payload, "rk")
    assert (
        telemetry_capture.point("maas.engine.input.missing", node="FAKE_BULK").value
        == 1
    )
    assert (
        telemetry_capture.point("maas.engine.input.documents", node="FAKE_BULK").value
        == 2
    )
    load_span = telemetry_capture.span("engine.load_input.test")
    assert load_span.attributes["maas.documents.missing"] == 1
