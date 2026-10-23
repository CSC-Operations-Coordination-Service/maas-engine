"""The operation record updates"""

import datetime
import logging

from opensearchpy.exceptions import NotFoundError

from maas_engine.replay.records import DONE, FAILED, RUNNING, OperationRecorder

NOW = datetime.datetime(2026, 10, 22, 12, 30, 5, tzinfo=datetime.UTC)


class FakeClient:
    def __init__(self, update_error=None, index_error=None):
        self.updates = []
        self.indexed = []
        self.update_error = update_error
        self.index_error = index_error

    def update(self, **kwargs):
        if self.update_error:
            raise self.update_error
        self.updates.append(kwargs)

    def index(self, **kwargs):
        if self.index_error:
            raise self.index_error
        self.indexed.append(kwargs)


def missing():
    return NotFoundError(404, "document_missing_exception", {})


def recorder(client, **kwargs):
    return OperationRecorder(client, "ops", clock=lambda: NOW, **kwargs)


def test_an_update_sets_the_status_and_the_time():
    client = FakeClient()
    assert recorder(client).update("op-1", RUNNING) is True
    assert client.updates == [
        {
            "index": "ops",
            "id": "op-1",
            "body": {
                "doc": {"status": "RUNNING", "updated_at": "2026-10-22T12:30:05Z"}
            },
            "retry_on_conflict": 3,
        }
    ]


def test_exit_code_and_details_are_sent_only_when_given():
    client = FakeClient()
    recorder(client).update("op-1", DONE, exit_code=0, details="tail")
    doc = client.updates[0]["body"]["doc"]
    assert doc["exit_code"] == 0 and doc["details"] == "tail"
    recorder(client).update("op-1", FAILED, exit_code=None, details="")
    doc = client.updates[1]["body"]["doc"]
    assert "exit_code" not in doc and doc["details"] == ""


def test_a_missing_record_is_an_error_not_an_exception(caplog):
    client = FakeClient(update_error=missing())
    with caplog.at_level(logging.ERROR):
        assert recorder(client).update("op-1", RUNNING) is False
    assert "op-1" in caplog.text
    assert client.indexed == []


def test_a_missing_record_can_be_created():
    client = FakeClient(update_error=missing())
    assert recorder(client, create_missing=True).update("op-1", RUNNING) is True
    assert client.indexed == [
        {
            "index": "ops",
            "id": "op-1",
            "body": {
                "message_id": "op-1",
                "status": "RUNNING",
                "updated_at": "2026-10-22T12:30:05Z",
            },
        }
    ]


def test_a_failed_creation_does_not_raise():
    client = FakeClient(update_error=missing(), index_error=RuntimeError("down"))
    assert recorder(client, create_missing=True).update("op-1", RUNNING) is False


def test_any_other_error_does_not_raise():
    client = FakeClient(update_error=ConnectionError("down"))
    assert recorder(client).update("op-1", RUNNING) is False
