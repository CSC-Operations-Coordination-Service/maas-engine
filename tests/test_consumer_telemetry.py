"""Every branch of on_message records one outcome on a consume span."""

import argparse
import json
import logging
from types import SimpleNamespace
from unittest import mock

import pytest
from opensearchpy.exceptions import OpenSearchException

from maas_engine.consumer.engine_consumer import MaasEngineConsumer
from maas_engine.consumer.telemetry import queue_name
from maas_engine.engine.base import Engine
from maas_engine.exceptions import (
    CannotProcessMessageException,
    HandleMessageException,
)

# registers QUERY_ENGINE, used by CONFIG
import maas_engine.engine.query  # noqa: F401  pylint: disable=unused-import

URL = "amqp://guest:guest@localhost:5672//"
EXCHANGE = "etl-exchange"
ROUTING_KEY = "new.maas.engine.exchange-test"

CONFIG = {
    "amqp": [
        {
            "name": EXCHANGE,
            "queues": [
                {
                    "name": "maas-engine-test",
                    "routing_key": ROUTING_KEY,
                    "events": ["QUERY_ENGINE"],
                }
            ],
        }
    ]
}

BODY = {"message_id": "m1"}


@pytest.fixture(name="consumer")
def fixture_consumer(tmp_path, monkeypatch):
    monkeypatch.setattr(Engine, "CONFIG_DICT", {"amqp": []})
    (tmp_path / "engine.json").write_text(json.dumps(CONFIG))
    namespace = argparse.Namespace(
        config=None,
        config_directory=str(tmp_path),
        force=False,
        es_reject_errors=False,
        amqp_url=URL,
        amqp_max_priority=10,
        amqp_heartbeat=600,
        amqp_retries=3,
    )
    consumer = MaasEngineConsumer(namespace)
    consumer.setup()
    consumer.producer = mock.Mock()
    consumer.on_start_message = mock.Mock()
    consumer.on_end_message = mock.Mock()
    return consumer


@pytest.fixture(name="queue")
def fixture_queue(consumer):
    name = queue_name(consumer.amqp_settings, EXCHANGE, ROUTING_KEY)
    assert name
    return name


@pytest.fixture(name="message")
def fixture_message():
    return SimpleNamespace(
        headers=None,
        delivery_info={
            "exchange": EXCHANGE,
            "routing_key": ROUTING_KEY,
            "redelivered": False,
        },
        properties={},
        ack=mock.Mock(),
        requeue=mock.Mock(),
        reject=mock.Mock(),
    )


def execute_raises(error):
    def setup(consumer, _body):
        consumer._execute_engines = mock.Mock(side_effect=error)

    return setup


def execute_returns(reports):
    def setup(consumer, _body):
        consumer._execute_engines = mock.Mock(return_value=reports)

    return setup


def start_raises(consumer, _body):
    consumer.on_start_message = mock.Mock(side_effect=RuntimeError("x"))


def stopping(consumer, _body):
    consumer.should_stop = True


BRANCHES = [
    ("invalid_body", None, "reject", "invalid_body", "reject"),
    ("stopping", stopping, "requeue", "stopping", None),
    ("start_failed", start_raises, "requeue", "start_failed", "requeue"),
    (
        "retryable",
        execute_raises(HandleMessageException("x")),
        "requeue",
        "retryable",
        "requeue",
    ),
    (
        "opensearch",
        execute_raises(OpenSearchException("x")),
        "requeue",
        "opensearch",
        "requeue",
    ),
    (
        "normal_reject",
        execute_raises(CannotProcessMessageException("x")),
        "reject",
        "normal",
        "reject",
    ),
    ("abnormal", execute_raises(ValueError("x")), "reject", "abnormal", "reject"),
    ("success", execute_returns([]), "ack", "normal", "ack"),
]


@pytest.mark.parametrize(
    "setup_branch, outcome, reason, action",
    [b[1:] for b in BRANCHES],
    ids=[b[0] for b in BRANCHES],
)
def test_outcome_per_branch(
    telemetry_capture, consumer, message, queue, setup_branch, outcome, reason, action
):
    body = BODY
    if setup_branch is None:
        body = "not a dict"
    else:
        setup_branch(consumer, body)
    consumer.on_message(body, message)

    point = telemetry_capture.point(
        "maas.messages.consumed", outcome=outcome, reason=reason
    )
    assert point.value == 1
    assert len(telemetry_capture.points("maas.messages.consumed")) == 1
    if action:
        getattr(message, action).assert_called_once()
    span = telemetry_capture.span(f"process {queue}")
    assert span.attributes["maas.message.outcome"] == outcome


def test_start_message_runs_in_connect_span(telemetry_capture, consumer, message):
    del consumer.on_start_message  # use the real method
    consumer.args.es_url = "http://localhost:9200"
    consumer.args.es_retries = 1
    consumer.args.es_timeout = 1
    consumer.args.es_ignore_certs_verification = True
    consumer._execute_engines = mock.Mock(return_value=[])
    consumer.on_message(BODY, message)
    assert telemetry_capture.span("opensearch.connect")


def test_disabled_on_message_unchanged(consumer, message, caplog):
    consumer._execute_engines = mock.Mock(return_value=[])
    with caplog.at_level(logging.INFO):
        consumer.on_message(BODY, message)
    message.ack.assert_called_once()
    assert [r.getMessage() for r in caplog.records] == [
        f"MSG m1 RECEIVED FROM {EXCHANGE}/{ROUTING_KEY} ancestors: N/A pipeline: N/A",
        f"MSG m1 ACK {ROUTING_KEY} Completed pipeline: []",
    ]
