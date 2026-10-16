"""Every branch of on_message records one outcome on a consume span."""

import argparse
import json
import logging
from types import SimpleNamespace
from unittest import mock

import pytest
import opensearchpy.connection.connections as db_connections
from opensearchpy.exceptions import OpenSearchException
from opentelemetry.trace import StatusCode
from maas_model.telemetry import amqp as _amqp  # noqa: F401
from maas_model import telemetry

from maas_engine.consumer.engine_consumer import MaasEngineConsumer
from maas_engine.consumer.telemetry import queue_name
from maas_engine.engine.base import Engine, EngineReport
from maas_engine.exceptions import (
    CannotProcessMessageException,
    HandleMessageException,
)

# registers QUERY_ENGINE, used by CONFIG
import maas_engine.engine.query  # noqa: F401  pylint: disable=unused-import

# registers FAKE_A, FAKE_B, FAKE_FAIL and FAKE_UNDECLARED
import fake_telemetry_engines  # noqa: F401  pylint: disable=unused-import

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


@pytest.fixture(name="default_connection_removed")
def fixture_default_connection_removed():
    """remove the global OpenSearch "default" connection the test registers"""
    yield
    try:
        db_connections.remove_connection("default")
    except KeyError:
        pass


@pytest.mark.usefixtures("default_connection_removed")
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


# --- folded-in gaps of the on_message review ---


@pytest.mark.parametrize("failing", ["notify_reports", "publish"])
def test_exception_in_success_branch(telemetry_capture, consumer, message, failing):
    consumer._execute_engines = mock.Mock(
        return_value=[EngineReport(action="a", data_ids=["1"])]
    )
    if failing == "notify_reports":
        consumer.notify_reports = mock.Mock(side_effect=RuntimeError("boom"))
    else:
        consumer.producer.publish.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        consumer.on_message(BODY, message)
    point = telemetry_capture.point(
        "maas.messages.consumed", outcome="error", reason="exception"
    )
    assert point.value == 1
    consumer.on_end_message.assert_called_once()


def test_start_failure_through_real_connect_span(
    telemetry_capture, consumer, message, monkeypatch
):
    del consumer.on_start_message  # use the real method
    consumer.args.es_url = "http://localhost:9200"
    consumer.args.es_retries = 1
    consumer.args.es_timeout = 1
    consumer.args.es_ignore_certs_verification = True
    monkeypatch.setattr(
        db_connections, "create_connection", mock.Mock(side_effect=RuntimeError("x"))
    )
    consumer.on_message(BODY, message)
    point = telemetry_capture.point(
        "maas.messages.consumed", outcome="requeue", reason="start_failed"
    )
    assert point.value == 1
    connect = telemetry_capture.span("opensearch.connect")
    assert connect.status.status_code == StatusCode.ERROR
    message.requeue.assert_called_once()


def test_queue_label_on_consumed_point(telemetry_capture, consumer, message, queue):
    consumer._execute_engines = mock.Mock(return_value=[])
    consumer.on_message(BODY, message)
    point = telemetry_capture.point("maas.messages.consumed", outcome="ack")
    assert point.attributes["queue"] == queue


# --- per-engine run, reports, observed routes, headers, returns ---


def map_engines(consumer, *engine_ids):
    consumer.amqp_settings.event_mapping[ROUTING_KEY] = list(engine_ids)


def valid_body(**extra):
    return {
        "message_id": "m1",
        "document_class": "Doc",
        "document_ids": ["a", "b"],
        **extra,
    }


def test_end_to_end_trace(telemetry_capture, consumer, message, queue):
    map_engines(consumer, "FAKE_A")
    with telemetry.span("collect.file"):
        with telemetry.amqp.publish_span(
            "collect-exchange", ROUTING_KEY
        ) as headers:
            pass
    message.headers = headers
    consumer.on_message(valid_body(), message)

    proc = telemetry_capture.span(f"process {queue}")
    run = telemetry_capture.span("engine.run FAKE_A")
    engine_pub = telemetry_capture.span(f"publish {EXCHANGE}")
    collector_pub = telemetry_capture.span("publish collect-exchange")
    assert proc.parent.span_id == collector_pub.context.span_id
    assert run.parent.span_id == proc.context.span_id
    assert run.attributes["maas.reports.count"] == 1
    assert run.attributes["maas.documents.input"] == 2
    assert engine_pub.context.trace_id == proc.context.trace_id
    assert engine_pub.parent.span_id == proc.context.span_id
    assert "traceparent" in consumer.producer.publish.call_args.kwargs["headers"]
    point = telemetry_capture.point(
        "maas.engine.run.duration", node="FAKE_A", outcome="ok"
    )
    assert point.count == 1


def test_reports_counted_per_engine_before_merge(
    telemetry_capture, consumer, message
):
    map_engines(consumer, "FAKE_A", "FAKE_B")
    consumer.on_message(valid_body(), message)

    for node, count in (("FAKE_A", 2), ("FAKE_B", 3)):
        point = telemetry_capture.point(
            "maas.engine.reports.documents",
            node=node,
            exchange=EXCHANGE,
            routing_key="next.rk",
        )
        assert point.value == count
        routes = telemetry_capture.points("maas.topology.route_info")
        assert any(
            p.attributes.get("node") == node
            and p.attributes.get("source") == "observed"
            and p.attributes.get("direction") == "out"
            for p in routes
        )
    consumer.producer.publish.assert_called_once()


def test_engine_error_outcome(telemetry_capture, consumer, message):
    map_engines(consumer, "FAKE_FAIL")
    consumer.on_message(valid_body(), message)

    point = telemetry_capture.point(
        "maas.engine.run.duration", node="FAKE_FAIL", outcome="error"
    )
    assert point.count == 1
    assert (
        telemetry_capture.point(
            "maas.messages.consumed", outcome="reject", reason="normal"
        ).value
        == 1
    )


def test_return_counted(telemetry_capture, consumer):
    consumer.handle_message_return(
        Exception(312, "NO_ROUTE"), EXCHANGE, "rk", None
    )
    point = telemetry_capture.point("maas.messages.returned", reason="NO_ROUTE")
    assert point.value == 1


def test_disabled_publish_headers_empty(consumer, message):
    map_engines(consumer, "FAKE_A")
    consumer.on_message(valid_body(), message)
    kwargs = consumer.producer.publish.call_args.kwargs
    assert kwargs["headers"] == {}
    body = consumer.producer.publish.call_args.args[0]
    assert json.loads(body)["document_ids"] == ["x", "y"]


def test_undeclared_report_exchange_observed_on_etl_exchange(
    telemetry_capture, consumer, message
):
    """the observed route and count name the exchange the report is published on"""
    map_engines(consumer, "FAKE_UNDECLARED")
    consumer.on_message(valid_body(), message)

    assert consumer.producer.publish.call_args.kwargs["exchange"] == EXCHANGE
    point = telemetry_capture.point(
        "maas.engine.reports.documents", node="FAKE_UNDECLARED"
    )
    assert point.attributes["exchange"] == EXCHANGE
    assert point.value == 2
    route = telemetry_capture.point(
        "maas.topology.route_info",
        node="FAKE_UNDECLARED",
        direction="out",
        source="observed",
    )
    assert route.attributes["exchange"] == EXCHANGE


def test_republish_carries_origin_interface_baggage(
    telemetry_capture, consumer, message, queue
):
    """the baggage of the incoming message travels on to the engine's reports"""
    map_engines(consumer, "FAKE_A")
    with telemetry.baggage({"maas.origin.interface": "PRIP_S1"}):
        with telemetry.amqp.publish_span("collect-exchange", ROUTING_KEY) as headers:
            pass
    assert "maas.origin.interface=PRIP_S1" in headers["baggage"]
    message.headers = headers
    consumer.on_message(valid_body(), message)

    proc = telemetry_capture.span(f"process {queue}")
    assert proc.attributes["maas.origin.interface"] == "PRIP_S1"
    republished = consumer.producer.publish.call_args.kwargs["headers"]
    assert "maas.origin.interface=PRIP_S1" in republished["baggage"]
