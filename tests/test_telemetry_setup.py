"""Telemetry set-up: flag, configure, declared topology and shutdown."""

import argparse
import json
import logging
from unittest import mock

import pytest

from maas_engine.cli.args import engine_parser
from maas_engine.consumer import engine_consumer
from maas_engine.consumer.engine_consumer import MaasEngineConsumer
from maas_engine.consumer.telemetry import declare_topology, queue_name, setup_telemetry
from maas_engine.engine.base import Engine

# registers QUERY_ENGINE, used by CONFIG
import maas_engine.engine.query  # noqa: F401  pylint: disable=unused-import

URL = "amqp://guest:guest@localhost:5672//"

CONFIG = {
    "amqp": [
        {
            "name": "etl-exchange",
            "queues": [
                {
                    "name": "etl-new.cds-product-s1",
                    "routing_key": "new.cds-product-s1",
                    "events": [
                        "QUERY_ENGINE",
                        {"id": "QUERY_ENGINE", "send_reports": True},
                    ],
                }
            ],
        }
    ]
}


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
    return consumer


def test_flag(monkeypatch):
    monkeypatch.delenv("MAAS_TELEMETRY", raising=False)
    parser = engine_parser()
    assert parser.parse_args(["--telemetry"]).telemetry is True
    assert parser.parse_args([]).telemetry is False


def test_registered_lists_query_engine():
    registry = Engine.registered()
    assert registry["QUERY_ENGINE"].__name__ == "QueryEngine"
    registry.clear()
    assert Engine.registered()


def test_declare_topology(telemetry_capture, consumer):
    declare_topology(consumer.amqp_settings)
    point = telemetry_capture.point(
        "maas.topology.route_info", node="QUERY_ENGINE", direction="in"
    )
    assert point.attributes["queue"] == "etl-new.cds-product-s1"
    assert point.attributes["exchange"] == "etl-exchange"
    node = telemetry_capture.point("maas.node_info", node="QUERY_ENGINE")
    assert node.attributes["impl"].endswith("QueryEngine")


def test_unregistered_engine_is_skipped(telemetry_capture, consumer):
    consumer.amqp_settings.event_mapping["new.cds-product-s1"] = ["NOPE"]
    declare_topology(consumer.amqp_settings)
    assert not telemetry_capture.points("maas.node_info")


def test_output_routes_declared(telemetry_capture, consumer, monkeypatch):
    query_engine = Engine.registered()["QUERY_ENGINE"]
    monkeypatch.setattr(query_engine, "OUTPUT_ROUTES", (("etl-exchange", "x.y"),))
    declare_topology(consumer.amqp_settings)
    point = telemetry_capture.point(
        "maas.topology.route_info", node="QUERY_ENGINE", direction="out"
    )
    assert point.attributes["exchange"] == "etl-exchange"
    assert point.attributes["routing_key"] == "x.y"
    assert point.attributes["source"] == "config"


@pytest.mark.parametrize(
    "routes",
    [
        ("etl-exchange", "new.x"),  # the outer tuple is missing
        ("ex", "rk"),  # the same, with strings of two characters
        (("etl-exchange",),),
        (("etl-exchange", "new.x", "extra"),),
        (("etl-exchange", 1),),
        None,
        42,
    ],
    ids=["flat", "flat-2-chars", "1-tuple", "3-tuple", "not-str", "none", "int"],
)
def test_malformed_output_routes_skipped(
    telemetry_capture, consumer, monkeypatch, caplog, routes
):
    query_engine = Engine.registered()["QUERY_ENGINE"]
    monkeypatch.setattr(query_engine, "OUTPUT_ROUTES", routes)
    with caplog.at_level(logging.WARNING):
        declare_topology(consumer.amqp_settings)
    out = [
        p
        for p in telemetry_capture.points("maas.topology.route_info")
        if p.attributes.get("direction") == "out"
    ]
    assert out == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "QUERY_ENGINE" in warnings[0].getMessage()
    # the in-route and the node are still declared
    assert telemetry_capture.point("maas.node_info", node="QUERY_ENGINE")


def test_valid_output_routes_kept_beside_malformed(
    telemetry_capture, consumer, monkeypatch, caplog
):
    query_engine = Engine.registered()["QUERY_ENGINE"]
    monkeypatch.setattr(
        query_engine,
        "OUTPUT_ROUTES",
        (("etl-exchange", "x.y"), "bad", ("a",), ["etl-exchange", "x.z"]),
    )
    # the engine of two queues still warns once
    consumer.amqp_settings.event_mapping["new.cds-product-s1"] = [
        "QUERY_ENGINE",
        "QUERY_ENGINE",
    ]
    with caplog.at_level(logging.WARNING):
        declare_topology(consumer.amqp_settings)
    keys = {
        p.attributes["routing_key"]
        for p in telemetry_capture.points("maas.topology.route_info")
        if p.attributes.get("direction") == "out"
    }
    assert keys == {"x.y", "x.z"}
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


def test_malformed_output_routes_telemetry_off(consumer, monkeypatch):
    monkeypatch.delenv("MAAS_TELEMETRY", raising=False)
    query_engine = Engine.registered()["QUERY_ENGINE"]
    monkeypatch.setattr(query_engine, "OUTPUT_ROUTES", ("etl-exchange", "new.x"))
    declare_topology(consumer.amqp_settings)


def test_queue_name(consumer):
    settings = consumer.amqp_settings
    assert queue_name(settings, "etl-exchange", "new.cds-product-s1") == (
        "etl-new.cds-product-s1"
    )
    assert queue_name(settings, "etl-exchange", "unknown") == ""
    assert queue_name(settings, "other", "new.cds-product-s1") == ""


def test_setup_telemetry_configures_engine(monkeypatch):
    configure = mock.Mock(return_value=True)
    monkeypatch.setattr("maas_engine.consumer.telemetry.configure", configure)
    assert setup_telemetry(argparse.Namespace(telemetry=True)) is True
    kwargs = configure.call_args.kwargs
    assert configure.call_args.args == ("engine",)
    assert kwargs["project"] == "maas-engine"
    assert kwargs["enabled"] is True
    setup_telemetry(argparse.Namespace(telemetry=False))
    assert configure.call_args.kwargs["enabled"] is None
    assert kwargs["instance_id"] is None


def test_setup_telemetry_passes_instance_id(monkeypatch):
    configure = mock.Mock(return_value=True)
    monkeypatch.setattr("maas_engine.consumer.telemetry.configure", configure)
    setup_telemetry(argparse.Namespace(telemetry=True), instance_id="engine.1-replay-7")
    assert configure.call_args.kwargs["instance_id"] == "engine.1-replay-7"


def test_main_configures_declares_and_shuts_down(monkeypatch):
    calls = []
    fake = mock.Mock()
    fake.run.side_effect = lambda: calls.append("run")
    monkeypatch.setattr(engine_consumer, "MaasEngineConsumer", lambda ns: fake)
    monkeypatch.setattr(engine_consumer, "setup_logging", lambda **kw: None)
    monkeypatch.setattr(
        engine_consumer.telemetry, "setup_telemetry", lambda ns: calls.append("conf")
    )
    monkeypatch.setattr(
        engine_consumer.telemetry, "declare_topology", lambda s: calls.append("topo")
    )
    monkeypatch.setattr(
        engine_consumer.telemetry, "shutdown", lambda: calls.append("shutdown")
    )
    MaasEngineConsumer.main(["--telemetry"])
    assert calls == ["conf", "topo", "run", "shutdown"]

    calls.clear()
    fake.run.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        MaasEngineConsumer.main([])
    assert calls[-1] == "shutdown"
