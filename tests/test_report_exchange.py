"""Each report is published on the exchange it names, else on the default one."""

import argparse
import json
import logging
from unittest import mock

import pytest

from maas_engine.consumer.engine_consumer import MaasEngineConsumer
from maas_engine.engine.base import Engine, EngineReport

# registers QUERY_ENGINE, used by CONFIG
import maas_engine.engine.query  # noqa: F401  pylint: disable=unused-import

URL = "amqp://guest:guest@localhost:5672//"

CONFIG = {
    "amqp": [
        {
            "name": exchange,
            "queues": [
                {
                    "name": f"maas-engine-{exchange}-test",
                    "routing_key": "new.maas.engine.exchange-test",
                    "events": ["QUERY_ENGINE"],
                }
            ],
        }
        for exchange in ("etl-exchange", "collect-exchange")
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


def publish(consumer, **report_args):
    report = EngineReport(
        action="new.raw.data.prip-product",
        document_class="PripProduct",
        data_ids=["a"],
        **report_args,
    )
    consumer.notify_reports(consumer.amqp_settings.exchanges["etl-exchange"], [report], 3)
    return consumer.producer.publish.call_args


def test_report_without_exchange_goes_to_etl(consumer):
    call = publish(consumer)
    assert call.kwargs["exchange"] == "etl-exchange"


def test_raw_report_goes_to_collect_exchange(consumer):
    call = publish(consumer, exchange="collect-exchange")
    assert call.kwargs["exchange"] == "collect-exchange"
    assert call.kwargs["routing_key"] == "new.raw.data.prip-product"
    assert json.loads(call.args[0])["document_class"] == "PripProduct"


def test_unknown_report_exchange_falls_back_to_etl(consumer, caplog):
    with caplog.at_level(logging.WARNING):
        call = publish(consumer, exchange="nope-exchange")
    assert call.kwargs["exchange"] == "etl-exchange"
    assert "nope-exchange" in caplog.text
