"""AMQP heartbeat: carried from the CLI to every connection the consumer opens.

Without it, a connection cut silently in the network path (haproxy timeout,
docker swarm IPVS idle expiry) stays "running" on the broker with its
consumer, which can then hold a prefetched message unacked forever.

kombu connections are lazy, so none of these tests needs a broker.
"""

import argparse
import json

import pytest

from maas_engine.cli.args import amqp_parser
from maas_engine.consumer.amqp_settings import AMQPSettings
from maas_engine.consumer.engine_consumer import MaasEngineConsumer
from maas_engine.engine.base import Engine

# registers QUERY_ENGINE, used by MINIMAL_CONFIG
import maas_engine.engine.query  # noqa: F401  pylint: disable=unused-import

URL = "amqp://guest:guest@localhost:5672//"


def test_connection_carries_heartbeat():
    connection = AMQPSettings(URL, heartbeat=600).connect()
    assert connection.heartbeat == 600


def test_consumer_reconnections_keep_heartbeat():
    # kombu's ConsumerMixin.create_connection() clones the base connection on
    # every (re)connection of the consume loop: the heartbeat must survive it.
    connection = AMQPSettings(URL, heartbeat=600).connect()
    assert connection.clone().heartbeat == 600


@pytest.mark.parametrize("heartbeat", [None, 0])
def test_heartbeat_disabled(heartbeat):
    connection = AMQPSettings(URL, heartbeat=heartbeat).connect()
    assert connection.heartbeat == 0


def test_cli_default(monkeypatch):
    monkeypatch.delenv("AMQP_HEARTBEAT", raising=False)
    namespace = amqp_parser().parse_args([])
    assert namespace.amqp_heartbeat == 600


def test_cli_from_environment(monkeypatch):
    # EnvDefault reads the environment when the parser is built
    monkeypatch.setenv("AMQP_HEARTBEAT", "30")
    namespace = amqp_parser().parse_args([])
    assert namespace.amqp_heartbeat == 30


def test_cli_flag_overrides_environment(monkeypatch):
    monkeypatch.setenv("AMQP_HEARTBEAT", "30")
    namespace = amqp_parser().parse_args(["--amqp-heartbeat", "0"])
    assert namespace.amqp_heartbeat == 0


# One queue on an engine maas-engine itself implements: tests/assets declares
# maas-cds engines, which setup() rejects as missing implementations here.
MINIMAL_CONFIG = {
    "amqp": [
        {
            "name": "etl-exchange",
            "queues": [
                {
                    "name": "maas-engine-heartbeat-test",
                    "routing_key": "new.maas.engine.heartbeat-test",
                    "events": ["QUERY_ENGINE"],
                }
            ],
        }
    ]
}


@pytest.fixture
def setup_consumer(tmp_path, monkeypatch):
    """run the real MaasEngineConsumer.setup(): no network, kombu is lazy"""
    # Engine.CONFIG_DICT is class-level state: start from an empty one
    monkeypatch.setattr(Engine, "CONFIG_DICT", {"amqp": []})
    (tmp_path / "engine.json").write_text(json.dumps(MINIMAL_CONFIG))

    def _setup(**amqp):
        namespace = argparse.Namespace(
            config=None,
            config_directory=str(tmp_path),
            force=False,
            es_reject_errors=False,
            amqp_url=URL,
            amqp_max_priority=10,
            **amqp,
        )
        consumer = MaasEngineConsumer(namespace)
        consumer.setup()
        return consumer

    return _setup


def test_consumer_setup_passes_cli_heartbeat(setup_consumer):
    consumer = setup_consumer(amqp_heartbeat=120)
    # the connection the consume loop clones, and the one reports go through
    assert consumer.connection.heartbeat == 120
    assert consumer.producer.connection.heartbeat == 120


def test_consumer_setup_without_option_keeps_old_behaviour(setup_consumer):
    assert setup_consumer().connection.heartbeat == 0
