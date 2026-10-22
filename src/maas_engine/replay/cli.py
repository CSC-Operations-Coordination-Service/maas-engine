"""Entry point: python -m maas_engine.replay --replay-config replay.json"""

import argparse
import logging
import signal
import sys

import kombu
import opensearchpy.connection.connections as db_connections

import maas_engine
import maas_engine.cli.args as engine_args
from maas_engine.cli.log import setup_logging
from maas_engine.health.serverthread import ServerThread
from maas_engine.replay.config import load_config
from maas_engine.replay.health import build_app


def replay_parser() -> argparse.ArgumentParser:
    """the options of the replay worker: the engine's connections, plus its own"""
    parser = argparse.ArgumentParser(
        description="Run the replay requests of an operator from RabbitMQ",
        parents=[
            engine_args.log_parser(),
            engine_args.es_parser(),
            engine_args.amqp_parser(),
            engine_args.vault_parser(),
        ],
    )
    parser.add_argument(
        "--replay-config",
        dest="replay_config",
        help="Replay configuration file (JSON, see docs/replay.md)",
        action=engine_args.EnvDefault,
        envvar="REPLAY_CONFIG",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--healthcheck-hostname",
        dest="healthcheck_hostname",
        help="Health check host name (default: %(default)s)",
        action=engine_args.EnvDefault,
        envvar="HEALTHCHECK_HOSTNAME",
        required=False,
        type=str,
        default="0.0.0.0",
    )
    parser.add_argument(
        "--healthcheck-port",
        dest="healthcheck_port",
        help="Health check port, 0 for none (default: %(default)s)",
        action=engine_args.EnvDefault,
        envvar="HEALTHCHECK_PORT",
        required=False,
        type=int,
        default=2501,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"maas_engine {maas_engine.__version__}",
    )
    return parser


def main(args: list[str]) -> None:
    """Run the worker until SIGINT or SIGTERM"""
    namespace = replay_parser().parse_args(args)
    setup_logging(loglevel=namespace.loglevel)
    logging.info("Starting the maas-engine replay worker %s", maas_engine.__version__)

    # a bad file stops the worker at startup, with what is wrong
    config = load_config(namespace.replay_config)
    handler = config.build_handler()

    namespace.es_url = engine_args.get_es_credentials_url(namespace)
    namespace.amqp_url = engine_args.get_amqp_credentials_url(namespace)
    db_connections.create_connection(
        hosts=[namespace.es_url],
        retry_on_timeout=True,
        max_retries=namespace.es_retries,
        verify_certs=not namespace.es_ignore_certs_verification,
        ssl_show_warn=not namespace.es_ignore_certs_verification,
    )
    client = db_connections.get_connection()

    heartbeat = getattr(namespace, "amqp_heartbeat", None) or 0

    def connection_factory() -> kombu.Connection:
        return kombu.Connection(namespace.amqp_url, heartbeat=heartbeat)

    worker = config.build_worker(client, connection_factory, handler)
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)

    health = None
    if namespace.healthcheck_port:
        health = ServerThread(
            build_app(worker),
            namespace.healthcheck_hostname,
            namespace.healthcheck_port,
        )
        health.daemon = True
        health.start()
    try:
        worker.run()
    finally:
        if health is not None:
            health.shutdown()


def run() -> None:
    """console script entry point"""
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
