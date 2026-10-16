"""Telemetry helpers of the engine: set-up and declared topology."""

import argparse
import logging
from typing import Any

from maas_model.telemetry import configure, shutdown, topology

import maas_engine
from maas_engine.consumer.amqp_settings import AMQPSettings
from maas_engine.engine.base import Engine

__all__ = [
    "setup_telemetry",
    "declare_topology",
    "valid_output_routes",
    "queue_name",
    "shutdown",
]

LOGGER = logging.getLogger(__name__)


def setup_telemetry(args: argparse.Namespace, instance_id: str | None = None) -> bool:
    """Configure telemetry for the engine component

    Telemetry is enabled by --telemetry, or by MAAS_TELEMETRY read by the library.

    Args:
        args (argparse.Namespace): parsed command line
        instance_id (str | None): service.instance.id, by default MAAS_INSTANCE_ID
            or <hostname>-<pid>. A replay gives its own so that its cumulative series
            do not collide with those of the engine whose container it runs in.

    Returns:
        bool: whether telemetry is enabled
    """
    return configure(
        "engine",
        project="maas-engine",
        project_version=maas_engine.__version__,
        enabled=True if args.telemetry else None,
        instance_id=instance_id,
    )


def declare_topology(amqp_settings: AMQPSettings) -> None:
    """Declare the routes and engines of the configuration

    Args:
        amqp_settings (AMQPSettings): exchanges, queues and event mapping
    """
    engines = Engine.registered()
    output_routes: dict[str, list[tuple[str, str]]] = {}
    for queue in amqp_settings.queues.values():
        for event in amqp_settings.event_mapping.get(queue.routing_key, []):
            engine_id = event if isinstance(event, str) else event["id"]
            cls = engines.get(engine_id)
            if cls is None:
                continue
            topology.declare_route(
                direction="in",
                node_type="engine",
                node=engine_id,
                exchange=queue.exchange.name,
                routing_key=queue.routing_key,
                queue=queue.name,
            )
            topology.declare_node(
                node_type="engine",
                node=engine_id,
                impl=f"{cls.__module__}.{cls.__qualname__}",
                summary=topology.node_summary(cls),
            )
            if engine_id not in output_routes:
                output_routes[engine_id] = valid_output_routes(
                    engine_id, cls.OUTPUT_ROUTES
                )
            for exchange, routing_key in output_routes[engine_id]:
                topology.declare_route(
                    direction="out",
                    node_type="engine",
                    node=engine_id,
                    exchange=exchange,
                    routing_key=routing_key,
                    source="config",
                )


def valid_output_routes(engine_id: str, routes: Any) -> list[tuple[str, str]]:
    """The (exchange, routing_key) pairs of OUTPUT_ROUTES, without the bad entries

    A malformed OUTPUT_ROUTES must not stop the engine, whether telemetry is on or
    off: its bad entries are skipped with one warning naming the engine.

    Args:
        engine_id (str): ENGINE_ID of the engine class, for the warning
        routes (Any): the engine class's OUTPUT_ROUTES

    Returns:
        list[tuple[str, str]]: the valid pairs, in order
    """
    try:
        entries = list(routes)
    except TypeError:
        entries = [routes]
    valid = []
    bad = []
    for entry in entries:
        if (
            isinstance(entry, (tuple, list))
            and len(entry) == 2
            and all(isinstance(item, str) for item in entry)
        ):
            valid.append((entry[0], entry[1]))
        else:
            bad.append(entry)
    if bad:
        LOGGER.warning(
            "Engine %s: skipping malformed OUTPUT_ROUTES entries %r, expected "
            "(exchange, routing_key) pairs of str, "
            'like (("etl-exchange", "new.compute-x"),)',
            engine_id,
            bad,
        )
    return valid


def queue_name(amqp_settings: AMQPSettings, exchange: str, routing_key: str) -> str:
    """Name of the first queue bound to exchange with routing_key, else ''"""
    for queue in amqp_settings.queues.values():
        if queue.exchange.name == exchange and queue.routing_key == routing_key:
            return queue.name
    return ""
