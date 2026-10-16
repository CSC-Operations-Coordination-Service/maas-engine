"""Telemetry helpers of the engine: set-up and declared topology."""

import argparse

from maas_model.telemetry import configure, shutdown, topology

import maas_engine
from maas_engine.consumer.amqp_settings import AMQPSettings
from maas_engine.engine.base import Engine

__all__ = ["setup_telemetry", "declare_topology", "queue_name", "shutdown"]


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
            for exchange, routing_key in cls.OUTPUT_ROUTES:
                topology.declare_route(
                    direction="out",
                    node_type="engine",
                    node=engine_id,
                    exchange=exchange,
                    routing_key=routing_key,
                    source="config",
                )


def queue_name(amqp_settings: AMQPSettings, exchange: str, routing_key: str) -> str:
    """Name of the first queue bound to exchange with routing_key, else ''"""
    for queue in amqp_settings.queues.values():
        if queue.exchange.name == exchange and queue.routing_key == routing_key:
            return queue.name
    return ""
