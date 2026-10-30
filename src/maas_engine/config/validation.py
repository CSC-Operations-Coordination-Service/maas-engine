"""
Checks of engine configurations, shared by the engines and maas-operator-backend.

A configuration is the content of one file: {amqp?, modules?, model?, defaults?,
logging?}. The checks here need nothing but the content, except import_problems,
which imports what the configuration names and so runs in an engine only.
"""

import importlib
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

__all__ = [
    "TOP_LEVEL_KEYS",
    "engine_ids",
    "import_problems",
    "routing_keys",
    "set_problems",
    "structure_problems",
]

TOP_LEVEL_KEYS = ("amqp", "modules", "model", "defaults", "logging")

LOGGING_LEVELS = ("CRITICAL", "FATAL", "ERROR", "WARNING", "WARN", "INFO", "DEBUG")

# (path, content) pairs, the shape every set check takes
Sources = Sequence[Tuple[str, Dict[str, Any]]]


def _event_id(event: Any) -> Optional[str]:
    """the engine id of an event declaration, None when malformed"""
    if isinstance(event, str):
        return event
    if isinstance(event, dict) and isinstance(event.get("id"), str):
        return event["id"]
    return None


def _queues(content: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """(exchange name, queue) pairs of a well-formed content"""
    for exchange in content.get("amqp", []):
        for queue in exchange.get("queues", []):
            yield exchange["name"], queue


def structure_problems(content: Any) -> List[str]:
    """What is wrong with the shape of one configuration

    Args:
        content (Any): the configuration, as read from JSON

    Returns:
        List[str]: problems, empty when the shape is right
    """
    if not isinstance(content, dict):
        return ["the configuration is not a JSON object"]

    problems = [
        f"unknown key {key!r} (expected one of {', '.join(TOP_LEVEL_KEYS)})"
        for key in content
        if key not in TOP_LEVEL_KEYS
    ]

    problems.extend(_amqp_problems(content.get("amqp", [])))

    modules = content.get("modules", [])
    if not isinstance(modules, list) or not all(
        isinstance(name, str) and name for name in modules
    ):
        problems.append("modules must be a list of module names")

    if "model" in content and not (
        isinstance(content["model"], str) and content["model"]
    ):
        problems.append("model must be a module name")

    defaults = content.get("defaults", [])
    if not isinstance(defaults, list) or not all(
        isinstance(item, dict) and "id" in item for item in defaults
    ):
        problems.append("defaults must be a list of objects with an id")

    levels = content.get("logging", {})
    if not isinstance(levels, dict):
        problems.append("logging must be an object of logger names to levels")
    else:
        problems.extend(
            f"logging level of {name!r} is not a level: {level!r}"
            for name, level in levels.items()
            if level not in LOGGING_LEVELS
        )

    return problems


def _amqp_problems(amqp: Any) -> List[str]:
    """problems of the amqp section"""
    if not isinstance(amqp, list):
        return ["amqp must be a list of exchanges"]

    problems = []
    for position, exchange in enumerate(amqp):
        if not isinstance(exchange, dict) or not isinstance(exchange.get("name"), str):
            problems.append(f"amqp[{position}] must be an object with a name")
            continue
        queues = exchange.get("queues")
        if not isinstance(queues, list):
            problems.append(f"exchange {exchange['name']}: queues must be a list")
            continue
        for queue in queues:
            problems.extend(_queue_problems(exchange["name"], queue))
    return problems


def _queue_problems(exchange_name: str, queue: Any) -> List[str]:
    """problems of one queue declaration"""
    if not isinstance(queue, dict):
        return [f"exchange {exchange_name}: a queue must be an object"]

    name = queue.get("name")
    where = f"exchange {exchange_name}, queue {name}"
    problems = []
    if not isinstance(name, str) or not name:
        problems.append(f"exchange {exchange_name}: a queue has no name")
    if not isinstance(queue.get("routing_key"), str) or not queue["routing_key"]:
        problems.append(f"{where}: routing_key must be a non-empty string")
    events = queue.get("events")
    if not isinstance(events, list):
        problems.append(f"{where}: events must be a list")
    else:
        problems.extend(
            f"{where}: an event must be an engine id or an object with an id"
            for event in events
            if _event_id(event) is None
        )
    return problems


def engine_ids(content: Dict[str, Any]) -> List[str]:
    """engine ids the events of a configuration name, sorted

    Args:
        content (Dict[str, Any]): a configuration without structure problems

    Returns:
        List[str]: unique engine ids
    """
    return sorted(
        {
            event_id
            for _, queue in _queues(content)
            for event_id in map(_event_id, queue.get("events", []))
            if event_id
        }
    )


def routing_keys(content: Dict[str, Any]) -> List[str]:
    """routing keys the queues of a configuration bind, sorted

    Args:
        content (Dict[str, Any]): a configuration without structure problems

    Returns:
        List[str]: unique routing keys
    """
    return sorted({queue["routing_key"] for _, queue in _queues(content)})


def set_problems(
    sources: Sources, known_engine_ids: Optional[Iterable[str]] = None
) -> List[str]:
    """What is wrong with a set of configurations one engine service would load

    Args:
        sources (Sources): (path, content) pairs
        known_engine_ids (Iterable[str], optional): engine ids that have an
            implementation. Not checked when None.

    Returns:
        List[str]: problems, each prefixed with the path concerned
    """
    problems = []
    well_formed = []
    for path, content in sources:
        found = structure_problems(content)
        problems.extend(f"{path}: {problem}" for problem in found)
        if not found:
            well_formed.append((path, content))

    declared: Dict[Tuple[str, str], str] = {}
    for path, content in well_formed:
        for exchange_name, queue in _queues(content):
            key = (exchange_name, queue["name"])
            if key in declared and declared[key] != path:
                problems.append(
                    f"{path}: queue {queue['name']} of {exchange_name} is declared"
                    f" by {declared[key]} too"
                )
            declared.setdefault(key, path)

    if known_engine_ids is not None:
        known = set(known_engine_ids)
        for path, content in well_formed:
            unknown = [name for name in engine_ids(content) if name not in known]
            if unknown:
                problems.append(f"{path}: unknown engine ids {', '.join(unknown)}")

    return problems


def import_problems(sources: Sources) -> List[str]:
    """Import the modules and the model the configurations name

    Importing registers the engines of the modules, so the caller can check the
    engine ids afterwards. Engine side only: the modules must be installed.

    Args:
        sources (Sources): (path, content) pairs without structure problems

    Returns:
        List[str]: the names that could not be imported, and why
    """
    problems = []
    for path, content in sources:
        names = list(content.get("modules", []))
        if "model" in content:
            names.append(content["model"])
        for name in names:
            try:
                importlib.import_module(name)
            except Exception as error:  # pylint: disable=broad-exception-caught
                logging.debug("Cannot import %s", name, exc_info=True)
                problems.append(f"{path}: cannot import {name}: {error}")
    return problems
