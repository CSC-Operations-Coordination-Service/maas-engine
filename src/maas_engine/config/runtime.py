"""
An engine service on the configuration index: what it loads at startup, and how it
follows the changes.

At startup the service loads the current versions it is served. When they cannot be
loaded, it falls back to the versions it last ran on (its catalogue entry says which),
so a bad edit never stops an engine. With nothing to load, it waits for a
configuration, its health check up, instead of exiting: a new service is configured
from maas-operator, without a script.

While it runs, it checks the index every refresh period. A change it can load makes
it exit with EXIT_CONFIG_CHANGED (swarm restarts it on the new versions); a change it
cannot load is rejected, recorded in the catalogue with the reason, and the engine
keeps running.
"""

import logging
import threading
import time
from typing import Callable, Dict, List, Optional

from flask import Flask

from maas_engine.config.catalogue import (
    RUNNING,
    WAITING,
    Catalogue,
    describe_model,
    new_entry,
    now,
)
from maas_engine.config.provider import (
    ConfigSource,
    OpenSearchConfigProvider,
    fingerprint,
)
from maas_engine.config.store import ConfigNotFoundError
from maas_engine.config.validation import import_problems, set_problems
from maas_engine.engine.base import Engine
from maas_engine.health.serverthread import ServerThread

__all__ = ["EXIT_CONFIG_CHANGED", "ConfigRuntime"]

LOGGER = logging.getLogger("ConfigRuntime")

# EX_TEMPFAIL, as the collectors: restart me, my configuration changed
EXIT_CONFIG_CHANGED = 75


def versions(sources: List[ConfigSource]) -> Dict[str, int]:
    """path -> version of a set"""
    return {source.path: source.version for source in sources}


def problems_of(sources: List[ConfigSource]) -> List[str]:
    """what prevents an engine from loading a set: its structure, the modules and
    model it imports, then its engine ids, known once the modules are imported"""
    pairs = [(source.path, source.content) for source in sources]
    return (
        set_problems(pairs)
        or import_problems(pairs)
        or set_problems(pairs, Engine.registered())
    )


def waiting_app() -> Flask:
    """health check of a service waiting for its configuration: it is alive"""
    app = Flask(__name__)
    app.add_url_rule(
        "/", "healthcheck", view_func=lambda: ("waiting for configuration", 200)
    )
    return app


class ConfigRuntime:
    """Configuration of one engine service, read from the index"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        client,
        service: str,
        config_index: str,
        catalogue_index: str,
        refresh_period: int,
        sleep: Callable[[float], None] = time.sleep,
    ):
        """
        Args:
            client (opensearchpy.OpenSearch): connected client
            service (str): the service name, CONFIG_SERVICE
            config_index (str): the maas-engine-config index
            catalogue_index (str): the maas-engine-catalogue index
            refresh_period (int): seconds between two checks
            sleep (Callable, optional): waits while there is no configuration
        """
        self.service = service
        self.refresh_period = refresh_period
        self.provider = OpenSearchConfigProvider(client, config_index, service)
        self.catalogue = Catalogue(client, catalogue_index)
        self.sleep = sleep
        # the entry before this start: the versions the service last ran on
        self.previous = self.catalogue.read(service)
        self.entry = new_entry(service, refresh_period)
        # the set loaded into the engine
        self.running: List[ConfigSource] = []
        # the current set last looked at, accepted or rejected
        self.seen = ""
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _publish(self) -> None:
        """write the entry, seen now"""
        self.entry["seen_at"] = now()
        self.catalogue.write(self.entry)

    def _reject(self, sources: List[ConfigSource], problems: List[str]) -> None:
        LOGGER.error(
            "configuration change rejected: %s: %s",
            versions(sources),
            "; ".join(problems),
        )
        self.entry["rejected"] = {
            "versions": versions(sources),
            "reason": "; ".join(problems),
            "at": now(),
        }

    def _fallback(self) -> List[ConfigSource]:
        """the versions the service last ran on, when they can still be loaded"""
        last = (self.previous or {}).get("loaded") or {}
        if not last:
            return []
        try:
            sources = self.provider.load_versions(last)
        except ConfigNotFoundError as error:
            LOGGER.error("cannot fall back to %s: %s", last, error)
            return []
        problems = problems_of(sources)
        if problems:
            LOGGER.error("cannot fall back to %s: %s", last, "; ".join(problems))
            return []
        LOGGER.warning("falling back to the versions last loaded: %s", last)
        return sources

    def _choose(self) -> List[ConfigSource]:
        """the set to load now: the current one, else the fallback; empty to wait"""
        current = self.provider.load()
        self.entry["current"] = versions(current)
        self.seen = fingerprint(current)
        if not current:
            LOGGER.info("waiting for a configuration of service %s", self.service)
            return []
        problems = problems_of(current)
        if not problems:
            self.entry["rejected"] = None
            return current
        self._reject(current, problems)
        return self._fallback()

    def start(self, health_host: str = "0.0.0.0", health_port: int = 0) -> None:
        """Load the configuration of the service into the engine, waiting for one
        while there is none

        Raises:
            opensearchpy.exceptions.OpenSearchException: the index is out of reach
        """
        sources = self._choose()
        health = None
        try:
            while not sources:
                self.entry["state"] = WAITING
                self._publish()
                if health is None and health_port:
                    health = ServerThread(waiting_app(), health_host, health_port)
                    health.start()
                self.sleep(self.refresh_period)
                sources = self._choose()
        finally:
            if health is not None:
                health.shutdown()
                health.join()

        Engine.load_config_sources(
            [(source.path, source.content) for source in sources]
        )
        self.running = sources
        self.entry["state"] = RUNNING
        self.entry["loaded"] = versions(sources)
        describe_model(self.entry, getattr(Engine, "MODEL_MODULE", None))
        LOGGER.info("running on %s", self.entry["loaded"])
        self._publish()

    def check_once(self) -> bool:
        """Look at the index once

        Returns:
            bool: whether the engine must restart on a new configuration
        """
        current = self.provider.load()
        self.entry["current"] = versions(current)
        current_print = fingerprint(current)

        changed = False
        if current_print != self.seen:
            self.seen = current_print
            if current_print == fingerprint(self.running):
                # back to what runs, a rejected change undone
                self.entry["rejected"] = None
            else:
                problems = problems_of(current) if current else []
                if problems:
                    self._reject(current, problems)
                else:
                    LOGGER.warning(
                        "configuration changed: %s -> %s, restarting",
                        versions(self.running),
                        versions(current),
                    )
                    changed = True

        self._publish()
        return changed

    def watch(self, on_change: Callable[[], None]) -> None:
        """check the index every refresh period in a thread, and call on_change once
        when the engine must restart"""

        def loop():
            while not self._stop.wait(self.refresh_period):
                try:
                    if self.check_once():
                        on_change()
                        return
                except Exception:  # pylint: disable=broad-exception-caught
                    LOGGER.warning("cannot check the configuration", exc_info=True)

        self._thread = threading.Thread(target=loop, name="config-watch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """end the watch"""
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
