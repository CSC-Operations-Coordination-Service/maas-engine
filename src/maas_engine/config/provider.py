"""
Where an engine service reads its configuration: the files it loads from the
maas-engine-config index.

A file is served to a service when its services hold the service name, or * (the
shared default files). The engine loads the current, non-deleted versions, in path
order, as load_config_directory loads files.
"""

import dataclasses
import hashlib
from typing import Any, Dict, List

from maas_engine.config.store import ALL_SERVICES, EngineConfigStore

__all__ = [
    "ConfigSource",
    "OpenSearchConfigProvider",
    "fingerprint",
    "serves",
]


@dataclasses.dataclass(frozen=True)
class ConfigSource:
    """one version of a configuration file"""

    path: str
    version: int
    content: Dict[str, Any]


def serves(services: List[str], service: str) -> bool:
    """whether a file with these services is loaded by a service"""
    return service in services or ALL_SERVICES in services


def fingerprint(sources: List[ConfigSource]) -> str:
    """identity of a set of versions: equal sets have equal fingerprints"""
    digest = hashlib.sha256()
    for path, version in sorted((source.path, source.version) for source in sources):
        digest.update(f"{path}@{version}\n".encode())
    return digest.hexdigest()


class OpenSearchConfigProvider:
    """The configuration of one engine service, from the index"""

    def __init__(self, client, index: str, service: str):
        """
        Args:
            client (opensearchpy.OpenSearch): connected client
            index (str): the maas-engine-config index
            service (str): the service name, CONFIG_SERVICE
        """
        self.store = EngineConfigStore(client, index)
        self.service = service

    def load(self) -> List[ConfigSource]:
        """current, non-deleted versions served to the service, by path"""
        return [
            ConfigSource(doc["path"], doc["version"], doc["content"])
            for doc in self.store.current(include_deleted=False)
            if serves(doc["services"], self.service)
        ]

    def current_versions(self) -> Dict[str, int]:
        """path -> version of what load() returns"""
        return {source.path: source.version for source in self.load()}

    def load_versions(self, versions: Dict[str, int]) -> List[ConfigSource]:
        """exact versions, by path: the set an engine ran on before

        Raises:
            ConfigNotFoundError: when a version does not exist any more
        """
        return [
            ConfigSource(path, version, self.store.version(path, version)["content"])
            for path, version in sorted(versions.items())
        ]
