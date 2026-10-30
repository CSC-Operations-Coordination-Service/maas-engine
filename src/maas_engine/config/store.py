"""
Versioned engine configurations in OpenSearch.

Every version of a configuration file is a document <path>@<version> of the
maas-engine-config index, and the latest flag marks the current version of each path.
A tombstone (deleted) hides a path: the services stop loading it, its history stays,
and a rollback brings it back. EngineConfigStore is the only writer: the admin CLI
and maas-operator-backend use it, the engines only read the index (see
provider.OpenSearchConfigProvider). It mirrors the collectors' maas-collector-config.
"""

import datetime
import logging
import re
from typing import Any, Dict, List, Optional

from opensearchpy.exceptions import ConflictError

from maas_engine.config.validation import (
    engine_ids,
    routing_keys,
    structure_problems,
)

__all__ = [
    "ALL_SERVICES",
    "CONFIG_INDEX",
    "CONFIG_INDEX_BODY",
    "ConfigConflictError",
    "ConfigInvalidError",
    "ConfigNotFoundError",
    "EngineConfigStore",
    "check_path",
    "check_service",
    "document_id",
]

LOGGER = logging.getLogger("EngineConfigStore")

CONFIG_INDEX = "maas-engine-config"

# a file of every service, like the shared default/*.json
ALL_SERVICES = "*"

# no index holds more versions than this: the searches read everything at once
MAX_DOCUMENTS = 10000

CONFIG_INDEX_BODY = {
    "settings": {"number_of_shards": 1},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "path": {"type": "keyword"},
            "version": {"type": "integer"},
            "latest": {"type": "boolean"},
            "deleted": {"type": "boolean"},
            "services": {"type": "keyword"},
            # the file JSON verbatim: stored, not indexed
            "content": {"type": "object", "enabled": False},
            "engine_ids": {"type": "keyword"},
            "routing_keys": {"type": "keyword"},
            "author": {"type": "keyword"},
            "comment": {"type": "text"},
            "created_at": {"type": "date"},
            "based_on": {"type": "integer"},
        },
    },
}


class ConfigConflictError(Exception):
    """another version of the path was published in the meantime"""


class ConfigNotFoundError(LookupError):
    """no such path or version"""


class ConfigInvalidError(ValueError):
    """the content cannot be an engine configuration"""

    def __init__(self, problems: List[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


def check_path(path: str):
    """A document path is a relative path to a .json file, with "/" separators, as
    under /conf/engine/ in a file deployment: specific/cds-query.json

    Raises:
        ValueError: on an empty, absolute or non-normalized path
    """
    segments = path.split("/") if path else []

    if (
        not segments
        or "\\" in path
        or any(segment in ("", ".", "..") for segment in segments)
        or not path.endswith(".json")
    ):
        raise ValueError(
            f"bad configuration path {path!r}: expected a relative path like"
            " specific/cds-query.json, with / separators and no . or .. segment"
        )


# a swarm service name of the stack, like query-01
SERVICE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def check_service(service: str):
    """A service is a service name of the stack, or * for every service

    Raises:
        ValueError: on anything else
    """
    if service != ALL_SERVICES and not (
        isinstance(service, str) and SERVICE_NAME.fullmatch(service)
    ):
        raise ValueError(
            f"bad service name {service!r}: expected a name like query-01, or *"
        )


def document_id(path: str, version: int) -> str:
    """identifier of a version document: specific/cds-query.json@3"""
    return f"{path}@{version}"


def now() -> str:
    """the current time, as the documents store it"""
    return datetime.datetime.now(tz=datetime.UTC).isoformat()


class EngineConfigStore:
    """Read and write the versions of the engine configurations"""

    def __init__(self, client, index: str = CONFIG_INDEX):
        """
        Args:
            client (opensearchpy.OpenSearch): connected client
            index (str, optional): index name. Defaults to CONFIG_INDEX.
        """
        self.client = client
        self.index = index

    def ensure_index(self) -> None:
        """create the index when it does not exist"""
        if not self.client.indices.exists(index=self.index):
            self.client.indices.create(index=self.index, body=CONFIG_INDEX_BODY)
            LOGGER.info("Created index %s", self.index)

    def _search(self, filters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """sources of the documents matching every term filter"""
        if not self.client.indices.exists(index=self.index):
            return []
        response = self.client.search(
            index=self.index,
            body={
                "query": {"bool": {"filter": filters}},
                "size": MAX_DOCUMENTS,
            },
        )
        return [hit["_source"] for hit in response["hits"]["hits"]]

    def current(self, include_deleted: bool = True) -> List[Dict[str, Any]]:
        """current version of every path, sorted by path

        Two documents of a path may carry the latest flag for a moment, while a
        publish clears the older one: the higher version wins.

        Args:
            include_deleted (bool, optional): keep the tombstones. Defaults to True.

        Returns:
            List[Dict[str, Any]]: documents
        """
        newest: Dict[str, Dict[str, Any]] = {}
        for doc in self._search([{"term": {"latest": True}}]):
            if doc["path"] not in newest or doc["version"] > newest[doc["path"]]["version"]:
                newest[doc["path"]] = doc
        return [
            newest[path]
            for path in sorted(newest)
            if include_deleted or not newest[path]["deleted"]
        ]

    def history(self, path: str) -> List[Dict[str, Any]]:
        """every version of a path, oldest first

        Raises:
            ConfigNotFoundError: when the path has no version
        """
        docs = self._search([{"term": {"path": path}}])
        if not docs:
            raise ConfigNotFoundError(f"{path} does not exist")
        return sorted(docs, key=lambda doc: doc["version"])

    def get(self, path: str) -> Optional[Dict[str, Any]]:
        """current version of a path, a tombstone included; None when absent"""
        try:
            return self.history(path)[-1]
        except ConfigNotFoundError:
            return None

    def version(self, path: str, version: int) -> Dict[str, Any]:
        """one version of a path

        Raises:
            ConfigNotFoundError: when the version does not exist
        """
        for doc in self._search(
            [{"term": {"path": path}}, {"term": {"version": version}}]
        ):
            return doc
        raise ConfigNotFoundError(f"{path} has no version {version}")

    # pylint: disable=too-many-arguments
    def _publish(
        self,
        path: str,
        content: Dict[str, Any],
        services: List[str],
        author: str,
        comment: str,
        base_version: int,
        deleted: bool = False,
        based_on: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Write a new version of a path, and make it the current one

        Raises:
            ConfigConflictError: when base_version is not the current version, or
                another publish wrote the same version first
            ConfigInvalidError: when the content is not an engine configuration
            ValueError: on a bad path or service, or without service
        """
        check_path(path)

        problems = structure_problems(content)
        if problems:
            raise ConfigInvalidError(problems)

        services = sorted(set(services))
        if not services:
            raise ValueError(f"{path}: a configuration needs at least one service")
        for service in services:
            check_service(service)

        self.ensure_index()

        current_doc = self.get(path)
        current = current_doc["version"] if current_doc else 0
        if base_version != current:
            raise ConfigConflictError(
                f"{path}: version {current} was published after version {base_version}"
            )

        version = current + 1
        document = {
            "path": path,
            "version": version,
            "latest": True,
            "deleted": deleted,
            "services": services,
            "content": content,
            "engine_ids": engine_ids(content),
            "routing_keys": routing_keys(content),
            "author": author,
            "comment": comment,
            "created_at": now(),
            "based_on": based_on if based_on is not None else (current or None),
        }

        try:
            self.client.index(
                index=self.index,
                id=document_id(path, version),
                body=document,
                op_type="create",
                refresh="wait_for",
            )
        except ConflictError as error:
            raise ConfigConflictError(
                f"{path}: version {version} was published concurrently"
            ) from error

        self._clear_latest(path, version)

        LOGGER.info("Published %s", document_id(path, version))

        return document

    def _clear_latest(self, path: str, version: int):
        """clear the latest flag of the versions below version. Also repairs the flags
        a crashed publish left behind. A higher version belongs to a later publish and
        keeps its flag."""
        for doc in self._search([{"term": {"path": path}}, {"term": {"latest": True}}]):
            if doc["version"] < version:
                self.client.update(
                    index=self.index,
                    id=document_id(path, doc["version"]),
                    body={"doc": {"latest": False}},
                    refresh="wait_for",
                )

    def put(
        self,
        path: str,
        content: Dict[str, Any],
        services: List[str],
        author: str,
        comment: str = "",
        base_version: int = 0,
    ) -> Dict[str, Any]:
        """Publish a new version of a path, a new path when base_version is 0

        Over a tombstone, base_version is the tombstone's version and the path comes
        back.

        Raises:
            ConfigConflictError: base_version is not the current version
            ConfigInvalidError: the content is not an engine configuration
            ValueError: bad path or service, or no service
        """
        return self._publish(path, content, services, author, comment, base_version)

    def delete(
        self, path: str, author: str, base_version: int, comment: str = ""
    ) -> Dict[str, Any]:
        """Publish a tombstone: the services stop loading the path

        Raises:
            ConfigNotFoundError: when the path does not exist or is deleted already
            ConfigConflictError: base_version is not the current version
        """
        current = self.get(path)
        if current is None or current["deleted"]:
            raise ConfigNotFoundError(f"{path} does not exist")
        return self._publish(
            path,
            current["content"],
            current["services"],
            author,
            comment,
            base_version,
            deleted=True,
        )

    def rollback(
        self,
        path: str,
        version: int,
        author: str,
        base_version: int,
        comment: str = "",
    ) -> Dict[str, Any]:
        """Publish the content, services and deleted flag of an older version again

        Raises:
            ConfigNotFoundError: when the version does not exist
            ConfigConflictError: base_version is not the current version
        """
        target = self.version(path, version)
        return self._publish(
            path,
            target["content"],
            target["services"],
            author,
            comment or f"rollback to version {version}",
            base_version,
            deleted=target["deleted"],
            based_on=version,
        )
