"""
The catalogue: what each engine service can run and what it runs on.

Each engine service overwrites one document of the maas-engine-catalogue index,
whose id is its service name: the engines its image registers, the document classes
of its model, the configuration versions it loaded, the last ones it rejected and
why, and when it was last seen. maas-operator reads the documents to offer engine ids
and document classes, and to show whether a configuration change took effect.
"""

import datetime
import importlib.metadata
import inspect
import logging
import socket
from types import ModuleType
from typing import Any, Dict, List, Optional

import maas_model
from maas_model.document import MAASDocument

import maas_engine
from maas_engine.engine.base import Engine

__all__ = [
    "CATALOGUE_INDEX",
    "CATALOGUE_INDEX_BODY",
    "RUNNING",
    "WAITING",
    "Catalogue",
    "describe_model",
    "document_class_entries",
    "engine_entries",
    "new_entry",
]

LOGGER = logging.getLogger("Catalogue")

CATALOGUE_INDEX = "maas-engine-catalogue"

WAITING = "waiting"
RUNNING = "running"

CATALOGUE_INDEX_BODY = {
    "settings": {"number_of_shards": 1},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "service": {"type": "keyword"},
            "host": {"type": "keyword"},
            "state": {"type": "keyword"},
            "started_at": {"type": "date"},
            "seen_at": {"type": "date"},
            "refresh_period": {"type": "integer"},
            # the rest is read whole by maas-operator: stored, not indexed
            "versions": {"type": "object", "enabled": False},
            "engines": {"type": "object", "enabled": False},
            "document_classes": {"type": "object", "enabled": False},
            "loaded": {"type": "object", "enabled": False},
            "current": {"type": "object", "enabled": False},
            "rejected": {"type": "object", "enabled": False},
        },
    },
}


def _summary(obj) -> str:
    """first line of a docstring"""
    doc = inspect.getdoc(obj) or ""
    return doc.splitlines()[0] if doc else ""


def engine_entries() -> List[Dict[str, str]]:
    """the engines registered in this process, by id"""
    return [
        {
            "id": engine_id,
            "implementation": f"{cls.__module__}.{cls.__qualname__}",
            "summary": _summary(cls),
        }
        for engine_id, cls in sorted(Engine.registered().items())
    ]


def document_class_entries(model_module: Optional[ModuleType]) -> List[Dict[str, Any]]:
    """the document classes of a model module, by name

    The index is the expression a search on the class uses, the one the query engine
    scans, so a count on it is the count the engine will see.
    """
    if model_module is None:
        return []

    entries = []
    for name, cls in sorted(vars(model_module).items()):
        if not (
            inspect.isclass(cls)
            and issubclass(cls, MAASDocument)
            and cls is not MAASDocument
            and getattr(cls.Index, "name", "")
        ):
            continue
        entries.append(
            {
                "name": name,
                "bases": [
                    base.__name__
                    for base in cls.__mro__[1:]
                    if issubclass(base, MAASDocument) and base is not MAASDocument
                ],
                # pylint: disable-next=protected-access
                "index": ",".join(cls.search()._index),
            }
        )
    return entries


def _model_package(model_module: Optional[ModuleType]) -> Optional[str]:
    """'<distribution> <version>' of the package holding the model, when known"""
    if model_module is None:
        return None
    top = model_module.__name__.split(".")[0]
    for dist in importlib.metadata.packages_distributions().get(top, []):
        return f"{dist} {importlib.metadata.version(dist)}"
    return None


def now() -> str:
    """the current time, as the documents store it"""
    return datetime.datetime.now(tz=datetime.UTC).isoformat()


def new_entry(service: str, refresh_period: int) -> Dict[str, Any]:
    """the entry of a service starting now, waiting for its configuration"""
    started = now()
    return {
        "service": service,
        "host": socket.gethostname(),
        "state": WAITING,
        "started_at": started,
        "seen_at": started,
        "refresh_period": refresh_period,
        "versions": {
            "maas_engine": maas_engine.__version__,
            "maas_model": maas_model.__version__,
            "model_package": None,
        },
        "engines": engine_entries(),
        "document_classes": [],
        "loaded": {},
        "current": {},
        "rejected": None,
    }


def describe_model(entry: Dict[str, Any], model_module: Optional[ModuleType]) -> None:
    """fill an entry with what a loaded configuration made known: the engines of
    its modules and the classes of its model"""
    entry["engines"] = engine_entries()
    entry["document_classes"] = document_class_entries(model_module)
    entry["versions"]["model_package"] = _model_package(model_module)


class Catalogue:
    """The catalogue documents, one per engine service"""

    def __init__(self, client, index: str = CATALOGUE_INDEX):
        """
        Args:
            client (opensearchpy.OpenSearch): connected client
            index (str, optional): index name. Defaults to CATALOGUE_INDEX.
        """
        self.client = client
        self.index = index

    def ensure_index(self) -> None:
        """create the index when it does not exist"""
        if not self.client.indices.exists(index=self.index):
            self.client.indices.create(index=self.index, body=CATALOGUE_INDEX_BODY)
            LOGGER.info("Created index %s", self.index)

    def write(self, entry: Dict[str, Any]) -> bool:
        """overwrite the entry of a service. A failure is logged, never raised: an
        engine keeps running without its catalogue entry

        Returns:
            bool: whether the entry was written
        """
        try:
            self.ensure_index()
            self.client.index(
                index=self.index, id=entry["service"], body=entry, refresh="wait_for"
            )
            return True
        except Exception:  # pylint: disable=broad-exception-caught
            LOGGER.warning(
                "Cannot write the catalogue entry of %s",
                entry["service"],
                exc_info=True,
            )
            return False

    def read(self, service: str) -> Optional[Dict[str, Any]]:
        """the entry of a service, None when it has none"""
        if not self.client.indices.exists(index=self.index):
            return None
        response = self.client.search(
            index=self.index,
            body={"query": {"bool": {"filter": [{"term": {"service": service}}]}}},
        )
        hits = response["hits"]["hits"]
        return hits[0]["_source"] if hits else None

    def entries(self) -> List[Dict[str, Any]]:
        """every entry, by service"""
        if not self.client.indices.exists(index=self.index):
            return []
        response = self.client.search(
            index=self.index,
            body={"query": {"bool": {"filter": []}}, "size": 10000},
        )
        return sorted(
            (hit["_source"] for hit in response["hits"]["hits"]),
            key=lambda entry: entry["service"],
        )
