"""In-memory stand-ins for the OpenSearch client, as far as maas_engine.config uses it"""

import copy

from opensearchpy.exceptions import ConflictError, NotFoundError


class FakeIndices:
    """client.indices"""

    def __init__(self, client):
        self.client = client

    def exists(self, index):
        return index in self.client.data

    def create(self, index, body=None):
        self.client.data.setdefault(index, {})
        self.client.bodies[index] = body


class FakeOpenSearch:
    """documents by index and id; searches understand bool filters of terms"""

    def __init__(self):
        self.data = {}
        self.bodies = {}
        self.indices = FakeIndices(self)
        # set to an exception to make every call raise it
        self.failure = None

    def _check(self):
        if self.failure is not None:
            raise self.failure

    def index(self, index, id, body, op_type="index", refresh=None):
        # pylint: disable=redefined-builtin,unused-argument
        self._check()
        documents = self.data.setdefault(index, {})
        if op_type == "create" and id in documents:
            raise ConflictError(409, "version_conflict_engine_exception", {})
        documents[id] = copy.deepcopy(body)
        return {"_id": id, "result": "created"}

    def update(self, index, id, body, refresh=None):
        # pylint: disable=redefined-builtin,unused-argument
        self._check()
        try:
            self.data[index][id].update(copy.deepcopy(body["doc"]))
        except KeyError as error:
            raise NotFoundError(404, "document_missing_exception", {}) from error

    def search(self, index, body):
        self._check()
        filters = body.get("query", {}).get("bool", {}).get("filter", [])
        hits = []
        for doc_id, doc in self.data.get(index, {}).items():
            if all(
                doc.get(field) == value
                or (isinstance(doc.get(field), list) and value in doc[field])
                for term in filters
                for field, value in term["term"].items()
            ):
                hits.append({"_id": doc_id, "_source": copy.deepcopy(doc)})
        return {"hits": {"hits": hits[: body.get("size", 10)]}}
