"""Updates of the operation records the backend wrote"""

import datetime
import logging
from collections.abc import Callable

from opensearchpy.exceptions import NotFoundError

RUNNING, DONE, FAILED = "RUNNING", "DONE", "FAILED"

LOGGER = logging.getLogger("maas_engine.replay")


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


class OperationRecorder:
    """Update operation records; never raises

    The backend writes the record (PUBLISHED) before it publishes the request, so
    the worker only updates it. A record that is missing is an error, unless
    `create_missing` is set: the record is then created with what is known, which
    is what the download-manager's worker does.
    """

    def __init__(
        self,
        client,
        index: str,
        create_missing: bool = False,
        clock: Callable[[], datetime.datetime] = _now,
    ):
        self.client = client
        self.index = index
        self.create_missing = create_missing
        self.clock = clock

    def update(
        self,
        operation_id: str,
        status: str,
        exit_code: int | None = None,
        details: str | None = None,
    ) -> bool:
        """Set status (and optionally exit_code, details) plus updated_at

        Returns:
            bool: False when the record is missing and not created, or the update failed
        """
        doc = {
            "status": status,
            "updated_at": self.clock()
            .astimezone(datetime.UTC)
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if exit_code is not None:
            doc["exit_code"] = exit_code
        if details is not None:
            doc["details"] = details
        try:
            self.client.update(
                index=self.index,
                id=operation_id,
                body={"doc": doc},
                retry_on_conflict=3,
            )
        except NotFoundError:
            return self._missing(operation_id, doc)
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Cannot update operation record %s", operation_id)
            return False
        return True

    def _missing(self, operation_id: str, doc: dict) -> bool:
        if not self.create_missing:
            LOGGER.error(
                "Operation record %s not found in %s", operation_id, self.index
            )
            return False
        try:
            self.client.index(
                index=self.index,
                id=operation_id,
                body={"message_id": operation_id, **doc},
            )
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Cannot create operation record %s", operation_id)
            return False
        return True
