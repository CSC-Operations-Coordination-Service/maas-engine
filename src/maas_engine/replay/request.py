"""The replay request a backend publishes"""

import datetime
import json
from dataclasses import dataclass, field

# fields the worker itself reads; any other field of the body is kept in `extra`
KNOWN_FIELDS = (
    "operation_id",
    "interface_name",
    "start_date",
    "end_date",
    "requested_by",
    "lane",
)


def load_body(body) -> dict:
    """Decode a message body into a dict

    Raises:
        ValueError: not UTF-8, not JSON, or not a JSON object
    """
    if isinstance(body, (bytes, bytearray)):
        try:
            body = bytes(body).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(f"invalid body encoding: {error}") from error
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid JSON body: {error}") from error
    if not isinstance(body, dict):
        raise ValueError("body must be a JSON object")
    return body


def operation_id_of(body) -> str:
    """operation_id of a body that failed to parse, empty when it has none"""
    try:
        data = load_body(body)
    except ValueError:
        return ""
    value = data.get("operation_id")
    return value if isinstance(value, str) else ""


def _required(data: dict, name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing or invalid field: {name}")
    return value


def _parse_date(data: dict, name: str) -> datetime.datetime:
    """Parse ISO 8601; naive values are taken as UTC, others converted to UTC"""
    value = _required(data, name)
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"invalid date in {name}: {value}") from error
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.UTC)
    return parsed.astimezone(datetime.UTC)


def format_date(value: datetime.datetime) -> str:
    """ISO 8601 UTC with a Z suffix"""
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value.isoformat() + "Z"


@dataclass(frozen=True)
class ReplayRequest:
    """A request to replay an interface over a window

    Attributes:
        operation_id: id of the operation record the backend wrote
        interface_name: what to replay
        start_date: start of the window, UTC
        end_date: end of the window, UTC, after start_date
        requested_by: who asked
        lane: lane label the backend chose; the queue the request came from
            decides, not this label
        extra: every other field of the body, for the handler
    """

    operation_id: str
    interface_name: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    requested_by: str = ""
    lane: str = ""
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_body(cls, body) -> "ReplayRequest":
        """Build from a message body

        Raises:
            ValueError: missing field or bad date, or start_date not before end_date
        """
        data = load_body(body)
        operation_id = _required(data, "operation_id")
        interface_name = _required(data, "interface_name")
        start = _parse_date(data, "start_date")
        end = _parse_date(data, "end_date")
        if start >= end:
            raise ValueError("start_date must be before end_date")
        return cls(
            operation_id=operation_id,
            interface_name=interface_name,
            start_date=start,
            end_date=end,
            requested_by=data.get("requested_by") or "",
            lane=data.get("lane") or "",
            extra={k: v for k, v in data.items() if k not in KNOWN_FIELDS},
        )

    @property
    def window(self) -> datetime.timedelta:
        """length of the window"""
        return self.end_date - self.start_date
