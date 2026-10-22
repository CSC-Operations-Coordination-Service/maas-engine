"""The replay request and its parsing"""

import datetime
import json

import pytest

from maas_engine.replay.request import (
    ReplayRequest,
    format_date,
    load_body,
    operation_id_of,
)

BODY = {
    "operation_id": "op-1",
    "interface_name": "PRIP_S3A_ACRI",
    "start_date": "2026-10-01T00:00:00Z",
    "end_date": "2026-10-02T00:00:00Z",
    "requested_by": "bob",
    "lane": "short",
}


def test_a_body_in_any_form_gives_the_same_request():
    from_dict = ReplayRequest.from_body(BODY)
    assert ReplayRequest.from_body(json.dumps(BODY)) == from_dict
    assert ReplayRequest.from_body(json.dumps(BODY).encode()) == from_dict
    assert from_dict.operation_id == "op-1"
    assert from_dict.interface_name == "PRIP_S3A_ACRI"
    assert from_dict.requested_by == "bob"
    assert from_dict.lane == "short"
    assert from_dict.start_date == datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
    assert from_dict.window == datetime.timedelta(days=1)


def test_dates_are_taken_as_utc():
    naive = ReplayRequest.from_body({**BODY, "start_date": "2026-10-01T00:00:00"})
    assert naive.start_date.tzinfo == datetime.UTC
    other = ReplayRequest.from_body({**BODY, "start_date": "2026-10-01T02:00:00+02:00"})
    assert other.start_date == datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)


def test_the_optional_fields_default_and_the_unknown_ones_are_kept():
    minimal = {k: v for k, v in BODY.items() if k not in ("requested_by", "lane")}
    request = ReplayRequest.from_body({**minimal, "product": "X", "n": 3})
    assert request.requested_by == ""
    assert request.lane == ""
    assert request.extra == {"product": "X", "n": 3}


def test_an_operation_id_may_start_with_a_dash():
    # it must reach the handler as it is: a handler that passes it on the command
    # line is the one that has to take care of the dash
    assert ReplayRequest.from_body({**BODY, "operation_id": "-x"}).operation_id == "-x"


@pytest.mark.parametrize(
    "name", ["operation_id", "interface_name", "start_date", "end_date"]
)
def test_a_missing_field_is_refused(name):
    body = {k: v for k, v in BODY.items() if k != name}
    with pytest.raises(ValueError, match=name):
        ReplayRequest.from_body(body)


@pytest.mark.parametrize("value", ["", 3, None])
def test_an_empty_or_non_string_field_is_refused(value):
    with pytest.raises(ValueError, match="interface_name"):
        ReplayRequest.from_body({**BODY, "interface_name": value})


def test_a_window_that_does_not_go_forward_is_refused():
    with pytest.raises(ValueError, match="before"):
        ReplayRequest.from_body({**BODY, "end_date": BODY["start_date"]})
    with pytest.raises(ValueError, match="before"):
        ReplayRequest.from_body({**BODY, "end_date": "2026-09-01T00:00:00Z"})


def test_a_bad_date_is_refused():
    with pytest.raises(ValueError, match="start_date"):
        ReplayRequest.from_body({**BODY, "start_date": "yesterday"})


@pytest.mark.parametrize("body", [b"\xff\xfe", "not json", "[1]", 3, None])
def test_a_body_that_is_not_a_json_object_is_refused(body):
    with pytest.raises(ValueError):
        load_body(body)


def test_the_operation_id_of_a_body_that_failed_to_parse():
    assert operation_id_of(json.dumps({"operation_id": "op-9"})) == "op-9"
    assert operation_id_of({"operation_id": 4}) == ""
    assert operation_id_of(b"\xff") == ""
    assert operation_id_of("nope") == ""


def test_dates_are_formatted_with_a_z():
    assert format_date(datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)) == (
        "2026-10-01T00:00:00Z"
    )
