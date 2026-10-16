"""publish_message_folder marks the messages it re-publishes as replays."""

from unittest import mock

from maas_engine.cli import publish_message_folder as module

BODY = '{"hello": "world"}'


def run_cli(tmp_path, monkeypatch):
    (tmp_path / "a.json").write_text(BODY, encoding="utf-8")
    producer = mock.Mock()
    # the capture fixture owns telemetry: do not reconfigure or shut it down
    monkeypatch.setattr(module, "setup_telemetry", mock.Mock(return_value=True))
    monkeypatch.setattr(module.telemetry, "shutdown", mock.Mock())
    with mock.patch("kombu.Connection"), mock.patch(
        "kombu.Producer", return_value=producer
    ):
        module.publish_message_folder_main(["ex", "rk", str(tmp_path)])
    return producer


def test_replay_header_and_body(telemetry_capture, tmp_path, monkeypatch):
    producer = run_cli(tmp_path, monkeypatch)
    producer.publish.assert_called_once()
    args, kwargs = producer.publish.call_args
    assert args[0] == BODY
    assert kwargs["headers"]["maas.replay"] == "true"


def test_replay_span(telemetry_capture, tmp_path, monkeypatch):
    producer = run_cli(tmp_path, monkeypatch)
    span = telemetry_capture.span("publish ex")
    assert span.attributes["maas.replay"] is True
    headers = producer.publish.call_args.kwargs["headers"]
    assert "traceparent" in headers
    module.telemetry.shutdown.assert_called_once()
    module.setup_telemetry.assert_called_once()


def test_disabled_still_marks_replay(tmp_path, monkeypatch):
    producer = run_cli(tmp_path, monkeypatch)
    assert producer.publish.call_args.kwargs["headers"] == {"maas.replay": "true"}
