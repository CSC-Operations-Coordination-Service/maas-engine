"""The configuration CLI"""

import json

import pytest

from maas_engine.cli import config as cli
from maas_engine.config.store import (
    ConfigConflictError,
    ConfigNotFoundError,
    EngineConfigStore,
)

from fakes import FakeOpenSearch

DEFAULTS = {"model": "maas_model"}
QUERY = {
    "amqp": [
        {
            "name": "etl-exchange",
            "queues": [
                {"name": "q", "routing_key": "query.cds", "events": ["QUERY_ENGINE"]}
            ],
        }
    ]
}


@pytest.fixture(name="client")
def fixture_client():
    return FakeOpenSearch()


def run(client, *argv):
    """run a command, returning what it printed"""
    lines = []
    cli.run(
        cli.build_parser().parse_args(["--author", "alice", *argv]),
        client,
        lines.append,
    )
    return lines


@pytest.fixture(name="tree")
def fixture_tree(tmp_path):
    (tmp_path / "default").mkdir()
    (tmp_path / "specific").mkdir()
    (tmp_path / "default" / "model.json").write_text(json.dumps(DEFAULTS))
    (tmp_path / "specific" / "cds-query.json").write_text(json.dumps(QUERY))
    services = tmp_path / "services.json"
    services.write_text(
        json.dumps(
            {"default/model.json": ["*"], "specific/cds-query.json": ["query-01"]}
        )
    )
    return tmp_path


def test_import_publishes_once(client, tree):
    argv = ["import", str(tree), "--services-map", str(tree / "services.json")]

    assert run(client, *argv)[-1] == "2 version(s) published"
    assert run(client, *argv) == [
        "default/model.json: unchanged (version 1)",
        "specific/cds-query.json: unchanged (version 1)",
        "0 version(s) published",
    ]

    (tree / "specific" / "cds-query.json").write_text(json.dumps({**QUERY, **DEFAULTS}))
    assert run(client, *argv)[1] == (
        "specific/cds-query.json: differs from version 1, not published"
    )
    assert run(client, *argv, "--update")[1] == (
        "specific/cds-query.json: published version 2"
    )


def test_commands(client, tree):
    assert run(client, "init-index") == [
        "maas-engine-config and maas-engine-catalogue ready"
    ]
    assert client.indices.exists(index="maas-engine-catalogue")

    path = str(tree / "specific" / "cds-query.json")
    out = run(
        client,
        "put",
        "specific/x.json",
        path,
        "--services",
        "query-01",
        "--base-version",
        "0",
    )
    assert out[0].startswith("specific/x.json@1 current services=query-01 by alice")

    with pytest.raises(ConfigConflictError):
        run(
            client,
            "put",
            "specific/x.json",
            path,
            "--services",
            "q",
            "--base-version",
            "0",
        )

    run(client, "delete", "specific/x.json", "--base-version", "1")
    assert run(client, "list")[0].startswith("specific/x.json@2 deleted")
    run(client, "rollback", "specific/x.json", "1", "--base-version", "2")
    assert len(run(client, "history", "specific/x.json")) == 3
    assert (
        json.loads("\n".join(run(client, "show", "specific/x.json", "--version", "1")))[
            "version"
        ]
        == 1
    )
    assert run(client, "diff", "specific/x.json", "1", "3") == []

    with pytest.raises(ConfigNotFoundError):
        run(client, "show", "nothing.json")

    assert EngineConfigStore(client).get("specific/x.json")["version"] == 3


def test_main_reports_refusals(monkeypatch, client, capsys):
    monkeypatch.setattr(cli, "OpenSearch", lambda **_kwargs: client)

    code = cli.main(
        ["--es-url", "http://os:9200", "delete", "nothing.json", "--base-version", "0"]
    )

    assert code == 1
    assert "nothing.json does not exist" in capsys.readouterr().err
