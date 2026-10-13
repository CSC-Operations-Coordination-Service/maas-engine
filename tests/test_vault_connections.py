"""Tests for the OpenSearch and AMQP connections read from OpenBao / Vault"""

import logging
from unittest.mock import Mock
from urllib.parse import quote

import pytest
from kombu import Connection
from opensearchpy import OpenSearch

from maas_engine.cli.args import (
    engine_parser,
    get_amqp_credentials_url,
    get_es_credentials_url,
)
from maas_engine.consumer.engine_consumer import MaasEngineConsumer
from maas_engine.update import migration
from maas_engine.update.migration import MaasMigrator, migration_main

OPENSEARCH = {
    "url": "https://opensearch.example:9200",
    "username": "maas",
    "password": "s3cr3t-opensearch",
}

AMQP = {
    "url": "amqp://rabbit.example:5672//",
    "username": "maas",
    "password": "s3cr3t-amqp",
}

ES_URL = "https://maas:s3cr3t-opensearch@opensearch.example:9200"

AMQP_URL = "amqp://maas:s3cr3t-amqp@rabbit.example:5672//"

CONNECTION_ENV = (
    "ES_URL",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_VAULT_PATH",
    "AMQP_URL",
    "AMQP_USERNAME",
    "AMQP_PASSWORD",
    "AMQP_VAULT_PATH",
    "VAULT_ADDR",
    "VAULT_KV_MOUNT",
    "VAULT_AUTH_METHOD",
    "VAULT_ROLE_ID",
    "VAULT_SECRET_ID_FILE",
    "VAULT_TOKEN_FILE",
    "VAULT_TOKEN",
)


@pytest.fixture
def vault_env(fake_vault, monkeypatch, tmp_path):
    """environment of an engine reading its connections from the fake vault"""
    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)

    secret_id_file = tmp_path / "secret_id"
    secret_id_file.write_text(fake_vault.secret_id + "\n", encoding="UTF-8")

    monkeypatch.setenv("VAULT_ADDR", fake_vault.url)
    monkeypatch.setenv("VAULT_ROLE_ID", fake_vault.role_id)
    monkeypatch.setenv("VAULT_SECRET_ID_FILE", str(secret_id_file))

    fake_vault.put("secret", "maas/cds/opensearch", OPENSEARCH)
    fake_vault.put("secret", "maas/cds/amqp", AMQP)

    return monkeypatch


def parse(argv: list):
    return engine_parser().parse_args(argv)


def test_connections_come_from_the_vault(vault_env):
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    namespace = parse([])

    assert get_es_credentials_url(namespace) == ES_URL
    assert get_amqp_credentials_url(namespace) == AMQP_URL


def test_vault_settings_come_from_the_command_line(vault_env, fake_vault, tmp_path):
    for name in ("VAULT_ADDR", "VAULT_ROLE_ID", "VAULT_SECRET_ID_FILE"):
        vault_env.delenv(name)
    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token, encoding="UTF-8")

    namespace = parse(
        [
            "--es-vault-path",
            "maas/cds/opensearch",
            "--amqp-vault-path",
            "maas/cds/amqp",
            "--vault-addr",
            fake_vault.url,
            "--vault-auth-method",
            "token",
            "--vault-token-file",
            str(token_file),
        ]
    )

    assert get_es_credentials_url(namespace) == ES_URL
    assert get_amqp_credentials_url(namespace) == AMQP_URL


def test_vault_settings_read_under_the_configured_mount(vault_env, fake_vault):
    fake_vault.put("kv", "maas/cds/opensearch", {"url": "https://other:9200"})
    vault_env.setenv("VAULT_KV_MOUNT", "kv")
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")

    assert get_es_credentials_url(parse([])) == "https://other:9200"


def test_vault_connection_replaces_the_environment(vault_env, caplog):
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")
    vault_env.setenv("AMQP_URL", "amqp://old-rabbit:5672//")
    vault_env.setenv("AMQP_USERNAME", "guest")
    vault_env.setenv("AMQP_PASSWORD", "old-plain-text-password")

    with caplog.at_level(logging.WARNING):
        url = get_amqp_credentials_url(parse([]))

    assert url == AMQP_URL

    warnings = " ".join(
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    )
    for name in ("AMQP_URL", "AMQP_USERNAME", "AMQP_PASSWORD"):
        assert name in warnings

    assert "old-plain-text-password" not in caplog.text
    assert "s3cr3t-amqp" not in caplog.text


def test_vault_credentials_reach_the_clients_unchanged(vault_env, fake_vault):
    """the vault holds raw values: a "/", "?" or "#" must not end the host part of
    the url, nor a "%41" turn into "A" """
    username = "maas@cds"
    password = "a/b#c%41?d:e@f"
    fake_vault.put(
        "secret",
        "maas/cds/opensearch",
        {**OPENSEARCH, "username": username, "password": password},
    )
    fake_vault.put(
        "secret", "maas/cds/amqp", {**AMQP, "username": username, "password": password}
    )
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    namespace = parse([])

    opensearch_host = OpenSearch(
        hosts=[get_es_credentials_url(namespace)]
    ).transport.hosts[0]
    assert opensearch_host["host"] == "opensearch.example"
    assert opensearch_host["port"] == 9200
    assert opensearch_host["http_auth"] == f"{username}:{password}"

    amqp = Connection(get_amqp_credentials_url(namespace))
    assert (amqp.hostname, amqp.port) == ("rabbit.example", 5672)
    assert amqp.virtual_host == "/"
    assert (amqp.userid, amqp.password) == (username, password)


def test_environment_credentials_are_used_as_they_are(vault_env):
    """ES_PASSWORD may hold an already encoded value: it is not encoded again"""
    vault_env.setenv("ES_URL", "http://localhost:9200")
    vault_env.setenv("ES_USERNAME", "admin")
    vault_env.setenv("ES_PASSWORD", "a%2Fb")

    assert get_es_credentials_url(parse([])) == "http://admin:a%2Fb@localhost:9200"


def test_vault_connection_without_authentication(vault_env, fake_vault):
    fake_vault.put("secret", "maas/cds/amqp", {"url": "amqp://rabbit.example:5672//"})
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    assert get_amqp_credentials_url(parse([])) == "amqp://rabbit.example:5672//"


def test_missing_vault_secret_stops_the_engine(vault_env):
    vault_env.setenv("ES_VAULT_PATH", "maas/other/opensearch")

    with pytest.raises(ValueError, match="OpenSearch.*maas/other/opensearch"):
        get_es_credentials_url(parse([]))


def test_vault_secret_without_url_stops_the_engine(vault_env, fake_vault):
    fake_vault.put("secret", "maas/cds/amqp", {"username": "maas", "password": "pw"})
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    with pytest.raises(ValueError, match="AMQP.*url"):
        get_amqp_credentials_url(parse([]))


def test_vault_path_needs_the_vault_settings(vault_env):
    vault_env.delenv("VAULT_ROLE_ID")
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")

    with pytest.raises(ValueError, match="VAULT_ROLE_ID"):
        get_es_credentials_url(parse([]))


def test_without_vault_path_the_environment_is_used(vault_env, fake_vault):
    vault_env.setenv("AMQP_URL", "amqp://localhost:5672//")
    vault_env.setenv("AMQP_USERNAME", "guest")
    vault_env.setenv("AMQP_PASSWORD", "guest")

    assert get_amqp_credentials_url(parse([])) == "amqp://guest:guest@localhost:5672//"
    assert fake_vault.logins == 0


def test_consumer_connects_with_the_vault_connections(vault_env):
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")
    started = []
    vault_env.setattr(MaasEngineConsumer, "setup", lambda self: started.append(self))
    vault_env.setattr(MaasEngineConsumer, "run", lambda self: None)

    MaasEngineConsumer.main([])

    assert started[0].args.es_url == ES_URL
    assert started[0].args.amqp_url == AMQP_URL


def test_migration_connects_with_the_vault_connection(vault_env, tmp_path):
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    (tmp_path / MaasMigrator.TEMPLATE_DIR).mkdir()
    connection_kwargs = {}

    def create_connection(**kwargs):
        connection_kwargs.update(kwargs)
        connection = Mock()
        connection.indices.get_alias.return_value = {}
        return connection

    vault_env.setattr(migration.db_connections, "create_connection", create_connection)

    with pytest.raises(SystemExit):
        # --list exits once the templates are listed
        migration_main(["-r", str(tmp_path), "--list"], MaasMigrator)

    assert connection_kwargs["hosts"] == [ES_URL]


def test_migration_log_masks_a_vault_password_with_reserved_characters(
    vault_env, fake_vault, tmp_path, caplog
):
    """encoded, the password stays in the user information of the url, where the
    migration log masks it"""
    password = "a/b#c%41?d:e@f"
    fake_vault.put(
        "secret", "maas/cds/opensearch", {**OPENSEARCH, "password": password}
    )
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    (tmp_path / MaasMigrator.TEMPLATE_DIR).mkdir()
    connection = Mock()
    connection.indices.get_alias.return_value = {}
    vault_env.setattr(
        migration.db_connections, "create_connection", lambda **kwargs: connection
    )

    with caplog.at_level(logging.INFO), pytest.raises(SystemExit):
        migration_main(["-r", str(tmp_path), "--list", "-v"], MaasMigrator)

    assert "https://maas:**@opensearch.example:9200" in caplog.text
    for fragment in (password, "a/b#c", quote(password, safe=""), "a%2Fb"):
        assert fragment not in caplog.text
