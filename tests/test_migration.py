"""Tests for maas_engine.update.migration helpers"""

import argparse
import logging
from unittest.mock import Mock

from maas_engine.update import migration
from maas_engine.update.migration import MaasMigrator


def test_setup_never_logs_the_opensearch_password(tmp_path, monkeypatch, caplog):
    """the connection url carries the credentials: only a masked form is logged"""
    (tmp_path / MaasMigrator.TEMPLATE_DIR).mkdir()
    args = argparse.Namespace(
        dry_run=False,
        resources=str(tmp_path),
        es_url="https://opensearch.test:9200/osrc/",
        es_username="admin",
        es_password="s3cret-pw",
        es_retries=1,
        es_timeout=1,
        es_ignore_certs_verification=True,
    )
    connection = Mock()
    connection.indices.get_alias.return_value = {}
    monkeypatch.setattr(
        migration.db_connections, "create_connection", lambda **kwargs: connection
    )

    with caplog.at_level(logging.INFO):
        MaasMigrator(args).setup()

    assert "s3cret-pw" not in caplog.text
    assert "https://admin:**@opensearch.test:9200/osrc/" in caplog.text


def test_strip_field_descriptions():
    """the maas-only "description" metadata is removed recursively while the
    rest of the mapping (type, meta, nested properties) is left untouched"""
    properties = {
        "mission": {"type": "keyword", "description": "Mission identifier"},
        "count": {"type": "integer"},
        "meta_obj": {
            "type": "object",
            "description": "Nested metadata block",
            "properties": {
                "inner": {"type": "keyword", "description": "Inner value"},
                "kept_meta": {
                    "type": "keyword",
                    "meta": {"_specific": "S1"},
                },
            },
        },
    }

    MaasMigrator.strip_field_descriptions(properties)

    assert properties == {
        "mission": {"type": "keyword"},
        "count": {"type": "integer"},
        "meta_obj": {
            "type": "object",
            "properties": {
                "inner": {"type": "keyword"},
                "kept_meta": {"type": "keyword", "meta": {"_specific": "S1"}},
            },
        },
    }
