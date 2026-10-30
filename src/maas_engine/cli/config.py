"""
Administration of the engine configurations stored in OpenSearch.

    python -m maas_engine.cli.config <command> ...

The OpenSearch connection comes from the same settings as the engines: ES_URL,
ES_USERNAME and ES_PASSWORD, or ES_VAULT_PATH with the VAULT_* settings. Writing
needs an OpenSearch user allowed to write to the configuration index. The day-to-day
edits are made in maas-operator; this is for the first import and for scripts.
"""

import argparse
import difflib
import getpass
import json
import logging
import os
import sys
from typing import Callable, Dict, List, Optional

from opensearchpy import OpenSearch

from maas_engine.cli import args as engine_args
from maas_engine.cli.log import setup_logging
from maas_engine.config.catalogue import Catalogue
from maas_engine.config.store import (
    ConfigConflictError,
    ConfigInvalidError,
    ConfigNotFoundError,
    EngineConfigStore,
)

__all__ = ["main"]


def build_parser() -> argparse.ArgumentParser:
    """the command line of the configuration CLI"""
    parser = argparse.ArgumentParser(
        prog="python -m maas_engine.cli.config",
        description="Engine configurations stored in OpenSearch",
        parents=[
            engine_args.log_parser(),
            engine_args.es_parser(),
            engine_args.vault_parser(),
            engine_args.config_index_parser(),
        ],
    )
    parser.add_argument(
        "--author", help="who writes (default: $USER, else the login name)"
    )
    parser.add_argument("--comment", default="", help="why")

    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser(
        "init-index", help="create the configuration and catalogue indices"
    )
    commands.add_parser(
        "list", help="current version of every path, tombstones included"
    )

    show = commands.add_parser("show", help="print a version")
    show.add_argument("path")
    show.add_argument("--version", type=int, dest="version_number")

    history = commands.add_parser("history", help="every version of a path")
    history.add_argument("path")

    diff = commands.add_parser("diff", help="compare two versions of a path")
    diff.add_argument("path")
    diff.add_argument("from_version", type=int)
    diff.add_argument("to_version", type=int)

    put = commands.add_parser("put", help="publish a file as the next version")
    put.add_argument("path")
    put.add_argument("file", type=argparse.FileType("r", encoding="utf-8"))
    put.add_argument("--services", nargs="+", required=True)
    put.add_argument("--base-version", type=int, required=True)

    delete = commands.add_parser("delete", help="publish a tombstone")
    delete.add_argument("path")
    delete.add_argument("--base-version", type=int, required=True)

    rollback = commands.add_parser("rollback", help="publish an older version again")
    rollback.add_argument("path")
    rollback.add_argument("version_number", metavar="version", type=int)
    rollback.add_argument("--base-version", type=int, required=True)

    imports = commands.add_parser(
        "import", help="publish the files of a directory that the services map names"
    )
    imports.add_argument("directory")
    imports.add_argument(
        "--update",
        action="store_true",
        help="also publish over a path whose current version differs; without it, such"
        " a path is only reported, so that an import never undoes the edits made in"
        " maas-operator",
    )
    imports.add_argument(
        "--services-map",
        required=True,
        type=argparse.FileType("r", encoding="utf-8"),
        help="JSON object: path relative to the directory -> list of services",
    )

    return parser


def default_author() -> str:
    """$USER, else the login name; a container user may have neither"""
    if os.environ.get("USER"):
        return os.environ["USER"]
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return "unknown"


def _row(doc: dict) -> str:
    """one line of a listing"""
    state = "deleted" if doc["deleted"] else "current"
    return (
        f"{doc['path']}@{doc['version']} {state} services={','.join(doc['services'])}"
        f" by {doc['author']} at {doc['created_at']}"
    )


def _text(doc: dict) -> List[str]:
    """lines of the content of a version"""
    return json.dumps(doc["content"], indent=2, sort_keys=True).splitlines()


def import_directory(
    store: EngineConfigStore,
    directory: str,
    services_map: Dict[str, List[str]],
    author: str,
    comment: str,
    out: Callable[[str], None] = print,
    update: bool = False,
) -> int:
    """Publish the files the services map names that the index does not hold yet

    A path whose current version differs (content, services, or a tombstone) is
    reported, and published only with update: the index is the reference once
    imported, and maas-operator edits it.

    Returns:
        int: number of versions published
    """
    published = 0
    for path, services in sorted(services_map.items()):
        with open(os.path.join(directory, path), encoding="utf-8") as config_file:
            content = json.load(config_file)
        current = store.get(path)
        if (
            current is not None
            and not current["deleted"]
            and current["content"] == content
            and current["services"] == sorted(set(services))
        ):
            out(f"{path}: unchanged (version {current['version']})")
            continue
        if current is not None and not update:
            out(f"{path}: differs from version {current['version']}, not published")
            continue
        doc = store.put(
            path,
            content,
            services,
            author,
            comment or "imported",
            base_version=current["version"] if current else 0,
        )
        out(f"{path}: published version {doc['version']}")
        published += 1
    return published


def run(namespace: argparse.Namespace, client, out: Callable[[str], None] = print):
    """run a parsed command against a client"""
    # pylint: disable=too-many-branches
    store = EngineConfigStore(client, namespace.config_index)
    author, comment = namespace.author or default_author(), namespace.comment

    if namespace.command == "init-index":
        store.ensure_index()
        Catalogue(client, namespace.catalogue_index).ensure_index()
        out(f"{namespace.config_index} and {namespace.catalogue_index} ready")
    elif namespace.command == "list":
        for doc in store.current():
            out(_row(doc))
    elif namespace.command == "show":
        doc = (
            store.version(namespace.path, namespace.version_number)
            if namespace.version_number
            else store.get(namespace.path)
        )
        if doc is None:
            raise ConfigNotFoundError(f"{namespace.path} does not exist")
        out(json.dumps(doc, indent=2))
    elif namespace.command == "history":
        for doc in store.history(namespace.path):
            out(_row(doc))
    elif namespace.command == "diff":
        before = store.version(namespace.path, namespace.from_version)
        after = store.version(namespace.path, namespace.to_version)
        for line in difflib.unified_diff(
            _text(before),
            _text(after),
            f"{namespace.path}@{namespace.from_version}",
            f"{namespace.path}@{namespace.to_version}",
            lineterm="",
        ):
            out(line)
    elif namespace.command == "put":
        doc = store.put(
            namespace.path,
            json.load(namespace.file),
            namespace.services,
            author,
            comment,
            namespace.base_version,
        )
        out(_row(doc))
    elif namespace.command == "delete":
        out(_row(store.delete(namespace.path, author, namespace.base_version, comment)))
    elif namespace.command == "rollback":
        doc = store.rollback(
            namespace.path,
            namespace.version_number,
            author,
            namespace.base_version,
            comment,
        )
        out(_row(doc))
    elif namespace.command == "import":
        count = import_directory(
            store,
            namespace.directory,
            json.load(namespace.services_map),
            author,
            comment,
            out,
            namespace.update,
        )
        out(f"{count} version(s) published")


def main(argv: Optional[List[str]] = None) -> int:
    """entry point

    Returns:
        int: exit code, 1 when the command was refused
    """
    namespace = build_parser().parse_args(argv)
    setup_logging(loglevel=namespace.loglevel or logging.WARNING)

    client = OpenSearch(
        hosts=[engine_args.get_es_credentials_url(namespace)],
        timeout=namespace.es_timeout,
        verify_certs=not namespace.es_ignore_certs_verification,
        ssl_show_warn=not namespace.es_ignore_certs_verification,
    )

    try:
        run(namespace, client)
    except ConfigInvalidError as error:
        for problem in error.problems:
            print(f"invalid: {problem}", file=sys.stderr)
        return 1
    except (ConfigConflictError, ConfigNotFoundError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
