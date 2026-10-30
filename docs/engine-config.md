# Engine configuration in OpenSearch

An engine reads its configuration from JSON files (`-c`, `-d`), or, with
`--config-backend opensearch`, from the OpenSearch index `maas-engine-config`. The
index is what maas-operator edits: an operator changes an engine's configuration from
Grafana, and the engine picks the change up by itself. It mirrors what the collectors
do with `maas-collector-config`.

## The index

Every version of a configuration file is a document `<path>@<version>`: the path is
the file's path under `/conf/engine/` in a file deployment (`specific/cds-query.json`),
and the content is the file's JSON. The `latest` flag marks the current version of
each path. A document names the services that load the file (`services`); `*` means
every service, as for the shared `default/*.json`.

Removing a file publishes a tombstone (`deleted`): the services stop loading it, its
history stays, and a rollback (publishing an older version again) brings it back.

`maas_engine.config.store.EngineConfigStore` is the only writer, used by maas-operator
and by the admin CLI:

    python -m maas_engine.cli.config init-index
    python -m maas_engine.cli.config import configs/sources --services-map services.json
    python -m maas_engine.cli.config list
    python -m maas_engine.cli.config history specific/cds-query.json
    python -m maas_engine.cli.config diff specific/cds-query.json 1 2
    python -m maas_engine.cli.config put specific/cds-query.json cds-query.json \
        --services query-01 --base-version 2 --comment "why"
    python -m maas_engine.cli.config delete specific/cds-query.json --base-version 3
    python -m maas_engine.cli.config rollback specific/cds-query.json 2 --base-version 4

A write names the version it started from (`--base-version`, 0 for a new path): a
version published in the meantime makes it fail, never a silent overwrite. `import`
publishes each file of the services map (`{"default/model.json": ["*"],
"specific/cds-query.json": ["query-01"]}`) unless its current version already has the
same content and services, so it can run again. The OpenSearch connection comes from
the engine's options (`ES_URL`... or `ES_VAULT_PATH` with the `VAULT_*` settings).

## An engine on the index

| option | variable | default |
| --- | --- | --- |
| `--config-backend` | `CONFIG_BACKEND` | `file`; `opensearch` reads the index |
| `--config-service` | `CONFIG_SERVICE` | none, required with `opensearch`: the service name the files are served to |
| `--config-index` | `CONFIG_INDEX` | `maas-engine-config` |
| `--config-refresh-period` | `CONFIG_REFRESH_PERIOD` | `300` seconds |
| `--catalogue-index` | `ENGINE_CATALOGUE_INDEX` | `maas-engine-catalogue` |

The engine loads the current, non-deleted versions served to its service, in path
order, as `-d` loads the files of a directory.

- **A change.** Every refresh period the engine looks at the index. A new set it can
  load (its structure is right, its modules and model import, its engine ids exist):
  it logs `configuration changed: ..., restarting`, stops consuming and exits with
  code 75. Swarm (`restart_policy: any`) restarts it on the new versions. A set it
  cannot load: it logs `configuration change rejected: ...`, records the reason in its
  catalogue entry, and keeps running on what it has. Undoing the change clears the
  rejection.
- **Startup with a bad current version** (saved while the engine was down): the engine
  falls back to the versions it last loaded, as its catalogue entry records them,
  records the rejection, and runs.
- **Startup with nothing to load** (a new service, or one whose files were removed):
  the engine does not exit. Its catalogue entry says `waiting`, its health check
  answers, and it looks at the index every refresh period until a configuration
  appears.
- **OpenSearch out of reach at startup**: the engine stops, and swarm retries.

## The catalogue

Each engine service overwrites one document of `maas-engine-catalogue`, whose id is its
service name:

| field | |
| --- | --- |
| `state` | `waiting` or `running` |
| `started_at`, `seen_at`, `refresh_period` | `seen_at` is refreshed at every check: a service not seen for three periods is not running |
| `versions` | maas-engine, maas-model and the model's package (e.g. `maas-cds 2.19.0`) |
| `engines` | the engine ids the image registers, with their class and summary |
| `document_classes` | the classes of the model, with their parent classes and the index a search on them uses |
| `loaded` | `{path: version}` the engine runs on |
| `current` | `{path: version}` the index held at the last check |
| `rejected` | `null`, or the versions last rejected, the reason and when |

maas-operator reads it to offer engine ids and document classes, and to show whether a
change took effect. A failed write is logged and does not stop the engine.
