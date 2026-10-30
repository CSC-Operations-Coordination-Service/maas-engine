# maas-engine

Task runner for maas project.

## Execute

The `maas_engine` module contains the main entry point with the following options:

```console
$ python -m maas_engine -h
usage: __main__.py [-h] [-v] [-vv] [--es-username ES_USERNAME] [--es-password ES_PASSWORD]
                   [--es-url ES_URL] [--es-vault-path ES_VAULT_PATH] [--es-timeout ES_TIMEOUT]
                   [--es-retries ES_RETRIES] [--es-reject-errors]
                   [--es-ignore-certs-verification ES_IGNORE_CERTS_VERIFICATION]
                   [--es-requeue-missing-input ES_REQUEUE_MISSING_INPUT]
                   [--amqp-username AMQP_USERNAME] [--amqp-password AMQP_PASSWORD]
                   [--amqp-url AMQP_URL] [--amqp-vault-path AMQP_VAULT_PATH]
                   [--amqp-retries AMQP_RETRIES] [--amqp-max-priority AMQP_MAX_PRIORITY]
                   [--amqp-heartbeat AMQP_HEARTBEAT] [--vault-addr VAULT_ADDR]
                   [--vault-kv-mount VAULT_KV_MOUNT] [--vault-auth-method {approle,token}]
                   [--vault-role-id VAULT_ROLE_ID] [--vault-secret-id-file VAULT_SECRET_ID_FILE]
                   [--vault-token-file VAULT_TOKEN_FILE] [-f] [--version] [-c CONFIG]
                   [-d CONFIG_DIRECTORY] [--healthcheck-hostname HEALTHCHECK_HOSTNAME]
                   [--healthcheck-port HEALTHCHECK_PORT]

options:
  -h, --help            show this help message and exit
  -v, --verbose         Activate verbose mode
  -vv, --very-verbose   set loglevel to DEBUG
  --es-username ES_USERNAME
                        opensearch user identifier (default: None)
  --es-password ES_PASSWORD
                        opensearch user password (default: None)
  --es-url ES_URL       opensearch cluster URL (default: None)
  --es-vault-path ES_VAULT_PATH
                        Vault secret holding the opensearch url, username and password, in place
                        of --es-url, --es-username and --es-password (default: None)
  --es-timeout ES_TIMEOUT
                        opensearch request timeout in seconds (default: 120)
  --es-retries ES_RETRIES
                        opensearch number of retries (default: 3)
  --es-reject-errors    Reject messages when opensearch errors
  --es-ignore-certs-verification ES_IGNORE_CERTS_VERIFICATION
                        If set, the SSL cert of opensearch is not verified (default: False)
  --es-requeue-missing-input ES_REQUEUE_MISSING_INPUT
                        If set, a message with missing input document will be requeued (default:
                        False)
  --amqp-username AMQP_USERNAME
                        AMQP user name (default: None)
  --amqp-password AMQP_PASSWORD
                        AMQP user password (default: None)
  --amqp-url AMQP_URL   AMQP cluster URL (default: None)
  --amqp-vault-path AMQP_VAULT_PATH
                        Vault secret holding the AMQP url, username and password, in place of
                        --amqp-url, --amqp-username and --amqp-password (default: None)
  --amqp-retries AMQP_RETRIES
                        AMQP number of retries (default: 0). 0 for infinite
  --amqp-max-priority AMQP_MAX_PRIORITY
                        AMQP max priority (default: 10). 1 to 10
  --amqp-heartbeat AMQP_HEARTBEAT
                        AMQP heartbeat timeout in seconds, 0 to disable (default: 600). Must
                        exceed the longest message processing time
  --vault-addr VAULT_ADDR
                        Vault address, like https://vault:8200 (default: )
  --vault-kv-mount VAULT_KV_MOUNT
                        Mount point of the KV version 2 secret engine (default: secret)
  --vault-auth-method {approle,token}
                        Vault authentication method (default: approle)
  --vault-role-id VAULT_ROLE_ID
                        AppRole role id (default: )
  --vault-secret-id-file VAULT_SECRET_ID_FILE
                        File holding the AppRole secret id (default: )
  --vault-token-file VAULT_TOKEN_FILE
                        File holding the vault token for the token authentication method,
                        VAULT_TOKEN is used without it (default: )
  -f, --force           Force data update
  --version             show program's version number and exit
  -c CONFIG, --config CONFIG
                        Configuration path (default: None)
  -d CONFIG_DIRECTORY, --config-directory CONFIG_DIRECTORY
                        Configuration directory (default: None)
  --healthcheck-hostname HEALTHCHECK_HOSTNAME
                        Health check host name (default: 0.0.0.0)
  --healthcheck-port HEALTHCHECK_PORT
                        Health check port (default: 2501)
```

## Configuration

From JSON files (`-c`, `-d`), or from the OpenSearch index `maas-engine-config` with
`--config-backend opensearch` and `--config-service`: the engine then follows the
changes made in maas-operator by itself, and publishes what it runs in the
`maas-engine-catalogue` index. See [docs/engine-config.md](docs/engine-config.md).

## AMQP heartbeat

`--amqp-heartbeat` / `AMQP_HEARTBEAT` (default **600** s) turns on AMQP
heartbeats for the engine's broker connection, including every connection the
consume loop re-establishes.

Without them an idle connection carries no traffic. Anything in the network
path that expires idle TCP flows then cuts it **without the broker noticing**:
a TCP proxy's idle timeout, or the docker swarm IPVS table (about 15 min). The
engine reconnects, but the broker keeps the old connection `running`, together
with its consumer. With `prefetch_count=1`, that zombie consumer can be handed
a message and hold it unacked forever. On the OCS production swarm (haproxy
`timeout tunnel 1h` in front of RabbitMQ) this showed up as one extra consumer
per engine per hour.

With heartbeats, the broker sends a frame every `timeout / 2` (which keeps the
path active) and closes a peer it hasn't heard from for about `timeout`, which
requeues its unacked message. The engine sends its own heartbeats between
messages, so **the value must exceed the longest time one message can block
the consume loop**. Otherwise the broker drops the connection mid-processing,
and the message is redelivered once the engine reconnects. Set `0` to restore
the previous behaviour (no heartbeat).

## Connections from OpenBao / Vault

`ES_VAULT_PATH` and `AMQP_VAULT_PATH` (`--es-vault-path`, `--amqp-vault-path`) read
the OpenSearch and AMQP url, username and password from an OpenBao or HashiCorp
Vault secret, in place of `ES_URL`, `ES_USERNAME`, `ES_PASSWORD` and their AMQP
counterparts. See [docs/vault.md](docs/vault.md).
