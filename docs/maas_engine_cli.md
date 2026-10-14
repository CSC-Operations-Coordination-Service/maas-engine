# `maas_engine_cli`: CLI one shot run

`maas-engine` provides a command-line utility that can run an engine in a one-shot way: `maas_engine_cli`.

It has the standard options of most MAAS projects:

```console
$ maas_engine_cli -h
usage: maas_engine_cli [-h] [-v] [-vv] [--es-username ES_USERNAME] [--es-password ES_PASSWORD]
                       [--es-url ES_URL] [--es-vault-path ES_VAULT_PATH] [--es-timeout ES_TIMEOUT]
                       [--es-retries ES_RETRIES] [--es-reject-errors]
                       [--es-ignore-certs-verification ES_IGNORE_CERTS_VERIFICATION]
                       [--es-requeue-missing-input ES_REQUEUE_MISSING_INPUT]
                       [--amqp-username AMQP_USERNAME] [--amqp-password AMQP_PASSWORD]
                       [--amqp-url AMQP_URL] [--amqp-vault-path AMQP_VAULT_PATH]
                       [--amqp-retries AMQP_RETRIES] [--amqp-max-priority AMQP_MAX_PRIORITY]
                       [--amqp-heartbeat AMQP_HEARTBEAT] [--vault-addr VAULT_ADDR]
                       [--vault-kv-mount VAULT_KV_MOUNT] [--vault-auth-method {approle,token}]
                       [--vault-role-id VAULT_ROLE_ID]
                       [--vault-secret-id-file VAULT_SECRET_ID_FILE]
                       [--vault-token-file VAULT_TOKEN_FILE] [-f] [--version] [-c CONFIG]
                       [-d CONFIG_DIRECTORY] [--healthcheck-hostname HEALTHCHECK_HOSTNAME]
                       [--healthcheck-port HEALTHCHECK_PORT] -e ENGINE_ID [-r ROUTING_KEY]
                       [-p PAYLOAD]

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
  -e ENGINE_ID, --engine-id ENGINE_ID
                        Engine identifier
  -r ROUTING_KEY, --routing-key ROUTING_KEY
                        Routing key (only if it has a meaning)
  -p PAYLOAD, --payload PAYLOAD
                        Json file containing a MAASMessage
```

The specific options of `maas_engine_cli` are:

- `-e ENGINE_ID, --engine-id ENGINE_ID`: the engine identifier, declared in the class attribute `ENGINE_ID` of the engine
- `-r ROUTING_KEY, --routing-key ROUTING_KEY`: optional routing key that the engine consumes. Only necessary if the engine implementation makes some use of it
- `-p PAYLOAD, --payload PAYLOAD`: optional JSON file path containing the `MAASMessage` payload as it is consumed from the amqp bus.

**Important**: `maas_engine_cli` does not connect to amqp bus (yet):

- It consumes the message provided by the `--payload` option (if any)
- It does not send report messages on the bus. Instead, reports are displayed on the standard input.

It can be useful for engine developments and cron-job on a cluster.
