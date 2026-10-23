# Changelog

## Version 3.2.0

- New replay worker, `python -m maas_engine.replay` (or `maas_replay`): it takes replay
  requests (an interface and a window) from RabbitMQ, runs a command as a child process
  under a deadline, and keeps the outcome in an operation record in OpenSearch (RUNNING,
  then DONE or FAILED). It is the "recollect" pattern of the download-manager and of the
  maas-collector operator, once, and knows nothing about collectors: a handler class
  named in the configuration prepares each replay and builds its command. See
  [docs/replay.md](docs/replay.md).
- It takes the engine's connection options, including the OpenBao ones, and its health
  check, but not its consumer: a replay lasts hours, longer than a consumer can hold a
  message, so a lane polls, acknowledges on receipt and holds no connection while the
  child runs.

## Version 3.1.0

- The OpenSearch and AMQP connections can be read from OpenBao or HashiCorp Vault. Set
  `ES_VAULT_PATH` (`--es-vault-path`) and `AMQP_VAULT_PATH` (`--amqp-vault-path`) to the
  path of a KV version 2 secret holding the connection's `url` and, when the service
  needs them, its `username` and `password`. The secret then replaces `ES_URL`,
  `ES_USERNAME` and `ES_PASSWORD`, or their `AMQP_*` counterparts. maas-collector 3.11.0
  and later read the same secrets, so a stack keeps each password in one place instead
  of its compose files. See [docs/vault.md](docs/vault.md).
- Without a vault path, a connection comes from the environment variables as before.
  One connection can come from the vault and the other from the environment.
- New settings to reach the vault, each with a matching `--vault-*` option:
  `VAULT_ADDR`, `VAULT_KV_MOUNT` (the KV mount holding the secrets, default `secret`),
  `VAULT_AUTH_METHOD` (`approle`, the default, or `token`), `VAULT_ROLE_ID`,
  `VAULT_SECRET_ID_FILE` and `VAULT_TOKEN_FILE`. Without `VAULT_TOKEN_FILE`, the `token`
  method uses `VAULT_TOKEN`. TLS verification of the vault follows `VAULT_CACERT` or
  `VAULT_CAPATH`.
- Every command that opens these connections reads the vault the same way: the engine
  (`python -m maas_engine`), `maas_engine_cli`, `maas_migrate` and the other migrations
  run through `migration_main()`, and the queue utilities `maas_delete_all_queues`,
  `maas_backup_queue` and `maas_publish_message`.
- The connections are read once, at startup. To rotate a password, write a new version
  of the secret, then restart the services that use it, for example with
  `docker service update --force`.
- A command stops at startup, with an error that never shows a secret value, when the
  vault settings are incomplete (like a missing `VAULT_ADDR` or `VAULT_ROLE_ID`), the
  vault is unreachable or refuses the login or the read, or the secret is missing or has
  no `url`.
- `ES_URL`, `ES_USERNAME` and `ES_PASSWORD` (`AMQP_*` alike) still set next to a vault
  path are ignored, with a warning that names them but never their values. Remove them
  once the secrets are in the vault, and rotate the passwords they held in plain text.
- The vault client follows `https_proxy` and `http_proxy`. When the engine runs behind
  an egress proxy, add the vault host to `NO_PROXY` and `no_proxy`. Otherwise the vault
  calls go through the proxy, which usually refuses port 8200, and the engine fails at
  startup with a `ProxyError`.
- `maas_migrate` no longer logs the OpenSearch password: with `-v` or `-vv`, it printed
  the connection url with the password in clear. The password now shows as `**`, also
  when it is written straight into `ES_URL`, unless a username or password set in the
  environment holds an unencoded `/`, `?` or `#`, as the next item explains.
- The username and password stored in the vault may contain any character, like `/`,
  `#` or `%`: the engine percent-encodes them in the connection url, as maas-collector
  3.11.0 does. `ES_URL`, `ES_USERNAME`, `ES_PASSWORD` and their `AMQP_*` counterparts
  are still used as they are, so a special character there still has to be
  percent-encoded by hand, like `/` as `%2F` and `%` as `%25`. Otherwise a `/`, `?` or
  `#` in the password makes the OpenSearch or AMQP client fail with
  `ValueError: Port could not be cast to integer value as '…'`, which quotes the start
  of the password, and `maas_migrate -v` logs all of it first.
- New requirement: maas-model 3.1.0 or a later 3.x, with its `vault` extra
  (`maas-model[vault]>=3.1.0.dev0,<4`). maas-model 3.1.0 is the first release with the
  vault client, and the `vault` extra installs `hvac`. The `.dev0` in the bound lets pip
  also install the maas-model development builds made after 3.1.0 (3.1.0.post1.devN)
  from the Nexus dev index, which holds no releases.
- Projects that build their own argument parser from `es_parser()` or `amqp_parser()`
  (or both), instead of `engine_parser()`, must add `vault_parser()` to its parents:
  `es_parser()` now takes `--es-vault-path` and `amqp_parser()` `--amqp-vault-path`, and
  without `vault_parser()` a set vault path fails with an `AttributeError` on
  `vault_addr`. maas-cds 2.16.0 adds it to the parser of satruman.

Earlier versions are only recorded in the git history.
