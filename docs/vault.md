# Connections from OpenBao / Vault

maas-engine can read its OpenSearch and AMQP connections from an
[OpenBao](https://openbao.org) or HashiCorp Vault server, instead of `ES_URL`,
`ES_USERNAME`, `ES_PASSWORD`, `AMQP_URL`, `AMQP_USERNAME` and `AMQP_PASSWORD`.
maas-collector reads the same secrets, so every service of a stack shares one secret
per connection, and no password is left in the compose files.

The daemon (`python -m maas_engine`), `maas_engine_cli`, `maas_migrate` and the queue
utilities all read them the same way.

## Settings

| CLI option             | Environment variable | Description                                                                    |
| ---------------------- | -------------------- | ------------------------------------------------------------------------------ |
| --es-vault-path        | ES_VAULT_PATH        | Secret holding the OpenSearch connection, like `maas/cds/opensearch`           |
| --amqp-vault-path      | AMQP_VAULT_PATH      | Secret holding the AMQP connection, like `maas/cds/amqp`                       |
| --vault-addr           | VAULT_ADDR           | Vault address, like `https://bao.internal:8200`                                |
| --vault-kv-mount       | VAULT_KV_MOUNT       | Mount point of the KV version 2 engine (default: secret)                       |
| --vault-auth-method    | VAULT_AUTH_METHOD    | `approle` or `token` (default: approle)                                        |
| --vault-role-id        | VAULT_ROLE_ID        | AppRole role id                                                                |
| --vault-secret-id-file | VAULT_SECRET_ID_FILE | File holding the AppRole secret id, like a Docker Swarm secret                 |
| --vault-token-file     | VAULT_TOKEN_FILE     | File holding a token for the `token` method. Without it, `VAULT_TOKEN` is used |

- Without `ES_VAULT_PATH`, the OpenSearch connection comes from the `ES_*` variables,
  as before. `AMQP_VAULT_PATH` works the same way. A service can take one connection
  from the vault and the other from the environment.
- TLS verification of the vault follows `VAULT_CACERT` or `VAULT_CAPATH`, like the
  vault CLI.
- Use `approle` in production. The `token` method is meant for development.

## The secrets

Each connection is one KV version 2 secret, at `<VAULT_KV_MOUNT>/<path>`, holding
`url`, and `username` and `password` when the service needs authentication:

```shell
# password=- reads the password from stdin, so it stays out of the shell history
bao kv put -mount=secret maas/cds/opensearch \
    url=https://opensearch.internal:9200 username=maas password=-
bao kv put -mount=secret maas/cds/amqp \
    url=amqp://rabbitmq.internal:5672// username=maas password=-
```

Setting up the vault itself (KV engine, policies, AppRole, secret id as a Swarm
secret) is described in the maas-collector documentation, page *Credentials from
OpenBao / HashiCorp Vault*. The engine AppRole only needs to read the two secrets:

```shell
bao policy write maas-cds-connections - <<EOF
path "secret/data/maas/cds/opensearch" {
  capabilities = ["read"]
}
path "secret/data/maas/cds/amqp" {
  capabilities = ["read"]
}
EOF
bao write auth/approle/role/maas-engine-cds \
    token_policies=maas-cds-connections \
    token_ttl=1h token_max_ttl=4h secret_id_num_uses=0
bao read -field=role_id auth/approle/role/maas-engine-cds/role-id
bao write -f -field=secret_id auth/approle/role/maas-engine-cds/secret-id \
    | docker secret create maas_engine_cds_vault_secret_id -
```

## Deploying

```yaml
services:
  maas-engine:
    image: <registry>/maas-engine:<version>
    environment:
      ES_VAULT_PATH: maas/cds/opensearch
      AMQP_VAULT_PATH: maas/cds/amqp
      VAULT_ADDR: https://bao.internal:8200
      VAULT_CACERT: /run/secrets/vault_ca
      VAULT_ROLE_ID: <role id>
      VAULT_SECRET_ID_FILE: /run/secrets/maas_engine_cds_vault_secret_id
      NO_PROXY: bao.internal
      no_proxy: bao.internal
    secrets:
      - maas_engine_cds_vault_secret_id
      - vault_ca

secrets:
  maas_engine_cds_vault_secret_id:
    external: true
  vault_ca:
    external: true
```

The vault client honours the `https_proxy` and `http_proxy` variables. When the
engine reaches the internet through a proxy, list the vault host in `NO_PROXY` and
`no_proxy`: an egress proxy usually refuses port 8200, and the engine would then fail
at startup with a `ProxyError`.

When `ES_URL`, `ES_USERNAME` or `ES_PASSWORD` (`AMQP_*` alike) are still set, they are
ignored, and a warning names them, never their values. Remove them once the secrets
are in the vault, and rotate the passwords they held in plain text.

## Rotating a password

The connections are read **once, at startup**. Write a new version of the secret,
then restart the services that use it:

```shell
bao kv patch -mount=secret maas/cds/opensearch password=-
docker service update --force <stack>_maas-engine
```

## When something is wrong

The engine stops at startup, with an error naming the secret and never a value,
when:

- the vault settings are incomplete, like a missing `VAULT_ADDR` or `VAULT_ROLE_ID`;
- the vault is unavailable, or refuses the secret id or the read;
- the secret does not exist, or has no `url`.

Swarm then restarts the service, as it does when OpenSearch or RabbitMQ is down.
