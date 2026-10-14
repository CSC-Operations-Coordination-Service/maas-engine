# Configuration

## Connections from OpenBao / Vault

`ES_VAULT_PATH` and `AMQP_VAULT_PATH` (`--es-vault-path`, `--amqp-vault-path`) read
the OpenSearch and AMQP url, username and password from an OpenBao or HashiCorp
Vault secret, in place of `ES_URL`, `ES_USERNAME`, `ES_PASSWORD` and their AMQP
counterparts. See [Connections from OpenBao / Vault](vault.md).
