"""tools for command line interface, Argument parsers"""

import logging
import os
import urllib
from argparse import Action, ArgumentParser, Namespace, FileType

from maas_model.vault import (
    ConnectionSettings,
    VaultClient,
    VaultSettings,
    read_connection,
)

import maas_engine

# class UrlAction(argparse.Action):
#     def __init__(self, option_strings, dest, nargs=None, **kwargs):
#         if nargs is not None:
#             raise ValueError("nargs not allowed")
#         super().__init__(option_strings, dest, **kwargs)

#     def __call__(self, parser, namespace, values, option_string=None):
#         print("%r %r %r" % (namespace, values, option_string))
#         setattr(namespace, self.dest, values)


class EnvDefault(Action):
    """argparse.Action that gets value from environment"""

    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar in os.environ:
            default = os.environ[envvar]
        if required and default:
            required = False
        super().__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def get_credentials_url(url: str, username: str, password: str) -> str:
    """Add credentials to an url

    Args:
        url (str): enpoint url
        username (str): user name
        password (str): user password

    Returns:
        str: url with credentials
    """

    new_url = ""
    if not username:
        new_url = url
    elif url:
        # parse arg url
        parse_result = urllib.parse.urlparse(url)
        # extract domain name
        domain = parse_result.netloc.split("@")[-1]
        new_username = parse_result.username
        new_password = parse_result.password
        # init credentials if provided in url and not override by args
        if username:
            new_username = username
        if password:
            new_password = password
        # Format the complete url with credentials
        new_url = (
            f"{parse_result.scheme}://{new_username}:{new_password}"
            f"@{domain}{parse_result.path}"
        )
    return new_url


def es_parser() -> ArgumentParser:
    """opensearch CLI argument parser factory

    Returns:
        ArgumentParser: opensearch argument parser

    """
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "--es-username",
        dest="es_username",
        help="opensearch user identifier (default: %(default)s)",
        action=EnvDefault,
        envvar="ES_USERNAME",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--es-password",
        dest="es_password",
        help="opensearch user password (default: %(default)s)",
        action=EnvDefault,
        envvar="ES_PASSWORD",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--es-url",
        dest="es_url",
        help="opensearch cluster URL (default: %(default)s)",
        action=EnvDefault,
        envvar="ES_URL",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--es-vault-path",
        dest="es_vault_path",
        help="Vault secret holding the opensearch url, username and password, in"
        " place of --es-url, --es-username and --es-password (default: %(default)s)",
        action=EnvDefault,
        envvar="ES_VAULT_PATH",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--es-timeout",
        dest="es_timeout",
        help="opensearch request timeout in seconds (default: %(default)s)",
        action=EnvDefault,
        envvar="ES_TIMEOUT",
        required=False,
        default=120,
        type=int,
    )
    parser.add_argument(
        "--es-retries",
        dest="es_retries",
        help="opensearch number of retries (default: %(default)s)",
        action=EnvDefault,
        envvar="ES_RETRIES",
        required=False,
        default=3,
        type=int,
    )

    parser.add_argument(
        "--es-reject-errors",
        dest="es_reject_errors",
        help="Reject messages when opensearch errors",
        action="store_true",
    )

    parser.add_argument(
        "--es-ignore-certs-verification",
        dest="es_ignore_certs_verification",
        help="If set, the SSL cert of opensearch is not verified (default: %(default)s)",
        action=EnvDefault,
        envvar="IGNORE_CERTS_VERIFICATION",
        required=False,
        type=bool,
        default=False,
    )

    parser.add_argument(
        "--es-requeue-missing-input",
        dest="es_requeue_missing_input",
        help="If set, a message with missing input document will be requeued "
        "(default: %(default)s)",
        action=EnvDefault,
        envvar="ES_REQUEUE_MISSING_INPUT",
        required=False,
        type=bool,
        default=False,
    )

    return parser


def get_es_credentials_url(namespace: Namespace) -> str:
    """get final url for opensearch connection

    Args:
        namespace (Namespace): result of ArgParser.parse()

    Returns:
        str: full connection url
    """
    if getattr(namespace, "es_vault_path", None):
        connection = get_vault_connection(
            namespace, namespace.es_vault_path, "OpenSearch", "es"
        )
        return get_vault_credentials_url(connection)

    return get_credentials_url(
        namespace.es_url, namespace.es_username, namespace.es_password
    )


def amqp_parser() -> ArgumentParser:
    """AMQP CLI argument parser factory

    Returns:
        ArgumentParser: AMQP argument parser

    """
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "--amqp-username",
        dest="amqp_username",
        help="AMQP user name (default: %(default)s)",
        action=EnvDefault,
        envvar="AMQP_USERNAME",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--amqp-password",
        dest="amqp_password",
        help="AMQP user password (default: %(default)s)",
        action=EnvDefault,
        envvar="AMQP_PASSWORD",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--amqp-url",
        dest="amqp_url",
        help="AMQP cluster URL (default: %(default)s)",
        action=EnvDefault,
        envvar="AMQP_URL",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--amqp-vault-path",
        dest="amqp_vault_path",
        help="Vault secret holding the AMQP url, username and password, in place of"
        " --amqp-url, --amqp-username and --amqp-password (default: %(default)s)",
        action=EnvDefault,
        envvar="AMQP_VAULT_PATH",
        required=False,
        type=str,
    )

    parser.add_argument(
        "--amqp-retries",
        dest="amqp_retries",
        help="AMQP number of retries (default: %(default)s). 0 for infinite",
        action=EnvDefault,
        envvar="AMQP_RETRIES",
        required=False,
        default=0,
        type=int,
    )

    parser.add_argument(
        "--amqp-max-priority",
        dest="amqp_max_priority",
        help="AMQP max priority (default: %(default)s). 1 to 10",
        action=EnvDefault,
        envvar="AMQP_MAX_PRIORITY",
        required=False,
        default=10,
        type=int,
    )

    # Without heartbeats an idle connection carries no traffic at all, so
    # anything in the path that expires idle TCP flows (haproxy timeouts, the
    # docker swarm IPVS table, ~15 min) cuts it without the broker noticing:
    # the broker keeps a "running" connection whose consumer can hold a
    # prefetched message unacked forever. The broker sends heartbeats every
    # timeout/2 and drops a peer after ~timeout of silence, so the value is
    # also the longest a single message may block the consumer loop.
    parser.add_argument(
        "--amqp-heartbeat",
        dest="amqp_heartbeat",
        help="AMQP heartbeat timeout in seconds, 0 to disable "
        "(default: %(default)s). Must exceed the longest message processing time",
        action=EnvDefault,
        envvar="AMQP_HEARTBEAT",
        required=False,
        default=600,
        type=int,
    )

    return parser


def get_amqp_credentials_url(namespace: Namespace) -> str:
    """get final url for AMQP connection

    Args:
        namespace (Namespace): result of ArgParser.parse()

    Returns:
        str: full connection url
    """
    if getattr(namespace, "amqp_vault_path", None):
        connection = get_vault_connection(
            namespace, namespace.amqp_vault_path, "AMQP", "amqp"
        )
        return get_vault_credentials_url(connection)

    return get_credentials_url(
        namespace.amqp_url, namespace.amqp_username, namespace.amqp_password
    )


def vault_parser() -> ArgumentParser:
    """OpenBao / HashiCorp Vault access CLI argument parser factory

    Returns:
        ArgumentParser: vault argument parser
    """
    parser = ArgumentParser(add_help=False)

    parser.add_argument(
        "--vault-addr",
        dest="vault_addr",
        help="Vault address, like https://vault:8200 (default: %(default)s)",
        action=EnvDefault,
        envvar="VAULT_ADDR",
        required=False,
        default="",
        type=str,
    )

    parser.add_argument(
        "--vault-kv-mount",
        dest="vault_kv_mount",
        help="Mount point of the KV version 2 secret engine (default: %(default)s)",
        action=EnvDefault,
        envvar="VAULT_KV_MOUNT",
        required=False,
        default="secret",
        type=str,
    )

    parser.add_argument(
        "--vault-auth-method",
        dest="vault_auth_method",
        help="Vault authentication method (default: %(default)s)",
        action=EnvDefault,
        envvar="VAULT_AUTH_METHOD",
        required=False,
        default="approle",
        type=str,
        choices=["approle", "token"],
    )

    parser.add_argument(
        "--vault-role-id",
        dest="vault_role_id",
        help="AppRole role id (default: %(default)s)",
        action=EnvDefault,
        envvar="VAULT_ROLE_ID",
        required=False,
        default="",
        type=str,
    )

    parser.add_argument(
        "--vault-secret-id-file",
        dest="vault_secret_id_file",
        help="File holding the AppRole secret id (default: %(default)s)",
        action=EnvDefault,
        envvar="VAULT_SECRET_ID_FILE",
        required=False,
        default="",
        type=str,
    )

    parser.add_argument(
        "--vault-token-file",
        dest="vault_token_file",
        help="File holding the vault token for the token authentication method,"
        " VAULT_TOKEN is used without it (default: %(default)s)",
        action=EnvDefault,
        envvar="VAULT_TOKEN_FILE",
        required=False,
        default="",
        type=str,
    )

    return parser


def get_vault_settings(namespace: Namespace) -> VaultSettings:
    """VaultSettings factory

    Args:
        namespace (Namespace): parsed arguments

    Returns:
        VaultSettings: vault settings
    """
    return VaultSettings(
        url=namespace.vault_addr,
        kv_mount=namespace.vault_kv_mount,
        auth_method=namespace.vault_auth_method,
        role_id=namespace.vault_role_id,
        secret_id_file=namespace.vault_secret_id_file,
        token_file=namespace.vault_token_file,
    )


def get_vault_connection(
    namespace: Namespace, path: str, service: str, option_prefix: str
) -> ConnectionSettings:
    """Read a connection from the vault, in place of its url, username and password
    options

    Args:
        namespace (Namespace): result of ArgParser.parse()
        path (str): path of the secret in the KV engine
        service (str): service name, for the messages
        option_prefix (str): prefix of the replaced options, like es or amqp

    Raises:
        ValueError: on incomplete vault settings, a missing secret or a secret
            without url

    Returns:
        ConnectionSettings: connection settings
    """
    connection = read_connection(
        VaultClient(get_vault_settings(namespace)), path, service
    )

    for field in ("url", "username", "password"):
        if getattr(namespace, f"{option_prefix}_{field}"):
            logging.warning(
                "%s_%s / --%s-%s is ignored: the %s connection comes from the vault",
                option_prefix.upper(),
                field.upper(),
                option_prefix,
                field,
                service,
            )

    return connection


def get_vault_credentials_url(connection: ConnectionSettings) -> str:
    """url of a connection read from the vault, with its credentials

    The vault holds the user name and password as they are: they are percent-encoded
    here, and opensearch-py and kombu decode them. Otherwise a "/", "?" or "#" would
    end the host part of the url, and a "%" start an escape. The ES_* and AMQP_*
    options are not encoded, as they may hold encoded values already.

    Args:
        connection (ConnectionSettings): connection read from the vault

    Returns:
        str: url with credentials
    """
    return get_credentials_url(
        connection.url,
        quote_url_credential(connection.username),
        quote_url_credential(connection.password),
    )


def quote_url_credential(value: str) -> str:
    """Percent-encode a user name or password for the user information of an url

    Args:
        value (str): user name or password, possibly empty

    Returns:
        str: encoded value, or the value itself when empty
    """
    return urllib.parse.quote(str(value), safe="") if value else value


def s3_parser():
    """S3 CLI argument parser factory

    Returns:
        ArgumentParser: S3 argument parser

    """
    parser = ArgumentParser(add_help=False)

    parser.add_argument(
        "--s3-endpoint",
        dest="s3_endpoint",
        help="S3 endpoint URL (default: %(default)s)",
        action=EnvDefault,
        envvar="S3_ENDPOINT",
        required=False,
        type=str,
    )

    parser.add_argument(
        "--s3-key-id",
        dest="s3_key_id",
        help="S3 key identifier (default: %(default)s)",
        action=EnvDefault,
        envvar="S3_KEY_ID",
        required=False,
        type=str,
    )

    parser.add_argument(
        "--s3-access-key",
        dest="s3_access_key",
        help="S3 access key  (default: %(default)s)",
        action=EnvDefault,
        envvar="S3_ACCESS_KEY",
        required=False,
        type=str,
    )

    parser.add_argument(
        "--s3-bucket",
        dest="s3_bucket",
        help="S3 bucket  (default: %(default)s)",
        action=EnvDefault,
        envvar="S3_BUCKET",
        required=False,
        type=str,
    )

    # sooner or later options
    # S3 related but could be combined with sftp ingestion
    # parser.add_argument(
    #     "--error-bucket",
    #     dest="error_bucket",
    #     help="Error bucket to store inconsistant files (default: %(default)s)",
    #     action=EnvDefault,
    #     envvar="S3_ERROR_BUCKET",
    #     required=False,
    #     default=""
    #     type=str,
    # )

    # S3 related but could be combined with sftp ingestion
    # parser.add_argument(
    #     "--archive-bucket",
    #     dest="archive_bucket",
    #     help="Archive bucket to store processed files (default: %(default)s)",
    #     action=EnvDefault,
    #     envvar="S3_ARCHIVE_BUCKET",
    #     required=False,
    #     type=str,
    # )

    return parser


def log_parser() -> ArgumentParser:
    """

    Parser for log verbosity
    """
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="Activate verbose mode",
        action="store_const",
        const=logging.INFO,
    )

    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    return parser


def engine_parser() -> ArgumentParser:
    """common cli parser factory

    Returns:
        ArgumentParser: common argument parser
    """
    parser = ArgumentParser(
        add_help=False,
        parents=[log_parser(), es_parser(), amqp_parser(), vault_parser()],
    )

    # FIXME temporary set force to true until next release as some corner cases
    # may cause some message lost
    parser.add_argument(
        "-f", "--force", action="store_true", help="Force data update", default=True
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"maas_engine {maas_engine.__version__}",
    )

    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        help="Configuration path (default: %(default)s)",
        action=EnvDefault,
        envvar="MAAS_ENGINE_CONFIG",
        required=False,
        type=FileType("r"),
    )

    parser.add_argument(
        "-d",
        "--config-directory",
        dest="config_directory",
        help="Configuration directory (default: %(default)s)",
        action=EnvDefault,
        envvar="MAAS_ENGINE_CONFIG_DIR",
        required=False,
        type=str,
    )

    # not EnvDefault: bool("false") is True, MAAS_TELEMETRY is read by the library
    parser.add_argument(
        "--telemetry",
        dest="telemetry",
        action="store_true",
        default=False,
        help="Send OpenTelemetry traces and metrics (or set MAAS_TELEMETRY) "
        "(default: %(default)s)",
    )

    parser.add_argument(
        "--healthcheck-hostname",
        dest="healthcheck_hostname",
        help="Health check host name (default: %(default)s)",
        action=EnvDefault,
        envvar="HEALTHCHECK_HOSTNAME",
        required=False,
        type=str,
        default="0.0.0.0",
    )

    parser.add_argument(
        "--healthcheck-port",
        dest="healthcheck_port",
        help="Health check port (default: %(default)s)",
        action=EnvDefault,
        envvar="HEALTHCHECK_PORT",
        required=False,
        type=int,
        default=2501,
    )

    return parser
