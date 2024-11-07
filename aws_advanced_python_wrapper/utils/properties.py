#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import Any, Dict, Optional
from urllib.parse import unquote

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages


class Properties(Dict[str, Any]):
    pass


class WrapperProperty:
    def __init__(self, name: str, description: str, default_value: Optional[Any] = None):
        self.name = name
        self.default_value = default_value
        self.description = description

    def get(self, props: Properties) -> Optional[str]:
        if self.default_value:
            return props.get(self.name, self.default_value)
        return props.get(self.name)

    def get_int(self, props: Properties) -> int:
        if self.default_value:
            return int(props.get(self.name, self.default_value))

        val = props.get(self.name)
        return int(val) if val else -1

    def get_float(self, props: Properties) -> float:
        if self.default_value:
            return float(props.get(self.name, self.default_value))

        val = props.get(self.name)
        return float(val) if val else -1

    def get_bool(self, props: Properties) -> bool:
        if not self.default_value:
            value = props.get(self.name)
        else:
            value = props.get(self.name, self.default_value)
        if isinstance(value, bool):
            return value
        else:
            return value is not None and value.lower() == "true"

    def set(self, props: Properties, value: Any):
        props[self.name] = value


class WrapperProperties:
    DEFAULT_PLUGINS = "aurora_connection_tracker,failover,host_monitoring"
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60

    PROFILE_NAME = WrapperProperty("profile_name", "Driver configuration profile name", None)
    PLUGINS = WrapperProperty(
        "plugins",
        "Comma separated list of connection plugin codes",
        DEFAULT_PLUGINS)
    USER = WrapperProperty("user", "Driver user name")
    PASSWORD = WrapperProperty("password", "Driver password")
    DATABASE = WrapperProperty("database", "Driver database name")

    CONNECT_TIMEOUT_SEC = WrapperProperty(
        "connect_timeout",
        "Max number of seconds to wait for a connection to be established before timing out.")
    SOCKET_TIMEOUT_SEC = WrapperProperty(
        "socket_timeout",
        "Max number of seconds to wait for a SQL query to complete before timing out.")
    TCP_KEEPALIVE = WrapperProperty(
        "tcp_keepalive", "Enable TCP keepalive functionality.")
    TCP_KEEPALIVE_TIME_SEC = WrapperProperty(
        "tcp_keepalive_time", "Number of seconds to wait before sending an initial keepalive probe.")
    TCP_KEEPALIVE_INTERVAL_SEC = WrapperProperty(
        "tcp_keepalive_interval",
        "Number of seconds to wait before sending additional keepalive probes after the initial probe has been sent.")
    TCP_KEEPALIVE_PROBES = WrapperProperty(
        "tcp_keepalive_probes", "Number of keepalive probes to send before concluding that the connection is invalid.")
    TRANSFER_SESSION_STATE_ON_SWITCH = WrapperProperty(
        "transfer_session_state_on_switch", "Enables session state transfer to a new connection", True)
    RESET_SESSION_STATE_ON_CLOSE = WrapperProperty(
        "reset_session_state_on_close",
        "Enables to reset connection session state before closing it.",
        True)
    ROLLBACK_ON_SWITCH = WrapperProperty(
        "rollback_on_switch",
        "Enables to rollback a current transaction being in progress when switching to a new connection.",
        True)

    # RdsHostListProvider
    TOPOLOGY_REFRESH_MS = WrapperProperty(
        "topology_refresh_ms",
        """Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the
        specified time, after which it will be updated during the next interaction with the connection.""",
        30_000)
    CLUSTER_ID = WrapperProperty(
        "cluster_id",
        """A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. If
        unspecified, a cluster id is automatically created for AWS RDS clusters.""")
    CLUSTER_INSTANCE_HOST_PATTERN = WrapperProperty(
        "cluster_instance_host_pattern",
        """The cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in
        this pattern should be used as a placeholder for cluster instance names. This pattern is required to be
        specified for IP address or custom domain connections to AWS RDS clusters. Otherwise, if unspecified, the
        pattern will be automatically created for AWS RDS clusters.""")

    IAM_HOST = WrapperProperty("iam_host", "Overrides the host that is used to generate the IAM token.")
    IAM_DEFAULT_PORT = WrapperProperty(
        "iam_default_port",
        "Overrides default port that is used to generate the IAM token.")
    IAM_REGION = WrapperProperty("iam_region", "Overrides AWS region that is used to generate the IAM token.")
    IAM_EXPIRATION = WrapperProperty(
        "iam_expiration",
        "IAM token cache expiration in seconds",
        _DEFAULT_TOKEN_EXPIRATION_SEC)
    SECRETS_MANAGER_SECRET_ID = WrapperProperty(
        "secrets_manager_secret_id",
        "The name or the ARN of the secret to retrieve.")
    SECRETS_MANAGER_REGION = WrapperProperty(
        "secrets_manager_region",
        "The region of the secret to retrieve.",
        "us-east-1")
    SECRETS_MANAGER_ENDPOINT = WrapperProperty(
        "secrets_manager_endpoint",
        "The endpoint of the secret to retrieve.")

    DIALECT = WrapperProperty("wrapper_dialect", "A unique identifier for the supported database dialect.")
    AUXILIARY_QUERY_TIMEOUT_SEC = WrapperProperty(
        "auxiliary_query_timeout_sec",
        """Network timeout, in seconds, used for auxiliary queries to the database.
        This timeout applies to queries executed by the wrapper driver to gain info about the connected database.
        It does not apply to queries requested by the driver client.""",
        5)

    # HostMonitoringPlugin
    FAILURE_DETECTION_ENABLED = WrapperProperty(
        "failure_detection_enabled",
        "Enable failure detection logic in the HostMonitoringPlugin.",
        True)
    FAILURE_DETECTION_TIME_MS = WrapperProperty(
        "failure_detection_time_ms",
        "Interval in milliseconds between sending SQL to the server and the first connection check.",
        30_000)
    FAILURE_DETECTION_INTERVAL_MS = WrapperProperty(
        "failure_detection_interval_ms",
        "Interval in milliseconds between consecutive connection checks.",
        5_000)
    FAILURE_DETECTION_COUNT = WrapperProperty(
        "failure_detection_count",
        "Number of failed connection checks before considering the database host unavailable.",
        3)
    MONITOR_DISPOSAL_TIME_MS = WrapperProperty(
        "monitor_disposal_time_ms",
        "Interval in milliseconds after which a monitor should be considered inactive and marked for disposal.",
        600_000)  # 10 minutes

    # Failover
    ENABLE_FAILOVER = WrapperProperty(
        "enable_failover",
        "Enable/disable cluster-aware failover logic",
        True)
    FAILOVER_MODE = WrapperProperty(
        "failover_mode",
        "Decide which host role (writer, reader, or either) to connect to during failover.")
    FAILOVER_TIMEOUT_SEC = WrapperProperty(
        "failover_timeout_sec",
        "Maximum allowed time in seconds for the failover process.",
        300)  # 5 minutes
    FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_SEC = WrapperProperty(
        "failover_cluster_topology_refresh_rate_sec",
        """Cluster topology refresh rate in seconds during a writer failover process.
        During the writer failover process,
        cluster topology may be refreshed at a faster pace than normal to speed up discovery of the newly promoted writer.""",
        2)
    FAILOVER_WRITER_RECONNECT_INTERVAL_SEC = WrapperProperty(
        "failover_writer_reconnect_interval_sec",
        "Interval of time in seconds to wait between attempts to reconnect to a failed writer during a writer failover process.",
        2)
    FAILOVER_READER_CONNECT_TIMEOUT_SEC = WrapperProperty(
        "failover_reader_connect_timeout_sec",
        "Reader connection attempt timeout in seconds during a reader failover process.",
        30)

    # CustomEndpointPlugin
    CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS = WrapperProperty(
        "custom_endpoint_info_refresh_rate_ms",
        "Controls how frequently custom endpoint monitors fetch custom endpoint info, in milliseconds.",
        30_000)
    CUSTOM_ENDPOINT_IDLE_MONITOR_EXPIRATION_MS = WrapperProperty(
        "custom_endpoint_idle_monitor_expiration_ms",
        "Controls how long a monitor should run without use before expiring and being removed, in milliseconds.",
        900_000)  # 15 minutes
    WAIT_FOR_CUSTOM_ENDPOINT_INFO = WrapperProperty(
        "wait_for_custom_endpoint_info",
        """Controls whether to wait for custom endpoint info to become available before connecting or executing a
        method. Waiting is only necessary if a connection to a given custom endpoint has not been opened or used
        recently. Note that disabling this may result in occasional connections to instances outside of the custom
        endpoint.""",
        True)
    WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS = WrapperProperty(
        "wait_for_custom_endpoint_info_timeout_ms",
        """Controls the maximum amount of time that the plugin will wait for custom endpoint info to be made
        available by the custom endpoint monitor, in milliseconds.""",
        5_000)

    # Host Availability Strategy
    DEFAULT_HOST_AVAILABILITY_STRATEGY = WrapperProperty(
        "default_host_availability_strategy",
        "An override for specifying the default host availability change strategy.",
        ""
    )

    HOST_AVAILABILITY_STRATEGY_MAX_RETRIES = WrapperProperty(
        "host_availability_strategy_max_retries",
        "Max number of retries for checking a host's availability.",
        "5"
    )

    HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME = WrapperProperty(
        "host_availability_strategy_initial_backoff_time",
        "The initial backoff time in seconds.",
        "30"
    )

    # Driver Dialect
    DRIVER_DIALECT = WrapperProperty(
        "wrapper_driver_dialect",
        "A unique identifier for the target driver dialect.")

    # Read/Write Splitting
    READER_HOST_SELECTOR_STRATEGY = WrapperProperty(
        "reader_host_selector_strategy",
        "The strategy that should be used to select a new reader host.",
        "random")

    # Plugin Sorting
    AUTO_SORT_PLUGIN_ORDER = WrapperProperty(
        "auto_sort_wrapper_plugin_order",
        "This flag is enabled by default, meaning that the plugins order will be automatically adjusted. "
        "Disable it at your own risk or if you really need plugins to be executed in a particular order.",
        True)

    # Host Selector
    ROUND_ROBIN_DEFAULT_WEIGHT = WrapperProperty("round_robin_default_weight", "The default weight for any hosts that have not been " +
                                                 "configured with the `round_robin_host_weight_pairs` parameter.",
                                                 1)

    ROUND_ROBIN_HOST_WEIGHT_PAIRS = WrapperProperty("round_robin_host_weight_pairs",
                                                    "Comma separated list of database host-weight pairs in the format of `<host>:<weight>`.",
                                                    "")
    # Federated Auth Plugin
    IDP_ENDPOINT = WrapperProperty("idp_endpoint",
                                   "The hosting URL of the Identity Provider",
                                   None)

    IDP_PORT = WrapperProperty("idp_port",
                               "The hosting port of the Identity Provider",
                               443)

    RELAYING_PARTY_ID = WrapperProperty("rp_identifier",
                                        "The relaying party identifier",
                                        "urn:amazon:webservices")

    IAM_ROLE_ARN = WrapperProperty("iam_role_arn",
                                   "The ARN of the IAM Role that is to be assumed.",
                                   None)

    IAM_IDP_ARN = WrapperProperty("iam_idp_arn",
                                  "The ARN of the Identity Provider",
                                  None)

    IAM_TOKEN_EXPIRATION = WrapperProperty("iam_token_expiration",
                                           "IAM token cache expiration in seconds",
                                           15 * 60 - 30)

    IDP_USERNAME = WrapperProperty("idp_username",
                                   "The federated user name",
                                   None)

    IDP_PASSWORD = WrapperProperty("idp_password",
                                   "The federated user password",
                                   None)

    HTTP_REQUEST_TIMEOUT = WrapperProperty("http_request_connect_timeout",
                                           "The timeout value in seconds to send the HTTP request data used by the FederatedAuthPlugin",
                                           60)

    SSL_SECURE = WrapperProperty("ssl_secure",
                                 "Whether the SSL session is to be secure and the server's certificates will be verified",
                                 False)

    IDP_NAME = WrapperProperty("idp_name",
                               "The name of the Identity Provider implementation used",
                               "adfs")

    DB_USER = WrapperProperty("db_user",
                              "The database user used to access the database",
                              None)

    # Okta

    APP_ID = WrapperProperty("app_id", "The ID of the AWS application configured on Okta", None)

    # Fastest Response Strategy
    RESPONSE_MEASUREMENT_INTERVAL_MILLIS = WrapperProperty("response_measurement_interval_ms",
                                                           "Interval in milliseconds between measuring response time to a database host",
                                                           30_000)

    # Telemetry
    ENABLE_TELEMETRY = WrapperProperty(
        "enable_telemetry",
        "Enables telemetry and observability of the driver.",
        False
    )

    TELEMETRY_SUBMIT_TOPLEVEL = WrapperProperty(
        "telemetry_submit_toplevel",
        "Force submitting traces related to Python calls as top level traces.",
        False
    )

    TELEMETRY_TRACES_BACKEND = WrapperProperty(
        "telemetry_traces_backend",
        "Method to export telemetry traces of the driver.",
        None
    )

    TELEMETRY_METRICS_BACKEND = WrapperProperty(
        "telemetry_metrics_backend",
        "Method to export telemetry metrics of the driver.",
        None
    )

    TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE = WrapperProperty(
        "telemetry_failover_additional_top_trace",
        "Post an additional top-level trace for failover process.",
        False
    )

    READER_INITIAL_HOST_SELECTOR_STRATEGY = WrapperProperty(
        "reader_initial_connection_host_selector_strategy",
        "The strategy that should be used to select a new reader host while opening a new connection.",
        "random")

    OPEN_CONNECTION_RETRY_TIMEOUT_MS = WrapperProperty(
        "open_connection_retry_timeout_ms",
        "Maximum allowed time for the retries opening a connection.",
        30000
    )

    OPEN_CONNECTION_RETRY_INTERVAL_MS = WrapperProperty(
        "open_connection_retry_interval_ms",
        "Time between each retry of opening a connection.",
        1000
    )


class PropertiesUtils:
    @staticmethod
    def parse_properties(conn_info: str, **kwargs: Any) -> Properties:
        if conn_info.startswith("postgresql://") or conn_info.startswith("postgres://"):
            props = PropertiesUtils.parse_pg_scheme_url(conn_info)
        else:
            props = PropertiesUtils.parse_key_values(conn_info)

        for key, value in kwargs.items():
            props[key] = value
        return props

    @staticmethod
    def parse_pg_scheme_url(conn_info: str) -> Properties:
        props = Properties()
        if conn_info.startswith("postgresql://"):
            to_parse = conn_info[len("postgresql://"):]
        elif conn_info.startswith("postgres://"):
            to_parse = conn_info[len("postgres://"):]
        else:
            raise AwsWrapperError(Messages.get_formatted("PropertiesUtils.InvalidPgSchemeUrl", conn_info))

        # Example URL: postgresql://user:password@host:port/dbname?some_prop=some_value
        # More examples here: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
        host_separator = to_parse.find("@")
        if host_separator >= 0:
            user_spec = to_parse[:host_separator]
            password_separator = user_spec.find(":")
            if password_separator >= 0:
                props[WrapperProperties.USER.name] = user_spec[:password_separator]
                props[WrapperProperties.PASSWORD.name] = user_spec[password_separator + 1:host_separator]
            else:
                props[WrapperProperties.USER.name] = user_spec
            to_parse = to_parse[host_separator + 1:]

        db_separator = to_parse.find("/")
        props_separator = to_parse.find("?")
        if db_separator >= 0:
            host_spec = to_parse[:db_separator]
            to_parse = to_parse[db_separator + 1:]
            props_separator = to_parse.find("?")
        elif props_separator >= 0:
            host_spec = to_parse[:props_separator]
            to_parse = to_parse[props_separator + 1:]
        else:
            host_spec = to_parse

        if host_spec.find(",") >= 0:
            raise AwsWrapperError(Messages.get_formatted("PropertiesUtils.MultipleHostsNotSupported", conn_info))

        # host_spec may be a percent-encoded unix domain socket, eg '%2Fvar%2Flib%2Fpostgresql'.
        # When stored as a kwarg instead of a connection string property, it should be decoded.
        host_spec = unquote(host_spec)
        if host_spec.startswith("["):
            # IPv6 addresses should be enclosed in square brackets, eg 'postgresql://[2001:db8::1234]/dbname'
            host_end = host_spec.find("]")
            props["host"] = host_spec[:host_end + 1]
            host_spec = host_spec[host_end + 1:]
            if len(host_spec) > 0:
                props["port"] = host_spec[1:]
        else:
            port_separator = host_spec.find(":")
            if port_separator >= 0:
                props["host"] = host_spec[:port_separator]
                props["port"] = host_spec[port_separator + 1:]
            else:
                if len(host_spec) > 0:
                    props["host"] = host_spec

        if db_separator >= 0:
            if props_separator >= 0:
                props[WrapperProperties.DATABASE.name] = to_parse[:props_separator]
                to_parse = to_parse[props_separator + 1:]
            else:
                props[WrapperProperties.DATABASE.name] = to_parse
                return props

        if props_separator >= 0:
            # Connection string properties must be percent-decoded when stored as kwargs
            props.update(PropertiesUtils.parse_key_values(to_parse, separator="&", percent_decode=True))
        return props

    @staticmethod
    def parse_key_values(conn_info: str, separator: str = " ", percent_decode: bool = False) -> Properties:
        props = Properties()
        to_parse = conn_info

        while to_parse.strip() != "":
            to_parse = to_parse.strip()
            sep_i = to_parse.find(separator)
            equals_i = to_parse.find("=")
            key_end = sep_i if -1 < sep_i < equals_i else equals_i
            if key_end == -1:
                raise AwsWrapperError(Messages.get_formatted("PropertiesUtils.ErrorParsingConnectionString", conn_info))

            key = to_parse[0:key_end]
            to_parse = to_parse[equals_i + 1:].lstrip()
            sep_i = to_parse.find(separator)

            value_end = sep_i if sep_i > -1 else len(to_parse)
            value = to_parse[0:value_end]
            to_parse = to_parse[value_end + 1:]

            if percent_decode:
                value = unquote(value)
            props[key] = value

        return props

    @staticmethod
    def remove_wrapper_props(props: Properties):
        persisting_properties = [WrapperProperties.USER.name, WrapperProperties.PASSWORD.name]

        for attr_name, attr_val in WrapperProperties.__dict__.items():
            if isinstance(attr_val, WrapperProperty):
                # Don't remove credentials
                if attr_val.name not in persisting_properties:
                    props.pop(attr_val.name, None)

        monitor_prop_keys = [key for key in props if key.startswith("monitoring-")]
        for key in monitor_prop_keys:
            props.pop(key, None)

    @staticmethod
    def get_url(props: Properties) -> str:
        host = props.get("host")
        port = props.get("port")

        if host is None:
            raise AwsWrapperError(Messages.get("PropertiesUtils.NoHostDefined"))

        return host if port is None else f"{host}:{port}"

    @staticmethod
    def log_properties(props: Properties):
        if not props:
            return "<empty>"

        return f"\n{props}"

    @staticmethod
    def mask_properties(props: Properties) -> Properties:
        masked_properties = Properties(props.copy())
        if WrapperProperties.PASSWORD.name in masked_properties:
            masked_properties[WrapperProperties.PASSWORD.name] = "***"

        return masked_properties
