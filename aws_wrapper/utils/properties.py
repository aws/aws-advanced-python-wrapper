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

from typing import Dict, Optional, Union

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.utils.messages import Messages


class Properties(Dict[str, str]):
    pass


class WrapperProperty:
    def __init__(self, name: str, description: str, default_value: Optional[str] = None):
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

    def get_bool(self, props: Properties) -> bool:
        if not self.default_value:
            value = props.get(self.name)
        else:
            value = props.get(self.name, self.default_value)
        return value is not None and value.lower() == "true"

    def set(self, props: Properties, value: str):
        props[self.name] = value


class WrapperProperties:
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60

    PLUGINS = WrapperProperty("plugins", "Comma separated list of connection plugin codes", "dummy")
    USER = WrapperProperty("user", "Driver user name")
    PASSWORD = WrapperProperty("password", "Driver password")
    DATABASE = WrapperProperty("database", "Driver database name")

    # AuroraHostListProvider
    TOPOLOGY_REFRESH_MS = WrapperProperty(
        "topology_refresh_ms",
        """Cluster topology refresh rate in millis. The cached topology for the cluster will be invalidated after the
        specified time, after which it will be updated during the next interaction with the connection.""",
        "30000")
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

    IAM_HOST = WrapperProperty("iam_host", "Overrides the host that is used to generate the IAM token")
    IAM_DEFAULT_PORT = WrapperProperty("iam_default_port",
                                       "Overrides default port that is used to generate the IAM token")
    IAM_REGION = WrapperProperty("iam_region", "Overrides AWS region that is used to generate the IAM token")
    IAM_EXPIRATION = WrapperProperty("iam_expiration", "IAM token cache expiration in seconds",
                                     str(_DEFAULT_TOKEN_EXPIRATION_SEC))
    SECRETS_MANAGER_SECRET_ID = WrapperProperty("secrets_manager_secret_id",
                                                "The name or the ARN of the secret to retrieve.")
    SECRETS_MANAGER_REGION = WrapperProperty("secrets_manager_region", "The region of the secret to retrieve.",
                                             "us-east-1")
    DIALECT = WrapperProperty("wrapper_dialect", "A unique identifier for the supported database dialect.")
    AUXILIARY_QUERY_TIMEOUT_SEC = WrapperProperty("auxiliary_query_timeout_sec",
                                                  """Network timeout, in seconds, used for auxiliary queries to the database.
                                                  This timeout applies to queries executed by the wrapper driver to
                                                  gain info about the connected database.
                                                  It does not apply to queries requested by the driver client.
                                                  """,
                                                  "5")

    # HostMonitoringPlugin
    FAILURE_DETECTION_ENABLED = WrapperProperty(
        "failure_detection_enabled",
        "Enable failure detection logic in the HostMonitoringPlugin",
        "True")
    FAILURE_DETECTION_TIME_MS = WrapperProperty(
        "failure_detection_time_ms",
        "Interval in milliseconds between sending SQL to the server and the first connection check.",
        "30000")
    FAILURE_DETECTION_INTERVAL_MS = WrapperProperty(
        "failure_detection_interval_ms",
        "Interval in milliseconds between consecutive connection checks.",
        "5000")
    FAILURE_DETECTION_COUNT = WrapperProperty(
        "failure_detection_count",
        "Number of failed connection checks before considering the database host unavailable.",
        "3")
    MONITOR_DISPOSAL_TIME_MS = WrapperProperty(
        "monitor_disposal_time_ms",
        "Interval in milliseconds after which a monitor should be considered inactive and marked for disposal.",
        "60000")

    # Failover
    ENABLE_FAILOVER = WrapperProperty("enable_failover",
                                      "Enable/disable cluster aware failover logic",
                                      "True")
    FAILOVER_MODE = WrapperProperty("failover_mode",
                                    "Decide which node role (writer, reader, or either) to connect to during failover",
                                    None)


class PropertiesUtils:

    @staticmethod
    def parse_properties(conn_info: str, **kwargs: Union[None, int, str]) -> Properties:
        props: Properties
        if conn_info == "":
            props = Properties()
        else:
            props = Properties(dict(x.split("=") for x in conn_info.split(" ")))
        for key, value in kwargs.items():
            props[key] = str(value)
        return props

    @staticmethod
    def remove_wrapper_props(props: Properties):
        persisting_properties = [WrapperProperties.USER.name, WrapperProperties.PASSWORD.name,
                                 WrapperProperties.DATABASE.name]

        for attr_name, attr_val in WrapperProperties.__dict__.items():
            if isinstance(attr_val, WrapperProperty):
                # Don't remove credentials
                if attr_val.name not in persisting_properties:
                    props.pop(attr_val.name, None)

    @staticmethod
    def get_url(props: Properties) -> str:
        host = props.get("host")
        port = props.get("port")

        if host is None:
            raise AwsWrapperError(Messages.get("PropertiesUtils.NoHostDefined"))

        return host if port is None else f"{host}:{port}"

    @staticmethod
    def log_properties(props: Properties, caption: str):
        if not props:
            return "<empty>"

        prefix = "" if not caption else caption
        return f"\n{prefix} {props}"
