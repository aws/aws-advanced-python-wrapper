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


class Properties(Dict[str, str]):
    ...


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

    def set(self, props: Properties, value: str):
        props[self.name] = value


class WrapperProperties:
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60

    PLUGINS = WrapperProperty("plugins", "Comma separated list of connection plugin codes", "dummy")
    USER = WrapperProperty("user", "Driver user name")
    PASSWORD = WrapperProperty("password", "Driver password")
    DATABASE = WrapperProperty("database", "Driver database name")

    # AuroraHostListProvider
    TOPOLOGY_REFRESH_MS = \
        WrapperProperty("topology_refresh_ms",
                        """Cluster topology refresh rate in millis. The cached topology for the cluster will be
                        invalidated after the specified time, after which it will be updated during the next
                        interaction with the connection.""",
                        "30000")
    CLUSTER_ID = \
        WrapperProperty("cluster_id",
                        """A unique identifier for the cluster. Connections with the same cluster id share a
                        cluster topology cache. If unspecified, a cluster id is automatically created for AWS
                        RDS clusters.""")
    CLUSTER_INSTANCE_HOST_PATTERN = \
        WrapperProperty("cluster_instance_host_pattern",
                        """The cluster instance DNS pattern that will be used to build a complete instance endpoint.
                        A \"?\" character in this pattern should be used as a placeholder for cluster instance names.
                        This pattern is required to be specified for IP address or custom domain connections to AWS RDS
                        clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS
                        clusters.""")

    IAM_HOST = WrapperProperty("iam_host", "Overrides the host that is used to generate the IAM token")
    IAM_DEFAULT_PORT = WrapperProperty("iam_default_port",
                                       "Overrides default port that is used to generate the IAM token")
    IAM_REGION = WrapperProperty("iam_region", "Overrides AWS region that is used to generate the IAM token")
    IAM_EXPIRATION = WrapperProperty("iam_expiration", "IAM token cache expiration in seconds",
                                     str(_DEFAULT_TOKEN_EXPIRATION_SEC))


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
    def log_properties(props: Properties, caption: str):
        if not props:
            return "<empty>"

        prefix = "" if not caption else caption
        return f"\n{prefix} {props}"
