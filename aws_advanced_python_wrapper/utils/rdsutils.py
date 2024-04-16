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

from re import search, sub
from typing import Optional

from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType


class RdsUtils:
    """
    Aurora DB clusters support different endpoints. More details about Aurora RDS endpoints
    can be found at
    https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.Endpoints.html

    Details how to use RDS Proxy endpoints can be found at
    https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html

    Values like "<...>" depend on particular Aurora cluster.
    For example: "<database-cluster-name>"

    Cluster (Writer) Endpoint: <database-cluster-name>.cluster-<xyz>.<aws-region>.rds.amazonaws.com
    Example: test-postgres.cluster-123456789012.us-east-2.rds.amazonaws.com

    Cluster Reader Endpoint: <database-cluster-name>.cluster-ro-<xyz>.<aws-region>.rds.amazonaws.com
    Example: test-postgres.cluster-ro-123456789012.us-east-2.rds.amazonaws.com

    Cluster Custom Endpoint: <cluster-name-alias>.cluster-custom-<xyz>.<aws-region>.rds.amazonaws.com
    Example: test-postgres-alias.cluster-custom-123456789012.us-east-2.rds.amazonaws.com

    Instance Endpoint: <instance-name>.<xyz>.<aws-region>.rds.amazonaws.com
    Example: test-postgres-instance-1.123456789012.us-east-2.rds.amazonaws.com


    Similar endpoints for China regions have different structure and are presented below.

    Cluster (Writer) Endpoint: <database-cluster-name>.cluster-<xyz>.rds.<aws-region>.amazonaws.com.cn
    Example: test-postgres.cluster-123456789012.rds.cn-northwest-1.amazonaws.com.cn

    Cluster Reader Endpoint: <database-cluster-name>.cluster-ro-<xyz>.rds.<aws-region>.amazonaws.com.cn
    Example: test-postgres.cluster-ro-123456789012.rds.cn-northwest-1.amazonaws.com.cn

    Cluster Custom Endpoint: <cluster-name-alias>.cluster-custom-<xyz>.rds.<aws-region>.amazonaws.com.cn
    Example: test-postgres-alias.cluster-custom-123456789012.rds.cn-northwest-1.amazonaws.com.cn

    Instance Endpoint: <instance-name>.<xyz>.rds.<aws-region>.amazonaws.com.cn
    Example: test-postgres-instance-1.123456789012.rds.cn-northwest-1.amazonaws.com.cn
    """

    AURORA_DNS_PATTERN = r"(?P<instance>.+)\." \
                         r"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?" \
                         r"(?P<domain>[a-zA-Z0-9]+\." \
                         r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn$)"
    AURORA_INSTANCE_PATTERN = r"(?P<instance>.+)\." \
                              r"(?P<domain>[a-zA-Z0-9]+\." \
                              r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn$)"
    AURORA_CLUSTER_PATTERN = r"(?P<instance>.+)\." \
                             r"(?P<dns>cluster-|cluster-ro-)+" \
                             r"(?P<domain>[a-zA-Z0-9]+\." \
                             r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn$)"
    AURORA_CUSTOM_CLUSTER_PATTERN = r"(?P<instance>.+)\." \
                                    r"(?P<dns>cluster-custom-)+" \
                                    r"(?P<domain>[a-zA-Z0-9]+\." \
                                    r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn$)"
    AURORA_PROXY_DNS_PATTERN = r"(?P<instance>.+)\." \
                               r"(?P<dns>proxy-)+" \
                               r"(?P<domain>[a-zA-Z0-9]+\." \
                               r"(?P<region>[a-zA-Z0-9\\-]+)\.rds\.amazonaws\.com)(?!\.cn$)"
    AURORA_CHINA_DNS_PATTERN = r"(?P<instance>.+)\." \
                               r"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?" \
                               r"(?P<domain>[a-zA-Z0-9]+\." \
                               r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)"
    AURORA_CHINA_INSTANCE_PATTERN = r"(?P<instance>.+)\." \
                                    r"(?P<domain>[a-zA-Z0-9]+\." \
                                    r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)"
    AURORA_CHINA_CLUSTER_PATTERN = r"(?P<instance>.+)\." \
                                   r"(?P<dns>cluster-|cluster-ro-)+" \
                                   r"(?P<domain>[a-zA-Z0-9]+\." \
                                   r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)"
    AURORA_CHINA_CUSTOM_CLUSTER_PATTERN = r"(?P<instance>.+)\." \
                                          r"(?P<dns>cluster-custom-)+" \
                                          r"(?P<domain>[a-zA-Z0-9]+\." \
                                          r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)"
    AURORA_CHINA_PROXY_DNS_PATTERN = r"(?P<instance>.+)\." \
                                     r"(?P<dns>proxy-)+" \
                                     r"(?P<domain>[a-zA-Z0-9]+\." \
                                     r"(?P<region>[a-zA-Z0-9\-])+\.rds\.amazonaws\.com\.cn)"

    IP_V4 = r"^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){1}" \
            r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
    IP_V6 = r"^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$"
    IP_V6_COMPRESSED = r"^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$"

    DNS_GROUP = "dns"
    DOMAIN_GROUP = "domain"
    INSTANCE_GROUP = "instance"
    REGION_GROUP = "region"

    def is_rds_cluster_dns(self, host: str) -> bool:
        return self._contains(host, [self.AURORA_CLUSTER_PATTERN, self.AURORA_CHINA_CLUSTER_PATTERN])

    def is_rds_custom_cluster_dns(self, host: str) -> bool:
        return self._contains(host, [self.AURORA_CUSTOM_CLUSTER_PATTERN, self.AURORA_CHINA_CUSTOM_CLUSTER_PATTERN])

    def is_rds_dns(self, host: str) -> bool:
        return self._contains(host, [self.AURORA_DNS_PATTERN, self.AURORA_CHINA_DNS_PATTERN])

    def is_rds_instance(self, host: str) -> bool:
        return (self._contains(host, [self.AURORA_INSTANCE_PATTERN, self.AURORA_CHINA_INSTANCE_PATTERN])
                and self.is_rds_dns(host))

    def is_rds_proxy_dns(self, host: str) -> bool:
        return self._contains(host, [self.AURORA_PROXY_DNS_PATTERN, self.AURORA_CHINA_PROXY_DNS_PATTERN])

    def get_rds_instance_host_pattern(self, host: str) -> str:
        if not host or not host.strip():
            return "?"

        match = self._find(host, [self.AURORA_DNS_PATTERN, self.AURORA_CHINA_DNS_PATTERN])
        if match:
            return f"?.{match.group(self.DOMAIN_GROUP)}"

        return "?"

    def get_rds_region(self, host: Optional[str]):
        if not host or not host.strip():
            return None

        match = self._find(host, [self.AURORA_DNS_PATTERN, self.AURORA_CHINA_DNS_PATTERN])
        if match:
            return match.group(self.REGION_GROUP)

        return None

    def is_writer_cluster_dns(self, host: str) -> bool:
        if not host or not host.strip():
            return False

        match = self._find(host, [self.AURORA_CLUSTER_PATTERN, self.AURORA_CHINA_CLUSTER_PATTERN])
        if match:
            return "cluster-".casefold() == match.group(self.DNS_GROUP).casefold()

        return False

    def is_reader_cluster_dns(self, host: str) -> bool:
        match = self._find(host, [self.AURORA_CLUSTER_PATTERN, self.AURORA_CHINA_CLUSTER_PATTERN])
        if match:
            return "cluster-ro-".casefold() == match.group(self.DNS_GROUP).casefold()

        return False

    def get_rds_cluster_host_url(self, host: str):
        if not host or not host.strip():
            return None

        if search(self.AURORA_CLUSTER_PATTERN, host):
            return sub(self.AURORA_CLUSTER_PATTERN, r"\g<instance>.cluster-\g<domain>", host)

        if search(self.AURORA_CHINA_CLUSTER_PATTERN, host):
            return sub(self.AURORA_CHINA_CLUSTER_PATTERN, r"\g<instance>.cluster-\g<domain>", host)

        return None

    def get_instance_id(self, host: str) -> Optional[str]:
        if not host or not host.strip():
            return None

        match = self._find(host, [self.AURORA_INSTANCE_PATTERN, self.AURORA_CHINA_INSTANCE_PATTERN])
        if match:
            return match.group(self.INSTANCE_GROUP)

        return None

    def is_ipv4(self, host: str) -> bool:
        return self._contains(host, [self.IP_V4])

    def is_ipv6(self, host: str) -> bool:
        return self._contains(host, [self.IP_V6, self.IP_V6_COMPRESSED])

    def is_dns_pattern_valid(self, host: str) -> bool:
        return "?" in host

    def identify_rds_type(self, host: Optional[str]) -> RdsUrlType:
        if host is None or not host.strip():
            return RdsUrlType.OTHER

        if self.is_ipv4(host) or self.is_ipv6(host):
            return RdsUrlType.IP_ADDRESS
        elif self.is_writer_cluster_dns(host):
            return RdsUrlType.RDS_WRITER_CLUSTER
        elif self.is_reader_cluster_dns(host):
            return RdsUrlType.RDS_READER_CLUSTER
        elif self.is_rds_custom_cluster_dns(host):
            return RdsUrlType.RDS_CUSTOM_CLUSTER
        elif self.is_rds_proxy_dns(host):
            return RdsUrlType.RDS_PROXY
        elif self.is_rds_instance(host):
            return RdsUrlType.RDS_INSTANCE

        return RdsUrlType.OTHER

    def _contains(self, host: str, patterns: list) -> bool:
        if not host or not host.strip():
            return False

        return len([pattern for pattern in patterns if search(pattern, host)]) > 0

    def _find(self, host: str, patterns: list):
        if not host or not host.strip():
            return None

        for pattern in patterns:
            match = search(pattern, host)
            if match:
                return match
