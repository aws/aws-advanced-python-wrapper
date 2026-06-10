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

from __future__ import annotations

from re import Match, search, sub
from typing import Callable, ClassVar, Dict, Optional

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

    AURORA_DNS_PATTERN = r"^(?P<instance>.+)\." \
                         r"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-|global-)?" \
                         r"(?P<domain>[a-zA-Z0-9]+\." \
                         r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn)$"
    AURORA_INSTANCE_PATTERN = r"^(?P<instance>.+)\." \
                              r"(?P<domain>[a-zA-Z0-9]+\." \
                              r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn)$"
    AURORA_CLUSTER_PATTERN = r"^(?P<instance>.+)\." \
                             r"(?P<dns>cluster-|cluster-ro-)+" \
                             r"(?P<domain>[a-zA-Z0-9]+\." \
                             r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn)$"
    AURORA_LIMITLESS_CLUSTER_PATTERN = r"^(?P<instance>.+)\." \
                                       r"(?P<dns>shardgrp-)+" \
                                       r"(?P<domain>[a-zA-Z0-9]+\." \
                                       r"(?P<region>[a-zA-Z0-9\-]+)" \
                                       r"\.rds\.(amazonaws\.com\.?|amazonaws\.com\.cn\.?|sc2s\.sgov\.gov\.?|c2s\.ic\.gov\.?))$"
    AURORA_CUSTOM_CLUSTER_PATTERN = r"^(?P<instance>.+)\." \
                                    r"(?P<dns>cluster-custom-)+" \
                                    r"(?P<domain>[a-zA-Z0-9]+\." \
                                    r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)(?!\.cn)$"
    AURORA_PROXY_DNS_PATTERN = r"^(?P<instance>.+)\." \
                               r"(?P<dns>proxy-)+" \
                               r"(?P<domain>[a-zA-Z0-9]+\." \
                               r"(?P<region>[a-zA-Z0-9\\-]+)\.rds\.amazonaws\.com)(?!\.cn)$"
    AURORA_OLD_CHINA_DNS_PATTERN = r"^(?P<instance>.+)\." \
                                   r"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-|global-)?" \
                                   r"(?P<domain>[a-zA-Z0-9]+\." \
                                   r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)$"
    AURORA_CHINA_DNS_PATTERN = r"^(?P<instance>.+)\." \
                               r"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-|global-)?" \
                               r"(?P<domain>[a-zA-Z0-9]+\." \
                               r"rds\.(?P<region>[a-zA-Z0-9\-]+)\.amazonaws\.com\.cn)$"
    AURORA_OLD_CHINA_CLUSTER_PATTERN = r"^(?P<instance>.+)\." \
                                       r"(?P<dns>cluster-|cluster-ro-)+" \
                                       r"(?P<domain>[a-zA-Z0-9]+\." \
                                       r"(?P<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)$"
    AURORA_CHINA_CLUSTER_PATTERN = r"^(?P<instance>.+)\." \
                                   r"(?P<dns>cluster-|cluster-ro-)+" \
                                   r"(?P<domain>[a-zA-Z0-9]+\." \
                                   r"rds\.(?P<region>[a-zA-Z0-9\-]+)\.amazonaws\.com\.cn)$"
    AURORA_GOV_DNS_PATTERN = r"^(?P<instance>.+)\." \
                             r"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-|global-)?" \
                             r"(?P<domain>[a-zA-Z0-9]+\.rds\.(?P<region>[a-zA-Z0-9\-]+)" \
                             r"\.(amazonaws\.com|c2s\.ic\.gov|sc2s\.sgov\.gov))$"
    AURORA_GOV_CLUSTER_PATTERN = r"^(?P<instance>.+)\." \
                                 r"(?P<dns>cluster-|cluster-ro-)+" \
                                 r"(?P<domain>[a-zA-Z0-9]+\.rds\.(?P<region>[a-zA-Z0-9\-]+)" \
                                 r"\.(amazonaws\.com|c2s\.ic\.gov|sc2s\.sgov\.gov))$"
    ELB_PATTERN = r"^(?P<instance>.+)\.elb\.((?P<region>[a-zA-Z0-9\-]+)\.amazonaws\.com)$"

    IP_V4 = r"^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){1}" \
            r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])"
    IP_V6 = r"^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}"
    IP_V6_COMPRESSED = r"^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)"

    BG_OLD_HOST_PATTERN = r".*(?P<prefix>-old1)\."
    BG_GREEN_HOST_PATTERN = r".*(?P<prefix>-green-[0-9a-z]{6})\."
    BG_GREEN_HOST_ID_PATTERN = r"(.*)-green-[0-9a-z]{6}"

    DNS_GROUP = "dns"
    DOMAIN_GROUP = "domain"
    INSTANCE_GROUP = "instance"
    REGION_GROUP = "region"

    CACHE_DNS_PATTERNS: Dict[str, Match[str]] = {}
    CACHE_PATTERNS: Dict[str, str] = {}

    _prepare_host_func: ClassVar[Optional[Callable[[str], Optional[str]]]] = None

    @staticmethod
    def set_prepare_host_func(func: Optional[Callable[[str], Optional[str]]]):
        RdsUtils._prepare_host_func = func

    @staticmethod
    def reset_prepare_host_func():
        RdsUtils._prepare_host_func = None

    @staticmethod
    def _get_prepared_host(host: Optional[str]) -> Optional[str]:
        func = RdsUtils._prepare_host_func
        if func is None or host is None:
            return host
        prepared = func(host)
        return host.strip() if prepared is None else prepared.strip()

    def is_rds_cluster_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() in ["cluster-", "cluster-ro-"]

    def is_rds_custom_cluster_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() == "cluster-custom-"

    def is_rds_dns(self, host: Optional[str]) -> bool:
        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return False

        pattern = self._find(prepared_host, [RdsUtils.AURORA_DNS_PATTERN,
                                             RdsUtils.AURORA_CHINA_DNS_PATTERN,
                                             RdsUtils.AURORA_OLD_CHINA_DNS_PATTERN,
                                             RdsUtils.AURORA_GOV_DNS_PATTERN])
        group = self._get_regex_group(pattern, RdsUtils.DNS_GROUP)

        if group:
            RdsUtils.CACHE_PATTERNS[prepared_host] = group

        return pattern is not None

    def is_rds_instance(self, host: Optional[str]) -> bool:
        prepared_host = self._get_prepared_host(host)
        return self._get_dns_group(prepared_host) is None and self.is_rds_dns(prepared_host)

    def is_rds_proxy_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() == "proxy-"

    def get_rds_instance_host_pattern(self, host: str) -> str:
        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return "?"

        match = self._get_group(prepared_host, RdsUtils.DOMAIN_GROUP)
        if match:
            return f"?.{match}"

        return "?"

    def get_rds_region(self, host: Optional[str]):
        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return None

        group = self._get_group(prepared_host, RdsUtils.REGION_GROUP)
        if group:
            return group

        elb_matcher = search(RdsUtils.ELB_PATTERN, prepared_host)
        if elb_matcher:
            return elb_matcher.group(RdsUtils.REGION_GROUP)
        return None

    def is_writer_cluster_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() == "cluster-"

    def is_reader_cluster_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() == "cluster-ro-"

    def is_global_db_writer_cluster_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() == "global-"

    def is_limitless_database_shard_group_dns(self, host: str) -> bool:
        dns_group = self._get_dns_group(self._get_prepared_host(host))
        return dns_group is not None and dns_group.casefold() == "shardgrp-"

    def get_rds_cluster_host_url(self, host: str):
        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return None

        for pattern in [RdsUtils.AURORA_CLUSTER_PATTERN,
                        RdsUtils.AURORA_CHINA_DNS_PATTERN,
                        RdsUtils.AURORA_OLD_CHINA_DNS_PATTERN,
                        RdsUtils.AURORA_GOV_DNS_PATTERN,
                        RdsUtils.AURORA_LIMITLESS_CLUSTER_PATTERN]:
            if m := search(pattern, prepared_host):
                group = self._get_regex_group(m, RdsUtils.DNS_GROUP)
                if group is not None:
                    if pattern == RdsUtils.AURORA_LIMITLESS_CLUSTER_PATTERN:
                        return sub(pattern, r"\g<instance>.shardgrp-\g<domain>", prepared_host)
                    else:
                        return sub(pattern, r"\g<instance>.cluster-\g<domain>", prepared_host)
                return None

        return None

    def get_cluster_id(self, host: str) -> Optional[str]:
        prepared_host = self._get_prepared_host(host)
        if prepared_host is None:
            return None

        if self._get_dns_group(prepared_host) is not None:
            return self._get_group(prepared_host, self.INSTANCE_GROUP)

        return None

    def get_instance_id(self, host: str) -> Optional[str]:
        prepared_host = self._get_prepared_host(host)
        if self._get_dns_group(prepared_host) is None:
            return self._get_group(prepared_host, self.INSTANCE_GROUP)

        return None

    def is_ip(self, host: str) -> bool:
        prepared_host = self._get_prepared_host(host)
        if prepared_host is None:
            return False
        return (search(RdsUtils.IP_V4, host) is not None or
                search(RdsUtils.IP_V6_COMPRESSED, host) is not None or
                search(RdsUtils.IP_V6, host) is not None)

    def is_dns_pattern_valid(self, host: str) -> bool:
        return "?" in host

    def identify_rds_type(self, host: Optional[str]) -> RdsUrlType:
        if host is None or not host.strip():
            return RdsUrlType.OTHER
        if self.is_ip(host):
            return RdsUrlType.IP_ADDRESS
        elif self.is_global_db_writer_cluster_dns(host):
            return RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        elif self.is_writer_cluster_dns(host):
            return RdsUrlType.RDS_WRITER_CLUSTER
        elif self.is_reader_cluster_dns(host):
            return RdsUrlType.RDS_READER_CLUSTER
        elif self.is_limitless_database_shard_group_dns(host):
            return RdsUrlType.RDS_AURORA_LIMITLESS_DB_SHARD_GROUP
        elif self.is_rds_custom_cluster_dns(host):
            return RdsUrlType.RDS_CUSTOM_CLUSTER
        elif self.is_rds_proxy_dns(host):
            return RdsUrlType.RDS_PROXY
        elif self.is_rds_instance(host):
            return RdsUrlType.RDS_INSTANCE

        return RdsUrlType.OTHER

    def is_green_instance(self, host: str) -> bool:
        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return False

        return search(RdsUtils.BG_GREEN_HOST_PATTERN, prepared_host) is not None

    def is_not_old_instance(self, host: str) -> bool:
        prepared_host = self._get_prepared_host(host)
        if prepared_host is None:
            return False
        return search(RdsUtils.BG_OLD_HOST_PATTERN, prepared_host) is None

    def is_not_green_or_old_instance(self, host: str) -> bool:
        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return False

        return search(RdsUtils.BG_GREEN_HOST_PATTERN, prepared_host) is None and \
            search(RdsUtils.BG_OLD_HOST_PATTERN, prepared_host) is None

    def remove_green_instance_prefix(self, host: str) -> str:
        if not host:
            return host

        prepared_host = self._get_prepared_host(host)
        if not prepared_host:
            return host

        host_match = search(RdsUtils.BG_GREEN_HOST_PATTERN, prepared_host)
        if host_match is None:
            host_id_match = search(RdsUtils.BG_GREEN_HOST_ID_PATTERN, prepared_host)
            if host_id_match:
                return host_id_match.group(0)
            else:
                return host

        prefix = host_match.group("prefix")
        if not prefix:
            return host

        return host.replace(f"{prefix}.", ".")

    def _find(self, host: Optional[str], patterns: list):
        if not host:
            return None

        for pattern in patterns:
            match = RdsUtils.CACHE_DNS_PATTERNS.get(host)
            if match:
                return match

            match = search(pattern, host)
            if match:
                RdsUtils.CACHE_DNS_PATTERNS[host] = match
                return match

        return None

    def _get_regex_group(self, pattern: Match[str], group_name: str):
        if pattern is None:
            return None
        return pattern.group(group_name)

    def _get_group(self, host: Optional[str], group: str):
        if not host:
            return None

        pattern = self._find(host, [RdsUtils.AURORA_DNS_PATTERN,
                                    RdsUtils.AURORA_CHINA_DNS_PATTERN,
                                    RdsUtils.AURORA_OLD_CHINA_DNS_PATTERN,
                                    RdsUtils.AURORA_GOV_DNS_PATTERN])
        return self._get_regex_group(pattern, group)

    def _get_dns_group(self, host: Optional[str]):
        return self._get_group(host, RdsUtils.DNS_GROUP)

    def remove_port(self, url: str):
        if not url or not url.strip():
            return None
        if ":" in url:
            return url.split(":")[0]
        return url

    @staticmethod
    def clear_cache():
        RdsUtils.CACHE_PATTERNS.clear()
        RdsUtils.CACHE_DNS_PATTERNS.clear()
