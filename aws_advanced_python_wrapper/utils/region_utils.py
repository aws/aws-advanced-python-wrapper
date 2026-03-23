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

import re
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

logger = Logger(__name__)


class RegionUtils:
    def __init__(self):
        self._rds_utils = RdsUtils()

    def get_region(self,
                   props: Properties,
                   prop_key: str,
                   hostname: Optional[str] = None,
                   host_info: Optional[HostInfo] = None) -> Optional[str]:
        region = props.get(prop_key)
        if region:
            return region

        return self.get_region_from_hostname(hostname)

    def get_region_from_hostname(self, hostname: Optional[str]) -> Optional[str]:
        return self._rds_utils.get_rds_region(hostname)


class GdbRegionUtils(RegionUtils):
    _GDB_CLUSTER_ARN_PATTERN = r"^arn:aws[^:]*:rds:(?P<region>[^:\n]*):[^:\n]*:([^:/\n]*[:/])?(.*)$"
    _REGION_GROUP = "region"

    def __init__(self, credentials: Optional[Dict[str, str]] = None):
        super().__init__()
        self._credentials = credentials

    def get_region(self,
                   props: Properties,
                   prop_key: str,
                   hostname: Optional[str] = None,
                   host_info: Optional[HostInfo] = None) -> Optional[str]:
        region = props.get(prop_key)
        if region:
            return region

        if not host_info:
            return None

        cluster_id = self._rds_utils.get_cluster_id(host_info.host)
        if not cluster_id:
            return None

        writer_cluster_arn = self._find_writer_cluster_arn(host_info, props, cluster_id)
        if not writer_cluster_arn:
            return None

        return self._get_region_from_cluster_arn(writer_cluster_arn)

    def _find_writer_cluster_arn(self, host_info: HostInfo, props: Properties, global_cluster_identifier: str) -> Optional[str]:
        session = AwsCredentialsManager.get_session(host_info, props)
        if self._credentials is not None:
            rds_client = session.client(
                'rds',
                aws_access_key_id=self._credentials.get('AccessKeyId'),
                aws_secret_access_key=self._credentials.get('SecretAccessKey'),
                aws_session_token=self._credentials.get('SessionToken')
            )
        else:
            rds_client = session.client('rds')

        try:
            response = rds_client.describe_global_clusters(GlobalClusterIdentifier=global_cluster_identifier)
            global_clusters = response.get("GlobalClusters", [])

            for cluster in global_clusters:
                members = cluster.get("GlobalClusterMembers", [])
                for member in members:
                    if member.get("IsWriter"):
                        return member.get("DBClusterArn")

            return None
        except Exception as e:
            logger.debug("GDBRegionUtils.UnableToRetrieveGlobalClusterARN", e)
            return None

    def _get_region_from_cluster_arn(self, cluster_arn: str) -> Optional[str]:
        match = re.match(self._GDB_CLUSTER_ARN_PATTERN, cluster_arn)
        if match:
            return match.group(self._REGION_GROUP)
        return None
