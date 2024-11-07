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

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.utils.properties import Properties

from boto3 import Session

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

logger = Logger(__name__)


class RegionUtils:
    def __init__(self):
        self._rds_utils = RdsUtils()

    def get_region(self,
                   props: Properties,
                   prop_key: str,
                   hostname: Optional[str] = None,
                   session: Optional[Session] = None) -> Optional[str]:
        region = props.get(prop_key)
        if region:
            return self.verify_region(region, session)

        return self.get_region_from_hostname(hostname, session)

    def get_region_from_hostname(self, hostname: Optional[str], session: Optional[Session] = None) -> Optional[str]:
        region = self._rds_utils.get_rds_region(hostname)
        return self.verify_region(region, session) if region else None

    def verify_region(self, region: str, session: Optional[Session] = None) -> str:
        session = session if session is not None else Session()
        if region not in session.get_available_regions("rds"):
            error_message = "AwsSdk.UnsupportedRegion"
            logger.debug(error_message, region)
            raise AwsWrapperError(Messages.get_formatted(error_message, region))

        return region
