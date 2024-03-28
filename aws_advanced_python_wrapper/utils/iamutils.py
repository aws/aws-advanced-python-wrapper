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

from datetime import datetime
from typing import TYPE_CHECKING

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


class IamAuthUtils:
    @staticmethod
    def get_iam_host(props: Properties, host_info: HostInfo):
        host = WrapperProperties.IAM_HOST.get(props) if WrapperProperties.IAM_HOST.get(props) else host_info.host
        IamAuthUtils.validate_iam_host(host)
        return host

    @staticmethod
    def validate_iam_host(host: str | None):
        if host is None:
            raise AwsWrapperError(Messages.get_formatted("IAMAuthPlugin.InvalidHost", "[No host provided]"))

        utils = RdsUtils()
        rds_type = utils.identify_rds_type(host)
        if rds_type == RdsUrlType.OTHER or rds_type == RdsUrlType.IP_ADDRESS:
            raise AwsWrapperError(Messages.get_formatted("IAMAuthPlugin.InvalidHost", host))

    @staticmethod
    def get_port(props: Properties, host_info: HostInfo, dialect_default_port: int) -> int:
        default_port: int = WrapperProperties.IAM_DEFAULT_PORT.get_int(props)
        if default_port > 0:
            return default_port

        if host_info.is_port_specified():
            return host_info.port

        return dialect_default_port


class TokenInfo:
    @property
    def token(self):
        return self._token

    @property
    def expiration(self):
        return self._expiration

    def __init__(self, token: str, expiration: datetime):
        self._token = token
        self._expiration = expiration

    def is_expired(self) -> bool:
        return datetime.now() > self._expiration
