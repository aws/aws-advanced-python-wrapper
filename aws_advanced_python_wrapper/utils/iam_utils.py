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
from typing import TYPE_CHECKING, Dict, Optional

import boto3

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from boto3 import Session

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

logger = Logger(__name__)


class IamAuthUtils:
    @staticmethod
    def get_iam_host(props: Properties, host_info: HostInfo):
        host = WrapperProperties.IAM_HOST.get(props) if WrapperProperties.IAM_HOST.get(props) else host_info.host
        IamAuthUtils.validate_iam_host(host)
        return host

    @staticmethod
    def validate_iam_host(host: str | None):
        if host is None:
            raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.InvalidHost", "[No host provided]"))

        utils = RdsUtils()
        rds_type = utils.identify_rds_type(host)
        if rds_type == RdsUrlType.OTHER or rds_type == RdsUrlType.IP_ADDRESS:
            raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.InvalidHost", host))

    @staticmethod
    def get_port(props: Properties, host_info: HostInfo, dialect_default_port: int) -> int:
        default_port: int = WrapperProperties.IAM_DEFAULT_PORT.get_int(props)
        if default_port > 0:
            return default_port

        if host_info.is_port_specified():
            return host_info.port

        return dialect_default_port

    @staticmethod
    def get_cache_key(user: Optional[str], hostname: Optional[str], port: int, region: Optional[str]) -> str:
        return f"{region}:{hostname}:{port}:{user}"

    @staticmethod
    def generate_authentication_token(
            plugin_service: PluginService,
            user: Optional[str],
            host_name: Optional[str],
            port: Optional[int],
            region: Optional[str],
            credentials: Optional[Dict[str, str]] = None,
            client_session: Optional[Session] = None) -> str:
        telemetry_factory = plugin_service.get_telemetry_factory()
        context = telemetry_factory.open_telemetry_context("fetch authentication token", TelemetryTraceLevel.NESTED)

        try:
            session = client_session if client_session else boto3.Session()

            if credentials is not None:
                client = session.client(
                    'rds',
                    region_name=region,
                    aws_access_key_id=credentials.get('AccessKeyId'),
                    aws_secret_access_key=credentials.get('SecretAccessKey'),
                    aws_session_token=credentials.get('SessionToken')
                )
            else:
                client = session.client(
                    'rds',
                    region_name=region
                )

            token = client.generate_db_auth_token(
                DBHostname=host_name,
                Port=port,
                DBUsername=user
            )

            client.close()

            logger.debug("IamAuthUtils.GeneratedNewAuthToken", token)
            return token
        except Exception as ex:
            context.set_success(False)
            context.set_exception(ex)
            raise ex
        finally:
            context.close_context()


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
