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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from boto3 import Session
    from aws_wrapper.generic_driver_dialect import DriverDialect
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService

from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Set

import boto3

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.plugin import Plugin, PluginFactory
from aws_wrapper.utils.log import Logger
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rdsutils import RdsUtils

logger = Logger(__name__)


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


class IamAuthPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect", "force_connect"}
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60

    _rds_utils: RdsUtils = RdsUtils()
    _token_cache: Dict[str, TokenInfo] = {}

    def __init__(self, plugin_service: PluginService, session: Optional[Session] = None):
        self._plugin_service = plugin_service
        self._session = session

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect(host_info, props, connect_func)

    def _connect(self, host_info: HostInfo, props: Properties, connect_func: Callable) -> Connection:
        if not WrapperProperties.USER.get(props):
            raise AwsWrapperError(Messages.get_formatted("IamPlugin.IsNoneOrEmpty", WrapperProperties.USER.name))

        host = WrapperProperties.IAM_HOST.get(props) if WrapperProperties.IAM_HOST.get(props) else host_info.host
        region = WrapperProperties.IAM_REGION.get(props) \
            if WrapperProperties.IAM_REGION.get(props) else self._get_rds_region(host)
        port = self._get_port(props, host_info)
        token_expiration_sec: int = WrapperProperties.IAM_EXPIRATION.get_int(props)

        cache_key: str = self._get_cache_key(
            WrapperProperties.USER.get(props),
            host,
            port,
            region
        )

        token_info = IamAuthPlugin._token_cache.get(cache_key)

        if token_info is not None and not token_info.is_expired():
            logger.debug("IamAuthPlugin.UseCachedIamToken", token_info.token)
            self._plugin_service.driver_dialect.set_password(props, token_info.token)
        else:
            token: str = self._generate_authentication_token(props, host, port, region)
            logger.debug("IamAuthPlugin.GeneratedNewIamToken", token)
            self._plugin_service.driver_dialect.set_password(props, token)
            IamAuthPlugin._token_cache[cache_key] = TokenInfo(token, datetime.now() + timedelta(
                seconds=token_expiration_sec))

        try:
            return connect_func()

        except Exception as e:
            logger.debug("IamAuthPlugin.ConnectException", e)

            is_cached_token = (token_info is not None and not token_info.is_expired())
            if not self._plugin_service.is_login_exception(error=e) or not is_cached_token:
                raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.ConnectException", e)) from e

            # Login unsuccessful with cached token
            # Try to generate a new token and try to connect again

            token = self._generate_authentication_token(props, host, port, region)
            logger.debug("IamAuthPlugin.GeneratedNewIamToken", token)
            self._plugin_service.driver_dialect.set_password(props, token)
            IamAuthPlugin._token_cache[token] = TokenInfo(token, datetime.now() + timedelta(
                seconds=token_expiration_sec))

            try:
                return connect_func()
            except Exception as e:
                raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.UnhandledException", e)) from e

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(host_info, props, force_connect_func)

    def _generate_authentication_token(self,
                                       props: Properties,
                                       hostname: Optional[str],
                                       port: Optional[int],
                                       region: Optional[str]) -> str:
        session = self._session if self._session else boto3.Session()
        client = session.client(
            'rds',
            region_name=region,
        )

        user = WrapperProperties.USER.get(props)

        token = client.generate_db_auth_token(
            DBHostname=hostname,
            Port=port,
            DBUsername=user
        )

        client.close()

        return token

    def _get_cache_key(self, user: Optional[str], hostname: Optional[str], port: int, region: Optional[str]) -> str:
        return f"{region}:{hostname}:{port}:{user}"

    def _get_port(self, props: Properties, host_info: HostInfo) -> int:
        if WrapperProperties.IAM_DEFAULT_PORT.get(props):
            default_port: int = WrapperProperties.IAM_DEFAULT_PORT.get_int(props)
            if default_port > 0:
                return default_port
            else:
                logger.debug("IamAuthPlugin.InvalidPort", default_port)

        if host_info.is_port_specified():
            return host_info.port

        if self._plugin_service.dialect is not None:
            return self._plugin_service.dialect.default_port

        raise AwsWrapperError(Messages.get("IamAuthPlugin.NoValidPorts"))

    def _get_rds_region(self, hostname: Optional[str]) -> str:
        rds_region = self._rds_utils.get_rds_region(hostname) if hostname else None

        if not rds_region:
            exception_message = "IamAuthPlugin.UnsupportedHostname"
            logger.debug(exception_message, hostname)
            raise AwsWrapperError(Messages.get_formatted(exception_message, hostname))

        session = self._session if self._session else boto3.Session()
        if rds_region not in session.get_available_regions("rds"):
            exception_message = "AwsSdk.UnsupportedRegion"
            logger.debug(exception_message, rds_region)
            raise AwsWrapperError(Messages.get_formatted(exception_message, rds_region))

        return rds_region


class IamAuthPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return IamAuthPlugin(plugin_service)
