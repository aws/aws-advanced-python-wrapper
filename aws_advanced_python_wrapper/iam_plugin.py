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

from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils, TokenInfo
from aws_advanced_python_wrapper.utils.region_utils import RegionUtils

if TYPE_CHECKING:
    from boto3 import Session
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Set

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

logger = Logger(__name__)


class IamAuthPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect", "force_connect"}
    # Leave 30 second buffer to prevent time-of-check to time-of-use errors
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30

    _rds_utils: RdsUtils = RdsUtils()
    _token_cache: Dict[str, TokenInfo] = {}

    def __init__(self, plugin_service: PluginService, session: Optional[Session] = None):
        self._plugin_service = plugin_service
        self._session = session

        self._region_utils = RegionUtils()
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = telemetry_factory.create_counter("iam.fetch_token.count")
        self._cache_size_gauge = telemetry_factory.create_gauge(
            "iam.token_cache.size", lambda: len(IamAuthPlugin._token_cache))

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
        user = WrapperProperties.USER.get(props)
        if not user:
            raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.IsNoneOrEmpty", WrapperProperties.USER.name))

        host = IamAuthUtils.get_iam_host(props, host_info)
        region = self._region_utils.get_region(props, WrapperProperties.IAM_REGION.name, host, self._session)
        if not region:
            error_message = "RdsUtils.UnsupportedHostname"
            logger.debug(error_message, host)
            raise AwsWrapperError(Messages.get_formatted(error_message, host))

        port = IamAuthUtils.get_port(props, host_info, self._plugin_service.database_dialect.default_port)
        token_expiration_sec: int = WrapperProperties.IAM_EXPIRATION.get_int(props)

        cache_key: str = IamAuthUtils.get_cache_key(
            user,
            host,
            port,
            region
        )

        token_info = IamAuthPlugin._token_cache.get(cache_key)

        if token_info is not None and not token_info.is_expired():
            logger.debug("IamAuthPlugin.UseCachedIamToken", token_info.token)
            self._plugin_service.driver_dialect.set_password(props, token_info.token)
        else:
            token_expiry = datetime.now() + timedelta(seconds=token_expiration_sec)
            self._fetch_token_counter.inc()
            token: str = IamAuthUtils.generate_authentication_token(self._plugin_service, user, host, port, region, client_session=self._session)
            self._plugin_service.driver_dialect.set_password(props, token)
            IamAuthPlugin._token_cache[cache_key] = TokenInfo(token, token_expiry)

        try:
            return connect_func()

        except Exception as e:
            logger.debug("IamAuthPlugin.ConnectException", e)

            is_cached_token = (token_info is not None and not token_info.is_expired())
            if not self._plugin_service.is_login_exception(error=e) or not is_cached_token:
                raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.ConnectException", e)) from e

            # Login unsuccessful with cached token
            # Try to generate a new token and try to connect again
            token_expiry = datetime.now() + timedelta(seconds=token_expiration_sec)
            self._fetch_token_counter.inc()
            token = IamAuthUtils.generate_authentication_token(self._plugin_service, user, host, port, region, client_session=self._session)
            self._plugin_service.driver_dialect.set_password(props, token)
            IamAuthPlugin._token_cache[cache_key] = TokenInfo(token, token_expiry)

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


class IamAuthPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return IamAuthPlugin(plugin_service)
