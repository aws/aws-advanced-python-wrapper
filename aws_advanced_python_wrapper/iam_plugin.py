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

from copy import deepcopy
from typing import TYPE_CHECKING

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils, TokenInfo
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.region_utils import (GdbRegionUtils,
                                                            RegionUtils)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

from datetime import datetime, timedelta
from typing import Callable, Set

from aws_advanced_python_wrapper.errors import AwsConnectError, AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

logger = Logger(__name__)


class IamAuthPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {DbApiMethod.CONNECT.method_name, DbApiMethod.FORCE_CONNECT.method_name}
    # Leave 30 second buffer to prevent time-of-check to time-of-use errors
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30

    _rds_utils: RdsUtils = RdsUtils()

    def __init__(self, plugin_service: PluginService):
        self._plugin_service = plugin_service
        self._storage_service = services_container.get_storage_service()
        self._storage_service.register(TokenInfo, item_expiration_time=timedelta(minutes=15))

        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = telemetry_factory.create_counter("iam.fetch_token.count")
        self._cache_size_gauge = telemetry_factory.create_gauge(
            "iam.token_cache.size", lambda: self._storage_service.size(TokenInfo))

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

        rds_type = self._rds_utils.identify_rds_type(host)
        if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER:
            self._region_utils: RegionUtils = GdbRegionUtils()
        else:
            self._region_utils = RegionUtils()

        region = self._region_utils.get_region(props, WrapperProperties.IAM_REGION.name, host, host_info)
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

        token_info = self._storage_service.get(TokenInfo, cache_key)

        if token_info is not None and not token_info.is_expired():
            logger.debug("IamAuthPlugin.UseCachedIamToken", token_info.token)
            self._plugin_service.driver_dialect.set_password(props, token_info.token)
        else:
            token_expiry = datetime.now() + timedelta(seconds=token_expiration_sec)
            if self._fetch_token_counter is not None:
                self._fetch_token_counter.inc()

            session_host_info = deepcopy(host_info)
            session_host_info.host = host
            session = AwsCredentialsManager.get_session(host_info, props, region)
            token: str = IamAuthUtils.generate_authentication_token(
                self._plugin_service,
                user,
                host,
                port,
                region,
                session)
            self._plugin_service.driver_dialect.set_password(props, token)
            self._storage_service.put(TokenInfo, cache_key, TokenInfo(token, token_expiry))

        try:
            return connect_func()

        except Exception as e:
            logger.debug("IamAuthPlugin.ConnectException", e)

            if self._plugin_service.is_network_exception(error=e):
                raise AwsConnectError(Messages.get_formatted("IamAuthPlugin.ConnectException", e)) from e

            is_cached_token = (token_info is not None and not token_info.is_expired())
            if not self._plugin_service.is_login_exception(error=e) or not is_cached_token:
                raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.ConnectException", e), e) from e
            # Login unsuccessful with cached token
            # Try to generate a new token and try to connect again
            token_expiry = datetime.now() + timedelta(seconds=token_expiration_sec)
            if self._fetch_token_counter is not None:
                self._fetch_token_counter.inc()

            session_host_info = deepcopy(host_info)
            session_host_info.host = host
            session = AwsCredentialsManager.get_session(session_host_info, props, region)
            token = IamAuthUtils.generate_authentication_token(self._plugin_service, user, host, port, region, session)
            self._plugin_service.driver_dialect.set_password(props, token)
            self._storage_service.put(TokenInfo, cache_key, TokenInfo(token, token_expiry))

            try:
                return connect_func()
            except Exception as e:
                raise AwsWrapperError(Messages.get_formatted("IamAuthPlugin.UnhandledException", e), e) from e

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
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return IamAuthPlugin(plugin_service)
