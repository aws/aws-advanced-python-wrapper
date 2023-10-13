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

from typing import TYPE_CHECKING, Callable, Dict, Optional, Protocol, Tuple

if TYPE_CHECKING:
    from aws_wrapper.driver_dialect import DriverDialect
    from aws_wrapper.hostinfo import HostInfo, HostRole
    from aws_wrapper.pep249 import Connection

from threading import Lock

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostselector import HostSelector, RandomHostSelector
from aws_wrapper.plugin import CanReleaseResources
from aws_wrapper.utils.log import Logger
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, PropertiesUtils

logger = Logger(__name__)


class ConnectionProvider(Protocol):

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str) -> HostInfo:
        ...

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            properties: Properties) -> Connection:
        ...


class DriverConnectionProvider(ConnectionProvider):
    _accepted_strategies: Dict[str, HostSelector] = {"random": RandomHostSelector()}

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        return True

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepted_strategies

    def get_host_info_by_strategy(self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str) -> HostInfo:
        host_selector: Optional[HostSelector] = self._accepted_strategies.get(strategy)
        if host_selector is not None:
            return host_selector.get_host(hosts, role)

        raise AwsWrapperError(
            Messages.get_formatted("DriverConnectionProvider.UnsupportedStrategy", strategy))

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            properties: Properties) -> Connection:
        prepared_properties = driver_dialect.prepare_connect_info(host_info, properties)
        logger.debug("DriverConnectionProvider.ConnectingToHost", host_info.host, PropertiesUtils.log_properties(prepared_properties))
        return target_func(**prepared_properties)


class ConnectionProviderManager:
    _lock: Lock = Lock()
    _conn_provider: Optional[ConnectionProvider] = None

    def __init__(self, default_provider: ConnectionProvider = DriverConnectionProvider()):
        self._default_provider: ConnectionProvider = default_provider

    @property
    def default_provider(self):
        return self._default_provider

    @staticmethod
    def set_connection_provider(connection_provider: ConnectionProvider):
        with ConnectionProviderManager._lock:
            ConnectionProviderManager._conn_provider = connection_provider

    def get_connection_provider(self, host_info: HostInfo, properties: Properties) -> ConnectionProvider:
        if ConnectionProviderManager._conn_provider is None:
            return self.default_provider

        with ConnectionProviderManager._lock:
            if ConnectionProviderManager._conn_provider is not None \
                    and ConnectionProviderManager._conn_provider.accepts_host_info(host_info, properties):
                return ConnectionProviderManager._conn_provider

        return self._default_provider

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        accepts_strategy: bool = False
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                accepts_strategy = ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy)

        if not accepts_strategy:
            accepts_strategy = self._default_provider.accepts_strategy(role, strategy)

        return accepts_strategy

    def get_host_info_by_strategy(self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str) -> HostInfo:
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if ConnectionProviderManager._conn_provider is not None \
                        and ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy):
                    return ConnectionProviderManager._conn_provider.get_host_info_by_strategy(hosts, role, strategy)

        return self._default_provider.get_host_info_by_strategy(hosts, role, strategy)

    @staticmethod
    def reset_provider():
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                ConnectionProviderManager._conn_provider = None

    @staticmethod
    def release_resources():
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if isinstance(ConnectionProviderManager._conn_provider, CanReleaseResources):
                    ConnectionProviderManager._conn_provider.release_resources()
