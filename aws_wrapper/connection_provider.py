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

from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_wrapper.hostinfo import HostInfo, HostRole
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.utils.properties import Properties

from typing import Callable, Dict, List, Optional, Protocol

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostselector import HostSelector, RandomHostSelector
from aws_wrapper.plugin import CanReleaseResources
from aws_wrapper.utils.messages import Messages


class ConnectionProvider(Protocol):

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        ...

    def connect(self, host_info: HostInfo, properties: Properties) -> Connection:
        ...


class ConnectionProviderManager:
    _lock: Lock = Lock()
    _conn_provider: Optional[ConnectionProvider] = None

    def __init__(self, default_provider: ConnectionProvider):
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

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if ConnectionProviderManager._conn_provider is not None \
                        and ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy):
                    return ConnectionProviderManager._conn_provider.get_host_info_by_strategy(hosts, role, strategy)

        return self._default_provider.get_host_info_by_strategy(hosts, role, strategy)

    @staticmethod
    def release_resources():
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if isinstance(ConnectionProviderManager._conn_provider, CanReleaseResources):
                    ConnectionProviderManager._conn_provider.release_resources()


class DriverConnectionProvider(ConnectionProvider):

    _accepted_strategies: Dict[str, HostSelector] = {"random": RandomHostSelector()}

    def __init__(self, connect_func: Callable):
        self._connect_func = connect_func

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        return True

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepted_strategies

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        host_selector: Optional[HostSelector] = self._accepted_strategies.get(strategy)
        if not host_selector:
            raise AwsWrapperError(
                Messages.get_formatted("DriverConnectionProvider.UnsupportedStrategy", strategy))
        else:
            return host_selector.get_host(hosts, role)

    def connect(self, host_info: HostInfo, properties: Properties) -> Connection:
        # TODO: Behavior based on dialects
        prop_copy = properties.copy()

        prop_copy["host"] = host_info.host

        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)

        return self._connect_func(**prop_copy)
