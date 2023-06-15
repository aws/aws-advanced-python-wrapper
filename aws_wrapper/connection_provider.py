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


import threading
from typing import Callable, Dict, List, Optional, Protocol

from aws_wrapper.errors import AwsWrapperError
from .hostinfo import HostInfo, HostRole
from .hostselector import HostSelector, RandomHostSelector
from .pep249 import Connection
from .utils.properties import Properties


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
    def __init__(self, default_provider: ConnectionProvider, connection_provider: Optional[ConnectionProvider] = None):
        self._default_provider: ConnectionProvider = default_provider
        self._connection_provider: Optional[ConnectionProvider] = connection_provider
        self._lock = threading.Lock()

    def set_connection_provider(self, connection_provider: ConnectionProvider):
        self._lock.acquire()
        try:
            self._connection_provider = connection_provider
        finally:
            self._lock.release()

    def get_connection_provider(self, host_info: HostInfo, properties: Properties) -> ConnectionProvider:
        if self._connection_provider is None:
            return self._default_provider

        self._lock.acquire()
        try:
            if self._connection_provider is not None and self._connection_provider.accepts_host_info(host_info, properties):
                return self._connection_provider
        finally:
            self._lock.release()

        return self._default_provider

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        accepts_strategy: bool = False
        if self._connection_provider is not None:
            self._lock.acquire()
            try:
                accepts_strategy = self._connection_provider.accepts_strategy(role, strategy)
            finally:
                self._lock.release()

        if not accepts_strategy:
            accepts_strategy = self._default_provider.accepts_strategy(role, strategy)

        return accepts_strategy

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        if self._connection_provider is not None:
            self._lock.acquire()
            try:
                if self._connection_provider.accepts_strategy(role, strategy):
                    return self._connection_provider.get_host_info_by_strategy(hosts, role, strategy)
            finally:
                self._lock.release()

        return self._default_provider.get_host_info_by_strategy(hosts, role, strategy)


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
        if host_selector is None:
            raise AwsWrapperError("DriverConnectionProvider does not support strategy: " + strategy)
        else:
            return host_selector.get_host(hosts, role)

    def connect(self, host_info: HostInfo, properties: Properties) -> Connection:
        # TODO: Behavior based on dialects
        prop_copy = properties.copy()

        prop_copy["host"] = host_info.host

        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)

        return self._connect_func(**prop_copy)
