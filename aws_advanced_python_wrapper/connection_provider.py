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
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.pep249 import Connection

from threading import Lock

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import (HostSelector,
                                                       RandomHostSelector,
                                                       RoundRobinHostSelector)
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils)

logger = Logger(__name__)


class ConnectionProvider(Protocol):

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        """
        Indicates whether this ConnectionProvider can provide connections for the given host and
        properties. Some :py:class:`ConnectionProvider` implementations may not be able to handle certain connection properties.

        :param host_info: the :py:class:`HostInfo` containing the host-port information for the host to connect to.
        :param props: the connection properties.
        :return: `True` if this :py:class:`ConnectionProvider` can provide connections for the given host. `False` otherwise.
        """
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        """
        Indicates whether the given selection strategy is supported by the connection provider.

        :param role: determines if the connection provider should return a reader host or a writer host.
        :param strategy: the host selection strategy to use.
        :return: whether the host selection strategy is supported.
        """
        ...

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        """
        Return a reader or a writer host using the specified strategy.

        This method should raise an :py:class:`AwsWrapperError` if the specified strategy is unsupported.

        :param hosts: the list of hosts to select from.
        :param role: determines if the connection provider should return a reader or a writer host.
        :param strategy: the host selection strategy to use.
        :param props: the connection properties.
        :return: the host selected using the specified strategy.
        """
        ...

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Connection:
        """
        Called once per connection that needs to be created.

        :param target_func: the `Connect` method used by target driver dialect.
        :param driver_dialect: a dialect that handles target driver specific implementation.
        :param database_dialect: a dialect that handles database engine specific implementation.
        :param host_info: the host details for the desired connection.
        :param props: the connection properties.
        :return: the established connection resulting from the given connection information.
        """
        ...


class DriverConnectionProvider(ConnectionProvider):
    _accepted_strategies: Dict[str, HostSelector] = \
        {"random": RandomHostSelector(), "round_robin": RoundRobinHostSelector()}

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        return True

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepted_strategies

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        host_selector: Optional[HostSelector] = self._accepted_strategies.get(strategy)
        if host_selector is not None:
            return host_selector.get_host(hosts, role, props)

        raise AwsWrapperError(
            Messages.get_formatted("DriverConnectionProvider.UnsupportedStrategy", strategy))

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Connection:
        prepared_properties = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared_properties)
        logger.debug("DriverConnectionProvider.ConnectingToHost", host_info.host,
                     PropertiesUtils.log_properties(PropertiesUtils.mask_properties(prepared_properties)))
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
        """
        Setter that can optionally be called to request a non-default :py:class:`ConnectionProvider`.
        The requested :py:class:`ConnectionProvider` will be used to establish future connections unless it does not support a requested host,
        in which case the default :py:class:`ConnectionProvider` will be used. See :py:method:`ConnectionProvider.accepts_host_info` for more details.
        :param connection_provider: the :py:class:`ConnectionProvider` to use to establish new connections.
        """
        with ConnectionProviderManager._lock:
            ConnectionProviderManager._conn_provider = connection_provider

    def get_connection_provider(self, host_info: HostInfo, props: Properties) -> ConnectionProvider:
        """
        Get the :py:class:`ConnectionProvider` to use to establish a connection using the given host details and properties.
        If a non-default :py:class:`ConnectionProvider` has been set using :py:method:`ConnectionProvider.set_connection_provider`
        and :py:method:`ConnectionProvider.accepts_host_info` returns `True`, the non-default :py:class:`ConnectionProvider` will be returned.
        Otherwise, the default :py:class:`ConnectionProvider` will be returned.

        :param host_info: the host info for the connection that will be established.
        :param props: the connection properties.
        :return: the :py:class:`ConnectionProvider` to use to establish a connection using the given host details and properties.
        """
        if ConnectionProviderManager._conn_provider is None:
            return self.default_provider

        with ConnectionProviderManager._lock:
            if ConnectionProviderManager._conn_provider is not None \
                    and ConnectionProviderManager._conn_provider.accepts_host_info(host_info, props):
                return ConnectionProviderManager._conn_provider

        return self._default_provider

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        """
        Indicates whether the given selection strategy is supported by the connection provider.

        :param role: determines if the connection provider should return a reader host or a writer host.
        :param strategy: the host selection strategy to use.
        :return: whether the host selection strategy is supported.
        """
        accepts_strategy: bool = False
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                accepts_strategy = ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy)

        if not accepts_strategy:
            accepts_strategy = self._default_provider.accepts_strategy(role, strategy)

        return accepts_strategy

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        """
        Return a reader or a writer host using the specified strategy.

        This method should raise an :py:class:`AwsWrapperError` if the specified strategy is unsupported.

        :param hosts: the list of hosts to select from.
        :param role: determines if the connection provider should return a reader or a writer host.
        :param strategy: the host selection strategy to use.
        :param props: the connection properties.
        :return: the host selected using the specified strategy.
        """
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if ConnectionProviderManager._conn_provider is not None \
                        and ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy):
                    return ConnectionProviderManager._conn_provider.get_host_info_by_strategy(
                        hosts, role, strategy, props)

        return self._default_provider.get_host_info_by_strategy(hosts, role, strategy, props)

    @staticmethod
    def reset_provider():
        """
        Clears the non-default :py:class:`ConnectionProvider` if it has been set.
        """
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                ConnectionProviderManager._conn_provider = None

    @staticmethod
    def release_resources():
        """
        Releases any resources held by the available :py:class:`ConnectionProvider` instances.
        :return:
        """
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if isinstance(ConnectionProviderManager._conn_provider, CanReleaseResources):
                    ConnectionProviderManager._conn_provider.release_resources()
