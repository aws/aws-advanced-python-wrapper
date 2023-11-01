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

from typing import TYPE_CHECKING, runtime_checkable

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Protocol, Set

from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)


class Plugin(ABC):
    """
    Interface for connection plugins. This class implements ways to execute a database API.
    """

    @property
    @abstractmethod
    def subscribed_methods(self) -> Set[str]:
        pass

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        """
        Establishes a connection to the given host using the given driver protocol and properties. If a
        non-default :py:class`ConnectionProvider` has been set with :py:method:`ConnectionProviderManager.set_connection_provider`,
        the connection will be created by the non-default ConnectionProvider.
        Otherwise, the connection will be created by the default :py:class`DriverConnectionProvider`.

        :param target_driver_func: the `Connect` method used by target driver dialect.
        :param driver_dialect: a dialect that handles target driver specific implementation.
        :param host_info: the host details for the desired connection.
        :param props: the connection properties.
        :param is_initial_connection: a `bool` indicating whether the current :py:class`Connection` is establishing an initial physical connection to
        the database or has already established a physical connection in the past.
        :param connect_func: the function to call to continue the connect request down the `connect` pipeline.
        :return: a :py:class`Connection` to the requested host.
        """
        return connect_func()

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        """
        Establishes a connection to the given host using the given driver protocol and properties.
        This call differs from connect in that the default :py:class`DriverConnectionProvider` will be used to establish the connection even if
        a non-default :py:class`ConnectionProvider` has been set via :py:method:`ConnectionProviderManager.set_connection_provider`.

        :param target_driver_func: the `Connect` method used by target driver dialect.
        :param driver_dialect: a dialect that handles target driver specific implementation.
        :param host_info: the host details for the desired connection.
        :param props: the connection properties.
        :param is_initial_connection: a `bool` indicating whether the current :py:class`Connection` is establishing an initial physical connection to
        the database or has already established a physical connection in the past.
        :param force_connect_func: the function to call to continue the connect request down the `force_connect` pipeline.
        :return:
        """
        return force_connect_func()

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        return execute_func()

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        return

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        return OldConnectionSuggestedAction.NO_OPINION

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        """
        Returns a boolean indicating if this py:class:`ConnectionPlugin` implements the specified host selection strategy
        for the given role in :py:method:`ConnectionPlugin.get_host_info_by_strategy`.

        :param role: the desired role of the selected host - either a reader host or a writer host.
        :param strategy: the strategy that should be used to pick a host (eg "random").
        :return: `True` if this py:class:`ConnectionPlugin` supports the selection of a host with the requested role and strategy
        via :py:method:`ConnectionPlugin.get_host_info_by_strategy`. Otherwise, return `False`.
        """
        return False

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
        """
        Selects a :py:class:`HostInfo` with the requested role from available hosts using the requested strategy.
        :py:method:`ConnectionPlugin.accepts_strategy` should be called first to evaluate if this py:class:`ConnectionPlugin` supports the selection
        of a host with the requested role and strategy.

        :param role: the desired role of the selected host - either a reader host or a writer host.
        :param strategy: the strategy that should be used to pick a host (eg "random").
        :return: a py:class:`HostInfo` with the requested role.
        """
        raise UnsupportedOperationError(Messages.get_formatted("Plugin.UnsupportedMethod", "get_host_info_by_strategy"))

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        return init_host_provider_func()


class PluginFactory(Protocol):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        pass


@runtime_checkable
class CanReleaseResources(Protocol):
    def release_resources(self):
        """
        Release all dangling resources held by the class implementing this method.
        This step allows implementing classes to clean up remaining resources or perform other cleanup tasks before shutting down.
        """
        pass
