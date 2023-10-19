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
        return connect_func()

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return force_connect_func()

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        return execute_func()

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        return

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) \
            -> OldConnectionSuggestedAction:
        return OldConnectionSuggestedAction.NO_OPINION

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return False

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
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
        pass