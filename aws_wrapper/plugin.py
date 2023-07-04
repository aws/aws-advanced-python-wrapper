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
    from aws_wrapper.host_list_provider import HostListProviderService
    from aws_wrapper.hostinfo import HostInfo, HostRole
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.utils.properties import Properties

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Protocol, Set

from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.notifications import (ConnectionEvent, HostEvent,
                                             OldConnectionSuggestedAction)


class Plugin(ABC):

    @property
    @abstractmethod
    def subscribed_methods(self) -> Set[str]:
        ...

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Connection:
        return connect_func()

    def force_connect(self, host_info: HostInfo, props: Properties,
                      initial: bool, force_connect_func: Callable) -> Connection:
        return force_connect_func()

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        return execute_func()

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        return

    # TODO: Should we pass in the old/new Connection, and/or the old/new HostInfo?
    #  Would this be useful info for the plugins?
    def notify_connection_changed(self, changes: Set[ConnectionEvent]) \
            -> OldConnectionSuggestedAction:
        return OldConnectionSuggestedAction.NO_OPINION

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return False

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
        raise NotImplementedError(Messages.get_formatted("Plugins.UnsupportedMethod", "get_host_info_by_strategy"))

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        return init_host_provider_func()


class PluginFactory(Protocol):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        ...
