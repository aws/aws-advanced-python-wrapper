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
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.utils.properties import Properties

from typing import Any, Callable, Set

from aws_wrapper.pep249 import Connection
from aws_wrapper.plugin import Plugin, PluginFactory
from aws_wrapper.utils.notifications import (ConnectionEvent,
                                             OldConnectionSuggestedAction)


class ReadWriteSplittingPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider",
                                     "connect",
                                     "notify_connection_changed",
                                     "Connection.set_read_only",
                                     "Connection.clear_warnings"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        ...

    def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            initial: bool,
            connect_func: Callable) -> Connection:
        return Connection()

    def force_connect(
            self,
            host_info: HostInfo,
            props: Properties,
            initial: bool,
            force_connect_func: Callable) -> Connection:
        return Connection()

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        return OldConnectionSuggestedAction.NO_OPINION

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any) -> Any:
        ...


class ReadWriteSplittingPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return ReadWriteSplittingPlugin(plugin_service, props)
