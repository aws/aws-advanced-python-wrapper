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
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.pep249 import Connection

import copy
from typing import Any, Callable, Set

from aws_wrapper.connection_provider import (ConnectionProvider,
                                             ConnectionProviderManager)
from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.plugin import Plugin
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, PropertiesUtils


class DefaultPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service: PluginService, default_conn_provider: ConnectionProvider):
        self._plugin_service: PluginService = plugin_service
        self._connection_provider_manager = ConnectionProviderManager(default_conn_provider)

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Any:
        target_driver_props = copy.copy(props)
        PropertiesUtils.remove_wrapper_props(target_driver_props)
        connection_provider: ConnectionProvider = \
            self._connection_provider_manager.get_connection_provider(host_info, target_driver_props)
        result = self._connect(host_info, target_driver_props, connection_provider)
        return result

    def _connect(self, host_info: HostInfo, props: Properties, conn_provider: ConnectionProvider) -> Connection:
        conn = conn_provider.connect(host_info, props)
        self._plugin_service.set_availability(host_info.all_aliases, HostAvailability.AVAILABLE)
        self._plugin_service.update_dialect(conn)
        return conn

    def force_connect(self, host_info: HostInfo, props: Properties,
                      initial: bool, force_connect_func: Callable) -> Connection:
        target_driver_props = copy.copy(props)
        PropertiesUtils.remove_wrapper_props(target_driver_props)
        return self._connect(host_info, target_driver_props, self._connection_provider_manager.default_provider)

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any) -> Any:
        result = execute_func()
        if self._plugin_service.current_connection is not None:
            self._plugin_service.update_in_transaction()

        return result

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if HostRole.UNKNOWN == role:
            return False
        return self._connection_provider_manager.accepts_strategy(role, strategy)

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
        if HostRole.UNKNOWN == role:
            raise AwsWrapperError(Messages.get("DefaultPlugin.UnknownHosts"))

        hosts = self._plugin_service.hosts

        if len(hosts) < 1:
            raise AwsWrapperError(Messages.get("DefaultPlugin.EmptyHosts"))

        return self._connection_provider_manager.get_host_info_by_strategy(hosts, role, strategy)

    @property
    def subscribed_methods(self) -> Set[str]:
        return DefaultPlugin._SUBSCRIBED_METHODS

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        # Do nothing
        # This is the last plugin in the plugin chain.
        # So init_host_provider_func will be a no-op and does not need to be called.
        pass
