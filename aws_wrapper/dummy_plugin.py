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
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.utils.properties import Properties

from typing import Any, Callable, Set

from aws_wrapper.plugin import Plugin, PluginFactory


class DummyPlugin(Plugin):
    _NEXT_ID: int = 0
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._id: int = DummyPlugin._NEXT_ID
        DummyPlugin._NEXT_ID += 1

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Any:
        # logger.debug("Plugin {}: connect before".format(self._id))
        result = connect_func()
        # logger.debug("Plugin {}: connect after".format(self._id))
        return result

    def force_connect(self, host_info: HostInfo, props: Properties,
                      initial: bool, force_connect_func: Callable) -> Connection:
        return force_connect_func()

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        # logger.debug("Plugin {}: execute before".format(self._id))
        result = execute_func()
        # logger.debug("Plugin {}: execute after".format(self._id))
        return result

    @property
    def subscribed_methods(self) -> Set[str]:
        return DummyPlugin._SUBSCRIBED_METHODS


class DummyPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return DummyPlugin(plugin_service, props)
