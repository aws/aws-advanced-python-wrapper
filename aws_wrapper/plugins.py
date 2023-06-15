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

from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Type

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostspec import HostRole, HostSpec
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.properties import Properties


class PluginService:
    @property
    def current_connection(self):
        return self.current_connection

    @property
    def current_host_spec(self):
        return self.current_host_spec

    @property
    def hosts(self):
        return []

    @property
    def host_list_provider(self):
        return self.host_list_provider

    @property
    def initial_connection_host_spec(self):
        return self.initial_connection_host_spec

    @initial_connection_host_spec.setter
    def initial_connection_host_spec(self, value: HostSpec):
        self.initial_connection_host_spec = value

    def accepts_strategy(self, role: HostRole, strategy: str):
        ...

    def get_host_spec_by_strategy(self, role: HostRole, strategy: str):
        ...

    def get_host_role(self):
        ...

    def refresh_host_list(self):
        ...

    def force_refresh_host_list(self):
        ...

    def connect(self, host_info: HostSpec, props: Properties):
        ...

    def force_connect(self, host_info: HostSpec, props: Properties):
        ...


class Plugin(Protocol):

    @property
    def subscribed_methods(self) -> Set[str]:
        return {""}

    def connect(self, host_info: HostSpec, props: Properties,
                initial: bool, connect_func: Callable) -> Connection:
        ...

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        ...


class PluginFactory(Protocol):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        ...


class DummyPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return DummyPlugin(plugin_service, props)


class DefaultPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service: PluginService = plugin_service
        self._props: Properties = props

    def connect(self, host_info: HostSpec, props: Properties,
                initial: bool, connect_func: Callable) -> Any:
        # logger.debug("Default plugin: connect before")
        result = connect_func()
        # logger.debug("Default plugin: connect after")
        return result

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        # logger.debug("Default plugin: execute before")
        result = execute_func()
        # logger.debug("Default plugin: execute after")
        return result

    @property
    def subscribed_methods(self) -> Set[str]:
        return DefaultPlugin._SUBSCRIBED_METHODS


class DummyPlugin(Plugin):
    _NEXT_ID: int = 0
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._id: int = DummyPlugin._NEXT_ID
        DummyPlugin._NEXT_ID += 1

    def connect(self, host_info: HostSpec, props: Properties,
                initial: bool, connect_func: Callable) -> Any:
        # logger.debug("Plugin {}: connect before".format(self._id))
        result = connect_func()
        # logger.debug("Plugin {}: connect after".format(self._id))
        return result

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        # logger.debug("Plugin {}: execute before".format(self._id))
        result = execute_func()
        # logger.debug("Plugin {}: execute after".format(self._id))
        return result

    @property
    def subscribed_methods(self) -> Set[str]:
        return DummyPlugin._SUBSCRIBED_METHODS


class PluginManager:
    _ALL_METHODS: str = "*"
    _CONNECT_METHOD: str = "connect"
    _DEFAULT_PLUGINS: str = "dummy,dummy"
    _PLUGIN_FACTORIES: Dict[str, Type[PluginFactory]] = {
        "dummy": DummyPluginFactory
    }

    def __init__(self, props: Properties, plugin_service: PluginService):
        self._props: Properties = props
        self._plugins: List[Plugin] = []
        self._execute_func: Optional[Callable] = None
        self._connect_func: Optional[Callable] = None
        self._function_cache: Dict[str, Callable] = {}

        requested_plugins: str
        if "plugins" in props:
            requested_plugins = props["plugins"]
        else:
            requested_plugins = PluginManager._DEFAULT_PLUGINS

        if requested_plugins == "":
            self._plugins.append(DefaultPlugin(plugin_service, props))
            return

        plugin_list: List[str] = requested_plugins.split(",")
        for plugin_code in plugin_list:
            plugin_code = plugin_code.strip()
            if plugin_code not in PluginManager._PLUGIN_FACTORIES:
                raise AwsWrapperError("Invalid plugin requested: ".join(plugin_code))
            factory: PluginFactory = object.__new__(PluginManager._PLUGIN_FACTORIES[plugin_code])
            plugin: Plugin = factory.get_instance(plugin_service, props)
            self._plugins.append(plugin)

        self._plugins.append(DefaultPlugin(plugin_service, props))

    @property
    def num_plugins(self) -> int:
        return len(self._plugins)

    def execute(self, target: object, method_name: str, target_driver_func: Callable, *args) -> Any:
        return self._execute_with_subscribed_plugins(method_name,
                                                     # next_plugin_func is defined later in make_pipeline
                                                     lambda plugin, next_plugin_func:
                                                     plugin.execute(target, method_name, next_plugin_func, *args),
                                                     target_driver_func)

    def _execute_with_subscribed_plugins(self, method_name: str, plugin_func: Callable, target_driver_func: Callable):
        pipeline_func: Optional[Callable] = self._function_cache.get(method_name)
        if pipeline_func is None:
            pipeline_func = self.make_pipeline(method_name)
            self._function_cache[method_name] = pipeline_func

        assert (pipeline_func is not None)
        return pipeline_func(plugin_func, target_driver_func)

    # Builds the plugin pipeline function chain. The pipeline is built in a way that allows plugins to perform logic
    # both before and after the target driver function call.
    def make_pipeline(self, method_name: str) -> Callable:
        pipeline_func: Optional[Callable] = None
        num_plugins: int = len(self._plugins)

        # Build the pipeline starting at the end and working backwards
        for i in range(num_plugins - 1, -1, -1):
            plugin: Plugin = self._plugins[i]
            subscribed_methods: Set[str] = plugin.subscribed_methods
            is_subscribed: bool = PluginManager._ALL_METHODS in subscribed_methods or method_name in subscribed_methods

            if is_subscribed:
                if pipeline_func is None:
                    # Defines the call to DefaultPlugin, which is the last plugin in the pipeline
                    pipeline_func = self.create_base_pipeline_func(plugin)
                else:
                    pipeline_func = self.extend_pipeline_func(plugin, pipeline_func)

        if pipeline_func is None:
            raise AwsWrapperError("A pipeline was requested but the created pipeline evaluated to None")
        else:
            return pipeline_func

    def create_base_pipeline_func(self, plugin):
        # The plugin passed here will be the DefaultPlugin, which is the last plugin in the pipeline
        # The second arg to plugin_func is the next call in the pipeline. Here, it is the target driver function
        return lambda plugin_func, target_driver_func: plugin_func(plugin, target_driver_func)

    def extend_pipeline_func(self, plugin, pipeline_so_far):
        # Defines the call to a plugin that precedes the DefaultPlugin in the pipeline
        # The second arg to plugin_func effectively appends the tail end of the pipeline to the current plugin's call
        return lambda plugin_func, target_driver_func: \
            plugin_func(plugin, lambda: pipeline_so_far(plugin_func, target_driver_func))

    def connect(self, host_info: HostSpec, props: Properties, is_initial: bool, target_driver_func: Callable) \
            -> Connection:
        return self._execute_with_subscribed_plugins(PluginManager._CONNECT_METHOD,
                                                     lambda plugin, func: plugin.connect(host_info, props, is_initial,
                                                                                         func),
                                                     target_driver_func)
