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

from contextlib import closing
from typing import TYPE_CHECKING, FrozenSet, Tuple

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin import Plugin, PluginFactory
    from aws_wrapper.connection_provider import ConnectionProvider
    from threading import Event

from abc import abstractmethod
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Type

from aws_wrapper.aurora_connection_tracker_plugin import (
    AuroraConnectionTrackerPluginFactory,
    AuroraHostListConnectionPluginFactory)
from aws_wrapper.aws_secrets_manager_plugin import \
    AwsSecretsManagerPluginFactory
from aws_wrapper.default_plugin import DefaultPlugin
from aws_wrapper.dialect import (Dialect, DialectManager,
                                 TopologyAwareDatabaseDialect)
from aws_wrapper.dummy_plugin import DummyPluginFactory
from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.exceptions import ExceptionHandler, ExceptionManager
from aws_wrapper.host_list_provider import (ConnectionStringHostListProvider,
                                            HostListProvider,
                                            HostListProviderService,
                                            StaticHostListProvider)
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.iam_plugin import IamAuthPluginFactory
from aws_wrapper.utils.cache_map import CacheMap
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.notifications import (ConnectionEvent, HostEvent,
                                             OldConnectionSuggestedAction)
from aws_wrapper.utils.properties import Properties, WrapperProperties

logger = getLogger(__name__)


class PluginServiceManagerContainer:
    @property
    def plugin_service(self):
        return self._plugin_service

    @plugin_service.setter
    def plugin_service(self, value):
        self._plugin_service = value

    @property
    def plugin_manager(self):
        return self._plugin_manager

    @plugin_manager.setter
    def plugin_manager(self, value):
        self._plugin_manager = value


class PluginService(ExceptionHandler, Protocol):
    @property
    @abstractmethod
    def hosts(self) -> List[HostInfo]:
        ...

    @property
    @abstractmethod
    def current_connection(self) -> Optional[Connection]:
        ...

    def set_current_connection(self, connection: Connection, host_info: HostInfo):
        ...

    @property
    @abstractmethod
    def current_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    @abstractmethod
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    @abstractmethod
    def host_list_provider(self) -> HostListProvider:
        ...

    @host_list_provider.setter
    @abstractmethod
    def host_list_provider(self, provider: HostListProvider):
        ...

    @property
    @abstractmethod
    def is_in_transaction(self) -> bool:
        ...

    def update_dialect(self, connection: Connection):
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(self, role: HostRole, strategy: str):
        ...

    def get_host_role(self, connection: Optional[Connection] = None):
        ...

    def refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def force_refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def connect(self, host_info: HostInfo, props: Properties) -> Connection:
        ...

    def force_connect(self, host_info: HostInfo, props: Properties, timeout_event: Event) -> Connection:
        ...

    def set_availability(self, host_aliases: FrozenSet[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None):
        ...

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        ...


class PluginServiceImpl(PluginService, HostListProviderService):
    _host_availability_expiring_cache: CacheMap[str, HostAvailability] = CacheMap()

    def __init__(
            self,
            container: PluginServiceManagerContainer,
            props: Properties):
        self._container = container
        self._container.plugin_service = self
        self._props = props
        self._host_list_provider: HostListProvider = ConnectionStringHostListProvider()

        self._hosts: List[HostInfo] = []
        self._current_connection: Optional[Connection] = None
        self._current_host_info: Optional[HostInfo] = None
        self._initial_connection_host_info: Optional[HostInfo] = None
        self._exception_manager: ExceptionManager = ExceptionManager()

        self._dialect = DialectManager().get_dialect(props)

    @property
    def hosts(self) -> List[HostInfo]:
        return self._hosts

    @hosts.setter
    def hosts(self, new_hosts: List[HostInfo]):
        self._hosts = new_hosts

    @property
    def current_connection(self) -> Optional[Connection]:
        return self._current_connection

    def set_current_connection(self, connection: Optional[Connection], host_info: Optional[HostInfo]):
        self._current_connection = connection
        self._current_host_info = host_info

    @property
    def current_host_info(self) -> Optional[HostInfo]:
        return self._current_host_info

    @property
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        return self._initial_connection_host_info

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: HostInfo):
        self._initial_connection_host_info = value

    @property
    def host_list_provider(self) -> HostListProvider:
        return self._host_list_provider

    @host_list_provider.setter
    def host_list_provider(self, value: HostListProvider):
        self._host_list_provider = value

    @property
    def is_in_transaction(self) -> bool:
        return False

    @property
    def dialect(self) -> Optional[Dialect]:
        return self._dialect

    def update_dialect(self, connection: Connection):
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.accepts_strategy(role, strategy)

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> Optional[HostInfo]:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.get_host_info_by_strategy(role, strategy)

    def get_host_role(self, connection: Optional[Connection] = None):
        ...

    def refresh_host_list(self, connection: Optional[Connection] = None):
        connection = self.current_connection if connection is None else connection
        updated_host_list: Tuple[HostInfo, ...] = self.host_list_provider.refresh(connection)
        if updated_host_list != self.hosts:
            self._update_host_availability(updated_host_list)
            self._update_hosts(updated_host_list)

    def force_refresh_host_list(self, connection: Optional[Connection] = None):
        connection = self.current_connection if connection is None else connection
        updated_host_list: Tuple[HostInfo, ...] = self.host_list_provider.force_refresh(connection)
        if updated_host_list != self.hosts:
            self._update_host_availability(updated_host_list)
            self._update_hosts(updated_host_list)

    def connect(self, host_info: HostInfo, props: Properties) -> Connection:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.connect(host_info, props, self.current_connection is None)

    def force_connect(self, host_info: HostInfo, props: Properties, timeout_event: Event) -> Connection:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.force_connect(host_info, props, self.current_connection is None)

    def set_availability(self, host_aliases: FrozenSet[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None) -> Optional[HostInfo]:
        if not isinstance(self.dialect, TopologyAwareDatabaseDialect):
            return None

        return self.host_list_provider.identify_connection(connection)

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        if connection is None:
            return

        if host_info is None:
            return

        if len(host_info.aliases) > 0:
            logger.debug(Messages.get_formatted("PluginServiceImpl.NonEmptyAliases", host_info.aliases))
            return

        host_info.add_alias(host_info.as_alias())

        try:
            with closing(connection.cursor()) as cursor:
                cursor.execute(self.dialect.host_alias_query)
                for row in cursor.fetchall():
                    host_info.add_alias(row[0])

        except Exception as e:
            # log and ignore
            logger.debug(Messages.get_formatted("PluginServiceImpl.FailedToRetrieveHostPort", e))

        host = self.identify_connection(connection)
        if host:
            host_info.add_alias(host.as_aliases())

    def is_static_host_list_provider(self) -> bool:
        return isinstance(self._host_list_provider, StaticHostListProvider)

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(dialect=self.dialect, error=error, sql_state=sql_state)

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(dialect=self.dialect, error=error, sql_state=sql_state)

    def _update_host_availability(self, hosts: Tuple[HostInfo, ...]):
        for host in hosts:
            availability: Optional[HostAvailability] = self._host_availability_expiring_cache.get(host.url)
            if availability:
                host.availability = availability

    def _update_hosts(self, new_hosts: Tuple[HostInfo, ...]):
        old_hosts_dict = {x.url: x for x in self.hosts}
        new_hosts_dict = {x.url: x for x in new_hosts}

        changes: Dict[str, Set[HostEvent]] = {}

        for host in self.hosts:
            corresponding_new_host = new_hosts_dict.get(host.url)
            if corresponding_new_host is None:
                changes[host.url] = {HostEvent.HOST_DELETED}
            else:
                host_changes: Set[HostEvent] = self._compare(host, corresponding_new_host)
                if len(host_changes) > 0:
                    changes[host.url] = host_changes

        for key, value in new_hosts_dict.items():
            if key not in old_hosts_dict:
                changes[key] = {HostEvent.HOST_ADDED}

        if len(changes) > 0:
            self.hosts = list(new_hosts) if new_hosts is not None else ()
            self._container.plugin_manager.notify_host_list_changed(changes)

    def _compare(self, host_a: HostInfo, host_b: HostInfo) -> Set[HostEvent]:
        changes: Set[HostEvent] = set()
        if host_a.host != host_b.host or host_a.port != host_b.port:
            changes.add(HostEvent.URL_CHANGED)

        if host_a.role != host_b.role:
            if host_b.role == HostRole.WRITER:
                changes.add(HostEvent.CONVERTED_TO_WRITER)
            elif host_b.role == HostRole.READER:
                changes.add(HostEvent.CONVERTED_TO_READER)

        if host_a.availability != host_b.availability:
            if host_b.availability == HostAvailability.AVAILABLE:
                changes.add(HostEvent.WENT_UP)
            elif host_b.availability == HostAvailability.NOT_AVAILABLE:
                changes.add(HostEvent.WENT_DOWN)

        if len(changes) > 0:
            changes.add(HostEvent.HOST_CHANGED)

        return changes


class PluginManager:
    _ALL_METHODS: str = "*"
    _CONNECT_METHOD: str = "connect"
    _FORCE_CONNECT_METHOD: str = "force_connect"
    _NOTIFY_CONNECTION_CHANGED_METHOD: str = "notify_connection_changed"
    _NOTIFY_HOST_LIST_CHANGED_METHOD: str = "notify_host_list_changed"
    _GET_HOST_INFO_BY_STRATEGY_METHOD: str = "get_host_info_by_strategy"
    _INIT_HOST_LIST_PROVIDER_METHOD: str = "init_host_provider"
    _DEFAULT_PLUGINS = ""

    _PLUGIN_FACTORIES: Dict[str, Type[PluginFactory]] = {
        "dummy": DummyPluginFactory,
        "iam": IamAuthPluginFactory,
        "aws_secrets_manager": AwsSecretsManagerPluginFactory,
        "aurora_connection_tracker": AuroraConnectionTrackerPluginFactory,
        "aurora_host_list": AuroraHostListConnectionPluginFactory
    }

    def __init__(
            self,
            container: PluginServiceManagerContainer,
            props: Properties,
            default_conn_provider: ConnectionProvider):
        self._props: Properties = props
        self._plugins: List[Plugin] = []
        self._function_cache: Dict[str, Callable] = {}
        self._container = container
        self._container.plugin_manager = self

        requested_plugins = WrapperProperties.PLUGINS.get(props)

        if requested_plugins is None:
            requested_plugins = self._DEFAULT_PLUGINS

        if requested_plugins == "":
            self._plugins.append(DefaultPlugin(self._container.plugin_service, default_conn_provider))
            return

        plugin_list: List[str] = requested_plugins.split(",")
        for plugin_code in plugin_list:
            plugin_code = plugin_code.strip()
            if plugin_code not in PluginManager._PLUGIN_FACTORIES:
                raise AwsWrapperError(Messages.get_joined("Plugins.InvalidPlugin", plugin_code))
            factory: PluginFactory = object.__new__(PluginManager._PLUGIN_FACTORIES[plugin_code])
            plugin: Plugin = factory.get_instance(self._container.plugin_service, props)
            self._plugins.append(plugin)

        self._plugins.append(DefaultPlugin(self._container.plugin_service, default_conn_provider))

    @property
    def num_plugins(self) -> int:
        return len(self._plugins)

    def execute(self, target: object, method_name: str, target_driver_func: Callable, *args) -> Any:
        return self._execute_with_subscribed_plugins(
            method_name,
            # next_plugin_func is defined later in make_pipeline
            lambda plugin, next_plugin_func: plugin.execute(target, method_name, next_plugin_func, *args),
            target_driver_func)

    def _execute_with_subscribed_plugins(self, method_name: str, plugin_func: Callable, target_driver_func: Callable):
        pipeline_func: Optional[Callable] = self._function_cache.get(method_name)
        if pipeline_func is None:
            pipeline_func = self._make_pipeline(method_name)
            self._function_cache[method_name] = pipeline_func

        assert (pipeline_func is not None)
        return pipeline_func(plugin_func, target_driver_func)

    # Builds the plugin pipeline function chain. The pipeline is built in a way that allows plugins to perform logic
    # both before and after the target driver function call.
    def _make_pipeline(self, method_name: str) -> Callable:
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
                    pipeline_func = self._create_base_pipeline_func(plugin)
                else:
                    pipeline_func = self._extend_pipeline_func(plugin, pipeline_func)

        if pipeline_func is None:
            raise AwsWrapperError(Messages.get("Plugins.NonePipeline"))
        else:
            return pipeline_func

    def _create_base_pipeline_func(self, plugin):
        # The plugin passed here will be the DefaultPlugin, which is the last plugin in the pipeline
        # The second arg to plugin_func is the next call in the pipeline. Here, it is the target driver function
        return lambda plugin_func, target_driver_func: plugin_func(plugin, target_driver_func)

    def _extend_pipeline_func(self, plugin, pipeline_so_far):
        # Defines the call to a plugin that precedes the DefaultPlugin in the pipeline
        # The second arg to plugin_func effectively appends the tail end of the pipeline to the current plugin's call
        return lambda plugin_func, target_driver_func: \
            plugin_func(plugin, lambda: pipeline_so_far(plugin_func, target_driver_func))

    def connect(self, host_info: Optional[HostInfo], props: Properties, is_initial: bool) \
            -> Connection:
        return self._execute_with_subscribed_plugins(
            PluginManager._CONNECT_METHOD,
            lambda plugin, func: plugin.connect(host_info, props, is_initial, func),
            # The final connect action will be handled by the ConnectionProvider, so this lambda will not be called.
            lambda: None)

    def force_connect(self, host_info: HostInfo, props: Properties, is_initial: bool) \
            -> Connection:
        return self._execute_with_subscribed_plugins(
            PluginManager._FORCE_CONNECT_METHOD,
            lambda plugin, func: plugin.force_connect(host_info, props, is_initial, func),
            # The final connect action will be handled by the ConnectionProvider, so this lambda will not be called.
            lambda: None)

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        old_conn_suggestions: Set[OldConnectionSuggestedAction] = set()
        self._notify_subscribed_plugins(
            PluginManager._NOTIFY_CONNECTION_CHANGED_METHOD,
            lambda plugin: self._notify_plugin_conn_changed(plugin, changes, old_conn_suggestions))

        if OldConnectionSuggestedAction.PRESERVE in old_conn_suggestions:
            return OldConnectionSuggestedAction.PRESERVE
        elif OldConnectionSuggestedAction.DISPOSE in old_conn_suggestions:
            return OldConnectionSuggestedAction.DISPOSE
        else:
            return OldConnectionSuggestedAction.NO_OPINION

    def _notify_subscribed_plugins(self, method_name: str, notify_plugin_func: Callable):
        for plugin in self._plugins:
            subscribed_methods = plugin.subscribed_methods
            is_subscribed = PluginManager._ALL_METHODS in subscribed_methods or method_name in subscribed_methods
            if is_subscribed:
                notify_plugin_func(plugin)

    def _notify_plugin_conn_changed(
            self,
            plugin: Plugin,
            changes: Set[ConnectionEvent],
            old_conn_suggestions: Set[OldConnectionSuggestedAction]):
        suggestion = plugin.notify_connection_changed(changes)
        old_conn_suggestions.add(suggestion)

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        self._notify_subscribed_plugins(PluginManager._NOTIFY_HOST_LIST_CHANGED_METHOD,
                                        lambda plugin: plugin.notify_host_list_changed(changes))

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        for plugin in self._plugins:
            plugin_subscribed_methods = plugin.subscribed_methods
            is_subscribed = \
                self._ALL_METHODS in plugin_subscribed_methods \
                or self._GET_HOST_INFO_BY_STRATEGY_METHOD in plugin_subscribed_methods
            if is_subscribed:
                if plugin.accepts_strategy(role, strategy):
                    return True

        return False

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> Optional[HostInfo]:
        for plugin in self._plugins:
            plugin_subscribed_methods = plugin.subscribed_methods
            is_subscribed = \
                self._ALL_METHODS in plugin_subscribed_methods \
                or self._GET_HOST_INFO_BY_STRATEGY_METHOD in plugin_subscribed_methods

            if is_subscribed:
                host: HostInfo = plugin.get_host_info_by_strategy(role, strategy)
                if host is not None:
                    return host
        return None

    def init_host_provider(self, props: Properties, host_list_provider_service: HostListProviderService):
        return self._execute_with_subscribed_plugins(
            PluginManager._INIT_HOST_LIST_PROVIDER_METHOD,
            lambda plugin, func: plugin.init_host_provider(props, host_list_provider_service, func),
            lambda: None)
