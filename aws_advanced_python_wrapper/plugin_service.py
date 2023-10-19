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
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.driver_dialect_manager import DriverDialectManager
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin import Plugin
    from threading import Event

from abc import abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import (Any, Callable, Dict, FrozenSet, List, Optional, Protocol,
                    Set, Tuple)

from aws_advanced_python_wrapper.connection_plugin_chain import get_plugins
from aws_advanced_python_wrapper.connection_provider import (
    ConnectionProvider, ConnectionProviderManager)
from aws_advanced_python_wrapper.database_dialect import (
    DatabaseDialect, DatabaseDialectManager, TopologyAwareDatabaseDialect,
    UnknownDatabaseDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.exception_handling import (ExceptionHandler,
                                                            ExceptionManager)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.host_list_provider import (
    ConnectionStringHostListProvider, HostListProvider,
    HostListProviderService, StaticHostListProvider)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from aws_advanced_python_wrapper.utils.decorators import \
    preserve_transaction_status_with_timeout
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)

logger = Logger(__name__)


class PluginServiceManagerContainer:
    @property
    def plugin_service(self) -> PluginService:
        return self._plugin_service

    @plugin_service.setter
    def plugin_service(self, value):
        self._plugin_service = value

    @property
    def plugin_manager(self) -> PluginManager:
        return self._plugin_manager

    @plugin_manager.setter
    def plugin_manager(self, value):
        self._plugin_manager = value


class PluginService(ExceptionHandler, Protocol):
    @property
    @abstractmethod
    def hosts(self) -> Tuple[HostInfo, ...]:
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

    @property
    @abstractmethod
    def driver_dialect(self) -> DriverDialect:
        ...

    @property
    @abstractmethod
    def dialect(self) -> DatabaseDialect:
        ...

    @property
    @abstractmethod
    def network_bound_methods(self) -> Set[str]:
        ...

    def is_network_bound_method(self, method_name: str) -> bool:
        ...

    def update_in_transaction(self, is_in_transaction: Optional[bool] = None):
        ...

    def update_dialect(self, connection: Optional[Connection] = None):
        ...

    def update_driver_dialect(self, connection_provider: ConnectionProvider):
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> Optional[HostInfo]:
        ...

    def get_host_role(self, connection: Optional[Connection] = None) -> HostRole:
        ...

    def refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def force_refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def connect(self, host_info: HostInfo, props: Properties) -> Connection:
        ...

    def force_connect(self, host_info: HostInfo, props: Properties, timeout_event: Optional[Event]) -> Connection:
        ...

    def set_availability(self, host_aliases: FrozenSet[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None) -> Optional[HostInfo]:
        ...

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        ...

    def get_connection_provider_manager(self) -> ConnectionProviderManager:
        ...


class PluginServiceImpl(PluginService, HostListProviderService, CanReleaseResources):
    _host_availability_expiring_cache: CacheMap[str, HostAvailability] = CacheMap()

    _executor: ClassVar[Executor] = ThreadPoolExecutor(thread_name_prefix="PluginServiceImplExecutor")

    def __init__(
            self,
            container: PluginServiceManagerContainer,
            props: Properties,
            target_func: Callable,
            driver_dialect_manager: DriverDialectManager,
            driver_dialect: DriverDialect):
        self._container = container
        self._container.plugin_service = self
        self._props = props
        self._original_url = PropertiesUtils.get_url(props)
        self._host_list_provider: HostListProvider = ConnectionStringHostListProvider(self, props)

        self._hosts: Tuple[HostInfo, ...] = ()
        self._current_connection: Optional[Connection] = None
        self._current_host_info: Optional[HostInfo] = None
        self._initial_connection_host_info: Optional[HostInfo] = None
        self._exception_manager: ExceptionManager = ExceptionManager()
        self._is_in_transaction: bool = False
        self._dialect_provider = DatabaseDialectManager()
        self._target_func = target_func
        self._driver_dialect_manager = driver_dialect_manager
        self._driver_dialect = driver_dialect
        self._dialect = self._dialect_provider.get_dialect(driver_dialect.dialect_code, props)

    @property
    def hosts(self) -> Tuple[HostInfo, ...]:
        return self._hosts

    @hosts.setter
    def hosts(self, new_hosts: Tuple[HostInfo, ...]):
        self._hosts = new_hosts

    @property
    def current_connection(self) -> Optional[Connection]:
        return self._current_connection

    def set_current_connection(self, connection: Optional[Connection], host_info: Optional[HostInfo]):
        old_connection = self._current_connection
        self._current_connection = connection
        self._current_host_info = host_info

        if old_connection is None:
            self._container.plugin_manager.notify_connection_changed({ConnectionEvent.INITIAL_CONNECTION})
        elif old_connection != connection:
            self.update_in_transaction()
            old_connection_suggested_action = \
                self._container.plugin_manager.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
            if old_connection_suggested_action != OldConnectionSuggestedAction.PRESERVE \
                    and not self.driver_dialect.is_closed(old_connection):
                try:
                    old_connection.close()
                except Exception:
                    pass

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
        return self._is_in_transaction

    @property
    def driver_dialect(self) -> DriverDialect:
        return self._driver_dialect

    @property
    def dialect(self) -> DatabaseDialect:
        return self._dialect

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._driver_dialect.network_bound_methods

    def update_in_transaction(self, is_in_transaction: Optional[bool] = None):
        if is_in_transaction is not None:
            self._is_in_transaction = is_in_transaction
        elif self.current_connection is not None:
            self._is_in_transaction = self.driver_dialect.is_in_transaction(self.current_connection)
        else:
            raise AwsWrapperError(Messages.get("PluginServiceImpl.UnableToUpdateTransactionStatus"))

    def is_network_bound_method(self, method_name: str):
        if len(self.network_bound_methods) == 1 and \
                list(self.network_bound_methods)[0] == "*":
            return True
        return method_name in self.network_bound_methods

    def update_dialect(self, connection: Optional[Connection] = None):
        # Updates both database dialects and driver dialect

        connection = self.current_connection if connection is None else connection
        if connection is None:
            raise AwsWrapperError(Messages.get("PluginServiceImpl.UpdateDialectConnectionNone"))

        original_dialect = self._dialect
        self._dialect = \
            self._dialect_provider.query_for_dialect(
                self._original_url,
                self._initial_connection_host_info,
                connection,
                self.driver_dialect)

        if original_dialect != self._dialect:
            host_list_provider_init = self._dialect.get_host_list_provider_supplier()
            self.host_list_provider = host_list_provider_init(self, self._props)

    def update_driver_dialect(self, connection_provider: ConnectionProvider):
        self._driver_dialect = self._driver_dialect_manager.get_pool_connection_driver_dialect(
            connection_provider, self._driver_dialect)

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.accepts_strategy(role, strategy)

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> Optional[HostInfo]:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.get_host_info_by_strategy(role, strategy)

    def get_host_role(self, connection: Optional[Connection] = None) -> HostRole:
        connection = connection if connection is not None else self.current_connection
        if connection is None:
            raise AwsWrapperError(Messages.get("PluginServiceImpl.GetHostRoleConnectionNone"))

        return self._host_list_provider.get_host_role(connection)

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
        return plugin_manager.connect(
            self._target_func, self._driver_dialect, host_info, props, self.current_connection is None)

    def force_connect(self, host_info: HostInfo, props: Properties, timeout_event: Optional[Event]) -> Connection:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.force_connect(
            self._target_func, self._driver_dialect, host_info, props, self.current_connection is None)

    def set_availability(self, host_aliases: FrozenSet[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None) -> Optional[HostInfo]:
        connection = self.current_connection if connection is None else connection

        if not isinstance(self.dialect, TopologyAwareDatabaseDialect):
            return None

        return self.host_list_provider.identify_connection(connection)

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        connection = self.current_connection if connection is None else connection
        host_info = self.current_host_info if host_info is None else host_info
        if connection is None or host_info is None:
            return

        if len(host_info.aliases) > 0:
            logger.debug("PluginServiceImpl.NonEmptyAliases", host_info.aliases)
            return

        host_info.add_alias(host_info.as_alias())

        driver_dialect = self._driver_dialect
        try:
            timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get(self._props)
            cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(PluginServiceImpl._executor,
                                                                                        timeout_sec, driver_dialect,
                                                                                        connection)(self._fill_aliases)
            cursor_execute_func_with_timeout(connection, host_info)

        except Exception as e:
            # log and ignore
            logger.debug("PluginServiceImpl.FailedToRetrieveHostPort", e)

        host = self.identify_connection(connection)
        if host:
            host_info.add_alias(*host.as_aliases())

    def _fill_aliases(self, conn: Connection, host_info: HostInfo) -> bool:
        with closing(conn.cursor()) as cursor:
            if not isinstance(self.dialect, UnknownDatabaseDialect):
                cursor.execute(self.dialect.host_alias_query)
                for row in cursor.fetchall():
                    host_info.add_alias(row[0])
                return True
        return False

    def is_static_host_list_provider(self) -> bool:
        return self._host_list_provider is StaticHostListProvider

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(dialect=self.dialect, error=error, sql_state=sql_state)

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(dialect=self.dialect, error=error, sql_state=sql_state)

    def get_connection_provider_manager(self) -> ConnectionProviderManager:
        return self._container.plugin_manager.connection_provider_manager

    def _update_host_availability(self, hosts: Tuple[HostInfo, ...]):
        for host in hosts:
            availability: Optional[HostAvailability] = self._host_availability_expiring_cache.get(host.url)
            if availability:
                host.set_availability(availability)

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
            self.hosts = tuple(new_hosts) if new_hosts is not None else ()
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

        if host_a.get_availability() != host_b.get_availability():
            if host_b.get_availability() == HostAvailability.AVAILABLE:
                changes.add(HostEvent.WENT_UP)
            elif host_b.get_availability() == HostAvailability.UNAVAILABLE:
                changes.add(HostEvent.WENT_DOWN)

        if len(changes) > 0:
            changes.add(HostEvent.HOST_CHANGED)

        return changes

    def release_resources(self):
        try:
            if self.current_connection is not None and not self.driver_dialect.is_closed(
                    self.current_connection):
                self.current_connection.close()
        except Exception:
            # ignore
            pass

        host_list_provider = self.host_list_provider
        if host_list_provider is not None and isinstance(host_list_provider, CanReleaseResources):
            host_list_provider.release_resources()


class PluginManager(CanReleaseResources):
    _ALL_METHODS: str = "*"
    _CONNECT_METHOD: str = "connect"
    _FORCE_CONNECT_METHOD: str = "force_connect"
    _NOTIFY_CONNECTION_CHANGED_METHOD: str = "notify_connection_changed"
    _NOTIFY_HOST_LIST_CHANGED_METHOD: str = "notify_host_list_changed"
    _GET_HOST_INFO_BY_STRATEGY_METHOD: str = "get_host_info_by_strategy"
    _INIT_HOST_LIST_PROVIDER_METHOD: str = "init_host_provider"

    def __init__(self, container: PluginServiceManagerContainer, props: Properties):
        self._props: Properties = props
        self._plugins: List[Plugin] = []
        self._function_cache: Dict[str, Callable] = {}
        self._container = container
        self._container.plugin_manager = self
        self._connection_provider_manager = ConnectionProviderManager()
        self._plugins = get_plugins(self._container.plugin_service, self._connection_provider_manager, self._props)

    @property
    def num_plugins(self) -> int:
        return len(self._plugins)

    @property
    def connection_provider_manager(self) -> ConnectionProviderManager:
        return self._connection_provider_manager

    def get_current_connection_provider(self, host_info: HostInfo, properties: Properties):
        return self.connection_provider_manager.get_connection_provider(host_info, properties)

    def execute(self, target: object, method_name: str, target_driver_func: Callable, *args, **kwargs) -> Any:
        plugin_service = self._container.plugin_service
        driver_dialect = plugin_service.driver_dialect
        conn: Optional[Connection] = driver_dialect.get_connection_from_obj(target)
        current_conn: Optional[Connection] = driver_dialect.unwrap_connection(plugin_service.current_connection)

        if method_name not in ["Connection.close", "Cursor.close"] and conn is not None and conn != current_conn:
            raise AwsWrapperError(Messages.get_formatted("PluginManager.MethodInvokedAgainstOldConnection", target))

        if conn is None and method_name in ["Connection.close", "Cursor.close"]:
            return

        return self._execute_with_subscribed_plugins(
            method_name,
            # next_plugin_func is defined later in make_pipeline
            lambda plugin, next_plugin_func: plugin.execute(target, method_name, next_plugin_func, *args, **kwargs),
            target_driver_func)

    def _execute_with_subscribed_plugins(self, method_name: str, plugin_func: Callable, target_driver_func: Callable):
        pipeline_func: Optional[Callable] = self._function_cache.get(method_name)
        if pipeline_func is None:
            pipeline_func = self._make_pipeline(method_name)
            self._function_cache[method_name] = pipeline_func

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
            raise AwsWrapperError(Messages.get("PluginManager.PipelineNone"))
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

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            host_info: Optional[HostInfo],
            props: Properties,
            is_initial_connection: bool) -> Connection:
        return self._execute_with_subscribed_plugins(
            PluginManager._CONNECT_METHOD,
            lambda plugin, func: plugin.connect(
                target_func, driver_dialect, host_info, props, is_initial_connection, func),
            # The final connect action will be handled by the ConnectionProvider, so this lambda will not be called.
            lambda: None)

    def force_connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            host_info: Optional[HostInfo],
            props: Properties,
            is_initial_connection: bool) -> Connection:
        return self._execute_with_subscribed_plugins(
            PluginManager._FORCE_CONNECT_METHOD,
            lambda plugin, func: plugin.force_connect(
                target_func, driver_dialect, host_info, props, is_initial_connection, func),
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
                try:
                    host: HostInfo = plugin.get_host_info_by_strategy(role, strategy)
                    if host is not None:
                        return host
                except UnsupportedOperationError:
                    # This plugin does not support the requested strategy, ignore exception and try the next plugin
                    pass
        return None

    def init_host_provider(self, props: Properties, host_list_provider_service: HostListProviderService):
        return self._execute_with_subscribed_plugins(
            PluginManager._INIT_HOST_LIST_PROVIDER_METHOD,
            lambda plugin, func: plugin.init_host_provider(props, host_list_provider_service, func),
            lambda: None)

    def release_resources(self):
        """
        Allows all connection plugins a chance to clean up any dangling resources
        or perform any last tasks before shutting down.
        """
        for plugin in self._plugins:
            if isinstance(plugin, CanReleaseResources):
                plugin.release_resources()