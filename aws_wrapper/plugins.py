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

import copy
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Type

import boto3

from aws_wrapper.connection_provider import (ConnectionProvider,
                                             ConnectionProviderManager)
from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.exceptions import ExceptionHandler, ExceptionManager
from aws_wrapper.host_list_provider import (ConnectionStringHostListProvider,
                                            HostListProvider,
                                            HostListProviderService,
                                            StaticHostListProvider)
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.dialect import Dialect
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.notifications import (ConnectionEvent, HostEvent,
                                             OldConnectionSuggestedAction)
from aws_wrapper.utils.properties import (Properties, PropertiesUtils,
                                          WrapperProperties, WrapperProperty)
from aws_wrapper.utils.rdsutils import RdsUtils

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

    @property
    @abstractmethod
    def is_in_transaction(self) -> bool:
        ...

    @property
    @abstractmethod
    def dialect(self) -> Dialect:
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

    def connect(self, host_info: HostInfo, props: Properties):
        ...

    def force_connect(self, host_info: HostInfo, props: Properties):
        ...

    def set_availability(self, host_aliases: Set[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None):
        ...

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        ...


class PluginServiceImpl(PluginService, HostListProviderService):
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

    @property
    def hosts(self) -> List[HostInfo]:
        return self._hosts

    @property
    def current_connection(self) -> Optional[Connection]:
        return self._current_connection

    def set_current_connection(self, connection: Connection, host_info: HostInfo):
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
    def dialect(self) -> Dialect:
        return Dialect()

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
        ...

    def force_refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def connect(self, host_info: HostInfo, props: Properties) -> Connection:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.connect(host_info, props, self.current_connection is None)

    def force_connect(self, host_info: HostInfo, props: Properties) -> Connection:
        plugin_manager: PluginManager = self._container.plugin_manager
        return plugin_manager.force_connect(host_info, props, self.current_connection is None)

    def set_availability(self, host_aliases: Set[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None):
        ...

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        ...

    def is_static_host_list_provider(self) -> bool:
        return isinstance(self._host_list_provider, StaticHostListProvider)

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(dialect=self.dialect, error=error, sql_state=sql_state)

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(dialect=self.dialect, error=error, sql_state=sql_state)


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

    def init_host_provider(self, initial_url: str, props: Properties,
                           host_list_provider_service: HostListProviderService, init_host_provider_func: Callable):
        return init_host_provider_func()


class PluginFactory(Protocol):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        ...


class DummyPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return DummyPlugin(plugin_service, props)


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
        # logger.debug("Default plugin: connect before")
        result = self._connect(host_info, target_driver_props, connection_provider)
        # logger.debug("Default plugin: connect after")
        return result

    def _connect(self, host_info: HostInfo, props: Properties, conn_provider: ConnectionProvider):
        result = conn_provider.connect(host_info, props)
        return result

    def force_connect(self, host_info: HostInfo, props: Properties,
                      initial: bool, force_connect_func: Callable) -> Connection:
        target_driver_props = copy.copy(props)
        PropertiesUtils.remove_wrapper_props(target_driver_props)
        return self._connect(host_info, target_driver_props, self._connection_provider_manager.default_provider)

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        # logger.debug("Default plugin: execute before")
        result = execute_func()
        # logger.debug("Default plugin: execute after")
        return result

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if HostRole.UNKNOWN == role:
            return False
        return self._connection_provider_manager.accepts_strategy(role, strategy)

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
        if HostRole.UNKNOWN == role:
            raise AwsWrapperError(Messages.get("Plugins.UnknownHosts"))

        hosts = self._plugin_service.hosts

        if len(hosts) < 1:
            raise AwsWrapperError(Messages.get("Plugins.EmptyHosts"))

        return self._connection_provider_manager.get_host_info_by_strategy(hosts, role, strategy)

    @property
    def subscribed_methods(self) -> Set[str]:
        return DefaultPlugin._SUBSCRIBED_METHODS

    def init_host_provider(self, initial_url: str, props: Properties,
                           host_list_provider_service: HostListProviderService, init_host_provider_func: Callable):
        # Do nothing
        # This is the last plugin in the plugin chain.
        # So init_host_provider_func will be a no-op and does not need to be called.
        pass


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


class TokenInfo:
    @property
    def token(self):
        return self.token

    @property
    def expiration(self):
        return self.expiration

    def __init__(self, token: str, expiration: datetime):
        self._token = token
        self._expiration = expiration

    def is_expired(self) -> bool:
        return datetime.now() > self.expiration


class IamAuthConnectionPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect", "force_connect"}
    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60

    IAM_HOST = WrapperProperty("iam_host", "Overrides the host that is used to generate the IAM token")
    IAM_DEFAULT_PORT = WrapperProperty("iam_default_port",
                                       "Overrides default port that is used to generate the IAM token")
    IAM_REGION = WrapperProperty("iam_region", "Overrides AWS region that is used to generate the IAM token")
    IAM_EXPIRATION = WrapperProperty("iam_expiration", "IAM token cache expiration in seconds",
                                     str(_DEFAULT_TOKEN_EXPIRATION_SEC))

    rds_utils: RdsUtils = RdsUtils()

    _TOKEN_CACHE: Dict[str, TokenInfo] = {}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def connect(self, host_info: HostInfo, props: Properties, initial: bool, connect_func: Callable) -> Connection:
        return self._connect(host_info, props, connect_func)

    def _connect(self, host_info: HostInfo, props: Properties, connect_func: Callable) -> Connection:
        if not WrapperProperties.USER.get(props):
            raise AwsWrapperError(f"{WrapperProperties.USER.name} is null or empty.")

        host: str = self.IAM_HOST.get(props) if self.IAM_HOST.get(props) else host_info.host
        region: str = self.IAM_REGION.get(props) if self.IAM_REGION.get(props) else self.get_rds_region(host)
        port: int = self.get_port(props, host_info)
        token_expiration_sec: int = self.IAM_EXPIRATION.get_int(props)

        cache_key: str = self.get_cache_key(
            WrapperProperties.USER.get(props),
            host,
            port,
            region
        )

        token_info: Optional[TokenInfo] = self._TOKEN_CACHE.get(cache_key)

        is_cached_token: bool = token_info and not token_info.is_expired()

        if is_cached_token:
            logger.debug(Messages.get_formatted("IamAuthConnectionPlugin.useCachedIamToken", token_info.token))
            WrapperProperties.PASSWORD.set(props, token_info.token)
        else:
            token: str = self._generate_authentication_token(props, host, port, region)
            logger.debug(Messages.get_formatted("IamAuthConnectionPlugin.generatedNewIamToken", token))
            WrapperProperties.PASSWORD.set(props, token)
            self._TOKEN_CACHE[token] = TokenInfo(token, datetime.now() + timedelta(seconds=token_expiration_sec))

        try:
            return connect_func()

        except Exception as e:
            logger.debug(Messages.get_formatted("IamAuthConnectionPlugin.connectException", e))

            if not self._plugin_service.is_login_exception(error=e) or not is_cached_token:
                raise e

            # Login unsuccessful with cached token
            # Try to generate a new token and try to connect again

            token = self._generate_authentication_token(props, host, port, region)
            logger.debug(Messages.get_formatted("IamAuthConnectionPlugin.generatedNewIamToken", token))
            WrapperProperties.PASSWORD.set(props, token)
            self._TOKEN_CACHE[token] = TokenInfo(token, datetime.now() + timedelta(seconds=token_expiration_sec))

            return connect_func()

    def force_connect(self, host_info: HostInfo, props: Properties, initial: bool,
                      force_connect_func: Callable) -> Connection:
        return self._connect(host_info, props, force_connect_func)

    def _generate_authentication_token(self, props: Properties, hostname: str, port: int,
                                       region: str) -> str:
        session = boto3.Session()
        client = session.client(
            'rds',
            region_name=region,
        )

        user = WrapperProperties.USER.get(props)

        token = client.generate_db_auth_token(
            DBHostname=hostname,
            Port=port,
            DBUsername=user
        )

        client.close()

        return token

    def get_cache_key(self, user: str, hostname: str, port: int, region: str):
        return f"{region}:{hostname}:{port}:{user}"

    def get_port(self, props: Properties, host_info: HostInfo) -> int:
        if self.IAM_DEFAULT_PORT.get(props):
            default_port: int = self.IAM_DEFAULT_PORT.get_int(props)
            if default_port > 0:
                return default_port
            else:
                logger.debug(Messages.get_formatted("IamAuthConnectionPlugin.invalidPort", default_port))

        if host_info.is_port_specified():
            return host_info.port
        else:
            return 5432  # TODO: update after implementing the dialect class

    def get_rds_region(self, hostname: str) -> str:
        rds_region = self.rds_utils.get_rds_region(hostname)

        if not rds_region:
            exception_message = Messages.get_formatted("IamAuthConnectionPlugin.unsupportedHostname", hostname)
            logger.debug(exception_message)
            raise AwsWrapperError(exception_message)

        session = boto3.Session()
        if rds_region not in session.get_available_regions("rds"):
            exception_message = Messages.get_formatted("IamAuthConnectionPlugin.unsupportedRegion", rds_region)
            logger.debug(exception_message)
            raise AwsWrapperError(exception_message)

        return rds_region


class IamAuthConnectionPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return IamAuthConnectionPlugin(plugin_service, props)


class PluginManager:
    _ALL_METHODS: str = "*"
    _CONNECT_METHOD: str = "connect"
    _FORCE_CONNECT_METHOD: str = "force_connect"
    _NOTIFY_CONNECTION_CHANGED_METHOD: str = "notify_connection_changed"
    _NOTIFY_HOST_LIST_CHANGED_METHOD: str = "notify_host_list_changed"
    _GET_HOST_INFO_BY_STRATEGY_METHOD: str = "get_host_info_by_strategy"
    _INIT_HOST_LIST_PROVIDER_METHOD: str = "init_host_provider"

    _PLUGIN_FACTORIES: Dict[str, Type[PluginFactory]] = {
        "dummy": DummyPluginFactory,
        "iam": IamAuthConnectionPluginFactory
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

    def connect(self, host_info: HostInfo, props: Properties, is_initial: bool) \
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

    def init_host_provider(self, initial_url: str, props: Properties,
                           host_list_provider_service: HostListProviderService):
        return self._execute_with_subscribed_plugins(
            PluginManager._INIT_HOST_LIST_PROVIDER_METHOD,
            lambda plugin, func: plugin.init_host_provider(initial_url, props, host_list_provider_service, func),
            lambda: None)
