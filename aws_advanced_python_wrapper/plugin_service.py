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

from typing import TYPE_CHECKING, ClassVar, List, Type

from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import \
    AuroraInitialConnectionStrategyPluginFactory
from aws_advanced_python_wrapper.custom_endpoint_plugin import \
    CustomEndpointPluginFactory
from aws_advanced_python_wrapper.fastest_response_strategy_plugin import \
    FastestResponseStrategyPluginFactory
from aws_advanced_python_wrapper.federated_plugin import \
    FederatedAuthPluginFactory
from aws_advanced_python_wrapper.okta_plugin import OktaAuthPluginFactory
from aws_advanced_python_wrapper.states.session_state_service import (
    SessionStateService, SessionStateServiceImpl)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.allowed_and_blocked_hosts import AllowedAndBlockedHosts
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.driver_dialect_manager import DriverDialectManager
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
    from threading import Event

from abc import abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor, TimeoutError
from contextlib import closing
from typing import (Any, Callable, Dict, FrozenSet, Optional, Protocol, Set,
                    Tuple)

from aws_advanced_python_wrapper.aurora_connection_tracker_plugin import \
    AuroraConnectionTrackerPluginFactory
from aws_advanced_python_wrapper.aws_secrets_manager_plugin import \
    AwsSecretsManagerPluginFactory
from aws_advanced_python_wrapper.connect_time_plugin import \
    ConnectTimePluginFactory
from aws_advanced_python_wrapper.connection_provider import (
    ConnectionProvider, ConnectionProviderManager)
from aws_advanced_python_wrapper.database_dialect import (
    DatabaseDialect, DatabaseDialectManager, TopologyAwareDatabaseDialect,
    UnknownDatabaseDialect)
from aws_advanced_python_wrapper.default_plugin import DefaultPlugin
from aws_advanced_python_wrapper.developer_plugin import DeveloperPluginFactory
from aws_advanced_python_wrapper.driver_configuration_profiles import \
    DriverConfigurationProfiles
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.exception_handling import (ExceptionHandler,
                                                            ExceptionManager)
from aws_advanced_python_wrapper.execute_time_plugin import \
    ExecuteTimePluginFactory
from aws_advanced_python_wrapper.failover_plugin import FailoverPluginFactory
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.host_list_provider import (
    ConnectionStringHostListProvider, HostListProvider,
    HostListProviderService, StaticHostListProvider)
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    HostMonitoringPluginFactory
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.iam_plugin import IamAuthPluginFactory
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    ReadWriteSplittingPluginFactory
from aws_advanced_python_wrapper.stale_dns_plugin import StaleDnsPluginFactory
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
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryContext, TelemetryFactory, TelemetryTraceLevel)

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
    def all_hosts(self) -> Tuple[HostInfo, ...]:
        ...

    @property
    @abstractmethod
    def hosts(self) -> Tuple[HostInfo, ...]:
        ...

    @property
    @abstractmethod
    def allowed_and_blocked_hosts(self) -> Optional[AllowedAndBlockedHosts]:
        ...

    @allowed_and_blocked_hosts.setter
    def allowed_and_blocked_hosts(self, allowed_and_blocked_hosts: Optional[AllowedAndBlockedHosts]):
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
    def session_state_service(self):
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
    def database_dialect(self) -> DatabaseDialect:
        ...

    @property
    @abstractmethod
    def network_bound_methods(self) -> Set[str]:
        ...

    @property
    @abstractmethod
    def props(self) -> Properties:
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
        """
        Returns a boolean indicating if any of the configured :py:class:`ConnectionPlugin` or :py:class:`ConnectionProvider` objects implement the
        specified host selection strategy for the given role in  :py:method:`ConnectionPlugin.get_host_info_by_strategy`
        or :py:method:`ConnectionProvider.get_host_info_by_strategy`.

        :param role: the desired role of the selected host - either a reader host or a writer host.
        :param strategy: the strategy that should be used to pick a host (eg "random").
        :return: `True` if any of the configured :py:class:`ConnectionPlugin` or :py:class:`ConnectionProvider` objects support the selection of a
        host with the requested role and strategy via :py:method:`ConnectionPlugin.get_host_info_by_strategy`
        or :py:method:`ConnectionProvider.get_host_info_by_strategy`. Otherwise, return `False`.
        """
        ...

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> Optional[HostInfo]:
        """
        Selects a :py:class:`HostInfo` with the requested role from available hosts using the requested strategy.
        :py:method:`PluginService.accepts_strategy` should be called first to evaluate if any of the configured :py:class:`ConnectionPlugin`
        or :py:class:`ConnectionProvider` objects support the selection of a host with the requested role and strategy.

        :param role: the desired role of the selected host - either a reader host or a writer host.
        :param strategy: the strategy that should be used to pick a host (eg "random").
        :return: a py:class:`HostInfo` with the requested role.
        """
        ...

    def get_host_role(self, connection: Optional[Connection] = None) -> HostRole:
        ...

    def refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def force_refresh_host_list(self, connection: Optional[Connection] = None):
        ...

    def connect(self, host_info: HostInfo, props: Properties) -> Connection:
        """
        Establishes a connection to the given host using the given driver protocol and properties. If a
        non-default :py:class`ConnectionProvider` has been set with :py:method:`ConnectionProviderManager.set_connection_provider`,
        the connection will be created by the non-default ConnectionProvider.
        Otherwise, the connection will be created by the default :py:class`DriverConnectionProvider`.

        :param host_info: the host details for the desired connection.
        :param props: the connection properties.
        :return: a :py:class`Connection` to the requested host.
        """
        ...

    def force_connect(self, host_info: HostInfo, props: Properties, timeout_event: Optional[Event]) -> Connection:
        """
        Establishes a connection to the given host using the given driver protocol and properties.
        This call differs from connect in that the default :py:class`DriverConnectionProvider` will be used to establish the connection even if
        a non-default :py:class`ConnectionProvider` has been set via :py:method:`ConnectionProviderManager.set_connection_provider`.

        :param host_info: the host details for the desired connection.
        :param props: the connection properties.
        :return: a :py:class`Connection` to the requested host.
        """
        ...

    def set_availability(self, host_aliases: FrozenSet[str], availability: HostAvailability):
        ...

    def identify_connection(self, connection: Optional[Connection] = None) -> Optional[HostInfo]:
        ...

    def fill_aliases(self, connection: Optional[Connection] = None, host_info: Optional[HostInfo] = None):
        ...

    def get_connection_provider_manager(self) -> ConnectionProviderManager:
        ...

    def get_telemetry_factory(self) -> TelemetryFactory:
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
            driver_dialect: DriverDialect,
            session_state_service: Optional[SessionStateService] = None):
        self._container = container
        self._container.plugin_service = self
        self._props = props
        self._original_url = PropertiesUtils.get_url(props)
        self._host_list_provider: HostListProvider = ConnectionStringHostListProvider(self, props)

        self._all_hosts: Tuple[HostInfo, ...] = ()
        self._allowed_and_blocked_hosts: Optional[AllowedAndBlockedHosts] = None
        self._current_connection: Optional[Connection] = None
        self._current_host_info: Optional[HostInfo] = None
        self._initial_connection_host_info: Optional[HostInfo] = None
        self._exception_manager: ExceptionManager = ExceptionManager()
        self._is_in_transaction: bool = False
        self._dialect_provider = DatabaseDialectManager(props)
        self._target_func = target_func
        self._driver_dialect_manager = driver_dialect_manager
        self._driver_dialect = driver_dialect
        self._database_dialect = self._dialect_provider.get_dialect(driver_dialect.dialect_code, props)
        self._session_state_service = session_state_service if session_state_service is not None else SessionStateServiceImpl(self, props)

    @property
    def all_hosts(self) -> Tuple[HostInfo, ...]:
        return self._all_hosts

    @property
    def hosts(self) -> Tuple[HostInfo, ...]:
        host_permissions = self.allowed_and_blocked_hosts
        if host_permissions is None:
            return self._all_hosts

        hosts = self._all_hosts
        allowed_ids = host_permissions.allowed_host_ids
        blocked_ids = host_permissions.blocked_host_ids

        if allowed_ids is not None:
            hosts = tuple(host for host in hosts if host.host_id in allowed_ids)

        if blocked_ids is not None:
            hosts = tuple(host for host in hosts if host.host_id not in blocked_ids)

        return hosts

    @property
    def allowed_and_blocked_hosts(self) -> Optional[AllowedAndBlockedHosts]:
        return self._allowed_and_blocked_hosts

    @allowed_and_blocked_hosts.setter
    def allowed_and_blocked_hosts(self, allowed_and_blocked_hosts: Optional[AllowedAndBlockedHosts]):
        self._allowed_and_blocked_hosts = allowed_and_blocked_hosts

    @property
    def current_connection(self) -> Optional[Connection]:
        return self._current_connection

    def set_current_connection(self, connection: Optional[Connection], host_info: Optional[HostInfo]):
        old_connection = self._current_connection

        if self._current_connection is None:
            self._current_connection = connection
            self._current_host_info = host_info
            self.session_state_service.reset()

            self._container.plugin_manager.notify_connection_changed({ConnectionEvent.INITIAL_CONNECTION})
            return

        if connection is not None and old_connection is not None and old_connection != connection:
            # Update an existing connection.

            is_in_transaction = self._is_in_transaction
            self.session_state_service.begin()

            try:
                self._current_connection = connection
                self._current_host_info = host_info

                self.session_state_service.apply_current_session_state(connection)
                self.update_in_transaction(False)

                if is_in_transaction and WrapperProperties.ROLLBACK_ON_SWITCH.get_bool(self.props):
                    try:
                        old_connection.rollback()
                    except Exception:
                        # Ignore any exception.
                        pass

                old_connection_suggested_action = \
                    self._container.plugin_manager.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})

                if old_connection_suggested_action != OldConnectionSuggestedAction.PRESERVE and not self.driver_dialect.is_closed(old_connection):

                    try:
                        self.session_state_service.apply_pristine_session_state(old_connection)
                    except Exception:
                        # Ignore any exception.
                        pass

                    try:
                        old_connection.close()
                    except Exception:
                        # Ignore any exception.
                        pass
            finally:
                self.session_state_service.complete()

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
    def session_state_service(self) -> SessionStateService:
        return self._session_state_service

    @property
    def is_in_transaction(self) -> bool:
        return self._is_in_transaction

    @property
    def driver_dialect(self) -> DriverDialect:
        return self._driver_dialect

    @property
    def database_dialect(self) -> DatabaseDialect:
        return self._database_dialect

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._driver_dialect.network_bound_methods

    @property
    def props(self) -> Properties:
        return self._props

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

        original_dialect = self._database_dialect
        self._database_dialect = \
            self._dialect_provider.query_for_dialect(
                self._original_url,
                self._initial_connection_host_info,
                connection,
                self.driver_dialect)

        if original_dialect != self._database_dialect:
            host_list_provider_init = self._database_dialect.get_host_list_provider_supplier()
            self.host_list_provider = host_list_provider_init(self, self._props)

    def update_driver_dialect(self, connection_provider: ConnectionProvider):
        self._driver_dialect = self._driver_dialect_manager.get_pool_connection_driver_dialect(
            connection_provider, self._driver_dialect, self._props)

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
        if updated_host_list != self._all_hosts:
            self._update_host_availability(updated_host_list)
            self._update_hosts(updated_host_list)

    def force_refresh_host_list(self, connection: Optional[Connection] = None):
        connection = self.current_connection if connection is None else connection
        updated_host_list: Tuple[HostInfo, ...] = self.host_list_provider.force_refresh(connection)
        if updated_host_list != self._all_hosts:
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

        if not isinstance(self.database_dialect, TopologyAwareDatabaseDialect):
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
            cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                PluginServiceImpl._executor, timeout_sec, driver_dialect, connection)(self._fill_aliases)
            cursor_execute_func_with_timeout(connection, host_info)
        except TimeoutError as e:
            raise QueryTimeoutError(Messages.get("PluginServiceImpl.FillAliasesTimeout")) from e
        except Exception as e:
            # log and ignore
            logger.debug("PluginServiceImpl.FailedToRetrieveHostPort", e)

        host = self.identify_connection(connection)
        if host:
            host_info.add_alias(*host.as_aliases())

    def _fill_aliases(self, conn: Connection, host_info: HostInfo) -> bool:
        with closing(conn.cursor()) as cursor:
            if not isinstance(self.database_dialect, UnknownDatabaseDialect):
                cursor.execute(self.database_dialect.host_alias_query)
                for row in cursor.fetchall():
                    host_info.add_alias(row[0])
                return True
        return False

    def is_static_host_list_provider(self) -> bool:
        return self._host_list_provider is StaticHostListProvider

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(
            dialect=self.database_dialect, error=error, sql_state=sql_state)

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(
            dialect=self.database_dialect, error=error, sql_state=sql_state)

    def get_connection_provider_manager(self) -> ConnectionProviderManager:
        return self._container.plugin_manager.connection_provider_manager

    def get_telemetry_factory(self) -> TelemetryFactory:
        return self._container.plugin_manager.telemetry_factory

    def _update_host_availability(self, hosts: Tuple[HostInfo, ...]):
        for host in hosts:
            availability: Optional[HostAvailability] = self._host_availability_expiring_cache.get(host.url)
            if availability:
                host.set_availability(availability)

    def _update_hosts(self, new_hosts: Tuple[HostInfo, ...]):
        old_hosts_dict = {x.url: x for x in self._all_hosts}
        new_hosts_dict = {x.url: x for x in new_hosts}

        changes: Dict[str, Set[HostEvent]] = {}

        for host in self._all_hosts:
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
            self._all_hosts = tuple(new_hosts) if new_hosts is not None else ()
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

    PLUGIN_FACTORIES: Dict[str, Type[PluginFactory]] = {
        "iam": IamAuthPluginFactory,
        "aws_secrets_manager": AwsSecretsManagerPluginFactory,
        "aurora_connection_tracker": AuroraConnectionTrackerPluginFactory,
        "host_monitoring": HostMonitoringPluginFactory,
        "failover": FailoverPluginFactory,
        "read_write_splitting": ReadWriteSplittingPluginFactory,
        "fastest_response_strategy": FastestResponseStrategyPluginFactory,
        "stale_dns": StaleDnsPluginFactory,
        "custom_endpoint": CustomEndpointPluginFactory,
        "connect_time": ConnectTimePluginFactory,
        "execute_time": ExecuteTimePluginFactory,
        "dev": DeveloperPluginFactory,
        "federated_auth": FederatedAuthPluginFactory,
        "okta": OktaAuthPluginFactory,
        "initial_connection": AuroraInitialConnectionStrategyPluginFactory
    }

    WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1

    # The final list of plugins will be sorted by weight, starting from the lowest values up to
    # the highest values. The first plugin of the list will have the lowest weight, and the
    # last one will have the highest weight.
    PLUGIN_FACTORY_WEIGHTS: Dict[Type[PluginFactory], int] = {
        CustomEndpointPluginFactory: 40,
        AuroraInitialConnectionStrategyPluginFactory: 50,
        AuroraConnectionTrackerPluginFactory: 100,
        StaleDnsPluginFactory: 200,
        ReadWriteSplittingPluginFactory: 300,
        FailoverPluginFactory: 400,
        HostMonitoringPluginFactory: 500,
        FastestResponseStrategyPluginFactory: 600,
        IamAuthPluginFactory: 700,
        AwsSecretsManagerPluginFactory: 800,
        FederatedAuthPluginFactory: 900,
        OktaAuthPluginFactory: 1000,
        ConnectTimePluginFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
        ExecuteTimePluginFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
        DeveloperPluginFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN
    }

    def __init__(
            self, container: PluginServiceManagerContainer, props: Properties, telemetry_factory: TelemetryFactory):
        self._props: Properties = props
        self._function_cache: Dict[str, Callable] = {}
        self._container = container
        self._container.plugin_manager = self
        self._connection_provider_manager = ConnectionProviderManager()
        self._telemetry_factory = telemetry_factory
        self._plugins = self.get_plugins()

    @property
    def num_plugins(self) -> int:
        return len(self._plugins)

    @property
    def connection_provider_manager(self) -> ConnectionProviderManager:
        return self._connection_provider_manager

    @property
    def telemetry_factory(self) -> TelemetryFactory:
        return self._telemetry_factory

    def get_current_connection_provider(self, host_info: HostInfo, properties: Properties):
        return self.connection_provider_manager.get_connection_provider(host_info, properties)

    @staticmethod
    def register_plugin(
            plugin_code: str, plugin_factory: Type[PluginFactory], weight: int = WEIGHT_RELATIVE_TO_PRIOR_PLUGIN):
        PluginManager.PLUGIN_FACTORIES[plugin_code] = plugin_factory
        PluginManager.PLUGIN_FACTORY_WEIGHTS[plugin_factory] = weight

    def get_plugins(self) -> List[Plugin]:
        plugin_factories: List[PluginFactory] = []
        plugins: List[Plugin] = []

        profile_name = WrapperProperties.PROFILE_NAME.get(self._props)
        if profile_name is not None:
            if not DriverConfigurationProfiles.contains_profile(profile_name):
                raise AwsWrapperError(
                    Messages.get_formatted("PluginManager.ConfigurationProfileNotFound", profile_name))
            plugin_factories = DriverConfigurationProfiles.get_plugin_factories(profile_name)
        else:
            plugin_codes = WrapperProperties.PLUGINS.get(self._props)
            if plugin_codes is None:
                plugin_codes = WrapperProperties.DEFAULT_PLUGINS

            if plugin_codes != "":
                plugin_factories = self.create_plugin_factories_from_list(plugin_codes.split(","))

        for factory in plugin_factories:
            plugin = factory.get_instance(self._container.plugin_service, self._props)
            plugins.append(plugin)

        plugins.append(DefaultPlugin(self._container.plugin_service, self._connection_provider_manager))

        return plugins

    def create_plugin_factories_from_list(self, plugin_code_list: List[str]) -> List[PluginFactory]:
        factory_types: List[Type[PluginFactory]] = []
        for plugin_code in plugin_code_list:
            plugin_code = plugin_code.strip()
            if plugin_code not in PluginManager.PLUGIN_FACTORIES:
                raise AwsWrapperError(Messages.get_formatted("PluginManager.InvalidPlugin", plugin_code))

            factory_types.append(PluginManager.PLUGIN_FACTORIES[plugin_code])

        if len(factory_types) <= 0:
            return []

        auto_sort_plugins = WrapperProperties.AUTO_SORT_PLUGIN_ORDER.get(self._props)
        if auto_sort_plugins:
            weights = PluginManager.get_factory_weights(factory_types)
            factory_types.sort(key=lambda factory_type: weights[factory_type])
            plugin_code_list.sort(key=lambda plugin_code: weights[PluginManager.PLUGIN_FACTORIES[plugin_code]])
            logger.debug("PluginManager.ResortedPlugins", plugin_code_list)

        factories: List[PluginFactory] = []
        for factory_type in factory_types:
            factory = object.__new__(factory_type)
            factories.append(factory)

        return factories

    @staticmethod
    def get_factory_weights(factory_types: List[Type[PluginFactory]]) -> Dict[Type[PluginFactory], int]:
        last_weight = 0
        weights: Dict[Type[PluginFactory], int] = {}
        for factory_type in factory_types:
            weight = PluginManager.PLUGIN_FACTORY_WEIGHTS[factory_type]
            if weight is None or weight == PluginManager.WEIGHT_RELATIVE_TO_PRIOR_PLUGIN:
                # This plugin factory is unknown, or it has relative (to prior plugin factory) weight.
                last_weight += 1
                weights[factory_type] = last_weight
            else:
                # Otherwise, use the wight assigned to this plugin factory
                weights[factory_type] = weight

                # Remember this weight for subsequent factories that may have relative (to this plugin factory) weight.
                last_weight = weight

        return weights

    def execute(self, target: object, method_name: str, target_driver_func: Callable, *args, **kwargs) -> Any:
        plugin_service = self._container.plugin_service
        driver_dialect = plugin_service.driver_dialect
        conn: Optional[Connection] = driver_dialect.get_connection_from_obj(target)
        current_conn: Optional[Connection] = driver_dialect.unwrap_connection(plugin_service.current_connection)

        if method_name not in ["Connection.close", "Cursor.close"] and conn is not None and conn != current_conn:
            raise AwsWrapperError(Messages.get_formatted("PluginManager.MethodInvokedAgainstOldConnection", target))

        if conn is None and method_name in ["Connection.close", "Cursor.close"]:
            return

        context: TelemetryContext
        context = self._telemetry_factory.open_telemetry_context(method_name, TelemetryTraceLevel.TOP_LEVEL)
        context.set_attribute("python_call", method_name)

        try:
            result = self._execute_with_subscribed_plugins(
                method_name,
                # next_plugin_func is defined later in make_pipeline
                lambda plugin, next_plugin_func: plugin.execute(target, method_name, next_plugin_func, *args, **kwargs),
                target_driver_func)

            context.set_success(True)

            return result
        except Exception as e:
            context.set_success(False)
            raise e
        finally:
            context.close_context()

    def _execute_with_telemetry(self, plugin_name: str, func: Callable):
        context = self._telemetry_factory.open_telemetry_context(plugin_name, TelemetryTraceLevel.NESTED)
        try:
            return func()
        finally:
            context.close_context()

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

    def _create_base_pipeline_func(self, plugin: Plugin):
        # The plugin passed here will be the DefaultPlugin, which is the last plugin in the pipeline
        # The second arg to plugin_func is the next call in the pipeline. Here, it is the target driver function
        plugin_name = plugin.__class__.__name__
        return lambda plugin_func, target_driver_func: self._execute_with_telemetry(
            plugin_name, lambda: plugin_func(plugin, target_driver_func))

    def _extend_pipeline_func(self, plugin: Plugin, pipeline_so_far: Callable):
        # Defines the call to a plugin that precedes the DefaultPlugin in the pipeline
        # The second arg to plugin_func effectively appends the tail end of the pipeline to the current plugin's call
        plugin_name = plugin.__class__.__name__
        return lambda plugin_func, target_driver_func: self._execute_with_telemetry(
            plugin_name, lambda: plugin_func(plugin, lambda: pipeline_so_far(plugin_func, target_driver_func)))

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            host_info: Optional[HostInfo],
            props: Properties,
            is_initial_connection: bool) -> Connection:
        context = self._telemetry_factory.open_telemetry_context("connect", TelemetryTraceLevel.NESTED)
        try:
            return self._execute_with_subscribed_plugins(
                PluginManager._CONNECT_METHOD,
                lambda plugin, func: plugin.connect(
                    target_func, driver_dialect, host_info, props, is_initial_connection, func),
                # The final connect action will be handled by the ConnectionProvider, so this lambda will not be called.
                lambda: None)
        finally:
            context.close_context()

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
        context = self._telemetry_factory.open_telemetry_context("init_host_provider", TelemetryTraceLevel.NESTED)
        try:
            return self._execute_with_subscribed_plugins(
                PluginManager._INIT_HOST_LIST_PROVIDER_METHOD,
                lambda plugin, func: plugin.init_host_provider(props, host_list_provider_service, func),
                lambda: None)
        finally:
            context.close_context()

    def release_resources(self):
        """
        Allows all connection plugins a chance to clean up any dangling resources
        or perform any last tasks before shutting down.
        """
        for plugin in self._plugins:
            if isinstance(plugin, CanReleaseResources):
                plugin.release_resources()
