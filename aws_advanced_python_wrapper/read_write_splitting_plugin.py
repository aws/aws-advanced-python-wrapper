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

from copy import deepcopy
from typing import TYPE_CHECKING, Any, Callable, Optional, Set, Tuple

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.connection_provider import ConnectionProviderManager

from aws_advanced_python_wrapper.errors import (AwsWrapperError, FailoverError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

logger = Logger(__name__)


class ReadWriteSplittingPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider",
                                     "connect",
                                     "notify_connection_changed",
                                     "Connection.set_read_only"}
    _POOL_PROVIDER_CLASS_NAME = "aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider"

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._host_list_provider_service: HostListProviderService
        self._writer_connection: Optional[Connection] = None
        self._reader_connection: Optional[Connection] = None
        self._reader_host_info: Optional[HostInfo] = None
        self._conn_provider_manager: ConnectionProviderManager = self._plugin_service.get_connection_provider_manager()
        self._is_reader_conn_from_internal_pool: bool = False
        self._is_writer_conn_from_internal_pool: bool = False
        self._in_read_write_split: bool = False

        self._reader_selector_strategy: str = ""
        strategy = WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(self._properties)
        if strategy is not None:
            self._reader_selector_strategy = strategy
        else:
            default_strategy = WrapperProperties.READER_HOST_SELECTOR_STRATEGY.default_value
            if default_strategy is not None:
                self._reader_selector_strategy = default_strategy

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        self._host_list_provider_service = host_list_provider_service
        init_host_provider_func()

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        if not self._plugin_service.accepts_strategy(host_info.role, self._reader_selector_strategy):
            raise AwsWrapperError(
                Messages.get_formatted("ReadWriteSplittingPlugin.UnsupportedHostInfoSelectorStrategy",
                                       self._reader_selector_strategy))

        return self.connect_internal(is_initial_connection, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self.connect_internal(is_initial_connection, force_connect_func)

    def connect_internal(self, is_initial_connection: bool, connect_func: Callable) -> Connection:
        current_conn = connect_func()

        if not is_initial_connection or self._host_list_provider_service.is_static_host_list_provider():
            return current_conn

        current_role = self._plugin_service.get_host_role(current_conn)
        if current_role is None or current_role == HostRole.UNKNOWN:
            self._log_and_raise_exception("ReadWriteSplittingPlugin.ErrorVerifyingInitialHostSpecRole")

        current_host = self._plugin_service.initial_connection_host_info
        if current_host is not None:
            if current_role == current_host.role:
                return current_conn

            updated_host = deepcopy(current_host)
            updated_host.role = current_role
            self._host_list_provider_service.initial_connection_host_info = updated_host

        return current_conn

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._update_internal_connection_info()

        if self._in_read_write_split:
            return OldConnectionSuggestedAction.PRESERVE

        return OldConnectionSuggestedAction.NO_OPINION

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        driver_dialect = self._plugin_service.driver_dialect
        conn: Optional[Connection] = driver_dialect.get_connection_from_obj(target)
        current_conn: Optional[Connection] = driver_dialect.unwrap_connection(self._plugin_service.current_connection)

        if conn is not None and conn != current_conn:
            msg = Messages.get_formatted("PluginManager.MethodInvokedAgainstOldConnection", target)
            raise AwsWrapperError(msg)

        if method_name == "Connection.set_read_only" and args is not None and len(args) > 0:
            self._switch_connection_if_required(args[0])

        try:
            return execute_func()
        except Exception as ex:
            if isinstance(ex, FailoverError):
                logger.debug("ReadWriteSplittingPlugin.FailoverExceptionWhileExecutingCommand", method_name)
                self._close_idle_connections()
            else:
                logger.debug("ReadWriteSplittingPlugin.ExceptionWhileExecutingCommand", method_name)
            raise ex

    def _update_internal_connection_info(self):
        current_conn = self._plugin_service.current_connection
        current_host = self._plugin_service.current_host_info
        if current_conn is None or current_host is None:
            return

        if current_host.role == HostRole.WRITER:
            self._set_writer_connection(current_conn, current_host)
        else:
            self._set_reader_connection(current_conn, current_host)

    def _set_writer_connection(self, writer_conn: Connection, writer_host_info: HostInfo):
        self._writer_connection = writer_conn
        logger.debug("ReadWriteSplittingPlugin.SetWriterConnection", writer_host_info.url)

    def _set_reader_connection(self, reader_conn: Connection, reader_host_info: HostInfo):
        self._reader_connection = reader_conn
        self._reader_host_info = reader_host_info
        logger.debug("ReadWriteSplittingPlugin.SetReaderConnection", reader_host_info.url)

    def _get_new_writer_connection(self, writer_host: HostInfo):
        conn = self._plugin_service.connect(writer_host, self._properties)
        provider = self._conn_provider_manager.get_connection_provider(writer_host, self._properties)
        self._is_writer_conn_from_internal_pool = (ReadWriteSplittingPlugin._POOL_PROVIDER_CLASS_NAME in str(type(provider)))
        self._set_writer_connection(conn, writer_host)
        self._switch_current_connection_to(conn, writer_host)

    def _switch_connection_if_required(self, read_only: bool):
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect

        if (current_conn is not None and
                driver_dialect is not None and driver_dialect.is_closed(current_conn)):
            self._log_and_raise_exception("ReadWriteSplittingPlugin.SetReadOnlyOnClosedConnection")

        if current_conn is not None and driver_dialect.can_execute_query(current_conn):
            try:
                self._plugin_service.refresh_host_list()
            except Exception:
                pass  # Swallow exception

        hosts = self._plugin_service.hosts
        if hosts is None or len(hosts) == 0:
            self._log_and_raise_exception("ReadWriteSplittingPlugin.EmptyHostList")

        current_host = self._plugin_service.current_host_info
        if current_host is None:
            self._log_and_raise_exception("ReadWriteSplittingPlugin.UnavailableHostInfo")
            return

        if read_only:
            if not self._plugin_service.is_in_transaction and current_host.role != HostRole.READER:
                try:
                    self._switch_to_reader_connection(hosts)
                except Exception:
                    if not self._is_connection_usable(current_conn, driver_dialect):
                        self._log_and_raise_exception("ReadWriteSplittingPlugin.ErrorSwitchingToReader")
                        return

                    logger.warning("ReadWriteSplittingPlugin.FallbackToWriter", current_host.url)
        elif current_host.role != HostRole.WRITER:
            if self._plugin_service.is_in_transaction:
                self._log_and_raise_exception("ReadWriteSplittingPlugin.SetReadOnlyFalseInTransaction")

            try:
                self._switch_to_writer_connection(hosts)
            except Exception:
                self._log_and_raise_exception("ReadWriteSplittingPlugin.ErrorSwitchingToWriter")

    def _switch_current_connection_to(self, new_conn: Connection, new_conn_host: HostInfo):
        current_conn = self._plugin_service.current_connection
        if current_conn == new_conn:
            return

        self._plugin_service.set_current_connection(new_conn, new_conn_host)

        logger.debug("ReadWriteSplittingPlugin.SettingCurrentConnection", new_conn_host.url)

    def _switch_to_writer_connection(self, hosts: Tuple[HostInfo, ...]):
        current_host = self._plugin_service.current_host_info
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if (current_host is not None and current_host.role == HostRole.WRITER and
                self._is_connection_usable(current_conn, driver_dialect)):
            return

        writer_host = self._get_writer(hosts)
        if writer_host is None:
            return

        self._in_read_write_split = True
        if not self._is_connection_usable(self._writer_connection, driver_dialect):
            self._get_new_writer_connection(writer_host)
        elif self._writer_connection is not None:
            self._switch_current_connection_to(self._writer_connection, writer_host)

        if self._is_reader_conn_from_internal_pool:
            self._close_connection_if_idle(self._reader_connection)

        logger.debug("ReadWriteSplittingPlugin.SwitchedFromReaderToWriter", writer_host.url)

    def _switch_to_reader_connection(self, hosts: Tuple[HostInfo, ...]):
        current_host = self._plugin_service.current_host_info
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if (current_host is not None and current_host.role == HostRole.READER and
                self._is_connection_usable(current_conn, driver_dialect)):
            return

        hostnames = [host_info.host for host_info in hosts]
        if self._reader_host_info is not None and self._reader_host_info.host not in hostnames:
            # The old reader cannot be used anymore because it is no longer in the list of allowed hosts.
            self._close_connection_if_idle(self._reader_connection)

        self._in_read_write_split = True
        if not self._is_connection_usable(self._reader_connection, driver_dialect):
            self._initialize_reader_connection(hosts)
        elif self._reader_connection is not None and self._reader_host_info is not None:
            try:
                self._switch_current_connection_to(self._reader_connection, self._reader_host_info)
                logger.debug("ReadWriteSplittingPlugin.SwitchedFromWriterToReader", self._reader_host_info.url)
            except Exception:
                logger.debug("ReadWriteSplittingPlugin.ErrorSwitchingToCachedReader", self._reader_host_info.url)

                self._reader_connection.close()
                self._reader_connection = None
                self._reader_host_info = None
                self._initialize_reader_connection(hosts)

        if self._is_writer_conn_from_internal_pool:
            self._close_connection_if_idle(self._writer_connection)

    def _initialize_reader_connection(self, hosts: Tuple[HostInfo, ...]):
        if len(hosts) == 1:
            writer_host = self._get_writer(hosts)
            if writer_host is not None:
                if not self._is_connection_usable(self._writer_connection, self._plugin_service.driver_dialect):
                    self._get_new_writer_connection(writer_host)
                logger.warning("ReadWriteSplittingPlugin.NoReadersFound", writer_host.url)
                return

        conn: Optional[Connection] = None
        reader_host: Optional[HostInfo] = None

        conn_attempts = len(self._plugin_service.hosts) * 2
        for _ in range(conn_attempts):
            host = self._plugin_service.get_host_info_by_strategy(HostRole.READER, self._reader_selector_strategy)
            if host is not None:
                try:
                    conn = self._plugin_service.connect(host, self._properties)
                    provider = self._conn_provider_manager.get_connection_provider(host, self._properties)
                    self._is_reader_conn_from_internal_pool = (ReadWriteSplittingPlugin._POOL_PROVIDER_CLASS_NAME in str(type(provider)))
                    reader_host = host
                    break
                except Exception:
                    logger.warning("ReadWriteSplittingPlugin.FailedToConnectToReader", host.url)

        if conn is None or reader_host is None:
            self._log_and_raise_exception("ReadWriteSplittingPlugin.NoReadersAvailable")
            return

        logger.debug("ReadWriteSplittingPlugin.SuccessfullyConnectedToReader", reader_host.url)

        self._set_reader_connection(conn, reader_host)
        self._switch_current_connection_to(conn, reader_host)

        logger.debug("ReadWriteSplittingPlugin.SwitchedFromWriterToReader", reader_host.url)

    def _close_connection_if_idle(self, internal_conn: Optional[Connection]):
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        try:
            if (internal_conn is not None and internal_conn != current_conn and
                    self._is_connection_usable(internal_conn, driver_dialect)):
                internal_conn.close()
                if internal_conn == self._writer_connection:
                    self._writer_connection = None
                if internal_conn == self._reader_connection:
                    self._reader_connection = None
                    self._reader_host_info = None

        except Exception:
            pass  # Swallow exception

    def _close_idle_connections(self):
        logger.debug("ReadWriteSplittingPlugin.ClosingInternalConnections")
        self._close_connection_if_idle(self._reader_connection)
        self._close_connection_if_idle(self._writer_connection)

    @staticmethod
    def _log_and_raise_exception(log_msg: str):
        logger.error(log_msg)
        raise ReadWriteSplittingError(Messages.get(log_msg))

    @staticmethod
    def _is_connection_usable(conn: Optional[Connection], driver_dialect: Optional[DriverDialect]):
        return conn is not None and driver_dialect is not None and not driver_dialect.is_closed(conn)

    @staticmethod
    def _get_writer(hosts: Tuple[HostInfo, ...]) -> Optional[HostInfo]:
        for host in hosts:
            if host.role == HostRole.WRITER:
                return host

        ReadWriteSplittingPlugin._log_and_raise_exception("ReadWriteSplittingPlugin.NoWriterFound")

        return None


class ReadWriteSplittingPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return ReadWriteSplittingPlugin(plugin_service, props)
