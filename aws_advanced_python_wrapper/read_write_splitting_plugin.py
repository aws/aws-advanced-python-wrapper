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

from abc import abstractmethod
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Callable, Optional, Set, Tuple

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.connection_provider import (
        ConnectionProviderManager,
    )

from aws_advanced_python_wrapper.errors import (AwsWrapperError, FailoverError,
                                                FailoverFailedError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

logger = Logger(__name__)


class AbstractReadWriteSplittingPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {
        DbApiMethod.INIT_HOST_PROVIDER.method_name,
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.NOTIFY_CONNECTION_CHANGED.method_name,
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,

        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_AUTOCOMMIT.method_name,
        DbApiMethod.CONNECTION_AUTOCOMMIT_SETTER.method_name,
        DbApiMethod.CONNECTION_IS_READ_ONLY.method_name,
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,

        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name
    }
    _POOL_PROVIDER_CLASS_NAME = "aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider"

    def __init__(
        self,
        plugin_service: PluginService,
        props: Properties,
    ):
        self._plugin_service: PluginService = plugin_service
        self._properties: Properties = props
        self._writer_connection: Optional[Connection] = None
        self._reader_connection: Optional[Connection] = None
        self._writer_host_info: Optional[HostInfo] = None
        self._reader_host_info: Optional[HostInfo] = None
        self._conn_provider_manager: ConnectionProviderManager = (
            self._plugin_service.get_connection_provider_manager()
        )
        self._is_reader_conn_from_internal_pool: bool = False
        self._is_writer_conn_from_internal_pool: bool = False
        self._in_read_write_split: bool = False
        self._host_list_provider_service: Optional[HostListProviderService] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def init_host_provider(
        self,
        props: Properties,
        host_list_provider_service: HostListProviderService,
        init_host_provider_func: Callable,
    ):
        self._host_list_provider_service = host_list_provider_service
        init_host_provider_func()

    def notify_connection_changed(
        self, changes: Set[ConnectionEvent]
    ) -> OldConnectionSuggestedAction:
        self._update_internal_connection_info()

        if self._in_read_write_split:
            return OldConnectionSuggestedAction.PRESERVE

        return OldConnectionSuggestedAction.NO_OPINION

    def execute(
        self,
        target: type,
        method_name: str,
        execute_func: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        driver_dialect = self._plugin_service.driver_dialect
        conn: Optional[Connection] = driver_dialect.get_connection_from_obj(target)
        current_conn: Optional[Connection] = driver_dialect.unwrap_connection(
            self._plugin_service.current_connection
        )

        if conn is not None and conn != current_conn:
            msg = Messages.get_formatted(
                "PluginManager.MethodInvokedAgainstOldConnection", target
            )
            raise AwsWrapperError(msg)

        if (
            method_name == DbApiMethod.CONNECTION_SET_READ_ONLY.method_name
            and args is not None
            and len(args) > 0
        ):
            self._switch_connection_if_required(args[0])

        try:
            return execute_func()
        except Exception as ex:
            if isinstance(ex, FailoverFailedError):
                # Evict the current connection from the pool right away since it is not reusable
                self._close_connections(False)
            if isinstance(ex, FailoverError):
                logger.debug(
                    "ReadWriteSplittingPlugin.FailoverExceptionWhileExecutingCommand",
                    method_name
                )
                self._close_connections(True)
            else:
                logger.debug(
                    "ReadWriteSplittingPlugin.ExceptionWhileExecutingCommand",
                    method_name
                )
            raise ex

    def _update_internal_connection_info(self):
        current_conn = self._plugin_service.current_connection
        current_host = self._plugin_service.current_host_info
        if current_conn is None or current_host is None:
            return

        if self._should_update_writer_connection(current_conn, current_host):
            self._set_writer_connection(current_conn, current_host)
        elif self._should_update_reader_connection(current_conn, current_host):
            self._set_reader_connection(current_conn, current_host)

    def _set_writer_connection(
        self, writer_conn: Connection, writer_host_info: HostInfo
    ):
        self._writer_connection = writer_conn
        self._writer_host_info = writer_host_info
        logger.debug(
            "ReadWriteSplittingPlugin.SetWriterConnection", writer_host_info.url
        )

    def _set_reader_connection(
        self, reader_conn: Connection, reader_host_info: HostInfo
    ):
        self._reader_connection = reader_conn
        self._reader_host_info = reader_host_info
        logger.debug(
            "ReadWriteSplittingPlugin.SetReaderConnection", reader_host_info.url
        )

    def _switch_connection_if_required(self, read_only: bool):
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect

        if (
            current_conn is not None
            and driver_dialect is not None
            and driver_dialect.is_closed(current_conn)
        ):
            self.log_and_raise_exception(
                "ReadWriteSplittingPlugin.SetReadOnlyOnClosedConnection"
            )

        self._refresh_and_store_topology(current_conn)

        current_host = self._plugin_service.current_host_info
        if current_host is None:
            self.log_and_raise_exception("ReadWriteSplittingPlugin.UnavailableHostInfo")
            return

        if read_only:
            if (
                not self._plugin_service.is_in_transaction
                and not self._is_reader(current_host)
            ):
                try:
                    self._switch_to_reader_connection()
                except Exception:
                    if not self._is_connection_usable(current_conn, driver_dialect):
                        self.log_and_raise_exception(
                            "ReadWriteSplittingPlugin.ErrorSwitchingToReader"
                        )
                        return

                    logger.warning(
                        "ReadWriteSplittingPlugin.FallbackToCurrentConnection",
                        current_host.url,
                    )
        elif not self._is_writer(current_host):
            if self._plugin_service.is_in_transaction:
                self.log_and_raise_exception(
                    "ReadWriteSplittingPlugin.SetReadOnlyFalseInTransaction"
                )

            try:
                self._switch_to_writer_connection()
            except Exception:
                self.log_and_raise_exception(
                    "ReadWriteSplittingPlugin.ErrorSwitchingToWriter"
                )

    def _switch_current_connection_to(
        self, new_conn: Connection, new_conn_host: HostInfo
    ):
        current_conn = self._plugin_service.current_connection
        if current_conn == new_conn:
            return

        self._plugin_service.set_current_connection(new_conn, new_conn_host)

        logger.debug(
            "ReadWriteSplittingPlugin.SettingCurrentConnection", new_conn_host.url
        )

    def _switch_to_writer_connection(self):
        current_host = self._plugin_service.current_host_info
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if (
            current_host is not None
            and self._is_writer(current_host)
            and self._is_connection_usable(current_conn, driver_dialect)
        ):
            # Already connected to the intended writer.
            return

        self._in_read_write_split = True
        if not self._is_connection_usable(self._writer_connection, driver_dialect):
            self._initialize_writer_connection()
        elif self._writer_connection is not None and self._writer_host_info is not None:
            self._switch_current_connection_to(
                self._writer_connection, self._writer_host_info
            )

        if self._is_reader_conn_from_internal_pool:
            self._close_connection(self._reader_connection)

        logger.debug(
            "ReadWriteSplittingPlugin.SwitchedFromReaderToWriter",
            self._writer_host_info.url,
        )

    def _switch_to_reader_connection(self):
        current_host = self._plugin_service.current_host_info
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if (
            current_host is not None
            and self._is_reader(current_host)
            and self._is_connection_usable(current_conn, driver_dialect)
        ):
            # Already connected to the intended reader.
            return

        self._close_reader_if_necessary()

        self._in_read_write_split = True
        if not self._is_connection_usable(self._reader_connection, driver_dialect):
            self._initialize_reader_connection()
        elif self._reader_connection is not None and self._reader_host_info is not None:
            try:
                self._switch_current_connection_to(
                    self._reader_connection, self._reader_host_info
                )
                logger.debug(
                    "ReadWriteSplittingPlugin.SwitchedFromWriterToReader",
                    self._reader_host_info.url,
                )
            except Exception:
                logger.debug(
                    "ReadWriteSplittingPlugin.ErrorSwitchingToCachedReader",
                    self._reader_host_info.url,
                )

                AbstractReadWriteSplittingPlugin.close_connection(self._reader_connection, driver_dialect)
                self._reader_connection = None
                self._reader_host_info = None
                self._initialize_reader_connection()

        if self._is_writer_conn_from_internal_pool:
            self._close_connection(self._writer_connection)

    def _close_connection(self, internal_conn: Optional[Connection], close_only_if_idle: bool = True):
        if internal_conn is None:
            return

        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect

        if close_only_if_idle and internal_conn == current_conn:
            # Connection is in use, do not close
            return

        try:
            if self._is_connection_usable(internal_conn, driver_dialect):
                driver_dialect.execute(DbApiMethod.CONNECTION_CLOSE.method_name, lambda: internal_conn.close())
        except Exception:
            # Ignore exceptions during cleanup - connection might already be dead
            pass
        finally:
            if internal_conn == self._writer_connection:
                self._writer_connection = None
                self._writer_host_info = None
            if internal_conn == self._reader_connection:
                self._reader_connection = None
                self._reader_host_info = None

    def _close_connections(self, close_only_if_idle: bool = True):
        logger.debug("ReadWriteSplittingPlugin.ClosingInternalConnections")
        self._close_connection(self._reader_connection, close_only_if_idle)
        self._close_connection(self._writer_connection, close_only_if_idle)

    @staticmethod
    def log_and_raise_exception(log_msg: str):
        logger.error(log_msg)
        raise ReadWriteSplittingError(Messages.get(log_msg))

    @staticmethod
    def _is_connection_usable(conn: Optional[Connection], driver_dialect: DriverDialect):
        if conn is None:
            return False
        try:
            return not driver_dialect.is_closed(conn)
        except Exception:
            # If we cannot determine connection state, assume unavailable.
            return False

    @staticmethod
    def close_connection(conn: Optional[Connection], driver_dialect: DriverDialect):
        if conn is not None:
            try:
                driver_dialect.execute(DbApiMethod.CONNECTION_CLOSE.method_name, lambda: conn.close())
            except Exception:
                # Swallow exception
                return

    # --- Abstract methods that concrete subclasses must implement ---

    @abstractmethod
    def _should_update_writer_connection(
        self, current_conn: Connection, current_host: HostInfo
    ) -> bool:
        """Return True if the current connection should update the cached writer connection."""
        ...

    @abstractmethod
    def _should_update_reader_connection(
        self, current_conn: Connection, current_host: HostInfo
    ) -> bool:
        """Return True if the current connection should update the cached reader connection."""
        ...

    @abstractmethod
    def _is_writer(self, current_host: HostInfo) -> bool:
        """Return True if the given host is a writer."""
        ...

    @abstractmethod
    def _is_reader(self, current_host: HostInfo) -> bool:
        """Return True if the given host is a reader."""
        ...

    @abstractmethod
    def _refresh_and_store_topology(self, current_conn: Optional[Connection]):
        """Refresh the host list and store it for later use."""
        ...

    @abstractmethod
    def _initialize_writer_connection(self):
        """Open a new writer connection and set it as the current connection."""
        ...

    @abstractmethod
    def _initialize_reader_connection(self):
        """Open a new reader connection and set it as the current connection."""
        ...

    @abstractmethod
    def _close_reader_if_necessary(self):
        """Close the cached reader connection if it can no longer be used."""
        ...


class ReadWriteSplittingPlugin(AbstractReadWriteSplittingPlugin):
    """Topology-based read/write splitting plugin.

    Uses the host list topology to determine writer/reader roles and
    select reader hosts via a configurable strategy.
    """

    def __init__(self, plugin_service: PluginService, props: Properties):
        # The read/write splitting plugin handles connections based on topology.
        super().__init__(plugin_service, props)
        strategy = WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(props)
        if strategy is not None:
            self._reader_selector_strategy = strategy
        else:
            default_strategy = (
                WrapperProperties.READER_HOST_SELECTOR_STRATEGY.default_value
            )
            if default_strategy is not None:
                self._reader_selector_strategy = default_strategy
        self._hosts: Tuple[HostInfo, ...] = ()

    def connect(
        self,
        target_driver_func: Callable,
        driver_dialect: DriverDialect,
        host_info: HostInfo,
        props: Properties,
        is_initial_connection: bool,
        connect_func: Callable,
    ) -> Connection:
        if not self._plugin_service.accepts_strategy(
            host_info.role, self._reader_selector_strategy
        ):
            raise AwsWrapperError(
                Messages.get_formatted(
                    "ReadWriteSplittingPlugin.UnsupportedHostInfoSelectorStrategy",
                    self._reader_selector_strategy,
                )
            )

        current_conn = connect_func()

        if not is_initial_connection or (
            self._host_list_provider_service is not None
            and self._host_list_provider_service.is_static_host_list_provider()
        ):
            return current_conn

        current_role = self._plugin_service.get_host_role(current_conn)
        if current_role is None or current_role == HostRole.UNKNOWN:
            AbstractReadWriteSplittingPlugin.log_and_raise_exception(
                "ReadWriteSplittingPlugin.ErrorVerifyingInitialHostSpecRole"
            )

        current_host = self._plugin_service.initial_connection_host_info
        if current_host is not None:
            if current_role == current_host.role:
                return current_conn

            updated_host = deepcopy(current_host)
            updated_host.role = current_role
            if self._host_list_provider_service is not None:
                self._host_list_provider_service.initial_connection_host_info = (
                    updated_host
                )

        return current_conn

    def _is_writer(self, current_host: HostInfo) -> bool:
        return current_host.role == HostRole.WRITER

    def _is_reader(self, current_host: HostInfo) -> bool:
        return current_host.role == HostRole.READER

    def _refresh_and_store_topology(self, current_conn: Optional[Connection]):
        driver_dialect = self._plugin_service.driver_dialect
        if current_conn is not None and driver_dialect.can_execute_query(current_conn):
            try:
                self._plugin_service.refresh_host_list()
            except Exception:
                pass

        hosts = self._plugin_service.hosts
        if hosts is None or len(hosts) == 0:
            AbstractReadWriteSplittingPlugin.log_and_raise_exception(
                "ReadWriteSplittingPlugin.EmptyHostList"
            )

        self._hosts = hosts
        self._writer_host_info = self._get_writer_host_info()

    def _initialize_writer_connection(self):
        writer_host = self._get_writer_host_info()
        if writer_host is None:
            self.log_and_raise_exception(
                "ReadWriteSplittingPlugin.FailedToConnectToWriter"
            )
            return

        conn = self._plugin_service.connect(writer_host, self._properties, self)
        if conn is None:
            self.log_and_raise_exception(
                "ReadWriteSplittingPlugin.FailedToConnectToWriter"
            )
            return

        provider = self._conn_provider_manager.get_connection_provider(
            writer_host, self._properties
        )
        self._is_writer_conn_from_internal_pool = (
            AbstractReadWriteSplittingPlugin._POOL_PROVIDER_CLASS_NAME
            in str(type(provider))
        )
        self._set_writer_connection(conn, writer_host)
        self._switch_current_connection_to(conn, writer_host)

    def _initialize_reader_connection(self):
        if len(self._hosts) == 1 and self._get_writer_host_info() is not None:
            if not self._is_connection_usable(
                self._writer_connection, self._plugin_service.driver_dialect
            ):
                self._initialize_writer_connection()
            logger.warning(
                "ReadWriteSplittingPlugin.NoReadersFound", self._writer_host_info.url
            )
            return

        conn, reader_host = self._open_new_reader_connection()

        if conn is None or reader_host is None:
            self.log_and_raise_exception("ReadWriteSplittingPlugin.NoReadersAvailable")
            return

        logger.debug(
            "ReadWriteSplittingPlugin.SuccessfullyConnectedToReader", reader_host.url
        )

        provider = self._conn_provider_manager.get_connection_provider(
            reader_host, self._properties
        )
        self._is_reader_conn_from_internal_pool = (
            AbstractReadWriteSplittingPlugin._POOL_PROVIDER_CLASS_NAME
            in str(type(provider))
        )

        self._set_reader_connection(conn, reader_host)
        self._switch_current_connection_to(conn, reader_host)

        logger.debug(
            "ReadWriteSplittingPlugin.SwitchedFromWriterToReader", reader_host.url
        )

    def _close_reader_if_necessary(self):
        # The old reader cannot be used anymore, close it.
        if (
            self._reader_host_info is not None
            and self._reader_connection is not None
            and not self._can_host_be_used(self._reader_host_info)
        ):
            self._close_connection(self._reader_connection)

    def _should_update_writer_connection(
        self, current_conn: Connection, current_host: HostInfo
    ) -> bool:
        return self._is_writer(current_host)

    def _should_update_reader_connection(
        self, current_conn: Connection, current_host: HostInfo
    ) -> bool:
        return True

    def _get_writer_host_info(self) -> Optional[HostInfo]:
        for host in self._hosts:
            if host.role == HostRole.WRITER:
                return host
        return None

    def _can_host_be_used(self, host_info: HostInfo) -> bool:
        hosts = [h.get_host_and_port() for h in self._hosts]
        return host_info.get_host_and_port() in hosts

    def _open_new_reader_connection(
        self,
    ) -> tuple[Optional[Connection], Optional[HostInfo]]:
        conn: Optional[Connection] = None
        reader_host: Optional[HostInfo] = None

        conn_attempts = len(self._plugin_service.hosts) * 2
        for _ in range(conn_attempts):
            host = self._plugin_service.get_host_info_by_strategy(
                HostRole.READER, self._reader_selector_strategy
            )
            if host is not None:
                try:
                    conn = self._plugin_service.connect(host, self._properties, self)
                    reader_host = host
                    break
                except Exception:
                    logger.warning(
                        "ReadWriteSplittingPlugin.FailedToConnectToReader", host.url
                    )

        return conn, reader_host


class ReadWriteSplittingPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return ReadWriteSplittingPlugin(plugin_service, props)
