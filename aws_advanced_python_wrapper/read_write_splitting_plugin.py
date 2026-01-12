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
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, Set, Tuple

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


class ReadWriteSplittingConnectionManager(Plugin):
    """Base class that manages connection switching logic."""

    _SUBSCRIBED_METHODS: Set[str] = {
        DbApiMethod.INIT_HOST_PROVIDER.method_name,
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.NOTIFY_CONNECTION_CHANGED.method_name,
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
    }
    _POOL_PROVIDER_CLASS_NAME = "aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider"

    def __init__(
        self,
        plugin_service: PluginService,
        props: Properties,
        connection_handler: ReadWriteConnectionHandler,
    ):
        self._plugin_service: PluginService = plugin_service
        self._properties: Properties = props
        self._connection_handler: ReadWriteConnectionHandler = connection_handler
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

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def init_host_provider(
        self,
        props: Properties,
        host_list_provider_service: HostListProviderService,
        init_host_provider_func: Callable,
    ):
        self._connection_handler.host_list_provider_service = host_list_provider_service
        init_host_provider_func()

    def connect(
        self,
        target_driver_func: Callable,
        driver_dialect: DriverDialect,
        host_info: HostInfo,
        props: Properties,
        is_initial_connection: bool,
        connect_func: Callable,
    ) -> Connection:
        return self._connection_handler.get_verified_initial_connection(
            host_info, is_initial_connection, lambda x: self._plugin_service.connect(x, props, self), connect_func)

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
            if isinstance(ex, FailoverError):
                logger.debug(
                    "ReadWriteSplittingPlugin.FailoverExceptionWhileExecutingCommand",
                    method_name
                )
                self._close_idle_connections()
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

        if self._connection_handler.should_update_writer_with_current_conn(
            current_conn, current_host, self._writer_connection
        ):
            self._set_writer_connection(current_conn, current_host)
        elif self._connection_handler.should_update_reader_with_current_conn(
            current_conn, current_host, self._reader_connection
        ):
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

    def _initialize_writer_connection(self):
        conn, writer_host = self._connection_handler.open_new_writer_connection(lambda x: self._plugin_service.connect(x, self._properties, self))

        if conn is None:
            self.log_and_raise_exception(
                "ReadWriteSplittingPlugin.FailedToConnectToWriter"
            )
            return None

        provider = self._conn_provider_manager.get_connection_provider(
            writer_host, self._properties
        )
        self._is_writer_conn_from_internal_pool = (
            ReadWriteSplittingConnectionManager._POOL_PROVIDER_CLASS_NAME
            in str(type(provider))
        )
        self._set_writer_connection(conn, writer_host)
        self._switch_current_connection_to(conn, writer_host)

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

        self._connection_handler.refresh_and_store_host_list(
            current_conn, driver_dialect
        )

        current_host = self._plugin_service.current_host_info
        if current_host is None:
            self.log_and_raise_exception("ReadWriteSplittingPlugin.UnavailableHostInfo")
            return

        if read_only:
            if (
                not self._plugin_service.is_in_transaction
                and not self._connection_handler.is_reader_host(current_host)
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
        elif not self._connection_handler.is_writer_host(current_host):
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
            and self._connection_handler.is_writer_host(current_host)
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
            self._close_connection_if_idle(self._reader_connection)

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
            and self._connection_handler.is_reader_host(current_host)
            and self._is_connection_usable(current_conn, driver_dialect)
        ):
            # Already connected to the intended reader.
            return

        if (
            self._reader_connection is not None
            and not self._connection_handler.can_host_be_used(
                self._reader_host_info
            )
        ):
            # The old reader cannot be used anymore, close it.
            self._close_connection_if_idle(self._reader_connection)

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

                ReadWriteSplittingConnectionManager.close_connection(self._reader_connection, driver_dialect)
                self._reader_connection = None
                self._reader_host_info = None
                self._initialize_reader_connection()

        if self._is_writer_conn_from_internal_pool:
            self._close_connection_if_idle(self._writer_connection)

    def _initialize_reader_connection(self):
        if self._connection_handler.has_no_readers():
            if not self._is_connection_usable(
                self._writer_connection, self._plugin_service.driver_dialect
            ):
                self._initialize_writer_connection()
            logger.warning(
                "ReadWriteSplittingPlugin.NoReadersFound", self._writer_host_info.url
            )
            return

        conn, reader_host = self._connection_handler.open_new_reader_connection(lambda x: self._plugin_service.connect(x, self._properties, self))

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
            ReadWriteSplittingConnectionManager._POOL_PROVIDER_CLASS_NAME
            in str(type(provider))
        )

        self._set_reader_connection(conn, reader_host)
        self._switch_current_connection_to(conn, reader_host)

        logger.debug(
            "ReadWriteSplittingPlugin.SwitchedFromWriterToReader", reader_host.url
        )

    def _close_connection_if_idle(self, internal_conn: Optional[Connection]):
        if internal_conn is None:
            return

        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect

        try:
            if internal_conn != current_conn and self._is_connection_usable(
                internal_conn, driver_dialect
            ):
                driver_dialect.execute(DbApiMethod.CONNECTION_CLOSE.method_name, lambda: internal_conn.close())
                if internal_conn == self._writer_connection:
                    self._writer_connection = None
                    self._writer_host_info = None
                if internal_conn == self._reader_connection:
                    self._reader_connection = None
                    self._reader_host_info = None
        except Exception:
            # Ignore exceptions during cleanup - connection might already be dead
            pass

    def _close_idle_connections(self):
        logger.debug("ReadWriteSplittingPlugin.ClosingInternalConnections")
        self._close_connection_if_idle(self._reader_connection)
        self._close_connection_if_idle(self._writer_connection)

        # Always clear cached references even if connections couldn't be closed
        self._reader_connection = None
        self._reader_host_info = None
        self._writer_connection = None
        self._writer_host_info = None

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


class ReadWriteConnectionHandler(Protocol):
    """Protocol for handling writer/reader connection logic."""

    @property
    def host_list_provider_service(self) -> Optional[HostListProviderService]:
        """Getter for the 'host_list_provider_service' attribute."""
        ...

    @host_list_provider_service.setter
    def host_list_provider_service(self, new_value: int) -> None:
        """The setter for the 'host_list_provider_service' attribute."""
        ...

    def open_new_writer_connection(
        self,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
    ) -> tuple[Optional[Connection], Optional[HostInfo]]:
        """Open a writer connection."""
        ...

    def open_new_reader_connection(
        self,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
    ) -> tuple[Optional[Connection], Optional[HostInfo]]:
        """Open a reader connection."""
        ...

    def get_verified_initial_connection(
        self,
        host_info: HostInfo,
        is_initial_connection: bool,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
        connect_func: Callable,
    ) -> Connection:
        """Verify initial connection or return normal workflow."""
        ...

    def should_update_writer_with_current_conn(
        self, current_conn: Connection, current_host: HostInfo, writer_conn: Connection
    ) -> bool:
        """Return true if the current connection fits the criteria of a writer connection."""
        ...

    def should_update_reader_with_current_conn(
        self, current_conn: Connection, current_host: HostInfo, reader_conn: Connection
    ) -> bool:
        """Return true if the current connection fits the criteria of a reader connection."""
        ...

    def is_writer_host(self, current_host: HostInfo) -> bool:
        """Return true if the current host fits the criteria of a writer host."""
        ...

    def is_reader_host(self, current_host: HostInfo) -> bool:
        """Return true if the current host fits the criteria of a reader host."""
        ...

    def can_host_be_used(self, host_info: HostInfo) -> bool:
        """Returns true if connections can be switched to the given host"""
        ...

    def has_no_readers(self) -> bool:
        """Return true if there are no readers in the host list"""
        ...

    def refresh_and_store_host_list(
        self, current_conn: Optional[Connection], driver_dialect: DriverDialect
    ):
        """Refreshes the host list and then stores it."""
        ...


class TopologyBasedConnectionHandler(ReadWriteConnectionHandler):
    """Topology based implementation of connection handling logic."""

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service: PluginService = plugin_service
        self._host_list_provider_service: Optional[HostListProviderService] = None
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

    @property
    def host_list_provider_service(self) -> Optional[HostListProviderService]:
        return self._host_list_provider_service

    @host_list_provider_service.setter
    def host_list_provider_service(self, new_value: HostListProviderService) -> None:
        self._host_list_provider_service = new_value

    def open_new_writer_connection(
        self,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
    ) -> tuple[Optional[Connection], Optional[HostInfo]]:
        writer_host = self._get_writer()
        if writer_host is None:
            return None, None

        conn = plugin_service_connect_func(writer_host)

        return conn, writer_host

    def open_new_reader_connection(
        self,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
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
                    conn = plugin_service_connect_func(host)
                    reader_host = host
                    break
                except Exception:
                    logger.warning(
                        "ReadWriteSplittingPlugin.FailedToConnectToReader", host.url
                    )

        return conn, reader_host

    def get_verified_initial_connection(
        self,
        host_info: HostInfo,
        is_initial_connection: bool,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
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
            ReadWriteSplittingConnectionManager.log_and_raise_exception(
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

    def can_host_be_used(self, host_info: HostInfo) -> bool:
        hostnames = [host_info.host for host_info in self._hosts]
        return host_info.host in hostnames

    def has_no_readers(self) -> bool:
        if len(self._hosts) == 1:
            return self._get_writer() is not None
        return False

    def refresh_and_store_host_list(
        self, current_conn: Optional[Connection], driver_dialect: DriverDialect
    ):
        if current_conn is not None and driver_dialect.can_execute_query(current_conn):
            try:
                self._plugin_service.refresh_host_list()
            except Exception:
                pass  # Swallow exception

        hosts = self._plugin_service.hosts
        if hosts is None or len(hosts) == 0:
            ReadWriteSplittingConnectionManager.log_and_raise_exception(
                "ReadWriteSplittingPlugin.EmptyHostList"
            )

        self._hosts = hosts

    def should_update_writer_with_current_conn(
        self, current_conn, current_host: HostInfo, writer_conn: Connection
    ) -> bool:
        return self.is_writer_host(current_host)

    def should_update_reader_with_current_conn(
        self, current_conn, current_host, reader_conn: Connection
    ) -> bool:
        return True

    def is_writer_host(self, current_host: HostInfo) -> bool:
        return current_host.role == HostRole.WRITER

    def is_reader_host(self, current_host) -> bool:
        return current_host.role == HostRole.READER

    def _get_writer(self) -> Optional[HostInfo]:
        for host in self._hosts:
            if host.role == HostRole.WRITER:
                return host

        ReadWriteSplittingConnectionManager.log_and_raise_exception(
            "ReadWriteSplittingPlugin.NoWriterFound"
        )
        return None


class ReadWriteSplittingPlugin(ReadWriteSplittingConnectionManager):
    def __init__(self, plugin_service: PluginService, props: Properties):
        # The read/write splitting plugin handles connections based on topology.
        connection_handler = TopologyBasedConnectionHandler(plugin_service, props)

        super().__init__(plugin_service, props, connection_handler)


class ReadWriteSplittingPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return ReadWriteSplittingPlugin(plugin_service, props)
