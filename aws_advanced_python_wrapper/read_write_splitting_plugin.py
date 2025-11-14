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
from enum import Enum, auto
from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, Set, Tuple

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

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

class BaseReadWriteSplittingConnectionManager(Plugin):
    """Base class that manages connection switching logic."""
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider",
                                     "connect",
                                     "notify_connection_changed",
                                     "Connection.set_read_only"}
    def __init__(self, plugin_service: PluginService, props, connection_handler: ConnectionHandler):
        self._plugin_service = plugin_service
        self._properties = props
        self._connection_handler = connection_handler
        self._writer_connection: Optional[Connection] = None
        self._reader_connection: Optional[Connection] = None
        self._writer_host_info: Optional[HostInfo] = None
        self._reader_host_info: Optional[HostInfo] = None
        self._in_read_write_split: bool = False

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        self._connection_handler.host_list_provider_service = host_list_provider_service
        init_host_provider_func()

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connection_handler.get_verified_initial_connection(host_info, props, is_initial_connection, connect_func)

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

        if self._connection_handler.should_update_writer_with_current_conn(current_conn, current_host, self._writer_connection):
            self._set_writer_connection(current_conn, current_host)
        elif self._connection_handler.should_update_reader_with_current_conn(current_conn, current_host, self._reader_connection):
            self._set_reader_connection(current_conn, current_host)

    def _set_writer_connection(self, writer_conn: Connection, writer_host_info: HostInfo):
        self._writer_connection = writer_conn
        self._writer_host_info = writer_host_info
        logger.debug("ReadWriteSplittingPlugin.SetWriterConnection", writer_host_info.url)

    def _set_reader_connection(self, reader_conn: Connection, reader_host_info: HostInfo):
        self._reader_connection = reader_conn
        self._reader_host_info = reader_host_info
        logger.debug("ReadWriteSplittingPlugin.SetReaderConnection", reader_host_info.url)

    def _get_new_writer_connection(self):
        conn, writer_host = self._connection_handler.get_new_writer_connection()
        self._set_writer_connection(conn, writer_host)
        self._switch_current_connection_to(conn, writer_host)

    def _switch_connection_if_required(self, read_only: bool):
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect

        if (current_conn is not None and
                driver_dialect is not None and driver_dialect.is_closed(current_conn)):
            self._log_and_raise_exception("ReadWriteSplittingPlugin.SetReadOnlyOnClosedConnection")

        self._connection_handler.refresh_and_store_host_list(current_conn, driver_dialect)
        current_host = self._plugin_service.current_host_info
        if current_host is None:
            self._log_and_raise_exception("ReadWriteSplittingPlugin.UnavailableHostInfo")
            return

        if read_only:
            if not self._plugin_service.is_in_transaction and not self._connection_handler.is_reader_host(current_host):
                try:
                    self._switch_to_reader_connection()
                except Exception as ex:
                    if ex is not None:
                        # do this 
                        ex = None
                    if not self._is_connection_usable(current_conn, driver_dialect):
                        self._log_and_raise_exception("ReadWriteSplittingPlugin.ErrorSwitchingToReader")
                        return

                    logger.warning("ReadWriteSplittingPlugin.FallbackToWriter", current_host.url)
        elif not self._connection_handler.is_writer_host(current_host):
            if self._plugin_service.is_in_transaction:
                self._log_and_raise_exception("ReadWriteSplittingPlugin.SetReadOnlyFalseInTransaction")

            try:
                self._switch_to_writer_connection()
            except Exception:
                self._log_and_raise_exception("ReadWriteSplittingPlugin.ErrorSwitchingToWriter")

    def _switch_current_connection_to(self, new_conn: Connection, new_conn_host: HostInfo):
        current_conn = self._plugin_service.current_connection
        if current_conn == new_conn:
            return

        self._plugin_service.set_current_connection(new_conn, new_conn_host)

        logger.debug("ReadWriteSplittingPlugin.SettingCurrentConnection", new_conn_host.url)

    def _switch_to_writer_connection(self):
        current_host = self._plugin_service.current_host_info
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if (current_host is not None and self._connection_handler.is_writer_host(current_host)and
                self._is_connection_usable(current_conn, driver_dialect)):
            # Already connected to the intended writer.
            return

        self._in_read_write_split = True
        if not self._is_connection_usable(self._writer_connection, driver_dialect):
            self._get_new_writer_connection()
        elif self._writer_connection is not None and self._writer_host_info is not None:
            self._switch_current_connection_to(self._writer_connection, self._writer_host_info)

        if self._connection_handler.should_close_reader_after_switch_to_writer():
            self._close_connection_if_idle(self._reader_connection)

        logger.debug("ReadWriteSplittingPlugin.SwitchedFromReaderToWriter", self._writer_host_info.url)

    def _switch_to_reader_connection(self):
        current_host = self._plugin_service.current_host_info
        current_conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if (current_host is not None and self._connection_handler.is_reader_host(current_host) and
                self._is_connection_usable(current_conn, driver_dialect)):
            # Already connected to the intended reader.
            return

        if self._reader_connection is not None and not self._connection_handler.old_reader_can_be_used(self._reader_host_info):
            # The old reader cannot be used anymore, close it.
            self._close_connection_if_idle(self._reader_connection)

        self._in_read_write_split = True
        if not self._is_connection_usable(self._reader_connection, driver_dialect):
            self._initialize_reader_connection()
        elif self._reader_connection is not None and self._reader_host_info is not None:
            try:
                self._switch_current_connection_to(self._reader_connection, self._reader_host_info)
                logger.debug("ReadWriteSplittingPlugin.SwitchedFromWriterToReader", self._reader_host_info.url)
            except Exception:
                logger.debug("ReadWriteSplittingPlugin.ErrorSwitchingToCachedReader", self._reader_host_info.url)

                self._reader_connection.close()
                self._reader_connection = None
                self._initialize_reader_connection()

        if self._connection_handler.should_close_writer_after_switch_to_reader():
            self._close_connection_if_idle(self._writer_connection)

    def _initialize_reader_connection(self):
        if self._connection_handler.need_connect_to_writer():
            if not self._is_connection_usable(self._writer_connection, self._plugin_service.driver_dialect):
                self._get_new_writer_connection()
            logger.warning("ReadWriteSplittingPlugin.NoReadersFound", self._writer_host_info.url)
            return
    
        conn, reader_host = self._connection_handler.get_new_reader_connection()

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

class ConnectionHandler(Protocol):
    """Protocol for handling writer/reader connection logic."""
    @property
    @abstractmethod
    def host_list_provider_service(self) -> HostListProviderService:
        ...
    
    def get_new_writer_connection(self) -> Optional[tuple[Connection, HostInfo]]:
        """Get or create a writer connection."""
        ...
    
    def get_new_reader_connection(self) -> Optional[tuple[Connection, HostInfo]]:
        """Get or create a reader connection."""
        ...

    def get_verified_initial_connection(self, host_info: HostInfo, props: Properties, is_initial_connection: bool, connect_func: Callable) -> Optional[Connection]:
        """Verify initial connection or return normal workflow."""
        ...
    
    def should_update_writer_with_current_conn(self, current_conn: Connection, current_host: HostInfo, writer_conn: Connection) -> bool:
        """Return true if the current connection fits the criteria of a writer connection."""
        ...

    def should_update_reader_with_current_conn(self, current_conn: Connection, current_host: HostInfo, reader_conn: Connection) -> bool:
        """Return true if the current connection fits the criteria of a reader connection."""
        ...

    def is_writer_host(self, current_host: HostInfo) -> bool:
        """Return true if the current host fits the criteria of a writer host."""
        ...

    def is_reader_host(self, current_host: HostInfo) -> bool:
        """Return true if the current host fits the criteria of a writer host."""
        ...

    def old_reader_can_be_used(self, reader_host_info: HostInfo) -> bool:
        """Return true if the current host can be used to switch connection to."""
        ...    

    def should_close_writer_after_switch_to_reader(self) -> bool:
        """Return true if the cached writer should be closed upon switch to reader."""
        ...    

    def should_close_reader_after_switch_to_writer(self) -> bool:
        """Return true if the cached reader should be closed upon switch to writer."""
        ...    

    def need_connect_to_writer(self) -> bool:
        """Return true if switching to reader should instead connect to writer."""
        ...    

    def refresh_and_store_host_list(self, current_conn: Connection, driver_dialect: DriverDialect):
        """Refreshes the host list and then stores it."""
        ... 

    @staticmethod
    def _log_and_raise_exception(log_msg: str):
        logger.error(log_msg)
        raise ReadWriteSplittingError(Messages.get(log_msg))

    @staticmethod
    def _is_connection_usable(conn: Optional[Connection], driver_dialect: Optional[DriverDialect]):
        return conn is not None and driver_dialect is not None and not driver_dialect.is_closed(conn)

class TopologyBasedConnectionHandler(ConnectionHandler):
    """Topology based implementation of connection handling logic."""
    _POOL_PROVIDER_CLASS_NAME = "aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider"
    
    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._host_list_provider_service: Optional[HostListProviderService] = None        
        self._conn_provider_manager: ConnectionProviderManager = self._plugin_service.get_connection_provider_manager()
        self._is_reader_conn_from_internal_pool: bool = False
        self._is_writer_conn_from_internal_pool: bool = False
        strategy = WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(self._properties)
        if strategy is not None:
            self._reader_selector_strategy = strategy
        else:
            default_strategy = WrapperProperties.READER_HOST_SELECTOR_STRATEGY.default_value
            if default_strategy is not None:
                self._reader_selector_strategy = default_strategy
        self._hosts = None

    @property
    def host_list_provider_service(self) -> HostListProviderService:
        return self._host_list_provider_service

    @host_list_provider_service.setter
    def host_list_provider_service(self, value: HostListProviderService):
        self._host_list_provider_service = value

    def get_new_writer_connection(self) -> Optional[tuple[Connection, HostInfo]]: 
        writer_host = self._get_writer(self._hosts)
        if writer_host is None:
            return
        
        conn = self._plugin_service.connect(writer_host, self._properties, self)
        provider = self._conn_provider_manager.get_connection_provider(writer_host, self._properties)
        self._is_writer_conn_from_internal_pool = (TopologyBasedConnectionHandler._POOL_PROVIDER_CLASS_NAME in str(type(provider)))

        return conn, writer_host
    
    def get_new_reader_connection(self) -> Optional[tuple[Connection, HostInfo]]:
        conn: Optional[Connection] = None
        reader_host: Optional[HostInfo] = None

        conn_attempts = len(self._plugin_service.hosts) * 2
        for _ in range(conn_attempts):
            host = self._plugin_service.get_host_info_by_strategy(HostRole.READER, self._reader_selector_strategy)
            if host is not None:
                try:
                    conn = self._plugin_service.connect(host, self._properties, self)
                    reader_host = host
                    provider = self._conn_provider_manager.get_connection_provider(host, self._properties)
                    self._is_reader_conn_from_internal_pool = (TopologyBasedConnectionHandler._POOL_PROVIDER_CLASS_NAME in str(type(provider)))
                    break
                except Exception:
                    logger.warning("ReadWriteSplittingPlugin.FailedToConnectToReader", host.url)

        return conn, reader_host

    def get_verified_initial_connection(self, host_info: HostInfo, props: Properties, is_initial_connection: bool, connect_func: Callable) -> Optional[Connection]:
        if not self._plugin_service.accepts_strategy(host_info.role, self._reader_selector_strategy):
            raise AwsWrapperError(
                Messages.get_formatted("ReadWriteSplittingPlugin.UnsupportedHostInfoSelectorStrategy",
                                       self._reader_selector_strategy))

        current_conn = connect_func()

        if not is_initial_connection or (self._host_list_provider_service is not None and self._host_list_provider_service.is_static_host_list_provider()):
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
            if self._host_list_provider_service is not None:
                self._host_list_provider_service.initial_connection_host_info = updated_host

        return current_conn

    def old_reader_can_be_used(self, reader_host_info: HostInfo) -> bool:
        hostnames = [host_info.host for host_info in self._hosts]
        return reader_host_info is not None and reader_host_info.host in hostnames

    def should_close_writer_after_switch_to_reader(self) -> bool:
        return self._is_writer_conn_from_internal_pool

    def should_close_reader_after_switch_to_writer(self) -> bool:
        return self._is_reader_conn_from_internal_pool

    def need_connect_to_writer(self) -> bool:
        if self._hosts is not None and len(self._hosts) == 1:
            return self._get_writer(self._hosts) is not None
        return False

    def refresh_and_store_host_list(self, current_conn: Connection, driver_dialect: DriverDialect):
        if current_conn is not None and driver_dialect.can_execute_query(current_conn):
            try:
                self._plugin_service.refresh_host_list()
            except Exception:
                pass  # Swallow exception

        hosts = self._plugin_service.hosts
        if hosts is None or len(hosts) == 0:
            self._log_and_raise_exception("ReadWriteSplittingPlugin.EmptyHostList")
        
        self._hosts = hosts

    @staticmethod
    def should_update_writer_with_current_conn(current_conn, current_host: HostInfo, writer_conn: Connection) -> bool:
        return TopologyBasedConnectionHandler.is_writer_host(current_host)

    @staticmethod   
    def should_update_reader_with_current_conn(current_conn, current_host, reader_conn: Connection) -> bool:
        return True
    
    @staticmethod
    def is_writer_host(current_host: HostInfo) -> bool:
        return current_host.role == HostRole.WRITER

    @staticmethod
    def is_reader_host(current_host) -> bool:
        return current_host.role == HostRole.READER

    @staticmethod
    def _get_writer(hosts: Tuple[HostInfo, ...]) -> Optional[HostInfo]:
        for host in hosts:
            if host.role == HostRole.WRITER:
                return host

        ReadWriteSplittingPlugin._log_and_raise_exception("ReadWriteSplittingPlugin.NoWriterFound")

        return None
    
class VerifyOpenedConnectionType(Enum):
    READER = auto()
    WRITER = auto()

    @staticmethod
    def parse_connection_type(phase_str: Optional[str]) -> VerifyOpenedConnectionType:
        if not phase_str:
            return None 
        
        phase_upper = phase_str.lower()
        if phase_upper == "reader":
            return VerifyOpenedConnectionType.READER
        elif phase_upper == "writer":
            return VerifyOpenedConnectionType.WRITER
        else:
            raise ValueError(Messages.get_formatted("SimpleReadWriteSplittingPlugin.IncorrectConfiguration", WrapperProperties.SRW_VERIFY_OPENED_CONNECTION_TYPE.name))
    
class EndpointBasedConnectionHandler(ConnectionHandler):
    """Endpoint based implementation of connection handling logic."""
    
    def __init__(self, plugin_service: PluginService, props: Properties):
        srw_read_endpoint = WrapperProperties.SRW_READ_ENDPOINT.get(props)
        if srw_read_endpoint is None:
            raise AwsWrapperError(Messages.get_formatted("SimpleReadWriteSplittingPlugin.MissingRequiredConfigParameter", WrapperProperties.SRW_READ_ENDPOINT.name))
        self.read_endpoint = srw_read_endpoint

        srw_write_endpoint = WrapperProperties.SRW_WRITE_ENDPOINT.get(props)
        if srw_write_endpoint is None:
            raise AwsWrapperError(Messages.get_formatted("SimpleReadWriteSplittingPlugin.MissingRequiredConfigParameter", WrapperProperties.SRW_WRITE_ENDPOINT.name))
        self.write_endpoint = srw_write_endpoint
    
        self.verify_new_connections = WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.get_bool(props)
        if self.verify_new_connections is True:
            srw_connect_retry_timeout_ms = WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.get_int(props)
            if srw_connect_retry_timeout_ms <= 0:
                raise ValueError(Messages.get_formatted("SimpleReadWriteSplittingPlugin.IncorrectConfiguration", WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name))
            self.connect_retry_timeout_ms = srw_connect_retry_timeout_ms

            srw_connect_retry_interval_ms = WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.get_int(props)
            if srw_connect_retry_interval_ms <= 0:
                raise ValueError(Messages.get_formatted("SimpleReadWriteSplittingPlugin.IncorrectConfiguration", WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name))
            self.connect_retry_interval_ms = srw_connect_retry_interval_ms

            self.verify_opened_connection_type = VerifyOpenedConnectionType.parse_connection_type(WrapperProperties.SRW_VERIFY_OPENED_CONNECTION_TYPE.get(props))
    
        self._plugin_service = plugin_service
        self._properties = props
        self._rds_utils = RdsUtils()
        self._host_list_provider_service: Optional[HostListProviderService] = None
        self._write_endpoint_host_info = None 
        self._read_endpoint_host_info = None 
       

    @property
    def host_list_provider_service(self) -> HostListProviderService:
        return self._host_list_provider_service

    @host_list_provider_service.setter
    def host_list_provider_service(self, value: HostListProviderService):
        self._host_list_provider_service = value

    def get_new_writer_connection(self) -> Optional[tuple[Connection, HostInfo]]: 
        if self._write_endpoint_host_info is None:
            self._write_endpoint_host_info = self.create_host_info(self.write_endpoint, HostRole.WRITER)

        conn: Optional[Connection] = None
        if self.verify_new_connections:
            conn = self._get_verified_connection(self._properties, self._write_endpoint_host_info, HostRole.WRITER)
        else:
            conn = self._plugin_service.connect(self._write_endpoint_host_info, self._properties, self)

        return conn, self._write_endpoint_host_info
    
    def get_new_reader_connection(self) -> Optional[tuple[Connection, HostInfo]]:
        if self._read_endpoint_host_info is None:
            self._read_endpoint_host_info = self.create_host_info(self.read_endpoint, HostRole.READER)

        conn: Optional[Connection] = None
        if self.verify_new_connections:
            conn = self._get_verified_connection(self._properties, self._read_endpoint_host_info, HostRole.READER)
        else:
            conn = self._plugin_service.connect(self._read_endpoint_host_info, self._properties, self)

        return conn, self._read_endpoint_host_info

    def get_verified_initial_connection(self, host_info: HostInfo, props: Properties, is_initial_connection: bool, connect_func: Callable) -> Optional[Connection]:
        if not is_initial_connection or not self.verify_new_connections:
            # No verification required, continue with normal workflow.
            return connect_func()
        
        url_type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)

        if url_type == RdsUrlType.RDS_WRITER_CLUSTER or (self.verify_opened_connection_type is not None and self.verify_opened_connection_type == VerifyOpenedConnectionType.WRITER):
            writer_candidate_conn: Optional[Connection] = self._get_verified_connection(props, host_info, HostRole.WRITER, connect_func)
            if writer_candidate_conn is None:
                # Can't get verified writer connection, continue with normal workflow.
                return connect_func()
            self.set_initial_connection_host_info(writer_candidate_conn, host_info)
            return writer_candidate_conn

        if url_type == RdsUrlType.RDS_READER_CLUSTER or (self.verify_opened_connection_type is not None and self.verify_opened_connection_type == VerifyOpenedConnectionType.READER):
            reader_candidate_conn: Optional[Connection] = self._get_verified_connection(props, host_info, HostRole.READER, connect_func)
            if reader_candidate_conn is None:
                # Can't get verified reader connection, continue with normal workflow.
                return connect_func()
            self.set_initial_connection_host_info(reader_candidate_conn, host_info)
            return reader_candidate_conn

        # Continue with normal workflow
        return connect_func()

    def set_initial_connection_host_info(self, conn: Connection, host_info: HostInfo):
        if host_info is None:
            try: 
                host_info = self._plugin_service.identify_connection(conn)
            except Exception:
                return
            
        if host_info is not None:
            self._host_list_provider_service.initial_connection_host_info = host_info
    
    def _get_verified_connection(self, props: Properties, host_info: HostInfo, role: HostRole, connect_func: Callable = None) -> Connection:
        end_time_nano = perf_counter_ns() + (self.connect_retry_timeout_ms * 1000000)

        candidate_conn: Optional[Connection]

        while perf_counter_ns() < end_time_nano:
            candidate_conn = None

            try:
                if host_info is None:
                    if connect_func is None:
                        # Unable to connect to verify role.
                        break
                    # No host_info provided, still verify role.
                    candidate_conn = connect_func()
                else:
                    candidate_conn = self._plugin_service.connect(host_info, props, self)

                if candidate_conn is None or self._plugin_service.get_host_role(candidate_conn) != role:
                    EndpointBasedConnectionHandler._close_connection(candidate_conn)
                    self._delay()
                    continue

                # Connection valid and verified.
                return candidate_conn
            except Exception as e:
                EndpointBasedConnectionHandler._close_connection(candidate_conn)
                self._delay()

        return None

    def old_reader_can_be_used(self, reader_host_info: HostInfo) -> bool:
        # Assume that the old reader can always be used, no topology-based information to check.
        return True

    def should_close_writer_after_switch_to_reader(self) -> bool:
        # Endpoint based connections do not use pooled connection providers.
        return False

    def should_close_reader_after_switch_to_writer(self) -> bool:
        # Endpoint based connections do not use pooled connection providers.
        return False

    def need_connect_to_writer(self) -> bool:
        # SetReadOnly(true) will always connect to the read_endpoint, and not the writer.
        return False

    def refresh_and_store_host_list(self, current_conn: Connection, driver_dialect: DriverDialect):
        # Endpoint based connections do not require a host list.
        return

    def should_update_writer_with_current_conn(self, current_conn: Connection, current_host: HostInfo, writer_conn: Connection) -> bool:
        return self.is_writer_host(current_host) and current_conn != writer_conn and (not self.verify_new_connections or self._plugin_service.get_host_role(current_conn) == HostRole.WRITER)

    def should_update_reader_with_current_conn(self, current_conn: Connection, current_host: HostInfo, reader_conn: Connection) -> bool:
        return self.is_reader_host(current_host) and current_conn != reader_conn and (not self.verify_new_connections or self._plugin_service.get_host_role(current_conn) == HostRole.READER)
    
    def is_writer_host(self, current_host: HostInfo) -> bool:
        return current_host.host.casefold() == self.write_endpoint.casefold()

    def is_reader_host(self, current_host: HostInfo) -> bool:
        return current_host.host.casefold() == self.read_endpoint.casefold()
    
    def create_host_info(self, endpoint, role: HostRole) -> HostInfo:
        port = self._plugin_service.database_dialect.default_port
        if self.host_list_provider_service is not None and self.host_list_provider_service.initial_connection_host_info is not None and self.host_list_provider_service.initial_connection_host_info.port != HostInfo.NO_PORT:
            port = self.host_list_provider_service.initial_connection_host_info.port
        return HostInfo(
            host=endpoint,
            port=port,
            role=role,
            availability=HostAvailability.AVAILABLE)

    def _delay(self):
        sleep(self.connect_retry_interval_ms / 1000)
    
    @staticmethod
    def _close_connection(connection: Connection):
        if connection is not None:
            try:
                connection.close()
            except:
                # Swallow exception
                return


class ReadWriteSplittingPlugin(BaseReadWriteSplittingConnectionManager):    
    def __init__(self, plugin_service, props: Properties):
        # The read/write splitting plugin handles connections based on topology.
        connection_handler = TopologyBasedConnectionHandler(
            plugin_service, 
            props,
        )
        
        super().__init__(plugin_service, props, connection_handler)

class ReadWriteSplittingPluginFactory(PluginFactory):
    def get_instance(self, plugin_service, props: Properties):
        return ReadWriteSplittingPlugin(plugin_service, props)
    
class SimpleReadWriteSplittingPlugin(BaseReadWriteSplittingConnectionManager):    
    def __init__(self, plugin_service, props: Properties):
        # The simple read/write splitting plugin handles connections based on configuration parameter endpoints.
        connection_handler = EndpointBasedConnectionHandler(
            plugin_service, 
            props,
        )
        
        super().__init__(plugin_service, props, connection_handler)

class SimpleReadWriteSplittingPluginFactory(PluginFactory):
    def get_instance(self, plugin_service, props: Properties):
        return SimpleReadWriteSplittingPlugin(plugin_service, props)