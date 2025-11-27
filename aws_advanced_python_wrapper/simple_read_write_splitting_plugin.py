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

from enum import Enum, auto
from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Callable, Optional

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.read_write_splitting_plugin import ReadWriteSplittingConnectionManager, ConnectionHandler
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

logger = Logger(__name__)    

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

            self.verify_opened_connection_type = EndpointBasedConnectionHandler._parse_connection_type(WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.get(props))
    
        self._plugin_service = plugin_service
        self._properties = props
        self._rds_utils = RdsUtils()
        self._host_list_provider_service: Optional[HostListProviderService] = None
        self._write_endpoint_host_info = None 
        self._read_endpoint_host_info = None 

    def open_new_writer_connection(self) -> Optional[tuple[Connection, HostInfo]]: 
        if self._write_endpoint_host_info is None:
            self._write_endpoint_host_info = self._create_host_info(self.write_endpoint, HostRole.WRITER)

        conn: Optional[Connection] = None
        if self.verify_new_connections:
            conn = self._get_verified_connection(self._properties, self._write_endpoint_host_info, HostRole.WRITER)
        else:
            conn = self._plugin_service.connect(self._write_endpoint_host_info, self._properties, None)

        return conn, self._write_endpoint_host_info
    
    def open_new_reader_connection(self) -> Optional[tuple[Connection, HostInfo]]:
        if self._read_endpoint_host_info is None:
            self._read_endpoint_host_info = self._create_host_info(self.read_endpoint, HostRole.READER)

        conn: Optional[Connection] = None
        if self.verify_new_connections:
            conn = self._get_verified_connection(self._properties, self._read_endpoint_host_info, HostRole.READER)
        else:
            conn = self._plugin_service.connect(self._read_endpoint_host_info, self._properties, None)

        return conn, self._read_endpoint_host_info

    def get_verified_initial_connection(self, host_info: HostInfo, props: Properties, is_initial_connection: bool, connect_func: Callable) -> Optional[Connection]:
        logger.debug(f"get_verified_initial_connection called: host={host_info.host if host_info else None}, is_initial={is_initial_connection}, verify_enabled={self.verify_new_connections}")
        
        if not is_initial_connection or not self.verify_new_connections:
            logger.debug("No verification required, using normal workflow")
            return connect_func()
        
        logger.debug(f"Starting connection verification for host: {host_info.host}")
        url_type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)
        logger.debug(f"Identified URL type: {url_type}")
        
        conn: Optional[Connection] = None

        if url_type == RdsUrlType.RDS_WRITER_CLUSTER or (self.verify_opened_connection_type is not None and self.verify_opened_connection_type == HostRole.WRITER):
            logger.debug("Attempting to get verified WRITER connection")
            conn = self._get_verified_connection(props, host_info, HostRole.WRITER, connect_func)
        elif url_type == RdsUrlType.RDS_READER_CLUSTER or (self.verify_opened_connection_type is not None and self.verify_opened_connection_type == HostRole.READER):
            logger.debug("Attempting to get verified READER connection")
            conn = self._get_verified_connection(props, host_info, HostRole.READER, connect_func)
        else:
            logger.debug(f"No specific verification needed for URL type: {url_type}")

        if conn is None:
            logger.debug("Could not get verified connection, falling back to normal workflow")
            conn = connect_func()
        else:
            logger.debug("Successfully obtained verified connection")
        
        logger.debug("Setting initial connection host info")
        self._set_initial_connection_host_info(conn, host_info)
        logger.debug("get_verified_initial_connection completed")
        return conn

    def _set_initial_connection_host_info(self, conn: Connection, host_info: HostInfo):
        logger.debug(f"_set_initial_connection_host_info called: host_info={host_info.host if host_info else None}")
        
        if self.set_host_list_provider_service is None:
            logger.debug("set_host_list_provider_service is None, returning early")
            return
        
        if host_info is None:
            logger.debug("host_info is None, attempting to identify connection")
            try: 
                host_info = self._plugin_service.identify_connection(conn)
                logger.debug(f"Identified connection host_info: {host_info.host if host_info else None}")
            except Exception as e:
                logger.debug(f"Failed to identify connection: {e}")
                return
            
        if host_info is not None and self._host_list_provider_service is not None:
            logger.debug(f"Setting initial_connection_host_info to: {host_info.host}")
            self._host_list_provider_service.initial_connection_host_info = host_info
        else:
            logger.debug(f"Cannot set host info: host_info={host_info is not None}, provider_service={self._host_list_provider_service is not None}")
    
    def _get_verified_connection(self, props: Properties, host_info: HostInfo, role: HostRole, connect_func: Callable = None) -> Connection:
        logger.debug(f"_get_verified_connection called: host={host_info.host if host_info else None}, role={role}, timeout_ms={self.connect_retry_timeout_ms}")
        
        end_time_nano = perf_counter_ns() + (self.connect_retry_timeout_ms * 1000000)
        logger.debug(f"Connection retry will timeout at: {end_time_nano}")

        candidate_conn: Optional[Connection]
        attempt = 0

        while perf_counter_ns() < end_time_nano:
            attempt += 1
            logger.debug(f"Connection verification attempt #{attempt}")
            candidate_conn = None

            try:
                if connect_func is not None:
                    logger.debug("Using provided connect_func to establish connection")
                    candidate_conn = connect_func()
                elif host_info is not None:
                    logger.debug(f"Using plugin_service.connect to connect to {host_info.host}")
                    candidate_conn = self._plugin_service.connect(host_info, props, None)
                else:
                    logger.debug("No connect_func or host_info provided, cannot verify role")
                    return None

                logger.debug(f"Connection established: {candidate_conn is not None}")
                
                if candidate_conn is None:
                    logger.debug("Connection is None, retrying")
                    self._delay()
                    continue
                
                actual_role = self._plugin_service.get_host_role(candidate_conn)
                logger.debug(f"Connection role verification: expected={role}, actual={actual_role}")
                
                if actual_role != role:
                    logger.debug(f"Role mismatch, closing connection and retrying")
                    ReadWriteSplittingConnectionManager.close_connection(candidate_conn)
                    self._delay()
                    continue

                logger.debug("Connection verified successfully")
                return candidate_conn
                
            except Exception as e:
                logger.debug(f"Exception during connection attempt #{attempt}: {e}")
                ReadWriteSplittingConnectionManager.close_connection(candidate_conn)
                self._delay()

        logger.debug(f"Connection verification timed out after {attempt} attempts")
        return None

    def old_reader_can_be_used(self, reader_host_info: HostInfo) -> bool:
        # Assume that the old reader can always be used, no topology-based information to check.
        return True

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
        return current_host.host.casefold() == self.write_endpoint.casefold() or current_host.url.casefold() == self.write_endpoint.casefold()

    def is_reader_host(self, current_host: HostInfo) -> bool:
        return current_host.host.casefold() == self.read_endpoint.casefold() or current_host.url.casefold() == self.read_endpoint.casefold()
    
    def _create_host_info(self, endpoint, role: HostRole) -> HostInfo:
        endpoint = endpoint.strip()
        host = endpoint
        port = self._plugin_service.database_dialect.default_port
        colon_index = endpoint.rfind(":")

        if colon_index != -1:
            port_str = endpoint[colon_index + 1:]
            if port_str.isdigit():
                host = endpoint[:colon_index]
                port = int(port_str)
            else: 
                if (self.set_host_list_provider_service is not None and self.set_host_list_provider_service.initial_connection_host_info is not None and 
                    self.set_host_list_provider_service.initial_connection_host_info.port != HostInfo.NO_PORT):
                    port = self.set_host_list_provider_service.initial_connection_host_info.port

        return HostInfo(
            host=host,
            port=port,
            role=role,
            availability=HostAvailability.AVAILABLE)

    def _delay(self):
        sleep(self.connect_retry_interval_ms / 1000)

    @staticmethod
    def _parse_connection_type(phase_str: Optional[str]) -> HostRole:
        if not phase_str:
            return None 
        
        phase_upper = phase_str.lower()
        if phase_upper == "reader":
            return HostRole.READER
        elif phase_upper == "writer":
            return HostRole.WRITER
        else:
            raise ValueError(Messages.get_formatted("SimpleReadWriteSplittingPlugin.IncorrectConfiguration", WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.name))
    
    
class SimpleReadWriteSplittingPlugin(ReadWriteSplittingConnectionManager):    
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