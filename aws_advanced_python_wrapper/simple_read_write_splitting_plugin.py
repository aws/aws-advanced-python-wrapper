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
                    ReadWriteSplittingConnectionManager._close_connection(candidate_conn)
                    self._delay()
                    continue

                # Connection valid and verified.
                return candidate_conn
            except Exception as e:
                ReadWriteSplittingConnectionManager._close_connection(candidate_conn)
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