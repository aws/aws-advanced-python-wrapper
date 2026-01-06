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

from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Callable, Optional, Type, TypeVar

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.read_write_splitting_plugin import (
    ReadWriteConnectionHandler, ReadWriteSplittingConnectionManager)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperty

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import PluginFactory
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties


class EndpointBasedConnectionHandler(ReadWriteConnectionHandler):
    """Endpoint based implementation of connection handling logic."""

    def __init__(self, plugin_service: PluginService, props: Properties):
        read_endpoint: str = EndpointBasedConnectionHandler._verify_parameter(
            WrapperProperties.SRW_READ_ENDPOINT, props, str, required=True
        )
        write_endpoint: str = EndpointBasedConnectionHandler._verify_parameter(
            WrapperProperties.SRW_WRITE_ENDPOINT, props, str, required=True
        )

        self._verify_new_connections: bool = EndpointBasedConnectionHandler._verify_parameter(
            WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS, props, bool
        )

        if self._verify_new_connections:
            self._connect_retry_timeout_ms: int = EndpointBasedConnectionHandler._verify_parameter(
                WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS, props, int, lambda x: x > 0
            )
            self._connect_retry_interval_ms: int = EndpointBasedConnectionHandler._verify_parameter(
                WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS, props, int, lambda x: x > 0
            )

            self._verify_initial_connection_type: Optional[HostRole] = (
                EndpointBasedConnectionHandler._parse_role(
                    WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.get(props)
                )
            )

        self._plugin_service: PluginService = plugin_service
        self._rds_utils: RdsUtils = RdsUtils()
        self._host_list_provider_service: Optional[HostListProviderService] = None
        self._write_endpoint_host_info: HostInfo = self._create_host_info(write_endpoint, HostRole.WRITER)
        self._read_endpoint_host_info: HostInfo = self._create_host_info(read_endpoint, HostRole.READER)
        self._write_endpoint = write_endpoint.casefold()
        self._read_endpoint = read_endpoint.casefold()

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
        if self._verify_new_connections:
            return self._get_verified_connection(self._write_endpoint_host_info, HostRole.WRITER, plugin_service_connect_func), \
                self._write_endpoint_host_info

        return plugin_service_connect_func(self._write_endpoint_host_info), self._write_endpoint_host_info

    def open_new_reader_connection(
        self,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
    ) -> tuple[Optional[Connection], Optional[HostInfo]]:
        if self._verify_new_connections:
            return self._get_verified_connection(self._read_endpoint_host_info, HostRole.READER, plugin_service_connect_func), \
                self._read_endpoint_host_info

        return plugin_service_connect_func(self._read_endpoint_host_info), self._read_endpoint_host_info

    def get_verified_initial_connection(
        self,
        host_info: HostInfo,
        is_initial_connection: bool,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
        connect_func: Callable,
    ) -> Connection:
        if not is_initial_connection or not self._verify_new_connections:
            return connect_func()

        url_type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)

        conn: Optional[Connection] = None

        if (
            url_type == RdsUrlType.RDS_WRITER_CLUSTER
            or self._verify_initial_connection_type == HostRole.WRITER
        ):
            conn = self._get_verified_connection(host_info, HostRole.WRITER, plugin_service_connect_func, connect_func)
        elif (
            url_type == RdsUrlType.RDS_READER_CLUSTER
            or self._verify_initial_connection_type == HostRole.READER
        ):
            conn = self._get_verified_connection(host_info, HostRole.READER, plugin_service_connect_func, connect_func)

        if conn is None:
            conn = connect_func()

        self._set_initial_connection_host_info(host_info)
        return conn

    def _set_initial_connection_host_info(self, host_info: HostInfo):
        if self._host_list_provider_service is None:
            return

        self._host_list_provider_service.initial_connection_host_info = host_info

    def _get_verified_connection(
        self,
        host_info: HostInfo,
        role: HostRole,
        plugin_service_connect_func: Callable[[HostInfo], Connection],
        connect_func: Optional[Callable] = None,
    ) -> Optional[Connection]:
        end_time_nano = perf_counter_ns() + (self._connect_retry_timeout_ms * 1000000)

        candidate_conn: Optional[Connection]

        while perf_counter_ns() < end_time_nano:
            candidate_conn = None

            try:
                if connect_func is not None:
                    candidate_conn = connect_func()
                else:
                    candidate_conn = plugin_service_connect_func(host_info)

                if candidate_conn is None:
                    self._delay()
                    continue

                actual_role = self._plugin_service.get_host_role(candidate_conn)

                if actual_role != role:
                    ReadWriteSplittingConnectionManager.close_connection(candidate_conn, self._plugin_service.driver_dialect)
                    self._delay()
                    continue

                return candidate_conn

            except Exception:
                ReadWriteSplittingConnectionManager.close_connection(candidate_conn, self._plugin_service.driver_dialect)
                self._delay()

        return None

    def can_host_be_used(self, host_info: HostInfo) -> bool:
        # Assume that the host can always be used, no topology-based information to check.
        return True

    def has_no_readers(self) -> bool:
        # SetReadOnly(true) will always connect to the read_endpoint, regardless of number of readers.
        return False

    def refresh_and_store_host_list(
        self, current_conn: Optional[Connection], driver_dialect: DriverDialect
    ):
        # Endpoint based connections do not require a host list.
        return

    def should_update_writer_with_current_conn(
        self, current_conn: Connection, current_host: HostInfo, writer_conn: Connection
    ) -> bool:
        return (
            self.is_writer_host(current_host)
            and current_conn != writer_conn
            and (
                not self._verify_new_connections
                or self._plugin_service.get_host_role(current_conn) == HostRole.WRITER
            )
        )

    def should_update_reader_with_current_conn(
        self, current_conn: Connection, current_host: HostInfo, reader_conn: Connection
    ) -> bool:
        return (
            self.is_reader_host(current_host)
            and current_conn != reader_conn
            and (
                not self._verify_new_connections
                or self._plugin_service.get_host_role(current_conn) == HostRole.READER
            )
        )

    def is_writer_host(self, current_host: HostInfo) -> bool:
        return (
            current_host.host.casefold() == self._write_endpoint
            or current_host.url.casefold() == self._write_endpoint
        )

    def is_reader_host(self, current_host: HostInfo) -> bool:
        return (
            current_host.host.casefold() == self._read_endpoint
            or current_host.url.casefold() == self._read_endpoint
        )

    def _create_host_info(self, endpoint: str, role: HostRole) -> HostInfo:
        endpoint = endpoint.strip()
        host = endpoint
        try:
            port = self._plugin_service.database_dialect.default_port if not self._plugin_service.current_host_info.is_port_specified() \
                else self._plugin_service.current_host_info.port
        except AwsWrapperError:  # if current_host_info cannot be determined fallback to default port
            port = self._plugin_service.database_dialect.default_port
        colon_index = endpoint.rfind(":")

        if colon_index != -1:
            host = endpoint[:colon_index]
            port_str = endpoint[colon_index + 1:]
            if port_str.isdigit():
                port = int(port_str)

        return HostInfo(
            host=host, port=port, role=role, availability=HostAvailability.AVAILABLE
        )

    T = TypeVar('T')

    @staticmethod
    def _verify_parameter(prop: WrapperProperty, props: Properties, expected_type: Type[T], validator=None, required=False):
        value = prop.get_type(props, expected_type)
        if required:
            if value is None:
                raise AwsWrapperError(
                    Messages.get_formatted(
                        "SimpleReadWriteSplittingPlugin.MissingRequiredConfigParameter",
                        prop.name,
                    )
                )

        if validator and not validator(value):
            raise ValueError(
                Messages.get_formatted(
                    "SimpleReadWriteSplittingPlugin.IncorrectConfiguration",
                    prop.name,
                )
            )
        return value

    def _delay(self):
        sleep(self._connect_retry_interval_ms / 1000)

    @staticmethod
    def _parse_role(role_str: Optional[str]) -> HostRole:
        if not role_str:
            return HostRole.UNKNOWN

        phase_lower = role_str.lower()
        if phase_lower == "reader":
            return HostRole.READER
        elif phase_lower == "writer":
            return HostRole.WRITER
        else:
            raise ValueError(
                Messages.get_formatted(
                    "SimpleReadWriteSplittingPlugin.IncorrectConfiguration",
                    WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.name,
                )
            )


class SimpleReadWriteSplittingPlugin(ReadWriteSplittingConnectionManager):
    def __init__(self, plugin_service: PluginService, props: Properties):
        # The simple read/write splitting plugin handles connections based on configuration parameter endpoints.
        connection_handler = EndpointBasedConnectionHandler(plugin_service, props)

        super().__init__(plugin_service, props, connection_handler)


class SimpleReadWriteSplittingPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service, props: Properties):
        return SimpleReadWriteSplittingPlugin(plugin_service, props)
