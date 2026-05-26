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
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    AbstractReadWriteSplittingPlugin
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperty

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import PluginFactory
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties


class SimpleReadWriteSplittingPlugin(AbstractReadWriteSplittingPlugin):
    """Endpoint-based read/write splitting plugin.

    Uses configured read and write endpoints to manage connection switching
    rather than relying on topology information.
    """

    T = TypeVar('T')

    def __init__(self, plugin_service: PluginService, props: Properties):
        # The simple read/write splitting plugin handles connections based on configuration parameter endpoints.
        super().__init__(plugin_service, props)

        read_endpoint: str = SimpleReadWriteSplittingPlugin._verify_parameter(
            WrapperProperties.SRW_READ_ENDPOINT, props, str, required=True
        )
        write_endpoint: str = SimpleReadWriteSplittingPlugin._verify_parameter(
            WrapperProperties.SRW_WRITE_ENDPOINT, props, str, required=True
        )

        self._verify_new_connections: bool = SimpleReadWriteSplittingPlugin._verify_parameter(
            WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS, props, bool
        )

        if self._verify_new_connections:
            self._connect_retry_timeout_ms: int = SimpleReadWriteSplittingPlugin._verify_parameter(
                WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS, props, int, lambda x: x > 0
            )
            self._connect_retry_interval_ms: int = SimpleReadWriteSplittingPlugin._verify_parameter(
                WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS, props, int, lambda x: x > 0
            )

            self._verify_initial_connection_type: Optional[HostRole] = (
                SimpleReadWriteSplittingPlugin._parse_role(
                    WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.get(props)
                )
            )

        self._rds_utils: RdsUtils = RdsUtils()
        self._write_endpoint_host_info: HostInfo = self._create_host_info(write_endpoint, HostRole.WRITER)
        self._read_endpoint_host_info: HostInfo = self._create_host_info(read_endpoint, HostRole.READER)
        self._write_endpoint = write_endpoint.casefold()
        self._read_endpoint = read_endpoint.casefold()

    def connect(
        self,
        target_driver_func: Callable,
        driver_dialect: DriverDialect,
        host_info: HostInfo,
        props: Properties,
        is_initial_connection: bool,
        connect_func: Callable,
    ) -> Connection:
        if not is_initial_connection or not self._verify_new_connections:
            return connect_func()

        url_type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)

        conn: Optional[Connection] = None

        if (
            url_type == RdsUrlType.RDS_WRITER_CLUSTER
            or url_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
            or self._verify_initial_connection_type == HostRole.WRITER
        ):
            conn = self._get_verified_connection(host_info, HostRole.WRITER,
                                                 lambda x: self._plugin_service.connect(x, props, self), connect_func)
        elif (
            url_type == RdsUrlType.RDS_READER_CLUSTER
            or self._verify_initial_connection_type == HostRole.READER
        ):
            conn = self._get_verified_connection(host_info, HostRole.READER,
                                                 lambda x: self._plugin_service.connect(x, props, self), connect_func)

        if conn is None:
            conn = connect_func()

        self._set_initial_connection_host_info(host_info)
        return conn

    def _is_writer(self, current_host: HostInfo) -> bool:
        return (
            current_host.host.casefold() == self._write_endpoint
            or current_host.url.casefold() == self._write_endpoint
        )

    def _is_reader(self, current_host: HostInfo) -> bool:
        return (
            current_host.host.casefold() == self._read_endpoint
            or current_host.url.casefold() == self._read_endpoint
        )

    def _refresh_and_store_topology(self, current_conn: Optional[Connection]):
        # Endpoint-based connections do not require a host list.
        return

    def _initialize_writer_connection(self):
        if self._verify_new_connections:
            conn = self._get_verified_connection(
                self._write_endpoint_host_info, HostRole.WRITER,
                lambda x: self._plugin_service.connect(x, self._properties, self))
        else:
            conn = self._plugin_service.connect(self._write_endpoint_host_info, self._properties, self)

        if conn is None:
            self.log_and_raise_exception(
                "ReadWriteSplittingPlugin.FailedToConnectToWriter",
                self._write_endpoint_host_info.url
            )
            return

        provider = self._conn_provider_manager.get_connection_provider(
            self._write_endpoint_host_info, self._properties
        )
        self._is_writer_conn_from_internal_pool = (
            AbstractReadWriteSplittingPlugin._POOL_PROVIDER_CLASS_NAME
            in str(type(provider))
        )
        self._set_writer_connection(conn, self._write_endpoint_host_info)
        self._switch_current_connection_to(conn, self._write_endpoint_host_info)

    def _initialize_reader_connection(self):
        if self._verify_new_connections:
            conn = self._get_verified_connection(
                self._read_endpoint_host_info, HostRole.READER,
                lambda x: self._plugin_service.connect(x, self._properties, self))
        else:
            conn = self._plugin_service.connect(self._read_endpoint_host_info, self._properties, self)

        if conn is None:
            self.log_and_raise_exception("ReadWriteSplittingPlugin.NoReadersAvailable")
            return

        provider = self._conn_provider_manager.get_connection_provider(
            self._read_endpoint_host_info, self._properties
        )
        self._is_reader_conn_from_internal_pool = (
            AbstractReadWriteSplittingPlugin._POOL_PROVIDER_CLASS_NAME
            in str(type(provider))
        )

        self._set_reader_connection(conn, self._read_endpoint_host_info)
        self._switch_current_connection_to(conn, self._read_endpoint_host_info)

    def _close_reader_if_necessary(self):
        # Endpoint-based connections always connect to the reader endpoint regardless.
        pass

    def _should_update_writer_connection(
        self, current_conn: Connection, current_host: HostInfo
    ) -> bool:
        return (
            self._is_writer(current_host)
            and current_conn != self._writer_connection
            and (
                not self._verify_new_connections
                or self._plugin_service.get_host_role(current_conn) == HostRole.WRITER
            )
        )

    def _should_update_reader_connection(
        self, current_conn: Connection, current_host: HostInfo
    ) -> bool:
        return (
            self._is_reader(current_host)
            and current_conn != self._reader_connection
            and (
                not self._verify_new_connections
                or self._plugin_service.get_host_role(current_conn) == HostRole.READER
            )
        )

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
                    AbstractReadWriteSplittingPlugin.close_connection(candidate_conn, self._plugin_service.driver_dialect)
                    self._delay()
                    continue

                return candidate_conn

            except Exception:
                AbstractReadWriteSplittingPlugin.close_connection(candidate_conn, self._plugin_service.driver_dialect)
                self._delay()

        return None

    def _create_host_info(self, endpoint: str, role: HostRole) -> HostInfo:
        endpoint = endpoint.strip()
        host = endpoint
        try:
            port = self._plugin_service.database_dialect.default_port if not self._plugin_service.current_host_info.is_port_specified() \
                else self._plugin_service.current_host_info.port
        except AwsWrapperError:
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


class SimpleReadWriteSplittingPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service, props: Properties):
        return SimpleReadWriteSplittingPlugin(plugin_service, props)
