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
from typing import TYPE_CHECKING, Callable, Optional, Set

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils


class AuroraInitialConnectionStrategyPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider",
                                     "connect",
                                     "force_connect"}

    _host_list_provider_service: Optional[HostListProviderService] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return AuroraInitialConnectionStrategyPlugin._SUBSCRIBED_METHODS

    def __init__(self, plugin_service: PluginService):
        super()
        self._plugin_service = plugin_service
        self._rds_utils = RdsUtils()

    def connect(self, target_driver_func: Callable, driver_dialect: DriverDialect, host_info: HostInfo, props: Properties,
                is_initial_connection: bool, connect_func: Callable) -> Connection:
        return self._connect(host_info, props, is_initial_connection, connect_func)

    def force_connect(self, target_driver_func: Callable, driver_dialect: DriverDialect, host_info: HostInfo, props: Properties,
                      is_initial_connection: bool, force_connect_func: Callable) -> Connection:
        return self._connect(host_info, props, is_initial_connection, force_connect_func)

    def _connect(self, host_info: HostInfo, props: Properties, is_initial_connection: bool, connect_func: Callable):
        type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)
        if not type.is_rds_cluster:
            return connect_func()

        if type == RdsUrlType.RDS_WRITER_CLUSTER:
            writer_candidate_conn: Optional[Connection] = self._get_verified_writer_connection(props, is_initial_connection, connect_func)
            if writer_candidate_conn is None:
                return connect_func()
            return writer_candidate_conn

        if type == RdsUrlType.RDS_READER_CLUSTER:
            reader_candidate_conn: Optional[Connection] = self._get_verified_reader_connection(props, is_initial_connection, connect_func)
            if reader_candidate_conn is None:
                return connect_func()
            return reader_candidate_conn

    def _get_verified_writer_connection(self, props: Properties, is_initial_connection: bool, connect_func: Callable) -> Connection | None:
        retry_delay_ms: int = WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props)
        end_time_nano = perf_counter_ns() + (WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props) * 1000000)

        writer_candidate_conn: Optional[Connection]
        writer_candidate: Optional[HostInfo]

        while perf_counter_ns() < end_time_nano:
            writer_candidate_conn = None
            writer_candidate = None

            try:
                writer_candidate = self._get_writer()
                if writer_candidate is None or self._rds_utils.is_rds_cluster_dns(writer_candidate.host):
                    # Writer is not found. Topology is outdated.
                    writer_candidate_conn = connect_func()
                    self._plugin_service.force_refresh_host_list(writer_candidate_conn)
                    writer_candidate = self._plugin_service.identify_connection(writer_candidate_conn)

                    if writer_candidate is not None and writer_candidate.role != HostRole.WRITER:
                        self._close_connection(writer_candidate_conn)
                        self._delay(retry_delay_ms)
                        continue

                    if is_initial_connection and self._host_list_provider_service is not None:
                        self._host_list_provider_service.initial_connection_host_info = writer_candidate

                    return writer_candidate_conn

                writer_candidate_conn = self._plugin_service.connect(writer_candidate, props)

                if self._plugin_service.get_host_role(writer_candidate_conn) != HostRole.WRITER:
                    self._plugin_service.force_refresh_host_list(writer_candidate_conn)
                    self._close_connection(writer_candidate_conn)
                    self._delay(retry_delay_ms)
                    continue

                # Writer connection is valid and verified.
                if is_initial_connection and self._host_list_provider_service is not None:
                    self._host_list_provider_service.initial_connection_host_info = writer_candidate
                return writer_candidate_conn
            except Exception as e:
                self._close_connection(writer_candidate_conn)
                raise e

        return None

    def _get_verified_reader_connection(self, props: Properties, is_initial_connection: bool, connect_func: Callable) -> Optional[Connection]:
        retry_delay_ms: int = WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props)
        end_time_nano = perf_counter_ns() + (WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props) * 1000000)

        reader_candidate_conn: Optional[Connection]
        reader_candidate: Optional[HostInfo]

        while perf_counter_ns() < end_time_nano:
            reader_candidate_conn = None
            reader_candidate = None

            try:
                reader_candidate = self._get_reader(props)
                if reader_candidate is None or self._rds_utils.is_rds_cluster_dns(reader_candidate.host):
                    # READER is not found. Topology is outdated.
                    reader_candidate_conn = connect_func()
                    self._plugin_service.force_refresh_host_list(reader_candidate_conn)
                    reader_candidate = self._plugin_service.identify_connection(reader_candidate_conn)

                    if reader_candidate is not None and reader_candidate.role != HostRole.READER:
                        if self._has_no_readers():
                            # Cluster has no readers. Simulate Aurora reader cluster endpoint logic and return the current writer connection.
                            if is_initial_connection and self._host_list_provider_service is not None:
                                self._host_list_provider_service.initial_connection_host_info = reader_candidate
                            return reader_candidate_conn

                        self._close_connection(reader_candidate_conn)
                        self._delay(retry_delay_ms)
                        continue

                    if is_initial_connection and self._host_list_provider_service is not None:
                        self._host_list_provider_service.initial_connection_host_info = reader_candidate
                    return reader_candidate_conn

                reader_candidate_conn = self._plugin_service.connect(reader_candidate, props)

                if self._plugin_service.get_host_role(reader_candidate_conn) != HostRole.READER:
                    # If the new connection resolves to a writer instance, the topology is outdated.
                    # Force refresh to update the topology.
                    self._plugin_service.force_refresh_host_list(reader_candidate_conn)

                    if self._has_no_readers():
                        # Cluster has no readers. Simulate Aurora reader cluster endpoint logic and return the current writer connection.
                        if is_initial_connection and self._host_list_provider_service is not None:
                            self._host_list_provider_service.initial_connection_host_info = reader_candidate
                        return reader_candidate_conn

                    self._close_connection(reader_candidate_conn)
                    self._delay(retry_delay_ms)
                    continue

                # Reader connection is valid and verified.
                if is_initial_connection and self._host_list_provider_service is not None:
                    self._host_list_provider_service.initial_connection_host_info = reader_candidate
                return reader_candidate_conn
            except Exception as e:
                self._close_connection(reader_candidate_conn)
                if not self._plugin_service.is_login_exception(e) and reader_candidate is not None:
                    self._plugin_service.set_availability(reader_candidate.as_aliases(), HostAvailability.UNAVAILABLE)

                raise e

        return None

    def _close_connection(self, connection: Optional[Connection]):
        if connection is not None:
            try:
                connection.close()
            except Exception:
                # ignore
                pass

    def _delay(self, delay_ms: int):
        sleep(delay_ms / 1000)

    def _get_writer(self) -> Optional[HostInfo]:
        for host in self._plugin_service.all_hosts:
            if host.role == HostRole.WRITER:
                return host

        return None

    def _get_reader(self, props: Properties) -> Optional[HostInfo]:
        strategy = WrapperProperties.READER_INITIAL_HOST_SELECTOR_STRATEGY.get(props)
        if (self._plugin_service is not None
                and strategy is not None
                and self._plugin_service.accepts_strategy(HostRole.READER, strategy)):
            try:
                return self._plugin_service.get_host_info_by_strategy(HostRole.READER, strategy)
            except Exception:
                # Host isn't found.
                return None

        raise AwsWrapperError(Messages.get_formatted("AuroraInitialConnectionStrategyPlugin.UnsupportedStrategy", strategy))

    def init_host_provider(self, props: Properties, host_list_provider_service: HostListProviderService, init_host_provider_func: Callable):
        self._host_list_provider_service = host_list_provider_service
        if host_list_provider_service.is_static_host_list_provider():
            raise AwsWrapperError(Messages.get("AuroraInitialConnectionStrategyPlugin.RequireDynamicProvider"))

        init_host_provider_func(props)

    def _has_no_readers(self) -> bool:
        if len(self._plugin_service.all_hosts) == 0:
            return False

        for host in self._plugin_service.all_hosts:
            if host.role == HostRole.READER:
                return False

        return True


class AuroraInitialConnectionStrategyPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraInitialConnectionStrategyPlugin(plugin_service)
