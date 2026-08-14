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

from typing import TYPE_CHECKING, Callable, List, Optional, Sequence, Tuple

from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.monitoring_connection_handler import \
    AbstractMonitoringConnectionHandler
from aws_advanced_python_wrapper.utils.gdb_monitoring_connection_priority import \
    GdbMonitoringConnectionPriority
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.monitoring_connection_priority import \
    MonitoringConnectionPriority
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.host_list_provider import TopologyUtils
    from aws_advanced_python_wrapper.hostinfo import Topology
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.atomic import AtomicReference
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
        ThreadSafeConnectionHolder

logger = Logger(__name__)


class AuroraMonitoringConnectionHandler(
        AbstractMonitoringConnectionHandler[MonitoringConnectionPriority]):
    """Standard Aurora monitoring connection handler.

    Uses :class:`MonitoringConnectionPriority` (role-only preferences) read from
    the ``monitoring_connection_priority`` property.
    """

    def __init__(
            self,
            monitoring_connection: ThreadSafeConnectionHolder,
            plugin_service: PluginService,
            topology_utils: TopologyUtils,
            props: Properties,
            monitoring_properties: Properties,
            upgrade_ready_notifier: Optional[Callable[[], None]] = None):
        priorities = MonitoringConnectionPriority.parse_list(
            WrapperProperties.MONITORING_CONNECTION_PRIORITY.get(props))
        super().__init__(
            monitoring_connection, plugin_service, topology_utils,
            monitoring_properties, priorities, upgrade_ready_notifier)

        writer_idx = -1
        reader_idx = -1
        for i, priority in enumerate(self._priorities):
            if writer_idx < 0 and priority.is_satisfied_by(True):
                writer_idx = i
            if reader_idx < 0 and priority.is_satisfied_by(False):
                reader_idx = i
        self._writer_priority_index = writer_idx
        self._reader_priority_index = reader_idx

    def _get_priority_index(self, host: HostInfo, is_writer: bool) -> int:
        return self._writer_priority_index if is_writer else self._reader_priority_index

    def _find_hosts_for_priority(self, priority_index: int, hosts: Sequence[HostInfo]) -> List[HostInfo]:
        priority = self._priorities[priority_index]
        if priority is MonitoringConnectionPriority.STRICT_WRITER:
            return [h for h in hosts if h.role == HostRole.WRITER]
        if priority is MonitoringConnectionPriority.STRICT_READER:
            return [h for h in hosts if h.role == HostRole.READER]
        if priority is MonitoringConnectionPriority.WRITER_OR_READER:
            return list(hosts)
        return []

    def _get_upgrade_thread_name(self) -> str:
        return "atmu"


class GdbMonitoringConnectionHandler(
        AbstractMonitoringConnectionHandler[GdbMonitoringConnectionPriority]):
    """Global Aurora Database monitoring connection handler.

    Uses :class:`GdbMonitoringConnectionPriority` with region and
    primary/secondary awareness, read from the
    ``gdb_monitoring_connection_priority`` property. The cluster's primary region
    is derived on demand from the current writer host.
    """

    def __init__(
            self,
            monitoring_connection: ThreadSafeConnectionHolder,
            plugin_service: PluginService,
            topology_utils: TopologyUtils,
            props: Properties,
            monitoring_properties: Properties,
            writer_host_info: AtomicReference[Optional[HostInfo]],
            upgrade_ready_notifier: Optional[Callable[[], None]] = None):
        priorities = GdbMonitoringConnectionPriority.parse_list(
            WrapperProperties.GDB_MONITORING_CONNECTION_PRIORITY.get(props))
        super().__init__(
            monitoring_connection, plugin_service, topology_utils,
            monitoring_properties, priorities, upgrade_ready_notifier)
        self._rds_utils = RdsUtils()
        self._writer_host_info = writer_host_info

    def accept_connections(
            self,
            connections: Sequence[Tuple[HostInfo, ThreadSafeConnectionHolder]],
            writer_host_info: Optional[HostInfo],
            topology: Optional[Topology]) -> Optional[HostInfo]:
        with self._lock:
            if not connections:
                return None

            # The primary region is seeded from the just-detected writer (if any),
            # else falls back to the cached writer.
            if writer_host_info is not None:
                primary_region: Optional[str] = self._rds_utils.get_rds_region(writer_host_info.host)
            else:
                primary_region = self._get_primary_region()

            best: Optional[Tuple[HostInfo, ThreadSafeConnectionHolder]] = None
            best_index = self._effective_index(-1)
            for host, holder in connections:
                if holder is None or holder.get() is None:
                    continue
                is_writer = (writer_host_info is not None
                             and self._host_and_port(writer_host_info) == self._host_and_port(host))
                effective_index = self._effective_index(
                    self._determine_priority_index(host, is_writer, primary_region))
                if best is None or effective_index < best_index:
                    best_index = effective_index
                    best = (host, holder)

            if best is None:
                return None

            best_host, best_holder = best
            best_conn = best_holder.get_and_set(None, close_previous=False)
            self._monitoring_connection.set(best_conn, close_previous=True)
            self._current_priority_index = best_index
            logger.debug("MonitoringConnectionHandler.ConnectionAccepted",
                         best_host.host, best_host.role,
                         self._format_priority_index(best_index))
            return best_host

    def _get_priority_index(self, host: HostInfo, is_writer: bool) -> int:
        return self._determine_priority_index(host, is_writer, self._get_primary_region())

    def _find_hosts_for_priority(self, priority_index: int, hosts: Sequence[HostInfo]) -> List[HostInfo]:
        priority = self._priorities[priority_index]
        return priority.find_matching_hosts(list(hosts), self._get_primary_region(), self._rds_utils)

    def _get_upgrade_thread_name(self) -> str:
        return "gatmu"

    def _get_primary_region(self) -> Optional[str]:
        writer = self._writer_host_info.get()
        if writer is not None:
            return self._rds_utils.get_rds_region(writer.host)
        return None

    def _determine_priority_index(
            self, host: HostInfo, is_writer: bool, primary_region: Optional[str]) -> int:
        effective_host = self._with_role(host, HostRole.WRITER if is_writer else HostRole.READER)
        for i, priority in enumerate(self._priorities):
            if priority.is_satisfied_by(effective_host, primary_region, self._rds_utils):
                return i
        return -1

    @staticmethod
    def _with_role(host: HostInfo, role: HostRole) -> HostInfo:
        if host.role == role:
            return host
        return HostInfo(
            host=host.host,
            port=host.port,
            role=role,
            availability=host.availability,
            weight=host.weight,
            host_id=host.host_id,
            last_update_time=host.last_update_time)
