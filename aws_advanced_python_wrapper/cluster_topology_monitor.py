#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#  express or implied. See the License for the specific language governing
#  permissions and limitations under the License.

from __future__ import annotations

import random
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter_ns
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Tuple

from aws_advanced_python_wrapper.concrete_monitoring_connection_handlers import (
    AuroraMonitoringConnectionHandler, GdbMonitoringConnectionHandler)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, Topology
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.accessible_regions import \
    AccessibleRegions
from aws_advanced_python_wrapper.utils.atomic import AtomicReference
from aws_advanced_python_wrapper.utils.decorators import \
    is_connection_abandoned
from aws_advanced_python_wrapper.utils.events import (EventBase,
                                                      MonitorResetEvent)
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
    ThreadSafeConnectionHolder
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.monitoring_connection_handler import \
        MonitoringConnectionHandler
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.host_list_provider import TopologyUtils, GlobalAuroraTopologyUtils

from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (PropertiesUtils,
                                                          WrapperProperties)

logger = Logger(__name__)


class ClusterTopologyMonitor(ABC):
    @abstractmethod
    def force_refresh(self, should_verify_writer: bool, timeout_sec: float) -> Topology:
        pass

    @abstractmethod
    def force_refresh_with_connection(self, connection: Connection, timeout_sec: float) -> Topology:
        pass

    @property
    @abstractmethod
    def can_dispose(self) -> bool:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

    @property
    @abstractmethod
    def last_activity_ns(self) -> int:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class ClusterTopologyMonitorImpl(ClusterTopologyMonitor):
    MONITOR_TERMINATION_TIMEOUT_SEC = 30
    CLOSE_CONNECTION_NETWORK_TIMEOUT_MS = 500
    DEFAULT_CONNECT_TIMEOUT_SEC = 5
    DEFAULT_SOCKET_TIMEOUT_SEC = 5
    TOPOLOGY_CACHE_EXPIRATION_NANO = 5 * 60 * 1_000_000_000  # 5 minutes in nanoseconds

    HIGH_REFRESH_PERIOD_AFTER_PANIC_NANO = 30 * 1_000_000_000  # 30 seconds in nanoseconds
    IGNORE_TOPOLOGY_REQUEST_NANO = 10 * 1_000_000_000  # 10 seconds in nanoseconds

    INITIAL_BACKOFF_MS = 100
    MAX_BACKOFF_MS = 10000

    def __init__(self, plugin_service: PluginService, topology_utils: TopologyUtils, cluster_id: str,
                 initial_host_info: HostInfo, properties: Properties, instance_template: HostInfo,
                 refresh_rate_nano: int, high_refresh_rate_nano: int):
        self._plugin_service = plugin_service
        self._topology_utils = topology_utils
        self._cluster_id = cluster_id
        self._initial_host_info: HostInfo = initial_host_info
        self._properties = properties
        self._instance_template = instance_template
        self._refresh_rate_nano = refresh_rate_nano
        self._high_refresh_rate_nano = high_refresh_rate_nano

        self._rds_utils = RdsUtils()
        self._writer_host_info: AtomicReference[Optional[HostInfo]] = AtomicReference(None)
        self._monitoring_connection: ThreadSafeConnectionHolder = ThreadSafeConnectionHolder(None)

        self._topology_updated = threading.Event()
        self._request_to_update_topology = threading.Event()
        self._ignore_new_topology_requests_end_time_nano = -1
        self._submitted_hosts: Dict[str, bool] = {}

        self._thread_pool_executor: AtomicReference[Optional[ThreadPoolExecutor]] = AtomicReference(None)
        self._host_threads_stop = threading.Event()
        self._host_threads_writer_connection: AtomicReference[Optional[Connection]] = AtomicReference(None)
        self._host_threads_writer_host_info: AtomicReference[Optional[HostInfo]] = AtomicReference(None)
        self._host_threads_reader_connection: AtomicReference[Optional[Connection]] = AtomicReference(None)
        self._host_threads_latest_topology: AtomicReference[Optional[Topology]] = AtomicReference(None)

        self._is_verified_writer_connection = False
        # Retained even after _writer_host_info is cleared; host threads use it
        # as the baseline (last known writer) for writer-change detection.
        self._last_known_writer_host_info: AtomicReference[Optional[HostInfo]] = AtomicReference(None)

        self._host_threads_connections: Dict[str, Tuple[HostInfo, ThreadSafeConnectionHolder]] = {}
        self._host_threads_map_lock = threading.Lock()
        self._reader_topologies_by_id: Dict[str, Topology] = {}
        self._completed_one_cycle: Dict[str, bool] = {}
        self._stable_topologies_start_nano = 0
        self._reader_observed_writer_host_info: AtomicReference[Optional[HostInfo]] = AtomicReference(None)

        self._high_refresh_rate_end_time_nano = 0
        self._stop = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._last_activity_ns: int = perf_counter_ns()

        self._monitoring_properties = PropertiesUtils.create_topology_monitoring_properties(properties)
        if WrapperProperties.SOCKET_TIMEOUT_SEC.get(self._monitoring_properties) is None:
            WrapperProperties.SOCKET_TIMEOUT_SEC.set(self._monitoring_properties, self.DEFAULT_SOCKET_TIMEOUT_SEC)
        if WrapperProperties.CONNECT_TIMEOUT_SEC.get(self._monitoring_properties) is None:
            WrapperProperties.CONNECT_TIMEOUT_SEC.set(self._monitoring_properties, self.DEFAULT_CONNECT_TIMEOUT_SEC)

        # Handler that manages the priority of the background monitoring
        # connection and asynchronously upgrades it to a higher-priority host.
        self._connection_handler: MonitoringConnectionHandler = self._create_connection_handler()

        self._start_monitoring()

    STABLE_TOPOLOGY_DURATION_NANO = 15 * 1_000_000_000  # 15 seconds in nanoseconds

    def get_stable_topologies_duration_ns(self) -> int:
        return ClusterTopologyMonitorImpl.STABLE_TOPOLOGY_DURATION_NANO

    def _create_connection_handler(self) -> MonitoringConnectionHandler:
        return AuroraMonitoringConnectionHandler(
            self._monitoring_connection,
            self._plugin_service,
            self._topology_utils,
            self._properties,
            self._monitoring_properties,
            self.wake_up_monitoring_loop)

    def wake_up_monitoring_loop(self) -> None:
        """Notify the main monitoring loop (e.g. when an async upgrade completes)."""
        self._request_to_update_topology.set()

    def force_refresh(self, should_verify_writer: bool, timeout_sec: float) -> Topology:
        current_time_nano = time.time_ns()
        if (self._ignore_new_topology_requests_end_time_nano > 0 and
                current_time_nano < self._ignore_new_topology_requests_end_time_nano):
            current_hosts = self._get_stored_hosts()
            if current_hosts is not None:
                logger.debug("ClusterTopologyMonitor.IgnoringTopologyRequest", self._cluster_id, LogUtils.log_topology(current_hosts))
                return current_hosts

        if should_verify_writer:
            self._monitoring_connection.clear()
            self._is_verified_writer_connection = False

        result = self._wait_till_topology_gets_updated(timeout_sec)
        return result

    def force_refresh_with_connection(self, connection: Connection, timeout_sec: float) -> Topology:
        if self._is_verified_writer_connection:
            return self._wait_till_topology_gets_updated(timeout_sec)
        return self._fetch_topology_and_update_cache(connection)

    def _wait_till_topology_gets_updated(self, timeout_sec: float) -> Topology:
        current_hosts = self._get_stored_hosts()

        self._request_to_update_topology.set()

        if timeout_sec == 0:
            logger.debug("ClusterTopologyMonitor.TimeoutSetToZero", self._cluster_id, LogUtils.log_topology(current_hosts))
            return current_hosts

        end_time = time.time() + timeout_sec
        while time.time() < end_time:
            latest_hosts = self._get_stored_hosts()
            if latest_hosts is not current_hosts:
                return latest_hosts

            if self._topology_updated.wait(1.0):
                self._topology_updated.clear()

        raise TimeoutError(
                    Messages.get_formatted(
                        "ClusterTopologyMonitor.TopologyNotUpdated",
                        self._cluster_id, timeout_sec * 1000))

    def _get_stored_hosts(self) -> Topology:
        hosts = services_container.get_storage_service().get(Topology, self._cluster_id)
        if hosts is None:
            return ()
        return hosts

    def stop(self) -> None:
        self._stop.set()
        self.close()

    @property
    def can_dispose(self) -> bool:
        return self._stop.is_set()

    @property
    def last_activity_ns(self) -> int:
        return self._last_activity_ns

    def process_event(self, event: EventBase) -> None:
        if isinstance(event, MonitorResetEvent) and event.cluster_id == self._cluster_id:
            logger.debug("ClusterTopologyMonitor.ResetEventReceived", self._cluster_id)
            self._host_threads_stop.set()
            self._close_host_monitors()
            self._close_connection_from_ref(self._host_threads_writer_connection)
            self._close_connection_from_ref(self._host_threads_reader_connection)
            self._host_threads_stop.clear()
            self._submitted_hosts.clear()
            self._host_threads_writer_host_info.set(None)
            self._host_threads_latest_topology.set(None)
            self._connection_handler.close()
            self._monitoring_connection.clear()
            self._is_verified_writer_connection = False
            self._writer_host_info.set(None)
            self._last_known_writer_host_info.set(None)
            self._high_refresh_rate_end_time_nano = 0

    def close(self) -> None:
        logger.debug("ClusterTopologyMonitor.ClosingMonitor", self._cluster_id)
        self._request_to_update_topology.set()

        self._close_host_monitors()

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(self.MONITOR_TERMINATION_TIMEOUT_SEC)

        # Step 3: Now safe to close connections - no threads are using them.
        # Close the handler first so any in-flight async upgrade thread is
        # cancelled and its connection released before we clear the rest.
        self._connection_handler.close()
        self._monitoring_connection.clear()
        self._close_connection_from_ref(self._host_threads_writer_connection)
        self._close_connection_from_ref(self._host_threads_reader_connection)

    def _start_monitoring(self) -> None:
        self._monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self._monitor_thread.start()

    def _monitor(self) -> None:
        try:
            logger.debug("ClusterTopologyMonitor.StartMonitoringThread", self._cluster_id, self._initial_host_info.host)

            while not self._stop.is_set():
                self._last_activity_ns = perf_counter_ns()
                if self._is_in_panic_mode():
                    if not self._submitted_hosts:
                        self._close_host_monitors()
                        self._host_threads_stop.clear()
                        self._host_threads_writer_host_info.set(None)
                        self._host_threads_latest_topology.set(None)

                        hosts = self._get_stored_hosts()
                        if not hosts:
                            hosts = self._open_any_connection_and_update_topology()

                        if hosts and not self._is_verified_writer_connection:
                            logger.debug("ClusterTopologyMonitor.StartingHostMonitoringThreads", self._cluster_id)
                            self._reader_observed_writer_host_info.set(None)
                            monitored_hosts = self._filter_hosts_for_host_monitoring(hosts)
                            # some_regions_inaccessible is true when monitoring
                            # filtering dropped a host, i.e. only for GDB with an
                            # accessible-regions restriction that actually applied.
                            some_regions_inaccessible = len(monitored_hosts) < len(hosts)
                            # Baseline writer for reader-observed writer-change
                            # detection: the last writer we believed in, retained
                            # even after _writer_host_info was cleared.
                            baseline_writer = self._last_known_writer_host_info.get()
                            for host_info in monitored_hosts:
                                if host_info.host not in self._submitted_hosts:
                                    try:
                                        worker = self._get_host_monitor(
                                            host_info, baseline_writer, some_regions_inaccessible)
                                        self._get_host_executor_service().submit(worker)
                                        self._submitted_hosts[host_info.host] = True
                                    except Exception as e:
                                        logger.debug(
                                            "ClusterTopologyMonitor.ExceptionStartingHostMonitor",
                                            self._cluster_id, host_info.host, e)
                    else:
                        # Check if writer has been detected
                        writer_host_info = self._host_threads_writer_host_info.get()
                        writer_connection = self._host_threads_writer_connection.get()
                        if (writer_connection is not None and writer_host_info is not None):
                            logger.debug("ClusterTopologyMonitor.WriterPickedUpFromHostMonitors", self._cluster_id, writer_host_info.host)
                            # Offer the writer connection to the handler, which
                            # sets it as the monitoring connection and seeds its
                            # priority index. In panic mode the monitoring
                            # connection is None, so the handler always accepts.
                            self._connection_handler.accept_connection(writer_connection, True, writer_host_info)
                            self._writer_host_info.set(writer_host_info)
                            self._last_known_writer_host_info.set(writer_host_info)
                            self._is_verified_writer_connection = True
                            self._high_refresh_rate_end_time_nano = (
                                time.time_ns() + self.HIGH_REFRESH_PERIOD_AFTER_PANIC_NANO)

                            if self._ignore_new_topology_requests_end_time_nano == -1:
                                self._ignore_new_topology_requests_end_time_nano = 0
                            else:
                                self._ignore_new_topology_requests_end_time_nano = (
                                    time.time_ns() + self.IGNORE_TOPOLOGY_REQUEST_NANO)

                            self._host_threads_stop.set()
                            self._close_host_monitors()
                            self._submitted_hosts.clear()
                            continue

                        # A reader worker observed a writer change while
                        # some regions are inaccessible. No host thread may be
                        # able to reach the new writer to verify it directly, so
                        # exit panic mode by harvesting the reader connections the
                        # workers already opened.
                        reader_observed_writer = self._reader_observed_writer_host_info.get()
                        if reader_observed_writer is not None:
                            logger.debug("ClusterTopologyMonitor.WriterChangeObservedByReader",
                                         self._cluster_id, reader_observed_writer.host)
                            if self._adopt_harvested_monitoring_connection(reader_observed_writer, self._get_stored_hosts()):
                                self._writer_host_info.set(reader_observed_writer)
                                self._last_known_writer_host_info.set(reader_observed_writer)
                                self._is_verified_writer_connection = True
                                self._high_refresh_rate_end_time_nano = (
                                    time.time_ns() + self.HIGH_REFRESH_PERIOD_AFTER_PANIC_NANO)
                                if self._ignore_new_topology_requests_end_time_nano == -1:
                                    self._ignore_new_topology_requests_end_time_nano = 0
                                else:
                                    self._ignore_new_topology_requests_end_time_nano = (
                                        time.time_ns() + self.IGNORE_TOPOLOGY_REQUEST_NANO)
                                continue

                        # Update host monitors with new topology
                        host_threads_topology = self._host_threads_latest_topology.get()
                        if host_threads_topology is not None and not self._host_threads_stop.is_set():
                            monitored_hosts = self._filter_hosts_for_host_monitoring(host_threads_topology)
                            some_regions_inaccessible = len(monitored_hosts) < len(host_threads_topology)
                            baseline_writer = self._last_known_writer_host_info.get()
                            for host_info in monitored_hosts:
                                if host_info.host not in self._submitted_hosts:
                                    try:
                                        worker = self._get_host_monitor(
                                            host_info, baseline_writer, some_regions_inaccessible)
                                        self._get_host_executor_service().submit(worker)
                                        self._submitted_hosts[host_info.host] = True
                                    except Exception as e:
                                        logger.debug(
                                            "ClusterTopologyMonitor.ExceptionStartingHostMonitor",
                                            self._cluster_id, host_info.host, e)

                        # If host threads never verified a writer (e.g. it
                        # lives in an inaccessible region) but the readers we did
                        # probe agree on a stable topology, harvest their
                        # connections to exit panic mode.
                        self._check_for_stable_reader_topologies()

                    self._delay(True)
                else:
                    # Regular mode
                    if self._submitted_hosts:
                        self._close_host_monitors()
                        self._submitted_hosts.clear()

                    hosts = self._fetch_topology_and_update_cache_safe()
                    if not hosts:
                        self._monitoring_connection.clear()
                        self._is_verified_writer_connection = False
                        self._writer_host_info.set(None)
                        continue

                    # Refresh the retained writer baseline from the freshly
                    # fetched topology so that, if the monitoring connection later
                    # breaks, panic-mode host threads have an accurate baseline for
                    # writer-change detection. Unlike _writer_host_info, this
                    # baseline is never cleared on fetch failure.
                    topology_writer = next(
                        (h for h in hosts if h.role == HostRole.WRITER), None)
                    if topology_writer is not None:
                        self._last_known_writer_host_info.set(topology_writer)

                    # Non-blocking: check for a completed async upgrade and, if
                    # the current connection is not the highest priority, kick off
                    # a new upgrade attempt for a higher-priority host. Filter the
                    # candidates so upgrades never target hosts in inaccessible
                    # regions when gdb_accessible_regions is set (base override is
                    # a no-op, so this is a pass-through for non-GDB monitors).
                    self._connection_handler.attempt_connection_upgrade(
                        self._filter_hosts_for_host_monitoring(hosts))

                    current_time_nano = time.time_ns()
                    if (self._high_refresh_rate_end_time_nano > 0 and
                            current_time_nano > self._high_refresh_rate_end_time_nano):
                        self._high_refresh_rate_end_time_nano = 0

                    self._delay(False)

                if (self._ignore_new_topology_requests_end_time_nano > 0 and
                        time.time_ns() > self._ignore_new_topology_requests_end_time_nano):
                    self._ignore_new_topology_requests_end_time_nano = 0

        except Exception as ex:
            logger.info("ClusterTopologyMonitor.ExceptionDuringMonitoringStop", self._cluster_id, ex)
        finally:
            self._stop.set()
            self._close_host_monitors()
            self._monitoring_connection.clear()
            logger.debug("ClusterTopologyMonitor.StopMonitoringThread", self._cluster_id, self._initial_host_info.host)

    def _is_in_panic_mode(self) -> bool:
        return self._monitoring_connection.get() is None or not self._is_verified_writer_connection

    def _get_host_monitor(self, host_info: HostInfo, writer_host_info: Optional[HostInfo],
                          some_regions_inaccessible: bool = False):
        return HostMonitor(self, host_info, writer_host_info, some_regions_inaccessible)

    def _filter_hosts_for_host_monitoring(self, hosts: Topology) -> Topology:
        return hosts

    def _adopt_harvested_monitoring_connection(
            self, writer_host_info: Optional[HostInfo], topology: Topology) -> bool:
        """Stop host monitors, then offer every connection they harvested into
        ``_host_threads_connections`` to the handler, which adopts the best one
        as the monitoring connection.

        Ordering is critical for process safety: ``_shutdown_host_executor()``
        joins the executor (``shutdown(wait=True)``) so no worker thread is still
        touching a harvested connection before we hand any off — this preserves
        the invariant behind the documented psycopg use-after-free fix. We join
        WITHOUT ``_close_host_monitors`` because that would empty the harvest map
        the workers just populated. Returns ``True`` when the handler adopted a
        connection (panic mode can exit).
        """
        # Join all workers first; their finally-blocks populate the map.
        self._shutdown_host_executor()
        self._submitted_hosts.clear()

        with self._host_threads_map_lock:
            connections: List[Tuple[HostInfo, ThreadSafeConnectionHolder]] = \
                list(self._host_threads_connections.values())

        if not connections:
            self._clear_host_threads_state()
            return False

        selected = self._connection_handler.accept_connections(
            connections, writer_host_info, topology)

        # Close every harvested connection the handler did not adopt.
        selected_key = self._host_and_port(selected) if selected is not None else None
        with self._host_threads_map_lock:
            for key, (_, holder) in self._host_threads_connections.items():
                if selected_key is None or key != selected_key:
                    holder.clear()
            self._host_threads_connections.clear()
            self._reader_topologies_by_id.clear()
            self._completed_one_cycle.clear()
            self._stable_topologies_start_nano = 0
        self._reader_observed_writer_host_info.set(None)
        return selected is not None

    def _check_for_stable_reader_topologies(self) -> None:
        """When host threads never verified a writer (e.g. it lives in an
        inaccessible region) but every reader we probed agrees on the same
        topology for ``get_stable_topologies_duration_ns()``, harvest the reader
        connections and exit panic mode.
        """
        latest_hosts = self._get_stored_hosts()
        if not latest_hosts:
            self._stable_topologies_start_nano = 0
            return

        # Only require completion from hosts we actually monitor; a subclass may
        # filter the topology (GDB drops inaccessible regions), and those hosts
        # would otherwise appear perpetually incomplete.
        monitored_ids = [
            self._host_and_port(h) for h in self._filter_hosts_for_host_monitoring(latest_hosts)]

        with self._host_threads_map_lock:
            for host_id in monitored_ids:
                if not self._completed_one_cycle.get(host_id, False):
                    # Not every monitored reader has attempted a cycle yet.
                    self._stable_topologies_start_nano = 0
                    return

            reader_topologies = list(self._reader_topologies_by_id.values())
            if not reader_topologies:
                self._stable_topologies_start_nano = 0
                return

            reader_topology = reader_topologies[0]
            # Do the reader-observed topologies all agree? Compare on
            # (host, port, availability, role) — weight is excluded.
            signatures = {self._topology_signature(t) for t in reader_topologies}
            if len(signatures) != 1:
                self._stable_topologies_start_nano = 0
                return

            if self._stable_topologies_start_nano == 0:
                self._stable_topologies_start_nano = time.time_ns()
            stable_since = self._stable_topologies_start_nano

        if time.time_ns() <= stable_since + self.get_stable_topologies_duration_ns():
            return

        # Reader topologies have been consistent long enough; treat them as
        # accurate and try to adopt one of the reader connections.
        with self._host_threads_map_lock:
            self._stable_topologies_start_nano = 0
        self._update_topology_cache(reader_topology)

        if self._monitoring_connection.get() is not None:
            return

        logger.debug("ClusterTopologyMonitor.StableReaderTopologiesExit", self._cluster_id)
        # Adopt with the live writer (typically None here — the writer is in an
        # inaccessible region) and the agreed reader topology.
        # _adopt_harvested_monitoring_connection joins all workers first (their
        # finally-blocks populate the connection map), so we intentionally do NOT
        # pre-check the map for emptiness here: while workers are alive the map is
        # empty by design, and the join is what fills it.
        if self._adopt_harvested_monitoring_connection(self._writer_host_info.get(), reader_topology):
            self._is_verified_writer_connection = True

    @staticmethod
    def _topology_signature(topology: Topology):
        return tuple(
            (h.host, h.port, h.availability, h.role) for h in topology)

    def _harvest_connection(self, host_info: HostInfo, connection: Connection) -> None:
        """Move ownership of a worker's live connection into the harvest map via
        a fresh ThreadSafeConnectionHolder. A pre-existing entry for the same
        host is closed to avoid leaks."""
        key = self._host_and_port(host_info)
        holder = ThreadSafeConnectionHolder(connection)
        with self._host_threads_map_lock:
            previous = self._host_threads_connections.get(key)
            self._host_threads_connections[key] = (host_info, holder)
        if previous is not None:
            previous[1].clear()

    def _mark_cycle_completed(self, host_info: HostInfo) -> None:
        with self._host_threads_map_lock:
            self._completed_one_cycle[self._host_and_port(host_info)] = True

    def _record_reader_topology(self, host_info: HostInfo, topology: Topology) -> None:
        with self._host_threads_map_lock:
            self._reader_topologies_by_id[self._host_and_port(host_info)] = topology

    def _clear_host_threads_state(self) -> None:
        with self._host_threads_map_lock:
            self._host_threads_connections.clear()
            self._reader_topologies_by_id.clear()
            self._completed_one_cycle.clear()
            self._stable_topologies_start_nano = 0
        self._reader_observed_writer_host_info.set(None)

    def _open_any_connection_and_update_topology(self) -> Topology:
        writer_verified_by_this_thread = False
        if self._monitoring_connection.get() is None:
            # Try to connect to the initial host first
            try:
                conn = self._plugin_service.force_connect(self._initial_host_info, self._monitoring_properties)
                self._monitoring_connection.set(conn, close_previous=False)
                logger.debug("ClusterTopologyMonitor.OpenedMonitoringConnection",
                             self._cluster_id, self._initial_host_info.host)

                try:
                    writer_id = self._topology_utils.get_writer_id_if_connected(
                        conn, self._plugin_service.driver_dialect)
                    if writer_id:
                        self._is_verified_writer_connection = True
                        writer_verified_by_this_thread = True

                        if self._rds_utils.is_rds_instance(self._initial_host_info.host):
                            writer_host_info = self._initial_host_info
                            self._writer_host_info.set(writer_host_info)
                        else:
                            instance_template = self._get_instance_template(writer_id, conn)
                            writer_host = instance_template.host.replace("?", writer_id)
                            port = instance_template.port \
                                if instance_template.is_port_specified() \
                                else self._initial_host_info.port
                            writer_host_info = HostInfo(
                                writer_host,
                                port,
                                HostRole.WRITER,
                                HostAvailability.AVAILABLE,
                                host_id=writer_id)
                            self._writer_host_info.set(writer_host_info)

                        self._last_known_writer_host_info.set(writer_host_info)
                        # Seed the handler's priority index with this writer
                        # connection so a later async upgrade is evaluated
                        # against the correct baseline.
                        self._connection_handler.accept_connection(conn, True, writer_host_info)

                        logger.debug("ClusterTopologyMonitor.WriterMonitoringConnection",
                                     self._cluster_id, writer_host_info.host)
                except Exception:
                    pass
            except Exception:
                return ()

        hosts = self._fetch_topology_and_update_cache_safe()
        if writer_verified_by_this_thread:
            if self._ignore_new_topology_requests_end_time_nano == -1:
                self._ignore_new_topology_requests_end_time_nano = 0
            else:
                self._ignore_new_topology_requests_end_time_nano = (
                        time.time_ns() + self.IGNORE_TOPOLOGY_REQUEST_NANO)

        if len(hosts) == 0:
            self._monitoring_connection.clear()
            self._is_verified_writer_connection = False
            self._writer_host_info.set(None)

        return hosts

    def _close_connection(self, connection: Optional[Connection]) -> None:
        try:
            # Skip connections an auxiliary-query worker could not be drained off
            # (is_connection_abandoned): closing one while the worker is still
            # using it is a cross-thread use-after-free that crashes the process.
            # The worker holds the last reference and frees it safely on its own
            # thread once it finishes.
            if connection is not None and not is_connection_abandoned(connection):
                connection.close()
        except Exception:
            pass

    def _close_connection_from_ref(self, connection: AtomicReference[Optional[Connection]]) -> None:
        connection_to_close: Optional[Connection] = connection.get_and_set(None)
        self._close_connection(connection_to_close)

    @staticmethod
    def _host_and_port(host_info: HostInfo) -> str:
        return f"{host_info.host}:{host_info.port}"

    def _host_thread_connection_cleanup(self) -> None:
        writer_connection = self._host_threads_writer_connection.get_and_set(None)
        if self._monitoring_connection.get() != writer_connection:
            self._close_connection(writer_connection)

        reader_connection = self._host_threads_reader_connection.get_and_set(None)
        if self._monitoring_connection.get() != reader_connection:
            self._close_connection(reader_connection)

        self._clean_up_harvested_connections()

    def _clean_up_harvested_connections(self) -> None:
        """Close every harvested connection except the active monitoring
        one, then empty the harvest map."""
        current_monitoring = self._monitoring_connection.get()
        with self._host_threads_map_lock:
            entries = list(self._host_threads_connections.values())
            self._host_threads_connections.clear()
            self._reader_topologies_by_id.clear()
            self._completed_one_cycle.clear()
            self._stable_topologies_start_nano = 0
        for _, holder in entries:
            if current_monitoring is not None and holder.get() is current_monitoring:
                # Don't close the active monitoring connection; just detach it.
                holder.get_and_set(None, close_previous=False)
            else:
                holder.clear()
        self._reader_observed_writer_host_info.set(None)

    def _shutdown_host_executor(self) -> None:
        """Stop and join all host-monitoring workers WITHOUT touching the
        harvest map, so a subsequent harvest can read the connections the
        workers handed off in their finally-blocks."""
        self._host_threads_stop.set()
        thread_pool_executor = self._thread_pool_executor.get_and_set(None)
        if thread_pool_executor is not None:
            thread_pool_executor.shutdown(wait=True, cancel_futures=True)

    def _close_host_monitors(self) -> None:
        self._shutdown_host_executor()
        self._host_thread_connection_cleanup()

        self._submitted_hosts.clear()

    def _get_host_executor_service(self) -> ThreadPoolExecutor:
        if self._stop.is_set():
            raise RuntimeError(Messages.get_formatted(
                "ClusterTopologyMonitor.CannotCreateExecutorWhenStopped", self._cluster_id))
        thread_pool_executor = self._thread_pool_executor.get()
        if thread_pool_executor is None:
            thread_pool_executor = ThreadPoolExecutor(thread_name_prefix=self._cluster_id)
            self._thread_pool_executor.compare_and_set(None, thread_pool_executor)
        return thread_pool_executor

    def _delay(self, use_high_refresh_rate: bool) -> None:
        current_time_nano = time.time_ns()
        if (self._high_refresh_rate_end_time_nano > 0 and
                current_time_nano < self._high_refresh_rate_end_time_nano):
            use_high_refresh_rate = True

        if self._request_to_update_topology.is_set():
            use_high_refresh_rate = True

        refresh_rate = self._high_refresh_rate_nano if use_high_refresh_rate else self._refresh_rate_nano
        delay_sec = refresh_rate / 1_000_000_000.0

        start_time = time.time()
        end_time = start_time + delay_sec

        while not self._request_to_update_topology.is_set() and time.time() < end_time and not self._stop.is_set():
            time.sleep(0.05)

    def _fetch_topology_and_update_cache(self, connection: Optional[Connection]) -> Topology:
        if connection is None:
            return ()

        try:
            hosts = self._query_for_topology(connection)
            if hosts:
                self._update_topology_cache(hosts)
                return hosts
            return ()
        except Exception as ex:
            logger.debug("ClusterTopologyMonitor.ErrorFetchingTopology", self._cluster_id, ex)
            return ()

    def _fetch_topology_and_update_cache_safe(self) -> Topology:
        """
        Safely fetch topology using ThreadSafeConnectionHolder to prevent race conditions.
        The lock is held during the entire query operation.
        """
        result = self._monitoring_connection.use_connection(
            lambda conn: self._fetch_topology_and_update_cache(conn)
        )
        return result if result is not None else ()

    def _query_for_topology(self, connection: Connection) -> Topology:
        hosts = self._topology_utils.query_for_topology(connection, self._plugin_service.driver_dialect)
        if hosts is not None:
            return hosts
        return ()

    def _get_instance_template(self, instance_id: str, connection: Connection) -> HostInfo:
        return self._instance_template

    def _update_topology_cache(self, hosts: Topology) -> None:
        services_container.get_storage_service().put(Topology, self._cluster_id, hosts)
        # Notify waiting threads
        self._request_to_update_topology.clear()
        self._topology_updated.set()


class HostMonitor:
    def __init__(self, monitor: ClusterTopologyMonitorImpl, host_info: HostInfo,
                 writer_host_info: Optional[HostInfo],
                 some_regions_inaccessible: bool = False):
        self._monitor: ClusterTopologyMonitorImpl = monitor
        self._host_info = host_info
        # Per-worker baseline writer for reader-observed writer-change
        # detection, seeded from the monitor's last known writer.
        self._writer_host_info = writer_host_info
        # Snapshot of whether some regions were inaccessible when this worker was
        # created. When True the worker signals a panic-mode exit on an observed
        # writer change and hands its connection off to the monitor's
        # harvest map on shutdown.
        self._some_regions_inaccessible = some_regions_inaccessible
        self._writer_changed = False
        self._connection_attempts = 0

    def __call__(self) -> None:
        connection = None
        update_topology = False
        start_time = time.time()
        handed_off = False

        try:
            while not self._monitor._host_threads_stop.is_set():
                if self._monitor._host_threads_stop.is_set():
                    return

                if connection is None:
                    try:
                        connection = self._monitor._plugin_service.force_connect(
                            self._host_info, self._monitor._monitoring_properties)
                        self._connection_attempts = 0
                    except Exception as ex:
                        if self._monitor._host_threads_stop.is_set():
                            return

                        if self._monitor._plugin_service.is_network_exception(ex):
                            time.sleep(0.1)
                            continue
                        elif self._monitor._plugin_service.is_login_exception(ex):
                            raise RuntimeError(ex)
                        else:
                            backoff = self._calculate_backoff_with_jitter(self._connection_attempts)
                            self._connection_attempts += 1
                            time.sleep(backoff / 1000.0)
                            continue

                if self._monitor._host_threads_stop.is_set():
                    return

                if connection is not None:
                    is_writer = False
                    try:
                        is_writer = self._monitor._topology_utils.get_writer_id_if_connected(
                            connection, self._monitor._plugin_service.driver_dialect) is not None
                    except Exception:
                        self._monitor._close_connection(connection)
                        connection = None
                        continue

                    if is_writer:
                        try:
                            if self._monitor._plugin_service.get_host_role(
                                    connection) != HostRole.WRITER:
                                is_writer = False
                        except Exception as ex:
                            logger.debug("HostMonitor.InvalidWriterQuery", ex)
                            continue

                    if is_writer:
                        if self._monitor._host_threads_writer_connection.compare_and_set(None, connection):
                            self._monitor._fetch_topology_and_update_cache(connection)
                            self._monitor._host_threads_writer_host_info.set(self._host_info)
                            logger.debug("HostMonitor.DetectedWriter", self._host_info.host)
                            self._monitor._host_threads_stop.set()
                            connection = None  # Prevent cleanup
                            return
                        else:
                            self._monitor._close_connection(connection)
                            connection = None
                            return
                    elif connection is not None:
                        # Reader connection
                        if self._monitor._host_threads_writer_connection.get() is None:
                            if update_topology:
                                self._reader_thread_fetch_topology(connection)
                            elif self._monitor._host_threads_reader_connection.compare_and_set(None, connection):
                                update_topology = True
                                self._reader_thread_fetch_topology(connection)

                # This worker has attempted at least one full cycle. Mark it so
                # the main loop's stable-reader-topology check does not
                # conclude stability before every monitored reader has tried.
                if self._some_regions_inaccessible:
                    self._monitor._mark_cycle_completed(self._host_info)

                time.sleep(0.1)

        except Exception as ex:
            logger.debug("HostMonitor.Exception", self._host_info.host, ex)
        finally:
            # When some regions are inaccessible, hand any live
            # connection off to the monitor's harvest map instead of closing it,
            # so the main loop can adopt it to exit panic mode. Ownership moves
            # to a fresh holder; `handed_off` prevents the trailing close from
            # touching it. This runs
            # only after the worker has stopped using the connection, and the main
            # loop only reads the map after joining all workers, so there is no
            # cross-thread use of a connection being closed.
            if self._some_regions_inaccessible:
                self._monitor._mark_cycle_completed(self._host_info)
                if connection is not None and not self._monitor._stop.is_set():
                    self._monitor._harvest_connection(self._host_info, connection)
                    handed_off = True
            if not handed_off:
                self._monitor._close_connection(connection)
            elapsed_time = (time.time() - start_time) * 1000
            logger.debug("HostMonitor.MonitorCompleted", self._host_info.host, elapsed_time)

    def _reader_thread_fetch_topology(self, connection: Connection) -> None:
        if connection is None:
            return

        try:
            hosts = self._monitor._query_for_topology(connection)
            if hosts is None:
                return
        except Exception:
            return

        self._monitor._host_threads_latest_topology.set(hosts)
        # Record this reader's observed topology so the main loop's
        # stable-reader-topology check can compare across readers.
        if self._some_regions_inaccessible:
            self._monitor._record_reader_topology(self._host_info, hosts)

        if self._writer_changed:
            self._monitor._update_topology_cache(hosts)
            return

        latest_writer_host = next((host for host in hosts if host.role == HostRole.WRITER), None)
        if (latest_writer_host is not None and self._writer_host_info is not None and
            (latest_writer_host.host != self._writer_host_info.host or
             latest_writer_host.port != self._writer_host_info.port)):
            self._writer_changed = True
            logger.debug("HostMonitor.WriterHostChanged", self._writer_host_info.host, latest_writer_host.host)
            self._monitor._update_topology_cache(hosts)

            # Signal a panic-mode exit only when some regions are
            # inaccessible. In that case no host thread may be able to reach the
            # new writer to confirm it via get_writer_id_if_connected(), so a
            # reader-observed writer change is the fastest way out of panic mode.
            # When all regions are accessible we defer to the standard exit path
            # (a host thread connecting to the new writer reports it directly),
            # which is more reliable since it also verifies a live writer
            # connection. CAS-from-None ensures only the first observer wins.
            if (self._some_regions_inaccessible
                    and self._monitor._reader_observed_writer_host_info.compare_and_set(
                        None, latest_writer_host)):
                logger.debug("HostMonitor.WriterChangeExitTriggered", latest_writer_host.host)
                self._monitor._host_threads_stop.set()

    def _calculate_backoff_with_jitter(self, attempt: int) -> int:
        backoff = ClusterTopologyMonitorImpl.INITIAL_BACKOFF_MS * (2 ** min(attempt, 6))
        backoff = min(backoff, ClusterTopologyMonitorImpl.MAX_BACKOFF_MS)
        return int(backoff * (0.5 + random.random() * 0.5))


class GlobalAuroraTopologyMonitor(ClusterTopologyMonitorImpl):
    def __init__(
            self,
            plugin_service: PluginService,
            topology_utils: GlobalAuroraTopologyUtils,
            cluster_id: str,
            initial_host_info: HostInfo,
            props: Properties,
            instance_template: HostInfo,
            refresh_rate_ns: int,
            high_refresh_rate_ns: int,
            instance_templates_by_region: dict[str, HostInfo]
    ):
        self._instance_templates_by_region = instance_templates_by_region
        self._global_topology_utils = topology_utils
        self._accessible_regions: Optional[FrozenSet[str]] = AccessibleRegions.parse(props)

        super().__init__(
            plugin_service,
            topology_utils,
            cluster_id,
            initial_host_info,
            props,
            instance_template,
            refresh_rate_ns,
            high_refresh_rate_ns
        )

    # Global Databases need a longer stable-topology window than standard Aurora
    # (30s vs 15s) because cross-region topology changes take longer to settle.
    GDB_STABLE_TOPOLOGY_DURATION_NANO = 30 * 1_000_000_000  # 30 seconds in nanoseconds

    def get_stable_topologies_duration_ns(self) -> int:
        return GlobalAuroraTopologyMonitor.GDB_STABLE_TOPOLOGY_DURATION_NANO

    def _create_connection_handler(self) -> MonitoringConnectionHandler:
        return GdbMonitoringConnectionHandler(
            self._monitoring_connection,
            self._plugin_service,
            self._topology_utils,
            self._properties,
            self._monitoring_properties,
            self._writer_host_info,
            self.wake_up_monitoring_loop)

    def _filter_hosts_for_host_monitoring(self, hosts: Topology) -> Topology:
        if self._accessible_regions is None:
            return hosts
        return tuple(
            host for host in hosts
            if AccessibleRegions.is_in_accessible_region(host.host, self._accessible_regions, self._rds_utils)
        )

    def _open_any_connection_and_update_topology(self) -> Topology:
        if self._accessible_regions is not None:
            region = self._rds_utils.get_rds_region(self._initial_host_info.host)
            if region is not None and region.casefold() not in self._accessible_regions:
                raise AwsWrapperError(
                    Messages.get_formatted(
                        "GlobalAuroraTopologyMonitor.InitialHostNotInAccessibleRegion",
                        self._initial_host_info.host, region, self._accessible_regions))
        return super()._open_any_connection_and_update_topology()

    def _get_instance_template(self, instance_id: str, connection: Connection) -> HostInfo:
        region = self._global_topology_utils.get_region(instance_id, connection)
        if region:
            instance_template = self._instance_templates_by_region.get(region)
            if instance_template is None:
                raise AwsWrapperError(
                    Messages.get_formatted("GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", region))
            return instance_template
        return self._instance_template

    def _query_for_topology(self, connection: Connection) -> Topology:
        result = self._global_topology_utils.query_for_topology_with_regions(
            connection, self._instance_templates_by_region)
        return result if result is not None else ()
