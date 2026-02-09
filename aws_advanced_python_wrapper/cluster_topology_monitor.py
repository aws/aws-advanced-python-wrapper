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
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.atomic import AtomicReference
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
    ThreadSafeConnectionHolder
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.host_list_provider import TopologyUtils

from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (PropertiesUtils,
                                                          WrapperProperties)

logger = Logger(__name__)


class ClusterTopologyMonitor(ABC):
    @abstractmethod
    def force_refresh(self, should_verify_writer: bool, timeout_sec: int) -> Tuple[HostInfo, ...]:
        pass

    @abstractmethod
    def force_refresh_with_connection(self, connection: Connection, timeout_sec: int) -> Tuple[HostInfo, ...]:
        pass

    @abstractmethod
    def can_dispose(self) -> bool:
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

    _topology_map: CacheMap[str, Tuple[HostInfo, ...]] = CacheMap()

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
        self._host_threads_latest_topology: AtomicReference[Optional[Tuple[HostInfo, ...]]] = AtomicReference(None)

        self._is_verified_writer_connection = False
        self._high_refresh_rate_end_time_nano = 0
        self._stop = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

        self._monitoring_properties = PropertiesUtils.create_topology_monitoring_properties(properties)
        if WrapperProperties.SOCKET_TIMEOUT_SEC.get(self._monitoring_properties) is None:
            WrapperProperties.SOCKET_TIMEOUT_SEC.set(self._monitoring_properties, self.DEFAULT_SOCKET_TIMEOUT_SEC)
        if WrapperProperties.CONNECT_TIMEOUT_SEC.get(self._monitoring_properties) is None:
            WrapperProperties.CONNECT_TIMEOUT_SEC.set(self._monitoring_properties, self.DEFAULT_CONNECT_TIMEOUT_SEC)

        self._start_monitoring()

    def force_refresh(self, should_verify_writer: bool, timeout_sec: int) -> Tuple[HostInfo, ...]:
        current_time_nano = time.time_ns()
        if (self._ignore_new_topology_requests_end_time_nano > 0 and
                current_time_nano < self._ignore_new_topology_requests_end_time_nano):
            current_hosts = self._get_stored_hosts()
            if current_hosts is not None:
                logger.debug("ClusterTopologyMonitorImpl.IgnoringTopologyRequest", self._cluster_id, LogUtils.log_topology(current_hosts))
                return current_hosts

        if should_verify_writer:
            self._monitoring_connection.clear()
            self._is_verified_writer_connection = False

        result = self._wait_till_topology_gets_updated(timeout_sec)
        return result

    def force_refresh_with_connection(self, connection: Connection, timeout_sec: int) -> Tuple[HostInfo, ...]:
        if self._is_verified_writer_connection:
            return self._wait_till_topology_gets_updated(timeout_sec)
        return self._fetch_topology_and_update_cache(connection)

    def _wait_till_topology_gets_updated(self, timeout_sec: int) -> Tuple[HostInfo, ...]:
        current_hosts = self._get_stored_hosts()

        self._request_to_update_topology.set()

        if timeout_sec == 0:
            logger.debug("ClusterTopologyMonitorImpl.TimeoutSetToZero", self._cluster_id, LogUtils.log_topology(current_hosts))
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
                        "ClusterTopologyMonitorImpl.TopologyNotUpdated",
                        self._cluster_id, timeout_sec * 1000))

    def _get_stored_hosts(self) -> Tuple[HostInfo, ...]:
        hosts = ClusterTopologyMonitorImpl._topology_map.get(self._cluster_id)
        if hosts is None:
            return ()
        return hosts

    def can_dispose(self) -> bool:
        return self._stop.is_set()

    def close(self) -> None:
        logger.debug("ClusterTopologyMonitorImpl.ClosingMonitor", self._cluster_id)
        self._stop.set()
        self._request_to_update_topology.set()

        self._close_host_monitors()

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(self.MONITOR_TERMINATION_TIMEOUT_SEC)

        # Step 3: Now safe to close connections - no threads are using them
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
                            logger.debug("ClusterTopologyMonitorImpl.StartingHostMonitoringThreads", self._cluster_id)
                            writer_host_info = self._writer_host_info.get()
                            for host_info in hosts:
                                if host_info.host not in self._submitted_hosts:
                                    try:
                                        worker = self._get_host_monitor(host_info, writer_host_info)
                                        self._get_host_executor_service().submit(worker)
                                        self._submitted_hosts[host_info.host] = True
                                    except Exception as e:
                                        logger.debug(
                                            "ClusterTopologyMonitorImpl.ExceptionStartingHostMonitor",
                                            self._cluster_id, host_info.host, e)
                    else:
                        # Check if writer has been detected
                        writer_host_info = self._host_threads_writer_host_info.get()
                        writer_connection = self._host_threads_writer_connection.get()
                        if (writer_connection is not None and writer_host_info is not None):
                            logger.debug("ClusterTopologyMonitorImpl.WriterPickedUpFromHostMonitors", self._cluster_id, writer_host_info.host)
                            # Transfer the writer connection to monitoring connection
                            self._monitoring_connection.set(writer_connection, close_previous=True)
                            self._writer_host_info.set(writer_host_info)
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

                        # Update host monitors with new topology
                        host_threads_topology = self._host_threads_latest_topology.get()
                        if host_threads_topology is not None and not self._host_threads_stop.is_set():
                            for host_info in host_threads_topology:
                                if host_info.host not in self._submitted_hosts:
                                    try:
                                        worker = self._get_host_monitor(host_info, self._writer_host_info.get())
                                        self._get_host_executor_service().submit(worker)
                                        self._submitted_hosts[host_info.host] = True
                                    except Exception as e:
                                        logger.debug(
                                            "ClusterTopologyMonitorImpl.ExceptionStartingHostMonitor",
                                            self._cluster_id, host_info.host, e)

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

                    current_time_nano = time.time_ns()
                    if (self._high_refresh_rate_end_time_nano > 0 and
                            current_time_nano > self._high_refresh_rate_end_time_nano):
                        self._high_refresh_rate_end_time_nano = 0

                    self._delay(False)

                if (self._ignore_new_topology_requests_end_time_nano > 0 and
                        time.time_ns() > self._ignore_new_topology_requests_end_time_nano):
                    self._ignore_new_topology_requests_end_time_nano = 0

        except Exception as ex:
            logger.info("ClusterTopologyMonitorImpl.ExceptionDuringMonitoringStop", self._cluster_id, ex)
        finally:
            self._stop.set()
            self._close_host_monitors()
            self._monitoring_connection.clear()
            logger.debug("ClusterTopologyMonitor.StopMonitoringThread", self._cluster_id, self._initial_host_info.host)

    def _is_in_panic_mode(self) -> bool:
        return self._monitoring_connection.get() is None or not self._is_verified_writer_connection

    def _get_host_monitor(self, host_info: HostInfo, writer_host_info: Optional[HostInfo]):
        return HostMonitor(self, host_info, writer_host_info)

    def _open_any_connection_and_update_topology(self) -> Tuple[HostInfo, ...]:
        writer_verified_by_this_thread = False
        if self._monitoring_connection.get() is None:
            # Try to connect to the initial host first
            try:
                conn = self._plugin_service.force_connect(self._initial_host_info, self._monitoring_properties)
                self._monitoring_connection.set(conn, close_previous=False)
                logger.debug("ClusterTopologyMonitorImpl.OpenedMonitoringConnection", self._cluster_id, self._initial_host_info.host)

                try:
                    writer_id = self._topology_utils.get_writer_host_if_connected(
                            conn, self._plugin_service.driver_dialect)
                    if writer_id:
                        self._is_verified_writer_connection = True
                        writer_verified_by_this_thread = True

                        if self._rds_utils.is_rds_instance(self._initial_host_info.host):
                            writer_host_info = self._initial_host_info
                            self._writer_host_info.set(writer_host_info)
                        else:
                            writer_host = self._instance_template.host.replace("?", writer_id)
                            port = self._instance_template.port \
                                if self._instance_template.is_port_specified() \
                                else self._initial_host_info.port
                            writer_host_info = HostInfo(
                                writer_host,
                                port,
                                HostRole.WRITER,
                                HostAvailability.AVAILABLE,
                                host_id=writer_id)
                            self._writer_host_info.set(writer_host_info)

                        logger.debug("ClusterTopologyMonitorImpl.WriterMonitoringConnection",
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
            if connection is not None:
                connection.close()
        except Exception:
            pass

    def _close_connection_from_ref(self, connection: AtomicReference[Optional[Connection]]) -> None:
        connection_to_close: Optional[Connection] = connection.get_and_set(None)
        self._close_connection(connection_to_close)

    def _host_thread_connection_cleanup(self) -> None:
        writer_connection = self._host_threads_writer_connection.get_and_set(None)
        if self._monitoring_connection.get() != writer_connection:
            self._close_connection(writer_connection)

        reader_connection = self._host_threads_reader_connection.get_and_set(None)
        if self._monitoring_connection.get() != reader_connection:
            self._close_connection(reader_connection)

    def _close_host_monitors(self) -> None:
        self._host_threads_stop.set()

        thread_pool_executor = self._thread_pool_executor.get_and_set(None)
        if thread_pool_executor is not None:
            thread_pool_executor.shutdown(wait=True, cancel_futures=True)
        self._host_thread_connection_cleanup()

        self._submitted_hosts.clear()

    def _get_host_executor_service(self) -> ThreadPoolExecutor:
        if self._stop.is_set():
            raise RuntimeError(Messages.get_formatted(
                "ClusterTopologyMonitorImpl.CannotCreateExecutorWhenStopped", self._cluster_id))
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

    def _fetch_topology_and_update_cache(self, connection: Optional[Connection]) -> Tuple[HostInfo, ...]:
        if connection is None:
            return ()

        try:
            hosts = self._query_for_topology(connection)
            if hosts:
                self._update_topology_cache(hosts)
                return hosts
            return ()
        except Exception as ex:
            logger.debug("ClusterTopologyMonitorImpl.ErrorFetchingTopology", self._cluster_id, ex)
            return ()

    def _fetch_topology_and_update_cache_safe(self) -> Tuple[HostInfo, ...]:
        """
        Safely fetch topology using ThreadSafeConnectionHolder to prevent race conditions.
        The lock is held during the entire query operation.
        """
        result = self._monitoring_connection.use_connection(
            lambda conn: self._fetch_topology_and_update_cache(conn)
        )
        return result if result is not None else ()

    def _query_for_topology(self, connection: Connection) -> Tuple[HostInfo, ...]:
        hosts = self._topology_utils.query_for_topology(connection, self._plugin_service.driver_dialect)
        if hosts is not None:
            return hosts
        return ()

    def _update_topology_cache(self, hosts: Tuple[HostInfo, ...]) -> None:
        ClusterTopologyMonitorImpl._topology_map.put(
            self._cluster_id, hosts, ClusterTopologyMonitorImpl.TOPOLOGY_CACHE_EXPIRATION_NANO)

        # Notify waiting threads
        self._request_to_update_topology.clear()
        self._topology_updated.set()


class HostMonitor:
    def __init__(self, monitor: ClusterTopologyMonitorImpl, host_info: HostInfo,
                 writer_host_info: Optional[HostInfo]):
        self._monitor: ClusterTopologyMonitorImpl = monitor
        self._host_info = host_info
        self._writer_host_info = writer_host_info
        self._writer_changed = False
        self._connection_attempts = 0

    def __call__(self) -> None:
        connection = None
        update_topology = False
        start_time = time.time()

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
                        is_writer = self._monitor._topology_utils.get_writer_host_if_connected(
                            connection, self._monitor._plugin_service.driver_dialect) is not None
                    except Exception:
                        self._monitor._close_connection(connection)
                        connection = None
                        continue

                    if is_writer:
                        try:
                            if self._monitor._topology_utils.get_host_role(
                                    connection, self._monitor._plugin_service.driver_dialect) != HostRole.WRITER:
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

                time.sleep(0.1)

        except Exception as ex:
            logger.debug("HostMonitor.Exception", self._host_info.host, ex)
        finally:
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

    def _calculate_backoff_with_jitter(self, attempt: int) -> int:
        backoff = ClusterTopologyMonitorImpl.INITIAL_BACKOFF_MS * (2 ** min(attempt, 6))
        backoff = min(backoff, ClusterTopologyMonitorImpl.MAX_BACKOFF_MS)
        return int(backoff * (0.5 + random.random() * 0.5))
