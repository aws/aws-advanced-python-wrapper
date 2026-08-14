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

import random
import threading
from abc import ABC, abstractmethod
from typing import (TYPE_CHECKING, Callable, Generic, List, Optional, Sequence,
                    Tuple, TypeVar)

from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
    ThreadSafeConnectionHolder

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.host_list_provider import TopologyUtils
    from aws_advanced_python_wrapper.hostinfo import HostInfo, Topology
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)

P = TypeVar("P")

# Sentinel priority index representing "does not match any configured priority".
# A large value so unmatched hosts sort last for effective ordering.
_NO_PRIORITY_INDEX = 2 ** 63 - 1


class MonitoringConnectionHandler(ABC):
    """Handles the monitoring connection lifecycle.

    The topology monitor accepts whatever connection it obtains first, then this
    handler asynchronously upgrades to a higher-priority connection.
    """

    @abstractmethod
    def accept_connection(self, conn: Connection, is_writer: bool, host_info: Optional[HostInfo]) -> bool:
        """Offer a single connection to the handler.

        Returns ``True`` if the connection was accepted as the monitoring
        connection, ``False`` if it was rejected (the caller should close it).
        """
        ...

    @abstractmethod
    def accept_connections(
            self,
            connections: Sequence[Tuple[HostInfo, ThreadSafeConnectionHolder]],
            writer_host_info: Optional[HostInfo],
            topology: Optional[Topology]) -> Optional[HostInfo]:
        """Offer a batch of harvested connections; adopt the best by priority.

        ``connections`` is a sequence of ``(host, holder)`` pairs. A sequence of
        pairs is used rather than a mapping keyed by host because
        :class:`HostInfo` is unhashable; the semantics are identical.

        Returns the host of the connection that was selected and set as the
        monitoring connection, or ``None`` if none was selected.
        """
        ...

    @abstractmethod
    def attempt_connection_upgrade(self, current_topology: Optional[Topology]) -> None:
        """Non-blocking attempt to upgrade to a higher-priority host."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Cancel pending upgrade attempts and release held connections."""
        ...


class AbstractMonitoringConnectionHandler(MonitoringConnectionHandler, Generic[P]):
    """Priority-driven monitoring connection lifecycle shared by concrete handlers.

    A thread-safe connection wrapper is provided by
    :class:`ThreadSafeConnectionHolder`, and the async upgrade runs on a single
    background :class:`threading.Thread` guarded by a :class:`threading.Event`.

    Lower priority index == higher preference. ``priorities`` is a list of
    priority objects (a ``MonitoringConnectionPriority`` or
    ``GdbMonitoringConnectionPriority``); this base class is agnostic to their
    concrete type and defers role/region reasoning to the subclass hooks
    :meth:`_get_priority_index` and :meth:`_find_hosts_for_priority`.
    """

    # Seconds to wait for the async upgrade thread to finish on close().
    UPGRADE_JOIN_TIMEOUT_SEC = 5.0

    def __init__(
            self,
            monitoring_connection: ThreadSafeConnectionHolder,
            plugin_service: PluginService,
            topology_utils: TopologyUtils,
            monitoring_properties: Properties,
            priorities: Sequence[P],
            upgrade_ready_notifier: Optional[Callable[[], None]] = None):
        self._monitoring_connection = monitoring_connection
        self._upgrade_connection = ThreadSafeConnectionHolder(None)
        self._plugin_service = plugin_service
        self._topology_utils = topology_utils
        self._monitoring_properties = monitoring_properties
        self._priorities: List[P] = list(priorities)
        self._upgrade_ready_notifier = upgrade_ready_notifier

        self._lock = threading.RLock()
        self._current_priority_index = -1
        self._upgrade_thread: Optional[threading.Thread] = None
        self._upgrade_done = threading.Event()
        self._upgrade_cancelled = threading.Event()
        self._upgrade_connected_host: Optional[HostInfo] = None

    # ---- Subclass hooks -------------------------------------------------

    @abstractmethod
    def _get_priority_index(self, host: HostInfo, is_writer: bool) -> int:
        """Return the priority index for ``host`` (or -1 when it matches none)."""
        ...

    @abstractmethod
    def _find_hosts_for_priority(self, priority_index: int, hosts: Sequence[HostInfo]) -> List[HostInfo]:
        """Return hosts matching the priority at ``priority_index``."""
        ...

    @abstractmethod
    def _get_upgrade_thread_name(self) -> str:
        ...

    # ---- Helpers --------------------------------------------------------

    @staticmethod
    def _effective_index(priority_index: int) -> int:
        return priority_index if priority_index >= 0 else _NO_PRIORITY_INDEX

    @staticmethod
    def _format_priority_index(index: int) -> str:
        return "<none>" if index == _NO_PRIORITY_INDEX else str(index)

    def _find_upgrade_candidates(self, hosts: Topology) -> List[List[HostInfo]]:
        candidates_by_priority: List[List[HostInfo]] = []
        limit = min(self._current_priority_index, len(self._priorities))
        for i in range(limit):
            matching = self._find_hosts_for_priority(i, hosts)
            if matching:
                candidates_by_priority.append(matching)
        return candidates_by_priority

    # ---- MonitoringConnectionHandler ------------------------------------

    def accept_connection(self, conn: Connection, is_writer: bool, host_info: Optional[HostInfo]) -> bool:
        with self._lock:
            priority_index = -1 if host_info is None else self._get_priority_index(host_info, is_writer)
            effective_index = self._effective_index(priority_index)
            host_label = host_info.host if host_info is not None else "unknown"

            if self._monitoring_connection.get() is None or self._current_priority_index < 0:
                self._monitoring_connection.set(conn, close_previous=True)
                self._current_priority_index = effective_index
                logger.debug("MonitoringConnectionHandler.ConnectionAccepted",
                             host_label, "WRITER" if is_writer else "READER",
                             self._format_priority_index(effective_index))
                return True

            if effective_index < self._current_priority_index:
                self._monitoring_connection.set(conn, close_previous=True)
                self._current_priority_index = effective_index
                logger.debug("MonitoringConnectionHandler.ConnectionAccepted",
                             host_label, "WRITER" if is_writer else "READER",
                             self._format_priority_index(effective_index))
                return True

            logger.debug("MonitoringConnectionHandler.ConnectionRejected",
                         host_label, is_writer,
                         self._format_priority_index(self._current_priority_index),
                         self._format_priority_index(effective_index))
            return False

    def accept_connections(
            self,
            connections: Sequence[Tuple[HostInfo, ThreadSafeConnectionHolder]],
            writer_host_info: Optional[HostInfo],
            topology: Optional[Topology]) -> Optional[HostInfo]:
        with self._lock:
            if not connections:
                return None

            best: Optional[Tuple[HostInfo, ThreadSafeConnectionHolder]] = None
            best_index = _NO_PRIORITY_INDEX
            for host, holder in connections:
                if holder is None or holder.get() is None:
                    continue
                is_writer = (writer_host_info is not None
                             and self._host_and_port(writer_host_info) == self._host_and_port(host))
                effective_index = self._effective_index(self._get_priority_index(host, is_writer))
                if best is None or effective_index < best_index:
                    best_index = effective_index
                    best = (host, holder)

            if best is None:
                return None

            best_host, best_holder = best
            # Detach the connection from the holder without closing it, then
            # adopt it as the monitoring connection.
            best_conn = best_holder.get_and_set(None, close_previous=False)
            self._monitoring_connection.set(best_conn, close_previous=True)
            self._current_priority_index = best_index
            logger.debug("MonitoringConnectionHandler.ConnectionAccepted",
                         best_host.host, best_host.role,
                         self._format_priority_index(best_index))
            return best_host

    def attempt_connection_upgrade(self, current_topology: Optional[Topology]) -> None:
        with self._lock:
            if self._current_priority_index <= 0:
                return

            thread = self._upgrade_thread
            if thread is not None:
                if not self._upgrade_done.is_set():
                    # Upgrade attempt still running.
                    return

                # Safe to read _upgrade_connected_host / _upgrade_connection
                # here: the worker publishes both BEFORE calling
                # _upgrade_done.set(), and we only reach this point after
                # _upgrade_done.is_set() returned True (see _start_upgrade_thread
                # for the memory-ordering contract).
                conn = self._upgrade_connection.get()
                connected_host = self._upgrade_connected_host
                if conn is not None and connected_host is not None:
                    try:
                        is_writer = self._topology_utils.get_writer_id_if_connected(
                            conn, self._plugin_service.driver_dialect) is not None
                    except Exception:
                        self._upgrade_connection.set(None, close_previous=True)
                        self._reset_upgrade_state()
                        return

                    new_index = self._get_priority_index(connected_host, is_writer)
                    if 0 <= new_index < self._current_priority_index:
                        # Adopt the upgraded connection without closing it.
                        self._upgrade_connection.set(None, close_previous=False)
                        self._monitoring_connection.set(conn, close_previous=True)
                        self._current_priority_index = new_index
                        logger.debug("MonitoringConnectionHandler.UpgradedMonitoringConnection",
                                     connected_host.host, str(self._priorities[new_index]),
                                     self._format_priority_index(new_index))
                    else:
                        self._upgrade_connection.set(None, close_previous=True)
                elif conn is not None:
                    self._upgrade_connection.set(None, close_previous=True)
                self._reset_upgrade_state()

            if self._upgrade_thread is None and current_topology is not None:
                candidates_by_priority = self._find_upgrade_candidates(current_topology)
                # Flatten buckets highest-priority-first. Within each priority
                # bucket the hosts are equivalent, so shuffle before flattening to
                # spread monitoring-connection load across them.
                candidates: List[HostInfo] = []
                for bucket in candidates_by_priority:
                    random.shuffle(bucket)
                    candidates.extend(bucket)
                if not candidates:
                    return

                self._start_upgrade_thread(candidates)

    def close(self) -> None:
        with self._lock:
            self._upgrade_cancelled.set()
            thread = self._upgrade_thread
        if thread is not None and thread.is_alive():
            thread.join(self.UPGRADE_JOIN_TIMEOUT_SEC)
        with self._lock:
            self._upgrade_connection.clear()
            self._reset_upgrade_state()
            self._upgrade_cancelled.clear()
            self._current_priority_index = -1

    # ---- Internal -------------------------------------------------------

    @staticmethod
    def _host_and_port(host: HostInfo) -> str:
        return f"{host.host}:{host.port}"

    def _reset_upgrade_state(self) -> None:
        self._upgrade_thread = None
        self._upgrade_connected_host = None
        self._upgrade_done.clear()

    def _start_upgrade_thread(self, candidates: List[HostInfo]) -> None:
        self._upgrade_done.clear()
        self._upgrade_cancelled.clear()

        def _run() -> None:
            for candidate in candidates:
                if self._upgrade_cancelled.is_set():
                    break
                try:
                    conn = self._plugin_service.force_connect(candidate, self._monitoring_properties)
                    # Memory-ordering contract: publish the host and the
                    # connection BEFORE _upgrade_done.set() below. The reader in
                    # attempt_connection_upgrade holds self._lock but reads these
                    # fields without it, gated only on _upgrade_done.is_set();
                    # Event.set()/is_set() provide the happens-before barrier
                    # (via the Event's internal lock). Do NOT reorder these three
                    # statements or move _upgrade_done.set() earlier.
                    self._upgrade_connected_host = candidate
                    self._upgrade_connection.set(conn, close_previous=True)
                    if self._upgrade_ready_notifier is not None:
                        self._upgrade_ready_notifier()
                    break
                except Exception as ex:
                    logger.debug("MonitoringConnectionHandler.UpgradeAttemptFailed",
                                 candidate.host, ex)
            self._upgrade_done.set()

        thread = threading.Thread(
            target=_run, name=self._get_upgrade_thread_name(), daemon=True)
        self._upgrade_thread = thread
        thread.start()
