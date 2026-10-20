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

import threading
from threading import Thread
from time import perf_counter_ns
from typing import (TYPE_CHECKING, Any, Callable, ClassVar, Dict, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.utils import Utils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection

    from aws_advanced_python_wrapper.utils.properties import Properties

from _weakrefset import WeakSet

from aws_advanced_python_wrapper.errors import FailoverError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

logger = Logger(__name__)


class OpenedConnectionTracker:
    _opened_connections: ClassVar[Dict[str, WeakSet]] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _prune_thread: ClassVar[Optional[Thread]] = None
    _prune_thread_started: ClassVar[bool] = False
    _shutdown_event: ClassVar[threading.Event] = threading.Event()
    _safe_to_check_closed_classes: ClassVar[Set[str]] = {"psycopg"}
    _default_sleep_time: ClassVar[int] = 30

    @classmethod
    def _start_prune_thread(cls):
        with cls._lock:
            if not cls._prune_thread_started:
                cls._prune_thread_started = True
                cls._prune_thread = Thread(daemon=True, target=cls._prune_connections_loop)
                cls._prune_thread.start()

    @classmethod
    def release_resources(cls):
        cls._shutdown_event.set()
        with cls._lock:
            thread_to_join = cls._prune_thread
        if thread_to_join is not None:
            thread_to_join.join()
        with cls._lock:
            cls._opened_connections.clear()

    @classmethod
    def _prune_connections_loop(cls):
        while not cls._shutdown_event.is_set():
            try:
                cls._prune_connections()
                if cls._shutdown_event.wait(timeout=cls._default_sleep_time):
                    break
            except Exception:
                pass

    @classmethod
    def _prune_connections(cls):
        with cls._lock:
            opened_connections = list(cls._opened_connections.items())

        to_remove_by_host = {}
        for host, conn_set in opened_connections:
            to_remove = []
            for conn in list(conn_set):
                if conn is None:
                    to_remove.append(conn)
                else:
                    try:
                        # The following classes do not check connection validity via a DB server call
                        # so it is safe to check whether connection is already closed.
                        if any(safe_class in conn.__module__ for safe_class in cls._safe_to_check_closed_classes) and conn.is_closed():
                            to_remove.append(conn)
                    except Exception:
                        pass

            if to_remove:
                to_remove_by_host[host] = (conn_set, to_remove)

        with cls._lock:
            for host, (conn_set, to_remove) in to_remove_by_host.items():
                for conn in to_remove:
                    conn_set.discard(conn)

                # Remove empty connection sets
                if not conn_set and host in cls._opened_connections:
                    del cls._opened_connections[host]

    def _tracking_keys(self, host_info: HostInfo) -> Tuple[str, ...]:
        """The keys ``host_info``'s connections are tracked under.

        An instance endpoint names the instance already, so one key is enough.
        Anything else may be a custom domain name, which is tracked both under
        the host id and under the endpoint the caller used.
        """
        if self._rds_utils.is_rds_instance(host_info.host):
            return (host_info.get_host_and_port(),)
        return tuple(key for key in (host_info.host_id, host_info.get_host_and_port()) if key)

    def populate_opened_connection_set(self, host_info: Optional[HostInfo], conn: Optional[Connection]):
        """
        Add the given connection to the set of tracked connections.

        :param host_info: host information of the given connection.
        :param conn: currently opened connection.
        """
        if host_info is None or conn is None:
            return

        keys = self._tracking_keys(host_info)
        if not keys:
            logger.debug("OpenedConnectionTracker.UnableToPopulateOpenedConnectionSet")
            return

        for key in keys:
            self._track_connection(key, conn)

    def invalidate_all_connections(self, host_info: Optional[HostInfo]):
        """
        Invalidates all opened connections pointing to the same host in a daemon thread.

        :param host_info: the host whose connections should be closed.
        """
        if host_info is None:
            return
        self.invalidate_all_connections_for_keys(
            host_info.get_host_and_port(), host_info.host, host_info.host_id)

    def invalidate_all_connections_for_keys(self, *keys: Optional[str]):
        """Invalidate the connections tracked under any of ``keys``.

        A connection tracked under more than one key is closed once: closing
        a pool-proxied connection twice would invalidate whatever the pool
        handed out in between.
        """
        present = [key for key in keys if key]
        seen: Set[int] = set()
        to_close: list = []

        with self._lock:
            for key in present:
                connection_set: Optional[WeakSet] = self._opened_connections.get(key)
                if connection_set is None:
                    continue
                for conn in list(connection_set):
                    if id(conn) not in seen:
                        seen.add(id(conn))
                        to_close.append(conn)
                connection_set.clear()

        if to_close:
            self._log_connection_set(", ".join(present), to_close)
            self._invalidate_connections(to_close)

    def remove_connection_tracking(self, host_info: HostInfo, connection: Connection | None):
        if not connection:
            return

        # Every key populate_opened_connection_set used, so a custom-domain
        # connection is untracked from both of its keys.
        with self._lock:
            for key in self._tracking_keys(host_info):
                connection_set = self._opened_connections.get(key)
                if connection_set:
                    connection_set.discard(connection)

    def _track_connection(self, instance_endpoint: str, conn: Connection):
        with self._lock:
            connection_set = self._opened_connections.setdefault(instance_endpoint, WeakSet())
            connection_set.add(conn)
        self._start_prune_thread()
        self.log_opened_connections()

    @staticmethod
    def _task(connections_list: list):
        for conn_reference in connections_list:
            if conn_reference is None:
                continue

            # For pool-proxied connections (e.g. SQLAlchemy's
            # _ConnectionFairy), prefer .invalidate() over .close().
            # close() checks the connection back into the pool, which first
            # runs rollback-on-return against the failed writer -- an
            # unbounded blocking call on this thread when the host is
            # unreachable -- and re-pools the connection if that rollback
            # happens to succeed (e.g. the old writer came back as a
            # reader). invalidate() skips the rollback and discards the
            # connection so the pool opens a fresh one on next checkout.
            try:
                inv = getattr(conn_reference, "invalidate", None)
                if callable(inv):
                    inv()
                else:
                    conn_reference.close()
            except Exception:
                # Swallow this exception, current connection should be useless anyway
                pass

    def _invalidate_connections(self, connections_list: list):
        invalidate_connection_thread: Thread = Thread(daemon=True, target=self._task,
                                                      args=[connections_list])  # type: ignore[arg-type]
        invalidate_connection_thread.start()

    def log_opened_connections(self):
        with self._lock:
            opened_connections = [(key, list(conn_set)) for key, conn_set in self._opened_connections.items()]

        msg_parts = []
        for key, conn_list in opened_connections:
            conn_parts = [f"\n\t\t{item}" for item in conn_list]
            conn = "".join(conn_parts)
            msg_parts.append(f"\t[{key} : {conn}]")

        msg = "".join(msg_parts)
        return logger.debug("OpenedConnectionTracker.OpenedConnectionsTracked", msg)

    def _log_connection_set(self, host: str, connections: Optional[list]):
        if not connections:
            return

        conn_parts = [f"\n\t\t{item}" for item in connections]
        conn = "".join(conn_parts)
        msg = host + f"[{conn}\n]"
        logger.debug("OpenedConnectionTracker.InvalidatingConnections", msg)


class AuroraConnectionTrackerPlugin(Plugin):
    _host_list_refresh_end_time_nano: ClassVar[int] = 0
    _refresh_lock: ClassVar[threading.Lock] = threading.Lock()
    _TOPOLOGY_CHANGES_EXPECTED_TIME_NANO: ClassVar[int] = 3 * 60 * 1_000_000_000  # 3 minutes

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._subscribed_methods

    def __init__(self,
                 plugin_service: PluginService,
                 props: Properties,
                 rds_utils: RdsUtils = RdsUtils(),
                 tracker: OpenedConnectionTracker = OpenedConnectionTracker()):
        self._plugin_service = plugin_service
        self._props = props
        self._rds_utils = rds_utils
        self._tracker = tracker
        self._current_writer: Optional[HostInfo] = None
        self._need_update_current_writer: bool = False
        self._subscribed_methods: Set[str] = {DbApiMethod.CONNECT.method_name,
                                              DbApiMethod.CONNECTION_CLOSE.method_name,
                                              DbApiMethod.CONNECT.method_name,
                                              DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name}
        self._subscribed_methods.update(self._plugin_service.network_bound_methods)

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        conn = connect_func()

        if conn:
            routed = self._plugin_service.routed_host_info
            connection_host_info = routed if routed is not None else host_info
            url_type = self._rds_utils.identify_rds_type(connection_host_info.host)
            if Utils.requires_host_identification(url_type):
                # The endpoint does not name the instance the connection
                # landed on, so ask the database and track that instead.
                # Best-effort: a connect must not fail because the host id
                # query did.
                try:
                    identified = self._plugin_service.identify_connection(conn)
                except Exception:  # noqa: BLE001 - identification is best-effort
                    identified = None
                if identified is not None:
                    connection_host_info = identified
                    self._plugin_service.routed_host_info = identified

            self._tracker.populate_opened_connection_set(connection_host_info, conn)
            self._tracker.log_opened_connections()

        return conn

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        current_host = self._plugin_service.current_host_info
        if self._current_writer is None or self._need_update_current_writer:
            self._current_writer = Utils.get_writer(self._plugin_service.all_hosts)
            self._need_update_current_writer = False

        try:
            if not method_name == DbApiMethod.CONNECTION_CLOSE.method_name:
                need_refresh_host_lists = False
                with AuroraConnectionTrackerPlugin._refresh_lock:
                    local_host_list_refresh_end_time_nano = AuroraConnectionTrackerPlugin._host_list_refresh_end_time_nano
                    if local_host_list_refresh_end_time_nano > 0:
                        if local_host_list_refresh_end_time_nano > perf_counter_ns():
                            # The time specified in hostListRefreshThresholdTimeNano isn't yet reached.
                            # Need to continue to refresh host list.
                            need_refresh_host_lists = True
                        else:
                            # The time specified in hostListRefreshThresholdTimeNano is reached, and we can stop further refreshes
                            # of host list.
                            AuroraConnectionTrackerPlugin._host_list_refresh_end_time_nano = 0

                if self._need_update_current_writer or need_refresh_host_lists:
                    # Calling this method may effectively close/abort a current connection
                    self._check_writer_changed(need_refresh_host_lists)

            result = execute_func()
            if method_name == DbApiMethod.CONNECTION_CLOSE.method_name:
                self._tracker.remove_connection_tracking(current_host, self._plugin_service.current_connection)
            return result

        except Exception as e:
            if isinstance(e, FailoverError):
                with AuroraConnectionTrackerPlugin._refresh_lock:
                    AuroraConnectionTrackerPlugin._host_list_refresh_end_time_nano = (
                            perf_counter_ns() + AuroraConnectionTrackerPlugin._TOPOLOGY_CHANGES_EXPECTED_TIME_NANO)
                # Calling this method may effectively close/abort a current connection
                self._check_writer_changed(True)
            raise

    def _check_writer_changed(self, need_refresh_host_lists: bool):
        if need_refresh_host_lists:
            self._plugin_service.refresh_host_list()

        host_info_after_failover = Utils.get_writer(self._plugin_service.all_hosts)
        if host_info_after_failover is None:
            return

        if self._current_writer is None:
            self._current_writer = host_info_after_failover
            self._need_update_current_writer = False
        elif not self._current_writer.get_host_and_port() == host_info_after_failover.get_host_and_port():
            self._tracker.invalidate_all_connections(host_info=self._current_writer)
            self._tracker.log_opened_connections()
            self._current_writer = host_info_after_failover
            self._need_update_current_writer = False

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        for host, host_changes in changes.items():
            if HostEvent.CONVERTED_TO_READER in host_changes:
                # Change keys are urls; tracking keys are not.
                self._tracker.invalidate_all_connections_for_keys(host.rstrip("/"))
            if HostEvent.CONVERTED_TO_WRITER in host_changes:
                self._need_update_current_writer = True


class AuroraConnectionTrackerPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraConnectionTrackerPlugin(plugin_service, props)
