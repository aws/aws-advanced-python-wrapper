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

from logging import getLogger
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set

from aws_wrapper.errors import FailoverError

if TYPE_CHECKING:
    from aws_wrapper.generic_target_driver_dialect import TargetDriverDialect
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.pep249 import Connection

    from aws_wrapper.utils.rds_url_type import RdsUrlType
    from aws_wrapper.utils.properties import Properties

from _weakrefset import WeakSet

from aws_wrapper.hostinfo import HostInfo, HostRole
from aws_wrapper.plugin import Plugin, PluginFactory
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.rdsutils import RdsUtils

logger = getLogger(__name__)


class OpenedConnectionTracker:
    logger = getLogger(__name__)

    _opened_connections: Dict[str, WeakSet] = {}
    _rds_utils = RdsUtils()

    def populate_opened_connection_set(self, host_info: HostInfo, conn: Connection):
        aliases: Set[str] = host_info.as_aliases()
        host: str = host_info.as_alias()

        if self._rds_utils.is_rds_instance(host):
            self._track_connection(host, conn)
            return

        instance_endpoint: Optional[str] = next((alias for alias in aliases if self._rds_utils.is_rds_instance(alias)),
                                                None)
        if not instance_endpoint:
            logger.debug(Messages.get("OpenedConnectionTracker.UnableToPopulateOpenedConnectionSet"))
            return

        self._track_connection(instance_endpoint, conn)

    def invalidate_all_connections(self, host_info: Optional[HostInfo] = None, node: Optional[Set[str]] = None):
        """Invalidates all opened connections pointing to the same node in a daemon thread.

        Parameters:
            host_info (HostInfo): The HostInfo object containing the url of the node.
            node (Set[str]): The set of aliases representing a node.
        """

        if host_info:
            self.invalidate_all_connections(node={host_info.as_alias()})
            self.invalidate_all_connections(node=host_info.as_aliases())
            return

        instance_endpoint: Optional[str] = None
        if node is None:
            return

        for instance in node:
            if instance is not None and self._rds_utils.is_rds_instance(instance):
                instance_endpoint = instance
                break

        if not instance_endpoint:
            return

        connection_set: Optional[WeakSet] = self._opened_connections.get(instance_endpoint)
        if connection_set is not None:
            self._log_connection_set(instance_endpoint, connection_set)
            self._invalidate_connections(connection_set)

    def _track_connection(self, instance_endpoint: str, conn: Connection):
        connection_set: Optional[WeakSet] = self._opened_connections.get(instance_endpoint)
        if connection_set is None:
            connection_set = WeakSet()
            connection_set.add(conn)
            self._opened_connections[instance_endpoint] = connection_set
        else:
            connection_set.add(conn)

        self.log_opened_connections()

    @staticmethod
    def _task(connection_set: WeakSet):
        while connection_set is not None and len(connection_set) > 0:
            conn_reference = connection_set.pop()

            if conn_reference is None:
                continue

            try:
                conn_reference.close()
            except Exception:
                # Swallow this exception, current connection should be useless anyway
                pass

    def _invalidate_connections(self, connection_set: WeakSet):
        invalidate_connection_thread: Thread = Thread(daemon=True, target=self._task,
                                                      args=[connection_set])  # type: ignore
        invalidate_connection_thread.start()

    def log_opened_connections(self):
        msg = ""
        for key, conn_set in self._opened_connections.items():
            conn = ""
            for item in list(conn_set):
                conn += f"\n\t\t{item}"

            msg += f"\t[{key} : {conn}]"

        return logger.debug(Messages.get_formatted("OpenedConnectionTracker.OpenedConnectionsTracked", msg))

    def _log_connection_set(self, host: str, conn_set: Optional[WeakSet]):
        if conn_set is None or len(conn_set) == 0:
            return

        conn = ""
        for item in list(conn_set):
            conn += f"\n\t\t{item}"

        msg = host + f"[{conn}\n]"
        logger.debug(Messages.get_formatted("OpenedConnectionTracker.InvalidatingConnections", msg))


class AuroraConnectionTrackerPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"*"}
    _current_writer: Optional[HostInfo] = None
    _need_update_current_writer: bool = False

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def __init__(self,
                 plugin_service: PluginService,
                 props: Properties,
                 rds_utils: RdsUtils = RdsUtils(),
                 tracker: OpenedConnectionTracker = OpenedConnectionTracker()):
        self._plugin_service = plugin_service
        self._props = props
        self._rds_utils = rds_utils
        self._tracker = tracker

    def connect(
            self,
            target_driver_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect(host_info, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(host_info, force_connect_func)

    def _connect(self, host_info: HostInfo, connect_func: Callable):
        conn = connect_func()

        if conn:
            url_type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)
            if url_type.is_rds_cluster:
                host_info.reset_aliases()
                self._plugin_service.fill_aliases(conn, host_info)

            self._tracker.populate_opened_connection_set(host_info, conn)
            self._tracker.log_opened_connections()

        return conn

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        if self._current_writer is None or self._need_update_current_writer:
            self._current_writer = self._get_writer(self._plugin_service.hosts)
            self._need_update_current_writer = False

        try:
            return execute_func()

        except Exception as e:
            if isinstance(e, FailoverError):
                self._tracker.invalidate_all_connections(host_info=self._current_writer)
                self._tracker.log_opened_connections()
                self._need_update_current_writer = True
            raise e

    def _get_writer(self, hosts: List[HostInfo]) -> Optional[HostInfo]:
        for host in hosts:
            if host.role == HostRole.WRITER:
                return host
        return None


class AuroraConnectionTrackerPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraConnectionTrackerPlugin(plugin_service, props)
