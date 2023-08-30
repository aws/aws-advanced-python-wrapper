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

import socket
from contextlib import closing
from enum import Enum, auto
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set

from aws_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_wrapper.host_list_provider import HostListProviderService
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.utils.notifications import HostEvent
    from aws_wrapper.utils.properties import Properties

from aws_wrapper.hostinfo import HostRole
from aws_wrapper.plugin import Plugin, PluginFactory
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.rdsutils import RdsUtils

logger = getLogger(__name__)


class NodeChangeOptions(Enum):
    HOSTNAME = auto()
    PROMOTED_TO_WRITER = auto()
    PROMOTED_TO_READER = auto()
    WENT_UP = auto()
    WENT_DOWN = auto()
    CONNECTION_OBJECT_CHANGED = auto()
    INITIAL_CONNECTION = auto()
    NODE_ADDED = auto()
    NODE_CHANGED = auto()
    NODE_DELETED = auto()


class AuroraStaleDnsHelper:
    RETRIES: int = 3

    def __init__(self, plugin_service: PluginService) -> None:
        self._plugin_service = plugin_service
        self._rds_helper = RdsUtils()
        self.writer_host_info: Optional[HostInfo] = None
        self.writer_host_address: Optional[str] = None

    def get_verified_connection(self, is_initial_connection: bool, host_list_provider_service: HostListProviderService, host_info: HostInfo,
                                props: Properties, connect_func: Callable) -> Connection:
        if not self._rds_helper.is_writer_cluster_dns(host_info.host):
            return connect_func()

        conn: Connection = connect_func()

        cluster_inet_address: Optional[str] = None
        try:
            cluster_inet_address = socket.gethostbyname(host_info.host)
        except Exception:
            pass

        host_inet_address: Optional[str] = cluster_inet_address

        logger.debug(Messages.get_formatted("AuroraStaleDnsHelper.ClusterEndpointDns", host_info.host, host_inet_address))

        if cluster_inet_address is None:
            return conn

        if self._plugin_service.get_host_role(conn) == HostRole.READER:
            self._plugin_service.force_refresh_host_list(conn)
        else:
            self._plugin_service.refresh_host_list(conn)

        logger.debug(LogUtils.log_topology(self._plugin_service.hosts))

        if self.writer_host_info is None:
            writer_candidate: Optional[HostInfo] = self.get_writer()
            if writer_candidate is not None and self._rds_helper.is_rds_cluster_dns(writer_candidate.host):
                return conn

            self.writer_host_info = writer_candidate

        logger.debug(Messages.get_formatted("AuroraStaleDnsHelper.WriterHostSpec", self.writer_host_info))

        if self.writer_host_info is None:
            return conn

        if self.writer_host_address is None:
            try:
                self.writer_host_address = socket.gethostbyname(self.writer_host_info.host)
            except Exception:
                pass

        logger.debug(Messages.get_formatted("AuroraStaleDnsHelper.WriterInetAddress", self.writer_host_address))

        if self.writer_host_address is None:
            return conn

        if not self.writer_host_address == cluster_inet_address:
            logger.debug(Messages.get_formatted("AuroraStaleDnsHelper.StaleDnsDetected", self.writer_host_info))

            writer_conn: Connection = self._plugin_service.connect(self.writer_host_info, props)
            if is_initial_connection:
                host_list_provider_service.initial_connection_host_info = self.writer_host_info

            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                return writer_conn

        return conn

    def notify_node_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        if self.writer_host_info is None:
            return
        msg = "Changes: "
        for key in changes:
            msg += f"\n\t[{key}]: {changes[key]}"
        logger.debug(msg)
        if key == self.writer_host_info.url and NodeChangeOptions.PROMOTED_TO_READER in changes[key]:
            logger.debug(Messages.get_formatted("AuroraStaleDnsHelper.Reset"))
            self.writer_host_info = None
            self.writer_host_address = ""

    def get_writer(self) -> Optional[HostInfo]:
        for host in self._plugin_service.hosts:
            if host.role == HostRole.WRITER:
                return host
        return None

    def is_read_only(self, conn: Connection, query: str) -> bool:
        for _ in range(self.RETRIES):
            try:
                with closing(conn.cursor()) as aws_cursor:
                    aws_cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
                    if aws_cursor.fetchone() is not None:
                        return True
            except Exception as ex:
                if not self._plugin_service.is_network_exception(ex):
                    return False

        return False


class AuroraStaleDnsPlugin(Plugin):

    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider",
                                     "connect",
                                     "force_connect",
                                     "notify_connection_changed"}

    def __init__(self, plugin_service: PluginService, properties: Properties) -> None:
        self._plugin_service = plugin_service
        self._rds_helper = AuroraStaleDnsHelper(self._plugin_service)

        AuroraStaleDnsPlugin._SUBSCRIBED_METHODS.update(self._plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def connect(self,
                host: HostInfo,
                properties: Properties,
                is_initial_connection: bool,
                connect_func: Callable) -> Connection:
        return self._rds_helper.get_verified_connection(is_initial_connection, self._host_list_provider_service, host, properties,
                                                        connect_func)

    def force_connect(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._rds_helper.get_verified_connection(is_initial_connection, self._host_list_provider_service, host, properties,
                                                        force_connect_func)

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        try:
            self._plugin_service.refresh_host_list()
        except Exception:
            pass

        return execute_func()

    def init_host_provider(
            self,
            properties: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):

        self._host_list_provider_service = host_list_provider_service

        if self._host_list_provider_service.is_static_host_list_provider():
            raise Exception(Messages.get_formatted("AuroraStaleDnsPlugin.RequireDynamicProvider"))

        init_host_provider_func()

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        self._rds_helper.notify_node_list_changed(changes)


class AuroraStaleDnsPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraStaleDnsPlugin(plugin_service, props)
