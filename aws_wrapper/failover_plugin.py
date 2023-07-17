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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_wrapper.failover_result import (ReaderFailoverResult,
                                             WriterFailoverResult)
    from aws_wrapper.plugin_service import PluginService

from enum import Enum, auto
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Set

from aws_wrapper.host_list_provider import (AuroraHostListProvider,
                                            HostListProvider,
                                            HostListProviderService)
from aws_wrapper.hostinfo import HostInfo, HostRole
from aws_wrapper.pep249 import (Error, FailoverSuccessError,
                                TransactionResolutionUnknownError)
from aws_wrapper.plugin import Plugin
from aws_wrapper.reader_failover_handler import (ReaderFailoverHandler,
                                                 ReaderFailoverHandlerImpl)
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.notifications import (ConnectionEvent,
                                             NodeChangeOptions,
                                             OldConnectionSuggestedAction)
from aws_wrapper.utils.rdsutils import RdsUtils
from aws_wrapper.writer_failover_handler import (WriterFailoverHandler,
                                                 WriterFailoverHandlerImpl)

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.utils.properties import Properties

logger = getLogger(__name__)


class FailoverMode(Enum):
    STRICT_WRITER = auto()
    STRICT_READER = auto()
    READER_OR_WRITER = auto()


class FailoverPlugin(Plugin):
    def __init__(self, plugin_service: PluginService, props: Properties):
        self.plugin_service = plugin_service
        self.properties = props
        self.host_list_provider_service: HostListProviderService
        self.reader_failover_handler: ReaderFailoverHandler
        self.writer_failover_handler: WriterFailoverHandler
        self.failover_mode = None
        self.is_in_transaction = False
        self.is_closed = False
        self.closed_explicitly = False
        self.rds_utils = RdsUtils()

        self.enable_failover_setting = False
        # TODO: Re-evaluate how we handle property handling
        if props.get("Enable Failover"):
            self.enable_failover_setting = True

    def init_host_provider(
            self,
            properties: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable,
            host_list_provider: Optional[HostListProvider] = None,
            reader_failover_handler: Optional[ReaderFailoverHandler] = None,
            writer_failover_handler: Optional[WriterFailoverHandler] = None):

        self.host_list_provider_service = host_list_provider_service
        if not self.enable_failover_setting:
            return

        if self.host_list_provider_service.is_static_host_list_provider():
            if host_list_provider is None:
                self.host_list_provider_service.host_list_provider = \
                    AuroraHostListProvider(self.host_list_provider_service, properties)
            else:
                self.host_list_provider_service.host_list_provider = host_list_provider

        if reader_failover_handler is None:
            self.reader_failover_handler = ReaderFailoverHandlerImpl(self.plugin_service, self.properties)
        else:
            self.reader_failover_handler = reader_failover_handler

        if writer_failover_handler is None:
            self.writer_failover_handler = WriterFailoverHandlerImpl()
        else:
            self.writer_failover_handler = writer_failover_handler

        init_host_provider_func()

        # TODO: Figure out how to parse failover mode from user properties

    def subscribed_methods(self):  # -> Set[str]:
        ...

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        if not self.enable_failover_setting or self._can_direct_execute(method_name):
            return execute_func()

        if self.is_closed and not self._allowed_on_closed_connection(method_name):
            self._invalid_invocation_on_closed_connection()

        self._update_topology(False)

        return execute_func()

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        return OldConnectionSuggestedAction.NO_OPINION

    def notify_node_list_changed(self, changes: Dict[str, Set[NodeChangeOptions]]):
        if not self.enable_failover_setting:
            return

        current_host = self.plugin_service.current_host_info
        if current_host is not None:
            if self._is_node_still_valid(current_host.url, changes):
                return

            for alias in current_host.aliases:
                if self._is_node_still_valid(alias + '/', changes):
                    return

            logger.debug(Messages.get_formatted("Failover.InvalidNode", current_host))

    def connect(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect_internal(host, properties, is_initial_connection, connect_func)

    def force_connect(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect_internal(host, properties, is_initial_connection, force_connect_func)

    def _connect_internal(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        conn: Connection = self._get_verified_connection(is_initial_connection, host, properties, connect_func)
        if is_initial_connection:
            self.plugin_service.refresh_host_list(conn)
        return conn

    def _get_verified_connection(
            self,
            is_initial_connection: bool,
            host: HostInfo,
            properties: Properties,
            connect_func: Callable) -> Connection:
        conn: Connection = connect_func()
        if not self.rds_utils.is_writer_cluster_dns(host.host):
            return conn
        ...  # TODO: Implement stale dsn logic
        return conn

    def _update_topology(self, force_update: bool):
        conn = self.plugin_service.current_connection

        if not self._is_failover_enabled() or conn is None or conn.is_closed():
            return

        if force_update:
            self.plugin_service.force_refresh_host_list()
        else:
            self.plugin_service.refresh_host_list()

    def _transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        ...  # TODO: Figure out what state information needs to be transferred

    def _failover(self, failed_host: HostInfo):
        if self.failover_mode == FailoverMode.STRICT_WRITER:
            self._failover_writer()
        else:
            self._failover_reader(failed_host)

        if self.is_in_transaction or self.plugin_service.is_in_transaction:
            error_msg = Messages.get("Failover.TransactionResolutionUnknownError")
            logger.debug(error_msg)
            raise TransactionResolutionUnknownError(error_msg)
        else:
            error_msg = Messages.get("Failover.ConnectionChangedError")
            logger.debug(error_msg)
            raise FailoverSuccessError(error_msg)

    def _failover_reader(self, failed_host: HostInfo):
        logger.debug(Messages.get("Failover.StartReaderFailover"))

        result: ReaderFailoverResult = self.reader_failover_handler.failover(self.plugin_service.hosts, failed_host)

        if result is not None and result.exception is not None:
            raise result.exception
        elif result is None or not result.is_connected:
            raise Error(Messages.get("Failover.UnableToConnectToReader"))

        self.plugin_service.set_current_connection(result.connection, result.new_host)

        if self.plugin_service.current_host_info is not None:
            old_aliases = self.plugin_service.current_host_info.aliases
            self.plugin_service.current_host_info.remove_alias(old_aliases)

        self._update_topology(True)

        logger.debug(Messages.get_formatted("Failover.EstablishedConnection", self.plugin_service.current_host_info))

    def _failover_writer(self):
        logger.debug(Messages.get("Failover.StartWriterFailover"))

        result: WriterFailoverResult = self.writer_failover_handler.failover(self.plugin_service.hosts)

        if result is not None and result.exception is not None:
            raise result.exception
        elif result is None or not result.is_connected:
            raise Error(Messages.get("Failover.UnableToConnectToWriter"))

        writer_host = self._get_writer(result.topology)
        self.plugin_service.set_current_connection(result.connection, writer_host)

        logger.debug(Messages.get_formatted("Failover.EstablishedConnection", self.plugin_service.current_host_info))

        self.plugin_service.refresh_host_list()

    def _invalidate_current_connection(self):
        conn = self.plugin_service.current_connection
        if conn is None:
            return

        if self.plugin_service.is_in_transaction:
            self.is_in_transaction = True
            try:
                conn.rollback()
            except Exception:
                pass  # Swallow this exception

        if not conn.is_closed():
            try:
                conn.close()
            except Exception:
                pass  # Swallow this exception

    def _invalid_invocation_on_closed_connection(self):
        if not self.closed_explicitly:
            self.is_closed = False
            self._pick_new_connection()

            error_msg = Messages.get("Failover.ConnectionChangedError")
            logger.debug(error_msg)
            raise FailoverSuccessError(error_msg)
        else:
            raise Error(Messages.get("Failover.NoOperationsAfterConnectionClosed"))

    def _pick_new_connection(self):
        if self.is_closed and self.closed_explicitly:
            logger.debug(Messages.get("Failover.TransactionResolutionUnknownError"))
            return

        if self.plugin_service.current_connection is None and not self._should_attempt_reader_connection():
            writer = self._get_current_writer()
            try:
                self._connect_to(writer)
            except Exception:
                self._failover(writer)
        else:
            self._failover(self.plugin_service.current_host_info)

    def _connect_to(self, host: HostInfo):
        try:
            connection_for_host = self.plugin_service.connect(host, self.properties)
            current_connection = self.plugin_service.current_connection

            if connection_for_host is not None and current_connection is not None and \
                    current_connection != connection_for_host:
                self._transfer_session_state(current_connection, connection_for_host)
                self._invalidate_current_connection()

            self.plugin_service.set_current_connection(connection_for_host, host)
            logger.debug(Messages.get_formatted("Failover.EstablishedConnection", host))
        except Exception as ex:
            if self.plugin_service is not None:
                msg = "Connection to "
                if host.role == HostRole.WRITER:
                    msg += "writer"
                else:
                    msg += "reader"
                msg += f" host '{host.url()}' failed"
                logger.debug(msg)
            raise ex

    def _should_attempt_reader_connection(self) -> bool:
        topology = self.plugin_service.hosts
        if topology is None or self.failover_mode == FailoverMode.STRICT_WRITER:
            return False

        for host in topology:
            if host.role == HostRole.READER:
                return True

        return False

    def _is_failover_enabled(self) -> bool:
        return self.enable_failover_setting and \
            self.plugin_service.hosts is not None and \
            len(self.plugin_service.hosts) > 0

    def _get_current_writer(self) -> Optional[HostInfo]:
        topology = self.plugin_service.hosts
        if topology is None:
            return None

        return self._get_writer(topology)

    @classmethod
    def _get_writer(cls, hosts: List[HostInfo]) -> Optional[HostInfo]:
        for host in hosts:
            if host.role == HostRole.WRITER:
                return host

        return None

    @classmethod
    def _is_node_still_valid(cls, node: str, changes: Dict[str, Set[NodeChangeOptions]]):
        if node in changes:
            options = changes.get(node)
            return options is not None and \
                NodeChangeOptions.NODE_DELETED not in options and NodeChangeOptions.WENT_DOWN not in options

        return True

    @classmethod
    def _can_direct_execute(cls, method_name):
        return method_name == "Connection.close" or \
            method_name == "Connection.abort" or \
            method_name == "Connection.isClosed"

    @classmethod
    def _allowed_on_closed_connection(cls, method_name: str):
        return method_name == "Connection.getAutoCommit" or \
            method_name == "Connection.getCatalog" or \
            method_name == "Connection.getSchema" or \
            method_name == "Connection.getTransactionIsolation"
