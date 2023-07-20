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
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService

from enum import Enum, auto
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Set

from aws_wrapper.errors import AwsWrapperError, wrap_exception
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
from aws_wrapper.utils.notifications import HostEvent
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rdsutils import RdsUtils
from aws_wrapper.writer_failover_handler import (WriterFailoverHandler,
                                                 WriterFailoverHandlerImpl)

logger = getLogger(__name__)


class FailoverMode(Enum):
    STRICT_WRITER = auto()
    STRICT_READER = auto()
    READER_OR_WRITER = auto()


class FailoverPlugin(Plugin):
    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._host_list_provider_service: HostListProviderService
        self._reader_failover_handler: ReaderFailoverHandler
        self._writer_failover_handler: WriterFailoverHandler
        self._enable_failover_setting = WrapperProperties.ENABLE_FAILOVER.get_bool(self._properties)
        self._failover_mode: FailoverMode
        self._is_in_transaction = False
        self._is_closed = False
        self._closed_explicitly = False
        self._rds_utils = RdsUtils()

    def init_host_provider(
            self,
            properties: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable,
            host_list_provider: Optional[HostListProvider] = None,
            reader_failover_handler: Optional[ReaderFailoverHandler] = None,
            writer_failover_handler: Optional[WriterFailoverHandler] = None):

        self._host_list_provider_service = host_list_provider_service
        if not self._enable_failover_setting:
            return

        if self._host_list_provider_service.is_static_host_list_provider():
            if host_list_provider is None:
                self._host_list_provider_service.host_list_provider = \
                    AuroraHostListProvider(self._host_list_provider_service, properties)
            else:
                self._host_list_provider_service.host_list_provider = host_list_provider

        if reader_failover_handler is None:
            self._reader_failover_handler = ReaderFailoverHandlerImpl(self._plugin_service, self._properties)
        else:
            self._reader_failover_handler = reader_failover_handler

        if writer_failover_handler is None:
            self._writer_failover_handler = WriterFailoverHandlerImpl()
        else:
            self._writer_failover_handler = writer_failover_handler

        init_host_provider_func()

        failover_mode = WrapperProperties.FAILOVER_MODE.get(self._properties)
        if failover_mode is None:
            self._failover_mode = FailoverMode.STRICT_WRITER
        else:
            failover_mode = failover_mode.lower()
            # TODO: reconsider the exact format we expect from the user here
            if failover_mode == "strict_writer":
                self._failover_mode = FailoverMode.STRICT_WRITER
            elif failover_mode == "strict_reader":
                self._failover_mode = FailoverMode.STRICT_READER
            else:
                self._failover_mode = FailoverMode.READER_OR_WRITER

        logger.debug(Messages.get_formatted("Failover.ParameterValue", "FAILOVER_MODE", self._failover_mode))

    def subscribed_methods(self):  # -> Set[str]:
        ...

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: tuple) -> Any:
        if not self._enable_failover_setting or self._can_direct_execute(method_name):
            return execute_func()

        if self._is_closed and not self._allowed_on_closed_connection(method_name):
            try:
                self._invalid_invocation_on_closed_connection()
            except Exception as ex:
                wrap_exception(target, ex)

        result = None
        try:
            self._update_topology(False)
            result = execute_func()
        except Exception as ex:
            # TODO: add extra exception handling logic here as appropriate
            raise ex

        return result

    def notify_node_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        if not self._enable_failover_setting:
            return

        current_host = self._plugin_service.current_host_info
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
        # TODO: replace with call to self._stale_dns_helper.get_verified_connection
        conn: Connection = connect_func()
        if is_initial_connection:
            self._plugin_service.refresh_host_list(conn)
        return conn

    def _update_topology(self, force_update: bool):
        if not self._is_failover_enabled():
            return

        conn = self._plugin_service.current_connection
        dialect = self._plugin_service.dialect
        if conn is None or (dialect is not None and dialect.is_closed(conn)):
            return

        if force_update:
            self._plugin_service.force_refresh_host_list()
        else:
            self._plugin_service.refresh_host_list()

    def _transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        ...  # TODO: Figure out what state information needs to be transferred

    def _failover(self, failed_host: HostInfo):
        if self._failover_mode == FailoverMode.STRICT_WRITER:
            self._failover_writer()
        else:
            self._failover_reader(failed_host)

        if self._is_in_transaction or self._plugin_service.is_in_transaction:
            self._plugin_service.is_in_transaction = False

            error_msg = Messages.get("Failover.TransactionResolutionUnknownError")
            logger.warning(error_msg)
            raise TransactionResolutionUnknownError(error_msg)
        else:
            error_msg = Messages.get("Failover.ConnectionChangedError")
            logger.error(error_msg)
            raise FailoverSuccessError(error_msg)

    def _failover_reader(self, failed_host: HostInfo):
        logger.debug(Messages.get("Failover.StartReaderFailover"))

        old_aliases = None
        if self._plugin_service.current_host_info is not None:
            old_aliases = self._plugin_service.current_host_info.aliases

        result: ReaderFailoverResult = self._reader_failover_handler.failover(self._plugin_service.hosts, failed_host)

        if result is not None and result.exception is not None:
            raise result.exception
        elif result is None or not result.is_connected:
            raise Error(Messages.get("Failover.UnableToConnectToReader"))

        if result.connection is not None and result.new_host is not None:
            self._plugin_service.set_current_connection(result.connection, result.new_host)

        if self._plugin_service.current_host_info is not None:
            self._plugin_service.current_host_info.remove_alias(old_aliases)

        self._update_topology(True)

        logger.debug(Messages.get_formatted("Failover.EstablishedConnection", self._plugin_service.current_host_info))

    def _failover_writer(self):
        logger.debug(Messages.get("Failover.StartWriterFailover"))

        result: WriterFailoverResult = self._writer_failover_handler.failover(self._plugin_service.hosts)

        if result is not None and result.exception is not None:
            raise result.exception
        elif result is None or not result.is_connected:
            raise Error(Messages.get("Failover.UnableToConnectToWriter"))

        writer_host = self._get_writer(result.topology)
        self._plugin_service.set_current_connection(result.connection, writer_host)

        logger.debug(Messages.get_formatted("Failover.EstablishedConnection", self._plugin_service.current_host_info))

        self._plugin_service.refresh_host_list()

    def _invalidate_current_connection(self):
        conn = self._plugin_service.current_connection
        if conn is None:
            return

        if self._plugin_service.is_in_transaction:
            self._is_in_transaction = True
            try:
                conn.rollback()
            except Exception:
                pass  # Swallow this exception

        dialect = self._plugin_service.dialect
        if dialect is not None and not dialect.is_closed(conn):
            try:
                conn.close()
            except Exception:
                pass  # Swallow this exception

    def _invalid_invocation_on_closed_connection(self):
        if not self._closed_explicitly:
            self._is_closed = False
            self._pick_new_connection()

            error_msg = Messages.get("Failover.ConnectionChangedError")
            logger.debug(error_msg)
            raise FailoverSuccessError(error_msg)
        else:
            raise AwsWrapperError(Messages.get("Failover.NoOperationsAfterConnectionClosed"))

    def _pick_new_connection(self):
        if self._is_closed and self._closed_explicitly:
            logger.debug(Messages.get("Failover.NoOperationsAfterConnectionClosed"))
            return

        if self._plugin_service.current_connection is None and not self._should_attempt_reader_connection():
            writer = self._get_current_writer()
            try:
                self._connect_to(writer)
            except Exception:
                self._failover(writer)
        else:
            self._failover(self._plugin_service.current_host_info)

    def _connect_to(self, host: HostInfo):
        try:
            connection_for_host = self._plugin_service.connect(host, self._properties)
            current_connection = self._plugin_service.current_connection

            if connection_for_host is not None and current_connection is not None and \
                    current_connection != connection_for_host:
                self._transfer_session_state(current_connection, connection_for_host)
                self._invalidate_current_connection()

            self._plugin_service.set_current_connection(connection_for_host, host)
            self._plugin_service.is_in_transaction = False

            logger.debug(Messages.get_formatted("Failover.EstablishedConnection", host))
        except Exception as ex:
            if self._plugin_service is not None:
                msg = f"Connection to {'writer' if host.role == HostRole.WRITER else 'reader'} host {host.url} failed"
                logger.debug(msg)
            raise ex

    def _should_attempt_reader_connection(self) -> bool:
        topology = self._plugin_service.hosts
        if topology is None or self._failover_mode == FailoverMode.STRICT_WRITER:
            return False

        for host in topology:
            if host.role == HostRole.READER:
                return True

        return False

    def _is_failover_enabled(self) -> bool:
        return self._enable_failover_setting and \
            self._plugin_service.hosts is not None and \
            len(self._plugin_service.hosts) > 0

    def _get_current_writer(self) -> Optional[HostInfo]:
        topology = self._plugin_service.hosts
        if topology is None:
            return None

        return self._get_writer(topology)

    @staticmethod
    def _get_writer(hosts: List[HostInfo]) -> Optional[HostInfo]:
        for host in hosts:
            if host.role == HostRole.WRITER:
                return host

        return None

    @staticmethod
    def _is_node_still_valid(node: str, changes: Dict[str, Set[HostEvent]]):
        if node in changes:
            options = changes.get(node)
            return options is not None and \
                HostEvent.HOST_DELETED not in options and HostEvent.WENT_DOWN not in options

        return True

    @staticmethod
    def _can_direct_execute(method_name):
        return method_name == "Connection.close" or \
            method_name == "Connection.abort" or \
            method_name == "Connection.isClosed"

    @staticmethod
    def _allowed_on_closed_connection(method_name: str):
        return method_name == "Connection.getAutoCommit" or \
            method_name == "Connection.getCatalog" or \
            method_name == "Connection.getSchema" or \
            method_name == "Connection.getTransactionIsolation"
