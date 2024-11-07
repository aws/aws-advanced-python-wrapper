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

from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.failover_result import ReaderFailoverResult, WriterFailoverResult
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

from typing import Any, Callable, Dict, Optional, Set

from aws_advanced_python_wrapper import LogUtils
from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.reader_failover_handler import (
    ReaderFailoverHandler, ReaderFailoverHandlerImpl)
from aws_advanced_python_wrapper.stale_dns_plugin import StaleDnsHelper
from aws_advanced_python_wrapper.utils.failover_mode import (FailoverMode,
                                                             get_failover_mode)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel
from aws_advanced_python_wrapper.writer_failover_handler import (
    WriterFailoverHandler, WriterFailoverHandlerImpl)

logger = Logger(__name__)


class FailoverPlugin(Plugin):
    """
    This plugin provides cluster-aware failover features.
    The plugin switches connections upon detecting communication related exceptions and/or cluster topology changes.
    """
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider",
                                     "connect",
                                     "force_connect",
                                     "notify_host_list_changed"}

    _METHODS_REQUIRE_UPDATED_TOPOLOGY: Set[str] = {
        "Connection.commit",
        "Connection.autocommit",
        "Connection.autocommit_setter",
        "Connection.rollback",
        "Connection.cursor",
        "Cursor.callproc",
        "Cursor.execute"
    }

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._host_list_provider_service: HostListProviderService
        self._reader_failover_handler: ReaderFailoverHandler
        self._writer_failover_handler: WriterFailoverHandler

        self._enable_failover_setting = WrapperProperties.ENABLE_FAILOVER.get_bool(self._properties)
        self._failover_timeout_sec = WrapperProperties.FAILOVER_TIMEOUT_SEC.get_float(self._properties)
        self._failover_cluster_topology_refresh_rate_sec = WrapperProperties.FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_SEC.get_float(
            self._properties)
        self._failover_writer_reconnect_interval_sec = WrapperProperties.FAILOVER_WRITER_RECONNECT_INTERVAL_SEC.get_float(
            self._properties)
        self._failover_reader_connect_timeout_sec = WrapperProperties.FAILOVER_READER_CONNECT_TIMEOUT_SEC.get_float(
            self._properties)
        self._telemetry_failover_additional_top_trace_setting = (
            WrapperProperties.TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.get_bool(self._properties))
        self._failover_mode: FailoverMode
        self._is_in_transaction: bool = False
        self._is_closed: bool = False
        self._closed_explicitly: bool = False
        self._last_exception: Optional[Exception] = None
        self._rds_utils = RdsUtils()
        self._rds_url_type: RdsUrlType = self._rds_utils.identify_rds_type(self._properties.get("host"))
        self._stale_dns_helper: StaleDnsHelper = StaleDnsHelper(plugin_service)
        self._saved_read_only_status: bool = False
        self._saved_auto_commit_status: bool = False

        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._failover_writer_triggered_counter = telemetry_factory.create_counter("writer_failover.triggered.count")
        self._failover_writer_success_counter = telemetry_factory.create_counter(
            "writer_failover.completed.success.count")
        self._failover_writer_failed_counter = telemetry_factory.create_counter(
            "writer_failover.completed.failed.count")
        self._failover_reader_triggered_counter = telemetry_factory.create_counter("reader_failover.triggered.count")
        self._failover_reader_success_counter = telemetry_factory.create_counter(
            "reader_failover.completed.success.count")
        self._failover_reader_failed_counter = telemetry_factory.create_counter(
            "reader_failover.completed.failed.count")

        FailoverPlugin._SUBSCRIBED_METHODS.update(self._plugin_service.network_bound_methods)

    def init_host_provider(
            self,
            properties: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):

        self._host_list_provider_service = host_list_provider_service
        if not self._enable_failover_setting:
            return

        self._reader_failover_handler = ReaderFailoverHandlerImpl(self._plugin_service, self._properties,
                                                                  self._failover_timeout_sec,
                                                                  self._failover_reader_connect_timeout_sec)
        self._writer_failover_handler = WriterFailoverHandlerImpl(self._plugin_service, self._reader_failover_handler,
                                                                  self._properties,
                                                                  self._failover_timeout_sec,
                                                                  self._failover_cluster_topology_refresh_rate_sec,
                                                                  self._failover_writer_reconnect_interval_sec)

        init_host_provider_func()

        failover_mode = get_failover_mode(self._properties)
        if failover_mode is None:
            if self._rds_url_type.is_rds_cluster:
                if self._rds_url_type == RdsUrlType.RDS_READER_CLUSTER:
                    failover_mode = FailoverMode.READER_OR_WRITER
                else:
                    failover_mode = FailoverMode.STRICT_WRITER
            else:
                failover_mode = FailoverMode.STRICT_WRITER

        self._failover_mode = failover_mode
        logger.debug("FailoverPlugin.ParameterValue", "FAILOVER_MODE", self._failover_mode)

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._is_in_transaction = self._plugin_service.is_in_transaction

        if not self._enable_failover_setting or self._can_direct_execute(method_name):
            return execute_func()

        if self._is_closed and not self._allowed_on_closed_connection(method_name):
            self._invalid_invocation_on_closed_connection()

        try:
            if self._requires_update_topology(method_name):
                self._update_topology(False)
            return execute_func()
        except Exception as ex:
            logger.debug("FailoverPlugin.DetectedException", str(ex))
            if self._last_exception != ex and self._should_exception_trigger_connection_switch(ex):
                self._invalidate_current_connection()
                if self._plugin_service.current_host_info is not None:
                    self._plugin_service.set_availability(
                        self._plugin_service.current_host_info.aliases, HostAvailability.UNAVAILABLE)

                self._pick_new_connection()
                self._last_exception = ex
            raise AwsWrapperError(Messages.get_formatted("FailoverPlugin.DetectedException", str(ex))) from ex

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        if not self._enable_failover_setting:
            return

        msg = ""
        for key in changes:
            msg += f"\n\tHost '{key}': {changes[key]}"
        logger.debug("FailoverPlugin.Changes", msg)

        current_host = self._plugin_service.current_host_info
        if current_host is not None:
            if self._is_host_still_valid(current_host.url, changes):
                return

            for alias in current_host.aliases:
                if self._is_host_still_valid(alias + '/', changes):
                    return

            logger.debug("FailoverPlugin.InvalidHost", current_host)

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect(host_info, props, is_initial_connection, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(host_info, props, is_initial_connection, force_connect_func)

    def _connect(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        conn: Connection = self._stale_dns_helper.get_verified_connection(is_initial_connection,
                                                                          self._host_list_provider_service, host,
                                                                          properties,
                                                                          connect_func)

        if is_initial_connection:
            self._plugin_service.refresh_host_list(conn)

        return conn

    def _update_topology(self, force_update: bool):
        if not self._is_failover_enabled():
            return

        conn = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if conn is None or (driver_dialect is not None and driver_dialect.is_closed(conn)):
            return

        if force_update:
            self._plugin_service.force_refresh_host_list()
        else:
            self._plugin_service.refresh_host_list()

    def _failover(self, failed_host: Optional[HostInfo]):
        """
        Initiates the failover procedure. This process tries to establish a new connection to an instance in the topology.

        :param failed_host: The host with network errors.
        """
        if failed_host is not None:
            self._plugin_service.set_availability(failed_host.as_aliases(), HostAvailability.UNAVAILABLE)

        if self._failover_mode == FailoverMode.STRICT_WRITER:
            self._failover_writer()
        else:
            self._failover_reader(failed_host)

        if self._is_in_transaction or self._plugin_service.is_in_transaction:
            self._plugin_service.update_in_transaction(False)

            error_msg = "FailoverPlugin.TransactionResolutionUnknownError"
            logger.warning(error_msg)
            raise TransactionResolutionUnknownError(Messages.get(error_msg))
        else:
            error_msg = "FailoverPlugin.ConnectionChangedError"
            logger.error(error_msg)
            raise FailoverSuccessError(Messages.get(error_msg))

    def _failover_reader(self, failed_host: Optional[HostInfo]):
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        context = telemetry_factory.open_telemetry_context("failover to replica", TelemetryTraceLevel.NESTED)
        self._failover_reader_triggered_counter.inc()

        try:
            logger.info("FailoverPlugin.StartReaderFailover")

            old_aliases = None
            if self._plugin_service.current_host_info is not None:
                old_aliases = self._plugin_service.current_host_info.aliases

            if failed_host is not None and failed_host.get_raw_availability() != HostAvailability.AVAILABLE:
                failed_host = None

            result: ReaderFailoverResult = self._reader_failover_handler.failover(self._plugin_service.hosts,
                                                                                  failed_host)

            if result is None or not result.is_connected:
                raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToConnectToReader"))
            else:
                if result.exception is not None:
                    raise result.exception
                if result.connection is not None and result.new_host is not None:
                    self._plugin_service.set_current_connection(result.connection, result.new_host)

            if self._plugin_service.current_host_info is not None and old_aliases is not None and len(old_aliases) > 0:
                self._plugin_service.current_host_info.remove_alias(old_aliases)

            self._update_topology(True)

            logger.info("FailoverPlugin.EstablishedConnection", self._plugin_service.current_host_info)

            self._failover_reader_success_counter.inc()
        except FailoverSuccessError as fse:
            context.set_success(True)
            context.set_exception(fse)
            self._failover_reader_success_counter.inc()
            raise fse
        except Exception as ex:
            context.set_success(False)
            context.set_exception(ex)
            self._failover_reader_failed_counter.inc()
            raise ex
        finally:
            context.close_context()
            if self._telemetry_failover_additional_top_trace_setting:
                telemetry_factory.post_copy(context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    def _failover_writer(self):
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        context = telemetry_factory.open_telemetry_context("failover to writer host", TelemetryTraceLevel.NESTED)
        self._failover_writer_triggered_counter.inc()

        try:
            logger.info("FailoverPlugin.StartWriterFailover")

            result: WriterFailoverResult = self._writer_failover_handler.failover(self._plugin_service.all_hosts)
            if result is not None and result.exception is not None:
                raise result.exception
            elif result is None or not result.is_connected:
                raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToConnectToWriter"))

            writer_host = self._get_writer(result.topology)
            allowed_hosts = self._plugin_service.hosts
            allowed_hostnames = [host.host for host in allowed_hosts]
            if writer_host.host not in allowed_hostnames:
                raise FailoverFailedError(
                    Messages.get_formatted(
                        "FailoverPlugin.NewWriterNotAllowed",
                        "<null>" if writer_host is None else writer_host.host,
                        LogUtils.log_topology(allowed_hosts)))

            self._plugin_service.set_current_connection(result.new_connection, writer_host)
            logger.info("FailoverPlugin.EstablishedConnection", self._plugin_service.current_host_info)

            self._plugin_service.refresh_host_list()

            self._failover_writer_success_counter.inc()
        except FailoverSuccessError as fse:
            context.set_success(True)
            context.set_exception(fse)
            self._failover_writer_success_counter.inc()
            raise fse
        except Exception as ex:
            context.set_success(False)
            context.set_exception(ex)
            self._failover_writer_failed_counter.inc()
            raise ex
        finally:
            context.close_context()
            if self._telemetry_failover_additional_top_trace_setting:
                telemetry_factory.post_copy(context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    def _invalidate_current_connection(self):
        """
        Invalidate the current connection before switching to a new connection.
        """
        conn = self._plugin_service.current_connection
        if conn is None:
            return

        if self._plugin_service.is_in_transaction:
            self._plugin_service.update_in_transaction(True)
            try:
                conn.rollback()
            except Exception:
                pass

        driver_dialect = self._plugin_service.driver_dialect
        if driver_dialect is not None and not driver_dialect.is_closed(conn):
            try:
                conn.close()
            except Exception:
                pass

    def _invalid_invocation_on_closed_connection(self):
        if not self._closed_explicitly:
            self._is_closed = False
            self._pick_new_connection()

            error_msg = "FailoverPlugin.ConnectionChangedError"
            logger.debug(error_msg)
            raise FailoverSuccessError(Messages.get(error_msg))
        else:
            raise AwsWrapperError(Messages.get("FailoverPlugin.NoOperationsAfterConnectionClosed"))

    def _pick_new_connection(self):
        if self._is_closed and self._closed_explicitly:
            logger.debug("FailoverPlugin.NoOperationsAfterConnectionClosed")
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
        """
        Connects this dynamic failover connection proxy to the specified host.

        :param host: The host to connect to.
        """
        try:
            connection_for_host = self._plugin_service.connect(host, self._properties)
            self._plugin_service.set_current_connection(connection_for_host, host)
            self._plugin_service.update_in_transaction(False)

            logger.info("FailoverPlugin.EstablishedConnection", host)
        except Exception as ex:
            if self._plugin_service is not None:
                logger.debug("FailoverPlugin.ConnectionToHostFailed",
                             'writer' if host.role == HostRole.WRITER else 'reader', host.url)
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
            self._rds_url_type != RdsUrlType.RDS_PROXY and \
            self._plugin_service.all_hosts is not None and \
            len(self._plugin_service.all_hosts) > 0

    def _get_current_writer(self) -> Optional[HostInfo]:
        topology = self._plugin_service.all_hosts
        if topology is None:
            return None

        return self._get_writer(topology)

    def _should_exception_trigger_connection_switch(self, ex: Exception) -> bool:
        """
        Checks whether the given exception is a network exception and should trigger the failover process.

        :param ex: The exception raised during the method call.
        :return: `True` if the exception should trigger failover. `False` otherwise.
        """
        if not self._is_failover_enabled():
            logger.debug("FailoverPlugin.FailoverDisabled")
            return False

        return self._plugin_service.is_network_exception(ex)

    @staticmethod
    def _get_writer(hosts: Tuple[HostInfo, ...]) -> Optional[HostInfo]:
        for host in hosts:
            if host.role == HostRole.WRITER:
                return host

        return None

    @staticmethod
    def _is_host_still_valid(host: str, changes: Dict[str, Set[HostEvent]]):
        if host in changes:
            options = changes.get(host)
            return options is not None and \
                HostEvent.HOST_DELETED not in options and HostEvent.WENT_DOWN not in options

        return True

    @staticmethod
    def _can_direct_execute(method_name):
        """
        Check whether the method provided can be executed directly without the failover functionality.

        :param method_name: The name of the method that is being called.
        :return: `True` if the method can be executed directly; `False` otherwise.
        """
        return method_name == "Connection.close" or \
            method_name == "Connection.is_closed" or \
            method_name == "Cursor.close"

    @staticmethod
    def _allowed_on_closed_connection(method_name: str):
        """
        Checks if the given method is allowed on closed connections.

        :param method_name: The method being executed at the moment.
        :return: `True` if the given method is allowed on closed connections.
        """
        return method_name == "Connection.autocommit"

    def _requires_update_topology(self, method_name: str):
        """
        Not all method calls require an updated topology, especially ones that don't require network connection.
        Updating the topology may execute the topology query in the middle of another query execution,
        this introduces overhead and may not be supported by all drivers.

        :param method_name: The method being executed at the moment.
        :return: `True` if the given method requires an updated topology before executing. `False` otherwise.
        """
        return method_name in FailoverPlugin._METHODS_REQUIRE_UPDATED_TOPOLOGY


class FailoverPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return FailoverPlugin(plugin_service, props)
