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

import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set

from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect

from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.stale_dns_plugin import StaleDnsHelper
from aws_advanced_python_wrapper.utils.failover_mode import (FailoverMode,
                                                             get_failover_mode)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

logger = Logger(__name__)


class ReaderFailoverResult:
    def __init__(self, connection: Connection, host_info: HostInfo):
        self._connection = connection
        self._host_info = host_info

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def host_info(self) -> HostInfo:
        return self._host_info


class FailoverV2Plugin(Plugin):
    """
    Failover Plugin v.2
    This plugin provides cluster-aware failover features. The plugin switches connections upon
    detecting communication related exceptions and/or cluster topology changes.
    """

    _SUBSCRIBED_METHODS: Set[str] = {DbApiMethod.INIT_HOST_PROVIDER.method_name,
                                     DbApiMethod.CONNECT.method_name,
                                     DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props

        self._failover_timeout_sec = WrapperProperties.FAILOVER_TIMEOUT_SEC.get_int(self._properties)
        self._failover_mode: Optional[FailoverMode] = None
        self._telemetry_failover_additional_top_trace = (
            WrapperProperties.TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.get_bool(self._properties))
        strategy = WrapperProperties.FAILOVER_READER_HOST_SELECTOR_STRATEGY.get(self._properties)
        self._failover_reader_host_selector_strategy: str = strategy if strategy is not None else ""
        self._enable_connect_failover = WrapperProperties.ENABLE_CONNECT_FAILOVER.get_bool(self._properties)

        self._closed_explicitly = False
        self._is_closed = False
        self._rds_helper = RdsUtils()
        self._last_exception_dealt_with: Optional[Exception] = None
        self._is_in_transaction = False
        self._rds_url_type: Optional[RdsUrlType] = None
        self._stale_dns_helper = StaleDnsHelper(plugin_service)
        self._SUBSCRIBED_METHODS.update(self._plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def execute(self, target: type, method_name: str, execute_func: Callable, *args, **kwargs) -> Any:
        self._is_in_transaction = self._plugin_service.is_in_transaction

        if self._can_direct_execute(method_name):
            if method_name == DbApiMethod.CONNECTION_CLOSE.method_name:
                self._closed_explicitly = True
            return execute_func()

        if self._is_closed:
            self._invalid_invocation_on_closed_connection()

        try:
            result = execute_func()
        except Exception as e:
            logger.debug("FailoverPlugin.DetectedException", str(e))
            self._deal_with_original_exception(e)

        return result

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        init_host_provider_func()

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        if self._host_list_provider_service is None:
            raise AwsWrapperError("Host list provider service not initialized")

        self._init_failover_mode()

        if not self._enable_connect_failover:
            return self._stale_dns_helper.get_verified_connection(
                is_initial_connection, self._host_list_provider_service,
                host_info, props, connect_func)

        host_with_availability = None
        for host in self._plugin_service.hosts:
            if host.host == host_info.host and host.port == host_info.port:
                host_with_availability = host
                break

        conn = None
        if host_with_availability is None or host_with_availability.availability != HostAvailability.UNAVAILABLE:
            try:
                conn = self._stale_dns_helper.get_verified_connection(
                    is_initial_connection, self._host_list_provider_service,
                    host_info, props, connect_func)
            except Exception as e:
                if not self._should_exception_trigger_connection_switch(e):
                    raise e

                self._plugin_service.set_availability(host_info.as_aliases(), HostAvailability.UNAVAILABLE)

                try:
                    self._failover()
                except FailoverSuccessError:
                    conn = self._plugin_service.current_connection
        else:
            try:
                self._plugin_service.refresh_host_list()
                self._failover()
            except FailoverSuccessError:
                conn = self._plugin_service.current_connection

        if conn is None:
            raise AwsWrapperError("Unable to connect")

        if is_initial_connection:
            self._plugin_service.refresh_host_list(conn)

        return conn

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]) -> None:
        self._stale_dns_helper.notify_host_list_changed(changes)

    def _is_failover_enabled(self) -> bool:
        return (self._rds_url_type != RdsUrlType.RDS_PROXY and
                len(self._plugin_service.all_hosts) > 0)

    def _invalid_invocation_on_closed_connection(self) -> None:
        if not self._closed_explicitly:
            self._is_closed = False
            self._pick_new_connection()
            logger.info(Messages.get("Failover.connectionChangedError"))
            raise FailoverSuccessError()
        else:
            raise AwsWrapperError("No operations allowed after connection closed")

    def _deal_with_original_exception(self, original_exception: Exception) -> None:
        if (self._last_exception_dealt_with != original_exception and
                (self._should_exception_trigger_connection_switch(original_exception))):
            self._invalidate_current_connection()
            self._plugin_service.set_availability(
                self._plugin_service.current_host_info.as_aliases(), HostAvailability.UNAVAILABLE)
            self._pick_new_connection()
            self._last_exception_dealt_with = original_exception

        raise AwsWrapperError(Messages.get_formatted("FailoverPlugin.DetectedException", str(original_exception)), original_exception) \
            from original_exception

    def _failover(self) -> None:
        if self._failover_mode == FailoverMode.STRICT_WRITER:
            self._failover_writer()
        else:
            self._failover_reader()

    def _failover_reader(self) -> None:
        logger.info("FailoverPlugin.StartReaderFailover")
        telemetry_context = self._plugin_service.get_telemetry_factory().open_telemetry_context(
            "failover to replica", TelemetryTraceLevel.NESTED)

        failover_start_time = time.time()
        try:
            if not self._plugin_service.force_monitoring_refresh_host_list(False, 0):
                raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToRefreshHostList"))

            try:
                result = self._get_reader_failover_connection()
                self._plugin_service.set_current_connection(result.connection, result.host_info)
            except TimeoutError:
                raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToConnectToReader"))

            logger.info("FailoverPlugin.EstablishedConnection", self._plugin_service.current_host_info)
            self._throw_failover_success_exception()

        except FailoverSuccessError as ex:
            if telemetry_context:
                telemetry_context.set_success(True)
                telemetry_context.set_exception(ex)
            raise ex
        except Exception as ex:
            if telemetry_context:
                telemetry_context.set_success(False)
                telemetry_context.set_exception(ex)
            raise ex
        finally:
            elapsed_time = (time.time() - failover_start_time) * 1000
            logger.info("FailoverPlugin.ReaderFailoverTime", elapsed_time)
            if telemetry_context:
                telemetry_context.close_context()
                if self._telemetry_failover_additional_top_trace:
                    self._plugin_service.get_telemetry_factory().post_copy(
                        telemetry_context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    def _get_reader_failover_connection(self) -> ReaderFailoverResult:
        failover_end_time = time.time() + self._failover_timeout_sec

        hosts = self._plugin_service.hosts
        reader_candidates = [host for host in hosts if host.role == HostRole.READER]
        original_writer = next((host for host in hosts if host.role == HostRole.WRITER), None)
        is_original_writer_still_writer = False

        while time.time() < failover_end_time:
            # Try all original readers
            remaining_readers = reader_candidates.copy()
            while remaining_readers and time.time() < failover_end_time:
                try:
                    reader_candidate: Optional[HostInfo] = self._plugin_service.get_host_info_by_strategy(
                        HostRole.READER, self._failover_reader_host_selector_strategy, remaining_readers)
                except Exception:
                    break

                if reader_candidate is None:
                    break

                try:
                    candidate_conn = self._plugin_service.connect(reader_candidate, self._properties, self)
                    role = self._plugin_service.get_host_role(candidate_conn)
                    if role == HostRole.READER or self._failover_mode != FailoverMode.STRICT_READER:
                        updated_host_info = HostInfo(reader_candidate.host, reader_candidate.port, role)
                        return ReaderFailoverResult(candidate_conn, updated_host_info)

                    remaining_readers.remove(reader_candidate)
                    self._plugin_service.driver_dialect.execute(
                        DbApiMethod.CONNECTION_CLOSE.method_name, lambda: candidate_conn.close())

                    if role == HostRole.WRITER:
                        reader_candidates.remove(reader_candidate)
                except Exception:
                    remaining_readers.remove(reader_candidate)

            # Try original writer
            if (original_writer is None or time.time() > failover_end_time or (self._failover_mode == FailoverMode.STRICT_READER
                                                                               and is_original_writer_still_writer)):
                continue

            try:
                candidate_conn = self._plugin_service.connect(original_writer, self._properties, self)
                role = self._plugin_service.get_host_role(candidate_conn)
                if role == HostRole.READER or self._failover_mode != FailoverMode.STRICT_READER:
                    updated_host_info = HostInfo(original_writer.host, original_writer.port, role)
                    return ReaderFailoverResult(candidate_conn, updated_host_info)

                self._plugin_service.driver_dialect.execute(
                        DbApiMethod.CONNECTION_CLOSE.method_name, lambda: candidate_conn.close())
                if role == HostRole.WRITER:
                    is_original_writer_still_writer = True
            except Exception:
                pass

        raise TimeoutError("Failover reader timeout")

    def _throw_failover_success_exception(self) -> None:
        if self._is_in_transaction or self._plugin_service.is_in_transaction:
            self._plugin_service.update_in_transaction(False)
            error_msg = "FailoverPlugin.TransactionResolutionUnknownError"
            logger.warning(error_msg)
            raise TransactionResolutionUnknownError(Messages.get(error_msg))
        else:
            error_msg = "FailoverPlugin.ConnectionChangedError"
            logger.error(error_msg)
            raise FailoverSuccessError(Messages.get(error_msg))

    def _failover_writer(self) -> None:
        logger.info("FailoverPlugin.StartWriterFailover")
        telemetry_context = self._plugin_service.get_telemetry_factory().open_telemetry_context(
            "failover to writer host", TelemetryTraceLevel.NESTED)

        failover_start_time = time.time()
        try:
            if not self._plugin_service.force_monitoring_refresh_host_list(True, self._failover_timeout_sec):
                raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToRefreshHostList"))

            updated_hosts = self._plugin_service.all_hosts
            writer_candidate = next((host for host in updated_hosts if host.role == HostRole.WRITER), None)

            if writer_candidate is None:
                raise FailoverFailedError(Messages.get_formatted(
                    "FailoverPlugin.NoWriterHostInTopology",
                    LogUtils.log_topology(updated_hosts)))

            logger.info("FailoverPlugin.FoundWriterCandidate", writer_candidate)

            allowed_hosts = self._plugin_service.hosts
            if not any(host.host == writer_candidate.host and host.port == writer_candidate.port
                       for host in allowed_hosts):
                raise FailoverFailedError(
                    Messages.get_formatted(
                        "FailoverPlugin.NewWriterNotAllowed",
                        "<null>" if writer_candidate is None else writer_candidate.host,
                        LogUtils.log_topology(allowed_hosts)))

            try:
                writer_candidate_conn = self._plugin_service.connect(writer_candidate, self._properties, self)
            except Exception as e:
                raise FailoverFailedError(Messages.get_formatted(
                    "FailoverPlugin.ExceptionConnectingToWriter", e))

            role = self._plugin_service.get_host_role(writer_candidate_conn)
            if role != HostRole.WRITER:
                try:
                    self._plugin_service.driver_dialect.execute(
                        DbApiMethod.CONNECTION_CLOSE.method_name, lambda: writer_candidate_conn.close())
                except Exception:
                    pass
                raise FailoverFailedError(Messages.get_formatted(
                    "FailoverPlugin.WriterFailoverConnectedToReader",
                    writer_candidate.host))

            self._plugin_service.set_current_connection(writer_candidate_conn, writer_candidate)
            logger.info("FailoverPlugin.EstablishedConnection", self._plugin_service.current_host_info)
            self._throw_failover_success_exception()

        except FailoverSuccessError as ex:
            if telemetry_context:
                telemetry_context.set_success(True)
                telemetry_context.set_exception(ex)
            raise ex
        except Exception as ex:
            if telemetry_context:
                telemetry_context.set_success(False)
                telemetry_context.set_exception(ex)
            raise ex
        finally:
            elapsed_time = (time.time() - failover_start_time) * 1000
            logger.info("FailoverPlugin.WriterFailoverTime", elapsed_time)
            if telemetry_context:
                telemetry_context.close_context()
                if self._telemetry_failover_additional_top_trace:
                    self._plugin_service.get_telemetry_factory().post_copy(
                        telemetry_context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    def _invalidate_current_connection(self) -> None:
        conn = self._plugin_service.current_connection
        if conn is None:
            return

        if self._is_in_transaction:
            try:
                conn.rollback()
            except Exception:
                pass

        try:
            self._plugin_service.driver_dialect.execute(
                        DbApiMethod.CONNECTION_CLOSE.method_name, lambda: conn.close())
        except Exception:
            pass

    def _pick_new_connection(self) -> None:
        if self._is_closed and self._closed_explicitly:
            logger.debug("FailoverPlugin.NoOperationsAfterConnectionClosed")
            return

        self._failover()

    def _should_exception_trigger_connection_switch(self, exception: Exception) -> bool:
        if not self._is_failover_enabled():
            logger.debug("FailoverPlugin.FailoverDisabled")
            return False

        if self._plugin_service.is_network_exception(exception):
            return True

        # For STRICT_WRITER failover mode when connection exception indicate that the connection's in read-only mode,
        # initiate a failover by returning true.
        return self._failover_mode == FailoverMode.STRICT_WRITER and \
            self._plugin_service.is_read_only_connection_exception(exception)

    def _can_direct_execute(self, method_name: str) -> bool:
        return method_name == DbApiMethod.CONNECTION_CLOSE.method_name or \
            method_name == DbApiMethod.CONNECTION_IS_CLOSED.method_name or \
            method_name == DbApiMethod.CURSOR_CLOSE.method_name

    def _init_failover_mode(self) -> None:
        if self._rds_url_type is None:
            self._failover_mode = get_failover_mode(self._properties)
            initial_host_spec = self._host_list_provider_service.initial_connection_host_info
            if initial_host_spec is not None:
                self._rds_url_type = self._rds_helper.identify_rds_type(initial_host_spec.host)

            if self._failover_mode is None:
                self._failover_mode = (FailoverMode.READER_OR_WRITER
                                       if self._rds_url_type is not None and self._rds_url_type == RdsUrlType.RDS_READER_CLUSTER
                                       else FailoverMode.STRICT_WRITER)

            logger.debug("FailoverPlugin.ParameterValue", "FAILOVER_MODE", self._failover_mode)


class FailoverV2PluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return FailoverV2Plugin(plugin_service, props)
