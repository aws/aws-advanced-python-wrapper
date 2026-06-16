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

import time
from typing import TYPE_CHECKING, Callable, List, Optional

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin_service import PluginService

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverFailedError,
                                                FailoverSuccessError)
from aws_advanced_python_wrapper.failover_v2_plugin import FailoverV2Plugin
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.gdb_failover_mode import GdbFailoverMode
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.retry_util import RetryUtil
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel
from aws_advanced_python_wrapper.utils.utils import LogUtils

logger = Logger(__name__)


class GdbFailoverPlugin(FailoverV2Plugin):

    def __init__(self, plugin_service: PluginService, props: Properties):
        super().__init__(plugin_service, props)

        self._home_region: Optional[str] = None
        self._active_home_failover_mode: Optional[GdbFailoverMode] = None
        self._inactive_home_failover_mode: Optional[GdbFailoverMode] = None
        self._retry_util = RetryUtil()

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

    @staticmethod
    def _inc(counter) -> None:
        if counter is not None:
            counter.inc()

    def _init_failover_mode(self) -> None:
        if self._rds_url_type is not None:
            return

        initial_host_info = self._host_list_provider_service.initial_connection_host_info
        if initial_host_info is None:
            raise AwsWrapperError(Messages.get("GdbFailoverPlugin.MissingInitialHost"))
        self._rds_url_type = self._rds_helper.identify_rds_type(initial_host_info.host)

        self._home_region = WrapperProperties.FAILOVER_HOME_REGION.get(self._properties)
        if self._home_region is None or not self._home_region.strip():
            if not self._rds_url_type.has_region:
                raise AwsWrapperError(Messages.get("GdbFailoverPlugin.MissingHomeRegion"))
            self._home_region = self._rds_helper.get_rds_region(initial_host_info.host)
            if self._home_region is None or not self._home_region.strip():
                raise AwsWrapperError(Messages.get("GdbFailoverPlugin.MissingHomeRegion"))

        logger.debug("FailoverPlugin.ParameterValue", "failover_home_region", self._home_region)

        self._active_home_failover_mode = GdbFailoverMode.from_value(
            WrapperProperties.ACTIVE_HOME_FAILOVER_MODE.get(self._properties))
        self._inactive_home_failover_mode = GdbFailoverMode.from_value(
            WrapperProperties.INACTIVE_HOME_FAILOVER_MODE.get(self._properties))

        default_mode = GdbFailoverMode.STRICT_WRITER \
            if self._rds_url_type in (RdsUrlType.RDS_WRITER_CLUSTER, RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER) \
            else GdbFailoverMode.HOME_READER_OR_WRITER
        if self._active_home_failover_mode is None:
            self._active_home_failover_mode = default_mode
        if self._inactive_home_failover_mode is None:
            self._inactive_home_failover_mode = default_mode

        logger.debug("FailoverPlugin.ParameterValue", "active_home_failover_mode", self._active_home_failover_mode)
        logger.debug("FailoverPlugin.ParameterValue", "inactive_home_failover_mode", self._inactive_home_failover_mode)

    def _is_home_region(self, region: Optional[str]) -> bool:
        return self._home_region is not None and region is not None \
            and self._home_region.casefold() == region.casefold()

    def _is_strict_writer_failover_mode(self) -> bool:
        current_region = self._rds_helper.get_rds_region(self._plugin_service.current_host_info.host)
        if self._is_home_region(current_region):
            return self._active_home_failover_mode == GdbFailoverMode.STRICT_WRITER
        return self._inactive_home_failover_mode == GdbFailoverMode.STRICT_WRITER

    def _failover(self) -> None:
        context = self._plugin_service.get_telemetry_factory().open_telemetry_context(
            "failover", TelemetryTraceLevel.NESTED)

        failover_start_time = time.time()
        failover_end_time = failover_start_time + self._failover_timeout_sec
        try:
            logger.info("GdbFailoverPlugin.StartFailover")

            # It's expected that this synchronously returns once topology is stabilized,
            # i.e. when the cluster control plane has already chosen a new writer.
            if not self._plugin_service.force_monitoring_refresh_host_list(True, self._failover_timeout_sec):
                # Assume this is a writer failover for telemetry purposes.
                self._inc(self._failover_writer_triggered_counter)
                self._inc(self._failover_writer_failed_counter)
                logger.error("FailoverPlugin.UnableToRefreshHostList")
                raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToRefreshHostList"))

            updated_hosts = self._plugin_service.all_hosts
            writer_candidate = next((host for host in updated_hosts if host.role == HostRole.WRITER), None)

            if writer_candidate is None:
                self._inc(self._failover_writer_triggered_counter)
                self._inc(self._failover_writer_failed_counter)
                message = Messages.get_formatted(
                    "FailoverPlugin.NoWriterHostInTopology", LogUtils.log_topology(updated_hosts))
                logger.error(message)
                raise FailoverFailedError(message)

            # Check writer region
            writer_region = self._rds_helper.get_rds_region(writer_candidate.host)
            is_home_region = self._is_home_region(writer_region)
            logger.debug("GdbFailoverPlugin.IsHomeRegion", is_home_region)

            current_failover_mode = self._active_home_failover_mode if is_home_region \
                else self._inactive_home_failover_mode
            logger.debug("GdbFailoverPlugin.CurrentFailoverMode", current_failover_mode)

            self._failover_with_mode(current_failover_mode, writer_candidate, failover_end_time)

            logger.info("FailoverPlugin.EstablishedConnection", self._plugin_service.current_host_info)
            self._throw_failover_success_exception()

        except FailoverSuccessError as ex:
            if context:
                context.set_success(True)
                context.set_exception(ex)
            raise ex
        except Exception as ex:
            if context:
                context.set_success(False)
                context.set_exception(ex)
            raise ex
        finally:
            elapsed_time = (time.time() - failover_start_time) * 1000
            logger.debug("GdbFailoverPlugin.FailoverElapsed", elapsed_time)
            if context:
                context.close_context()
                if self._telemetry_failover_additional_top_trace:
                    self._plugin_service.get_telemetry_factory().post_copy(
                        context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    def _failover_with_mode(
            self,
            mode: Optional[GdbFailoverMode],
            writer_candidate: HostInfo,
            failover_end_time: float) -> None:
        match mode:
            case GdbFailoverMode.STRICT_WRITER:
                self._failover_to_writer(writer_candidate, failover_end_time)
            case GdbFailoverMode.STRICT_HOME_READER:
                self._failover_to_allowed_host(
                    lambda: [host for host in self._plugin_service.hosts
                             if host.role == HostRole.READER
                             and self._is_home_region(self._rds_helper.get_rds_region(host.host))],
                    HostRole.READER,
                    failover_end_time)
            case GdbFailoverMode.STRICT_OUT_OF_HOME_READER:
                self._failover_to_allowed_host(
                    lambda: [host for host in self._plugin_service.hosts
                             if host.role == HostRole.READER
                             and not self._is_home_region(self._rds_helper.get_rds_region(host.host))],
                    HostRole.READER,
                    failover_end_time)
            case GdbFailoverMode.STRICT_ANY_READER:
                self._failover_to_allowed_host(
                    lambda: [host for host in self._plugin_service.hosts if host.role == HostRole.READER],
                    HostRole.READER,
                    failover_end_time)
            case GdbFailoverMode.HOME_READER_OR_WRITER:
                self._failover_to_allowed_host(
                    lambda: [host for host in self._plugin_service.hosts
                             if host.role == HostRole.WRITER
                             or (host.role == HostRole.READER
                                 and self._is_home_region(self._rds_helper.get_rds_region(host.host)))],
                    None,
                    failover_end_time)
            case GdbFailoverMode.OUT_OF_HOME_READER_OR_WRITER:
                self._failover_to_allowed_host(
                    lambda: [host for host in self._plugin_service.hosts
                             if host.role == HostRole.WRITER
                             or (host.role == HostRole.READER
                                 and not self._is_home_region(self._rds_helper.get_rds_region(host.host)))],
                    None,
                    failover_end_time)
            case GdbFailoverMode.ANY_READER_OR_WRITER:
                self._failover_to_allowed_host(
                    lambda: list(self._plugin_service.hosts),
                    None,
                    failover_end_time)
            case _:
                raise AwsWrapperError(Messages.get_formatted("GdbFailoverPlugin.UnsupportedFailoverMode", mode))

    def _failover_to_writer(self, writer_candidate: HostInfo, failover_end_time: float) -> None:
        self._inc(self._failover_writer_triggered_counter)

        result = None
        try:
            result = self._retry_util.get_writer_connection(
                self._plugin_service, self._properties, self, True, failover_end_time)
            self._plugin_service.set_current_connection(result.connection, result.host_info)
            result = None  # Prevents closing the returned connection in the finally block.
            self._inc(self._failover_writer_success_counter)
        except TimeoutError:
            self._inc(self._failover_writer_failed_counter)
            logger.error("FailoverPlugin.ExceptionConnectingToWriter", writer_candidate.host)
            raise FailoverFailedError(
                Messages.get_formatted("FailoverPlugin.ExceptionConnectingToWriter", writer_candidate.host))
        finally:
            if result is not None and result.connection is not self._plugin_service.current_connection:
                RetryUtil.close_connection(self._plugin_service, result.connection)

    def _failover_to_allowed_host(
            self,
            allowed_hosts: Callable[[], Optional[List[HostInfo]]],
            verify_role: Optional[HostRole],
            failover_end_time: float) -> None:
        self._inc(self._failover_reader_triggered_counter)

        result = None
        try:
            result = self._retry_util.get_allowed_connection(
                self._plugin_service,
                self._properties,
                self,
                allowed_hosts,
                self._failover_reader_host_selector_strategy,
                verify_role,
                failover_end_time)
            self._plugin_service.set_current_connection(result.connection, result.host_info)
            result = None  # Prevents closing the returned connection in the finally block.
            self._inc(self._failover_reader_success_counter)
        except TimeoutError:
            self._inc(self._failover_reader_failed_counter)
            logger.error("FailoverPlugin.UnableToConnectToReader")
            raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToConnectToReader"))
        finally:
            if result is not None and result.connection is not self._plugin_service.current_connection:
                RetryUtil.close_connection(self._plugin_service, result.connection)

    def _failover_reader(self) -> None:
        # Not used by the GDB Failover Plugin. See _failover() for implementation details.
        raise AwsWrapperError(Messages.get_formatted("Plugin.UnsupportedMethod", "_failover_reader"))

    def _failover_writer(self) -> None:
        # Not used by the GDB Failover Plugin. See _failover() for implementation details.
        raise AwsWrapperError(Messages.get_formatted("Plugin.UnsupportedMethod", "_failover_writer"))


class GdbFailoverPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return GdbFailoverPlugin(plugin_service, props)
