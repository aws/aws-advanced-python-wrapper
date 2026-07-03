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

"""Async failover plugin for Aurora Global Databases.

Async counterpart of :class:`GdbFailoverPlugin`. Extends
:class:`AsyncFailoverPlugin` with region awareness: on a writer change it picks
the failover target according to the :class:`GdbFailoverMode` configured for the
*region* of the newly elected writer -- the ``active_home_failover_mode`` when
the writer is in the home region, the ``inactive_home_failover_mode`` otherwise.

The sync plugin overrides ``FailoverV2Plugin._failover``; the async failover
plugin's orchestrator is :meth:`AsyncFailoverPlugin._do_failover`, so this class
overrides that instead. The mode-driven candidate selection mirrors the sync
plugin's ``_failover_with_mode`` exactly; the connection retry loop is delegated
to :class:`AsyncRetryUtil` (the async port of the sync ``RetryUtil``).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional

from aws_advanced_python_wrapper.aio.failover_plugin import AsyncFailoverPlugin
from aws_advanced_python_wrapper.aio.retry_util import AsyncRetryUtil
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverFailedError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.gdb_failover_mode import GdbFailoverMode
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService

logger = Logger(__name__)


class AsyncGdbFailoverPlugin(AsyncFailoverPlugin):
    """Async Global-Database (region-aware) failover plugin."""

    # Throttle between topology probes while waiting for the new writer to
    # surface (each probe is a real monitor query). Capped by the remaining
    # failover budget at the call site.
    _WRITER_DISCOVERY_DELAY_SEC = 1.0

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_list_provider: AsyncHostListProvider,
            props: Properties) -> None:
        super().__init__(plugin_service, host_list_provider, props)
        self._rds_utils = RdsUtils()
        self._rds_url_type: Optional[RdsUrlType] = None
        self._home_region: Optional[str] = None
        self._active_home_failover_mode: Optional[GdbFailoverMode] = None
        self._inactive_home_failover_mode: Optional[GdbFailoverMode] = None
        self._retry_util = AsyncRetryUtil()
        strategy = WrapperProperties.FAILOVER_READER_HOST_SELECTOR_STRATEGY.get(props)
        self._failover_reader_host_selector_strategy: str = strategy if strategy is not None else ""

    @staticmethod
    def _inc(counter: Any) -> None:
        if counter is not None:
            counter.inc()

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self._init_failover_mode(host_info)
        return await super().connect(
            target_driver_func, driver_dialect, host_info, props,
            is_initial_connection, connect_func)

    def _init_failover_mode(self, init_host_info: Optional[HostInfo] = None) -> None:
        if self._rds_url_type is not None:
            return

        initial_host_info = (init_host_info if init_host_info is not None
                             else self._plugin_service.initial_connection_host_info)
        if initial_host_info is None:
            raise AwsWrapperError(Messages.get("GdbFailoverPlugin.MissingInitialHost"))
        self._rds_url_type = self._rds_utils.identify_rds_type(initial_host_info.host)

        self._home_region = WrapperProperties.FAILOVER_HOME_REGION.get(self._props)
        if self._home_region is None or not self._home_region.strip():
            if not self._rds_url_type.has_region:
                raise AwsWrapperError(Messages.get("GdbFailoverPlugin.MissingHomeRegion"))
            self._home_region = self._rds_utils.get_rds_region(initial_host_info.host)
            if self._home_region is None or not self._home_region.strip():
                raise AwsWrapperError(Messages.get("GdbFailoverPlugin.MissingHomeRegion"))

        logger.debug("FailoverPlugin.ParameterValue", "failover_home_region", self._home_region)

        self._active_home_failover_mode = GdbFailoverMode.from_value(
            WrapperProperties.ACTIVE_HOME_FAILOVER_MODE.get(self._props))
        self._inactive_home_failover_mode = GdbFailoverMode.from_value(
            WrapperProperties.INACTIVE_HOME_FAILOVER_MODE.get(self._props))

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
        # Region-aware override of the base's read-only-exception escape hatch
        # (see AsyncFailoverPlugin._should_failover). The current connection's
        # region decides whether the active or inactive home mode applies.
        if self._rds_url_type is None:
            try:
                self._init_failover_mode()
            except Exception:  # noqa: BLE001 - fall back if init isn't possible yet
                return super()._is_strict_writer_failover_mode()
        current = self._plugin_service.current_host_info
        region = self._rds_utils.get_rds_region(current.host) \
            if current is not None and current.host else None
        mode = self._active_home_failover_mode if self._is_home_region(region) \
            else self._inactive_home_failover_mode
        return mode == GdbFailoverMode.STRICT_WRITER

    async def _do_failover(self, driver_dialect: AsyncDriverDialect) -> None:
        self._init_failover_mode()
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        context = telemetry_factory.open_telemetry_context(
            "failover", TelemetryTraceLevel.NESTED)
        deadline = asyncio.get_event_loop().time() + self._failover_timeout_sec
        try:
            logger.info("GdbFailoverPlugin.StartFailover")

            # Invalidate the current (likely broken) connection before failover
            # so any pool-proxied target is evicted from its owning pool.
            await self._invalidate_current_connection()

            # Discover the new writer. gdb needs the writer's *region* to pick
            # the active vs. inactive failover mode, so this must run before the
            # mode dispatch. A single probe is not enough: right after a failover
            # the just-elected writer is often not visible yet, and the async
            # topology monitor needs several panic-mode probes to surface it.
            # Retry force-refresh until a writer appears or the deadline passes,
            # mirroring sync's blocking force_monitoring_refresh_host_list and
            # AsyncFailoverPlugin's retry-until-writer loops.
            writer_candidate = await self._discover_writer(deadline)
            if writer_candidate is None:
                self._inc(self._failover_writer_triggered)
                self._inc(self._failover_writer_failed)
                message = Messages.get_formatted(
                    "FailoverPlugin.NoWriterHostInTopology",
                    LogUtils.log_topology(self._plugin_service.all_hosts))
                logger.error(message)
                raise FailoverFailedError(message)

            writer_region = self._rds_utils.get_rds_region(writer_candidate.host)
            is_home_region = self._is_home_region(writer_region)
            logger.debug("GdbFailoverPlugin.IsHomeRegion", is_home_region)

            current_failover_mode = self._active_home_failover_mode if is_home_region \
                else self._inactive_home_failover_mode
            logger.debug("GdbFailoverPlugin.CurrentFailoverMode", current_failover_mode)

            await self._failover_with_mode(current_failover_mode, writer_candidate, deadline, driver_dialect)

            logger.info("FailoverPlugin.EstablishedConnection", self._plugin_service.current_host_info)
            if context:
                context.set_success(True)
        except Exception as ex:
            if context:
                context.set_success(False)
                context.set_exception(ex)
            raise
        finally:
            if context:
                context.close_context()
                if self._telemetry_failover_additional_top_trace:
                    telemetry_factory.post_copy(context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    async def _discover_writer(self, deadline: float) -> Optional[HostInfo]:
        """Force-refresh topology until a writer is visible or the deadline
        passes; return the writer ``HostInfo`` (or ``None`` on timeout).

        Uses force refresh (monitor panic-mode probing), not the cache-windowed
        plain refresh, so a writer elected after the failure is actually
        discovered. Throttled per pass (bounded by the remaining budget) so the
        probe loop can't hammer topology discovery.
        """
        while asyncio.get_event_loop().time() < deadline:
            try:
                await self._within_deadline(
                    self._plugin_service.force_refresh_host_list(), deadline)
            except Exception:  # noqa: BLE001 - a failed probe just means retry
                pass
            writer = next(
                (host for host in self._plugin_service.all_hosts
                 if host.role == HostRole.WRITER), None)
            if writer is not None:
                return writer
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(self._WRITER_DISCOVERY_DELAY_SEC, remaining))
        return None

    def _allowed_hosts(self) -> List[HostInfo]:
        """The region/allowlist-filtered topology -- async equivalent of the
        sync ``plugin_service.hosts`` the sync plugin selects candidates from."""
        return self._plugin_service.filter_hosts(list(self._plugin_service.all_hosts))

    async def _failover_with_mode(
            self,
            mode: Optional[GdbFailoverMode],
            writer_candidate: HostInfo,
            deadline: float,
            driver_dialect: AsyncDriverDialect) -> None:
        match mode:
            case GdbFailoverMode.STRICT_WRITER:
                # Delegate to the inherited failover_v2 writer-failover, which
                # CONVERGES on the newly-elected writer: it re-verifies the
                # candidate's role via the data plane and, when the topology
                # still labels the just-demoted old writer as WRITER, refreshes
                # topology THROUGH the live (reader) connection and retries.
                # A plain retry-connect loop (AsyncRetryUtil.get_allowed_connection)
                # has neither, so it looped on the demoted old writer until the
                # deadline (validated on Aurora: 2592 reconnects to the stale
                # host over 5 min, then timeout).
                # STRICT_WRITER means "reconnect to the new writer" -- exactly
                # failover_v2's job. (Requires the real topology provider, i.e.
                # gdb_failover present in wrapper._TOPOLOGY_REQUIRING_PLUGINS.)
                await self._failover_writer(
                    self._plugin_service.all_hosts, driver_dialect, deadline, None)
            case GdbFailoverMode.STRICT_HOME_READER:
                await self._failover_to_allowed_host(
                    lambda: [host for host in self._allowed_hosts()
                             if host.role == HostRole.READER
                             and self._is_home_region(self._rds_utils.get_rds_region(host.host))],
                    HostRole.READER,
                    deadline)
            case GdbFailoverMode.STRICT_OUT_OF_HOME_READER:
                await self._failover_to_allowed_host(
                    lambda: [host for host in self._allowed_hosts()
                             if host.role == HostRole.READER
                             and not self._is_home_region(self._rds_utils.get_rds_region(host.host))],
                    HostRole.READER,
                    deadline)
            case GdbFailoverMode.STRICT_ANY_READER:
                await self._failover_to_allowed_host(
                    lambda: [host for host in self._allowed_hosts() if host.role == HostRole.READER],
                    HostRole.READER,
                    deadline)
            case GdbFailoverMode.HOME_READER_OR_WRITER:
                await self._failover_to_allowed_host(
                    lambda: [host for host in self._allowed_hosts()
                             if host.role == HostRole.WRITER
                             or (host.role == HostRole.READER
                                 and self._is_home_region(self._rds_utils.get_rds_region(host.host)))],
                    None,
                    deadline)
            case GdbFailoverMode.OUT_OF_HOME_READER_OR_WRITER:
                await self._failover_to_allowed_host(
                    lambda: [host for host in self._allowed_hosts()
                             if host.role == HostRole.WRITER
                             or (host.role == HostRole.READER
                                 and not self._is_home_region(self._rds_utils.get_rds_region(host.host)))],
                    None,
                    deadline)
            case GdbFailoverMode.ANY_READER_OR_WRITER:
                await self._failover_to_allowed_host(
                    lambda: list(self._allowed_hosts()),
                    None,
                    deadline)
            case _:
                raise AwsWrapperError(Messages.get_formatted("GdbFailoverPlugin.UnsupportedFailoverMode", mode))

    async def _failover_to_allowed_host(
            self,
            allowed_hosts: Callable[[], Optional[List[HostInfo]]],
            verify_role: Optional[HostRole],
            deadline: float) -> None:
        self._inc(self._failover_reader_triggered)

        result = None
        try:
            result = await self._retry_util.get_allowed_connection(
                self._plugin_service,
                self._props,
                self,
                allowed_hosts,
                self._failover_reader_host_selector_strategy,
                verify_role,
                deadline)
            await self._plugin_service.set_current_connection(result.connection, result.host_info)
            result = None  # Prevents closing the returned connection in the finally block.
            self._inc(self._failover_reader_completed)
        except TimeoutError:
            self._inc(self._failover_reader_failed)
            logger.error("FailoverPlugin.UnableToConnectToReader")
            raise FailoverFailedError(Messages.get("FailoverPlugin.UnableToConnectToReader"))
        finally:
            if result is not None and result.connection is not self._plugin_service.current_connection:
                await AsyncRetryUtil.close_connection(self._plugin_service, result.connection)

    async def _failover_reader(self, *args: Any, **kwargs: Any) -> None:
        # gdb does its OWN reader selection (region/allowlist-filtered, via
        # _failover_to_allowed_host) rather than the base read-replica failover,
        # so the inherited _failover_reader is never used here. Writer failover,
        # by contrast, IS reused: STRICT_WRITER in _failover_with_mode delegates
        # to the inherited AsyncFailoverPlugin._failover_writer (which converges
        # on the new writer) -- so _failover_writer is deliberately NOT overridden.
        raise AwsWrapperError(Messages.get_formatted("Plugin.UnsupportedMethod", "_failover_reader"))


__all__ = ["AsyncGdbFailoverPlugin"]
