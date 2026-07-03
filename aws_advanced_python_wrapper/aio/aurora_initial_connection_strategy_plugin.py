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

"""Async Aurora Initial Connection Strategy plugin.

Ports sync ``aurora_initial_connection_strategy_plugin.py``. On initial
``connect()`` against an Aurora cluster DNS endpoint, verifies the landed
connection's actual role matches what the URL implies -- and, if not,
retries via the topology's instance endpoints.

Async adaptations:

* Retry loop uses :func:`asyncio.sleep`.
* Instance connection goes through :meth:`AsyncPluginService.connect`,
  so auth plugins (IAM, Secrets, Federated, Okta) and connection
  tracker re-apply on the new connection just like a user-driven
  connect.
* ``identify_connection`` approximated via :meth:`get_host_role` plus a
  topology scan (async ``PluginService`` doesn't yet expose
  ``identify_connection``).
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Optional, Set,
                    Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService


logger = Logger(__name__)


class AsyncAuroraInitialConnectionStrategyPlugin(AsyncPlugin):
    """Async counterpart of :class:`AuroraInitialConnectionStrategyPlugin`."""

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    _DEFAULT_RETRY_TIMEOUT_MS = 30000
    _DEFAULT_RETRY_INTERVAL_MS = 1000

    def __init__(self, plugin_service: AsyncPluginService) -> None:
        self._plugin_service = plugin_service
        self._rds_utils = RdsUtils()

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        url_type: RdsUrlType = self._rds_utils.identify_rds_type(host_info.host)
        if not url_type.is_rds_cluster:
            return await connect_func()

        if url_type in (RdsUrlType.RDS_WRITER_CLUSTER,
                        RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER):
            verified = await self._get_verified_writer(
                driver_dialect, host_info, props, connect_func,
                is_initial_connection, target_driver_func)
            if verified is not None:
                return verified
            return await connect_func()

        if url_type == RdsUrlType.RDS_READER_CLUSTER:
            verified = await self._get_verified_reader(
                driver_dialect, host_info, props, connect_func,
                is_initial_connection, target_driver_func)
            if verified is not None:
                return verified
            return await connect_func()

        return await connect_func()

    # ---- writer verification ----

    async def _get_verified_writer(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]],
            is_initial_connection: bool,
            target_driver_func: Callable) -> Optional[Any]:
        timeout_ms, interval_ms = self._retry_bounds(props)
        loop = asyncio.get_event_loop()
        deadline = loop.time() + (timeout_ms / 1000.0)

        while loop.time() < deadline:
            candidate_conn: Optional[Any] = None
            try:
                writer_candidate = self._pick_writer()
                if (writer_candidate is None
                        or self._rds_utils.is_rds_cluster_dns(writer_candidate.host)):
                    # Topology is stale -- open via cluster endpoint, refresh, identify.
                    candidate_conn = await connect_func()
                    try:
                        await self._plugin_service.force_refresh_host_list(candidate_conn)
                    except Exception:  # noqa: BLE001
                        pass
                    writer_candidate = await self._identify_host_role(
                        candidate_conn, HostRole.WRITER)
                    if writer_candidate is None:
                        await self._close_best_effort(candidate_conn, driver_dialect)
                        await asyncio.sleep(interval_ms / 1000.0)
                        continue
                    if is_initial_connection:
                        self._plugin_service.initial_connection_host_info = writer_candidate
                    return candidate_conn

                # Open directly to the writer instance.
                candidate_conn = await self._open_direct(
                    driver_dialect, writer_candidate, props, target_driver_func)
                actual = await self._plugin_service.get_host_role(candidate_conn)
                if actual != HostRole.WRITER:
                    try:
                        await self._plugin_service.force_refresh_host_list(candidate_conn)
                    except Exception:  # noqa: BLE001
                        pass
                    await self._close_best_effort(candidate_conn, driver_dialect)
                    await asyncio.sleep(interval_ms / 1000.0)
                    continue
                if is_initial_connection:
                    self._plugin_service.initial_connection_host_info = writer_candidate
                return candidate_conn
            except Exception:  # noqa: BLE001 - retry on any error
                await self._close_best_effort(candidate_conn, driver_dialect)
                await asyncio.sleep(interval_ms / 1000.0)
        return None

    # ---- reader verification ----

    async def _get_verified_reader(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]],
            is_initial_connection: bool,
            target_driver_func: Callable) -> Optional[Any]:
        timeout_ms, interval_ms = self._retry_bounds(props)
        loop = asyncio.get_event_loop()
        deadline = loop.time() + (timeout_ms / 1000.0)

        while loop.time() < deadline:
            candidate_conn: Optional[Any] = None
            reader_candidate: Optional[HostInfo] = None
            try:
                reader_candidate = self._pick_reader(props)
                if (reader_candidate is None
                        or self._rds_utils.is_rds_cluster_dns(reader_candidate.host)):
                    candidate_conn = await connect_func()
                    try:
                        await self._plugin_service.force_refresh_host_list(candidate_conn)
                    except Exception:  # noqa: BLE001
                        pass
                    actual_host = await self._identify_host_role(
                        candidate_conn, HostRole.READER)
                    if actual_host is None:
                        if self._has_no_readers():
                            # Cluster has no readers -- simulate Aurora reader cluster
                            # endpoint and return the current writer connection.
                            if is_initial_connection:
                                self._plugin_service.initial_connection_host_info = \
                                    self._pick_writer() or host_info
                            return candidate_conn
                        await self._close_best_effort(candidate_conn, driver_dialect)
                        await asyncio.sleep(interval_ms / 1000.0)
                        continue
                    if is_initial_connection:
                        self._plugin_service.initial_connection_host_info = actual_host
                    return candidate_conn

                # Connect directly to the picked reader instance.
                candidate_conn = await self._open_direct(
                    driver_dialect, reader_candidate, props, target_driver_func)
                actual = await self._plugin_service.get_host_role(candidate_conn)
                if actual != HostRole.READER:
                    try:
                        await self._plugin_service.force_refresh_host_list(candidate_conn)
                    except Exception:  # noqa: BLE001
                        pass
                    if self._has_no_readers():
                        if is_initial_connection:
                            self._plugin_service.initial_connection_host_info = reader_candidate
                        return candidate_conn
                    await self._close_best_effort(candidate_conn, driver_dialect)
                    await asyncio.sleep(interval_ms / 1000.0)
                    continue
                if is_initial_connection:
                    self._plugin_service.initial_connection_host_info = reader_candidate
                return candidate_conn
            except AwsWrapperError:
                # Configuration errors (e.g., unsupported strategy) should
                # surface immediately rather than loop to exhaustion.
                await self._close_best_effort(candidate_conn, driver_dialect)
                raise
            except Exception as e:  # noqa: BLE001
                await self._close_best_effort(candidate_conn, driver_dialect)
                # On non-login failure, mark reader UNAVAILABLE so the next
                # iteration picks a different one.
                if (reader_candidate is not None
                        and not self._plugin_service.is_login_exception(error=e)):
                    self._plugin_service.set_availability(
                        reader_candidate.as_aliases(),
                        HostAvailability.UNAVAILABLE)
                await asyncio.sleep(interval_ms / 1000.0)
        return None

    # ---- helpers ----

    @staticmethod
    def _retry_bounds(props: Properties) -> Tuple[int, int]:
        timeout_ms = (
            WrapperProperties.OPEN_CONNECTION_RETRY_TIMEOUT_MS.get_int(props)
            or AsyncAuroraInitialConnectionStrategyPlugin._DEFAULT_RETRY_TIMEOUT_MS)
        interval_ms = (
            WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props)
            or AsyncAuroraInitialConnectionStrategyPlugin._DEFAULT_RETRY_INTERVAL_MS)
        return timeout_ms, interval_ms

    def _pick_writer(self) -> Optional[HostInfo]:
        for h in self._plugin_service.all_hosts:
            if h.role == HostRole.WRITER:
                return h
        return None

    def _pick_reader(self, props: Properties) -> Optional[HostInfo]:
        strategy = WrapperProperties.READER_INITIAL_HOST_SELECTOR_STRATEGY.get(props)
        if not strategy:
            return None
        if not self._plugin_service.accepts_strategy(HostRole.READER, strategy):
            raise AwsWrapperError(
                f"Unsupported READER_INITIAL_HOST_SELECTOR_STRATEGY: {strategy!r}")
        try:
            readers = [h for h in self._plugin_service.all_hosts
                       if h.role == HostRole.READER]
            return self._plugin_service.get_host_info_by_strategy(
                HostRole.READER, strategy, readers)
        except Exception:  # noqa: BLE001
            return None

    async def _identify_host_role(
            self,
            conn: Any,
            expected_role: HostRole) -> Optional[HostInfo]:
        """Probe ``conn``'s role; return the matching HostInfo from topology
        if role matches, else None."""
        try:
            actual = await self._plugin_service.get_host_role(conn)
        except Exception:  # noqa: BLE001
            return None
        if actual != expected_role:
            return None
        # Find a matching topology entry. Best-effort -- topology may not
        # contain the exact host we resolved to.
        for h in self._plugin_service.all_hosts:
            if h.role == expected_role:
                return h
        return None

    def _has_no_readers(self) -> bool:
        if not self._plugin_service.all_hosts:
            return False
        return not any(h.role == HostRole.READER
                       for h in self._plugin_service.all_hosts)

    async def _open_direct(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            target_driver_func: Callable) -> Any:
        new_props = Properties(dict(props))
        new_props["host"] = host_info.host
        if host_info.is_port_specified():
            new_props["port"] = str(host_info.port)
        # Routes through the plugin pipeline (skipping this plugin to
        # avoid recursion) so auth plugins (IAM, Secrets, Federated,
        # Okta) re-apply on the new instance connection.
        return await self._plugin_service.connect(
            host_info, new_props, plugin_to_skip=self)

    @staticmethod
    async def _close_best_effort(
            conn: Optional[Any],
            driver_dialect: AsyncDriverDialect) -> None:
        if conn is None:
            return
        try:
            await driver_dialect.abort_connection(conn)
        except Exception:  # noqa: BLE001
            pass


__all__ = ["AsyncAuroraInitialConnectionStrategyPlugin"]
