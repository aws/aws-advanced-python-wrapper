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

"""Async Stale DNS plugin.

Port of sync ``stale_dns_plugin.py``. Detects stale DNS cache entries
that map a writer cluster endpoint to a non-writer instance (e.g. just
after a failover) and transparently reconnects to the actual writer.

Async-specific adaptations:

* ``socket.gethostbyname`` is wrapped in :func:`asyncio.to_thread` so the
  blocking DNS lookup doesn't stall the event loop.
* The fresh-writer connection is opened via
  :meth:`AsyncPluginService.connect`, so caller plugins (IAM auth,
  Secrets, Federated, Okta, connection tracker) re-apply on the new
  connection just as they would on a user-driven connect.
"""

from __future__ import annotations

import asyncio
import socket
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional, Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo

logger = Logger(__name__)


class AsyncStaleDnsPlugin(AsyncPlugin):
    """Async counterpart of :class:`StaleDnsPlugin`.

    Subscribes to ``connect`` and ``notify_host_list_changed``. On
    connect to a writer cluster endpoint, verifies the connection
    actually landed on the writer instance; swaps to a fresh
    writer-direct connection when DNS is stale.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name,
    }

    def __init__(self, plugin_service: AsyncPluginService) -> None:
        self._plugin_service = plugin_service
        self._rds_helper = RdsUtils()
        self._writer_host_info: Optional[HostInfo] = None
        self._writer_host_address: Optional[str] = None

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
        # Guard: require a dynamic host list provider. Mirrors sync's
        # init_host_provider check; we enforce it lazily on first
        # connect since async plugins don't have an init_host_provider
        # hook.
        self._require_dynamic_provider()

        # Only act on Aurora writer-cluster DNS endpoints.
        if (not self._rds_helper.is_writer_cluster_dns(host_info.host)
                and not self._rds_helper.is_global_db_writer_cluster_dns(
                    host_info.host)):
            return await connect_func()

        conn = await connect_func()
        if conn is None:
            return conn

        # Resolve the cluster endpoint.
        cluster_inet = await self._resolve_dns(host_info.host)
        logger.debug(
            "StaleDnsHelper.ClusterEndpointDns", host_info.host, cluster_inet)
        if cluster_inet is None:
            return conn

        # Check what role we actually got.
        try:
            role = await self._plugin_service.get_host_role(conn)
        except Exception:  # noqa: BLE001 - best-effort probe
            return conn
        connected_to_reader = role == HostRole.READER

        # Refresh topology: force-refresh if we got a reader (topology
        # is stale); plain refresh otherwise.
        try:
            if connected_to_reader:
                await self._plugin_service.force_refresh_host_list(conn)
            else:
                await self._plugin_service.refresh_host_list(conn)
        except Exception:  # noqa: BLE001 - best-effort refresh
            pass

        # Pick the writer from topology.
        if self._writer_host_info is None:
            candidate = self._pick_writer()
            if candidate is not None and self._rds_helper.is_rds_cluster_dns(
                    candidate.host):
                # The reported writer is itself a cluster endpoint --
                # nothing we can do. Return the current conn.
                return conn
            self._writer_host_info = candidate

        if self._writer_host_info is None:
            return conn

        if self._writer_host_address is None:
            self._writer_host_address = await self._resolve_dns(
                self._writer_host_info.host)
        logger.debug(
            "StaleDnsHelper.WriterInetAddress", self._writer_host_address)

        if self._writer_host_address is None:
            return conn

        # If the writer's IP differs from the cluster endpoint's, DNS
        # is stale. Also swap if we landed on a reader.
        if (self._writer_host_address != cluster_inet
                or connected_to_reader):
            logger.debug(
                "StaleDnsHelper.StaleDnsDetected", self._writer_host_info)

            allowed = self._plugin_service.all_hosts
            if not self._contains_host_port(
                    allowed,
                    self._writer_host_info.host,
                    self._writer_host_info.port):
                raise AwsWrapperError(
                    "Stale DNS detected: current writer "
                    f"{self._writer_host_info.get_host_and_port()} "
                    "is not in the allowed topology.")

            # Open a fresh connection to the actual writer through the
            # plugin pipeline (skipping ourselves to avoid recursion),
            # so auth plugins (IAM, Secrets, Federated, Okta) and
            # connection tracker re-apply on the new connection.
            writer_props = self._props_with_host(
                props, self._writer_host_info)
            writer_conn = await self._plugin_service.connect(
                self._writer_host_info, writer_props, plugin_to_skip=self)
            if is_initial_connection:
                self._plugin_service.initial_connection_host_info = \
                    self._writer_host_info
            # Close the stale conn best-effort.
            await self._close_best_effort(conn, driver_dialect)
            return writer_conn

        return conn

    def notify_host_list_changed(
            self, changes: Dict[str, Set[HostEvent]]) -> None:
        if self._writer_host_info is None:
            return

        # Sync keys by `host_info.url`; match that shape first, then
        # fall back to the plain host-port key for older callers.
        key_url = self._writer_host_info.url
        key_host_port = self._writer_host_info.get_host_and_port()
        writer_changes = (
            changes.get(key_url)
            or changes.get(key_host_port)
            or changes.get(self._writer_host_info.host))
        if (writer_changes is not None
                and HostEvent.CONVERTED_TO_READER in writer_changes):
            logger.debug("StaleDnsHelper.Reset")
            self._writer_host_info = None
            self._writer_host_address = None

    # -- helpers ----------------------------------------------------------

    def _require_dynamic_provider(self) -> None:
        """Raise if the host list provider is static.

        Mirrors sync :meth:`StaleDnsPlugin.init_host_provider`. Async
        plugins don't have an init_host_provider pipeline hook, so the
        check fires on first connect.
        """
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            AsyncStaticHostListProvider
        hlp = self._plugin_service.host_list_provider
        if isinstance(hlp, AsyncStaticHostListProvider):
            raise AwsWrapperError(
                "StaleDnsPlugin requires a dynamic host list provider; "
                "a static provider is configured.")

    async def _resolve_dns(self, host: str) -> Optional[str]:
        try:
            return await asyncio.to_thread(socket.gethostbyname, host)
        except Exception:  # noqa: BLE001 - gaierror etc. swallowed
            return None

    def _pick_writer(self) -> Optional[HostInfo]:
        for h in self._plugin_service.all_hosts:
            if h.role == HostRole.WRITER:
                return h
        return None

    @staticmethod
    def _contains_host_port(
            hosts: Any, host: str, port: int) -> bool:
        return any(h.host == host and h.port == port for h in hosts)

    @staticmethod
    def _props_with_host(
            props: Properties, host_info: HostInfo) -> Properties:
        new_props = Properties(dict(props))
        new_props["host"] = host_info.host
        if host_info.is_port_specified():
            new_props["port"] = str(host_info.port)
        return new_props

    @staticmethod
    async def _close_best_effort(
            conn: Any, driver_dialect: AsyncDriverDialect) -> None:
        try:
            await driver_dialect.abort_connection(conn)
        except Exception:  # noqa: BLE001
            pass


__all__ = ["AsyncStaleDnsPlugin"]
