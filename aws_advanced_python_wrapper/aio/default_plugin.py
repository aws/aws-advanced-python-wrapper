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

"""``AsyncDefaultPlugin`` -- terminal plugin in every async pipeline.

Routes ``connect`` through :class:`AsyncConnectionProviderManager` so a
user-installed custom provider (e.g., an async pool) can claim the host
before the default driver path is used. Mirrors sync
:class:`DefaultPlugin` in ``default_plugin.py``.
"""

from __future__ import annotations

import asyncio
import copy
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional, Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.connection_provider import \
    DriverConnectionProvider
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncDefaultPlugin(AsyncPlugin):
    """Terminal plugin. Routes connect through the provider manager."""

    # Sync selector registry reused so sync+async share RoundRobin state.
    _SELECTORS = DriverConnectionProvider.accepted_strategies()

    def __init__(
            self,
            plugin_service: Optional[AsyncPluginService] = None) -> None:
        self._plugin_service = plugin_service

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.ALL.method_name}

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        # No plugin_service wired (legacy SP-1 callers) -- fall back to
        # driver-dialect direct connect. Production pipelines always pass
        # a service, so this branch only affects toy tests.
        if self._plugin_service is None:
            return await driver_dialect.connect(host_info, props, target_driver_func)

        target_driver_props = copy.copy(props)
        provider_manager = self._plugin_service.get_connection_provider_manager()
        provider = provider_manager.get_connection_provider(
            host_info, target_driver_props)
        database_dialect = self._plugin_service.database_dialect
        # database_dialect can be None in toy tests that skip dialect
        # resolution; the default provider only touches it via
        # ``prepare_conn_props`` so we guard before calling.
        if database_dialect is None:
            conn = await driver_dialect.connect(
                host_info, target_driver_props, target_driver_func)
        else:
            conn = await provider.connect(
                target_driver_func,
                driver_dialect,
                database_dialect,
                host_info,
                target_driver_props,
            )
        self._post_connect_bookkeeping(host_info, provider)
        return conn

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable[..., Awaitable[Any]]) -> Any:
        # force_connect always uses the default provider (mirrors sync).
        if self._plugin_service is None:
            return await driver_dialect.connect(host_info, props, target_driver_func)
        target_driver_props = copy.copy(props)
        provider = self._plugin_service.get_connection_provider_manager().default_provider
        database_dialect = self._plugin_service.database_dialect
        if database_dialect is None:
            conn = await driver_dialect.connect(
                host_info, target_driver_props, target_driver_func)
        else:
            conn = await provider.connect(
                target_driver_func,
                driver_dialect,
                database_dialect,
                host_info,
                target_driver_props,
            )
        self._post_connect_bookkeeping(host_info, provider)
        return conn

    def _post_connect_bookkeeping(self, host_info: HostInfo, connection_provider: Any) -> None:
        """Post-connect bookkeeping after a successful connect.

        Sync parity: ``DefaultPlugin._connect`` (default_plugin.py:80-82) marks
        every alias of the just-connected host AVAILABLE and lets the service
        re-pick the driver dialect for the provider that produced the
        connection (a no-op on async today). Sync additionally calls
        ``plugin_service.update_dialect(conn)`` to auto-upgrade the DATABASE
        dialect from the live connection; the async plugin service has no
        ``update_dialect`` yet -- the async connect path instead upgrades the
        dialect in ``AsyncAwsWrapperConnection.connect`` via
        ``_upgrade_database_dialect_after_connect``.
        """
        if self._plugin_service is None:
            return
        self._plugin_service.set_availability(
            host_info.as_aliases(), HostAvailability.AVAILABLE)
        self._plugin_service.update_driver_dialect(connection_provider)

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        # Per-operation socket-timeout bound (sync parity: DriverDialect.execute
        # wraps network-bound methods with SOCKET_TIMEOUT_SEC and aborts the
        # connection on expiry, driver_dialect.py:126-153). Like sync, the bound
        # applies only to the dialect's network_bound_methods -- a slow local
        # method must not be spuriously aborted with QueryTimeoutError. On
        # timeout the connection socket is severed (abort_connection) so the
        # wedged operation cannot poison a later reuse, then QueryTimeoutError
        # is raised exactly like sync.
        timeout_sec = self._socket_timeout_sec()
        if timeout_sec is not None and not self._is_network_bound(method_name):
            timeout_sec = None
        if timeout_sec is not None and timeout_sec > 0:
            try:
                result = await asyncio.wait_for(execute_func(), timeout_sec)
            except asyncio.TimeoutError as e:
                await self._abort_current_connection()
                raise QueryTimeoutError(Messages.get_formatted(
                    "DriverDialect.ExecuteTimeout", method_name)) from e
        else:
            result = await execute_func()
        # Track transaction state after each op so the failover plugin can tell
        # whether the caller was mid-transaction when failover struck (parity
        # with sync DefaultPlugin.execute:114). It must be refreshed here, while
        # the connection is healthy -- the post-failover connection is always
        # idle, so probing it then would always report "not in a transaction".
        if (self._plugin_service is not None
                and method_name != DbApiMethod.CONNECTION_CLOSE.method_name
                and self._plugin_service.current_connection is not None):
            try:
                await self._plugin_service.update_in_transaction()
            except Exception:  # noqa: BLE001 - tracking is best-effort
                pass
        return result

    def _socket_timeout_sec(self) -> Optional[float]:
        if self._plugin_service is None:
            return None
        try:
            timeout = WrapperProperties.SOCKET_TIMEOUT_SEC.get_float(
                self._plugin_service.props)
        except Exception:  # noqa: BLE001 - unset/malformed -> no bound
            return None
        return timeout if timeout > 0 else None

    def _is_network_bound(self, method_name: str) -> bool:
        # Sync-parity gate (driver_dialect.py:134): the socket-timeout bound
        # applies when the dialect declares ALL methods network-bound or lists
        # this method explicitly. Fail open (bounded) when no dialect is wired.
        if self._plugin_service is None:
            return True
        try:
            network_bound = self._plugin_service.driver_dialect.network_bound_methods
        except Exception:  # noqa: BLE001 - no dialect yet -> keep the bound
            return True
        return (DbApiMethod.ALL.method_name in network_bound
                or method_name in network_bound)

    async def _abort_current_connection(self) -> None:
        if self._plugin_service is None:
            return
        conn = self._plugin_service.current_connection
        if conn is None:
            return
        try:
            await self._plugin_service.driver_dialect.abort_connection(
                getattr(conn, "driver_connection", conn))
        except Exception:  # noqa: BLE001 - abort is best-effort
            pass

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if role == HostRole.UNKNOWN:
            return False
        # Defer to the provider manager if available so custom providers'
        # strategies count. Falls back to the built-in selector set.
        if self._plugin_service is not None:
            return self._plugin_service.get_connection_provider_manager().accepts_strategy(
                role, strategy)
        return strategy in self._SELECTORS

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        if role == HostRole.UNKNOWN:
            raise AwsWrapperError(Messages.get("DefaultPlugin.UnknownHosts"))
        if self._plugin_service is not None:
            # Sync parity (default_plugin.py:136): consult the FILTERED
            # ``hosts`` view (allowed/blocked custom-endpoint permissions),
            # not the raw ``all_hosts``; an explicit host_list still wins.
            hosts = (tuple(host_list) if host_list is not None
                     else self._plugin_service.hosts)
            if not hosts:
                raise AwsWrapperError(Messages.get("DefaultPlugin.EmptyHosts"))
            return self._plugin_service.get_connection_provider_manager().get_host_info_by_strategy(
                hosts, role, strategy, self._plugin_service.props)
        # Legacy path (no plugin_service): use built-in selectors directly.
        selector = self._SELECTORS.get(strategy)
        if selector is None or host_list is None:
            return None
        return selector.get_host(tuple(host_list), role)
