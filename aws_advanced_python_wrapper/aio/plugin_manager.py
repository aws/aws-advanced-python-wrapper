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

"""``AsyncPluginManager`` -- async counterpart of :class:`PluginManager`.

Builds + dispatches the plugin pipeline for ``connect`` and ``execute``, and
wraps dispatch in telemetry spans mirroring sync ``PluginManager`` (source of
truth: plugin_service.py:1004-1095): ``execute`` opens a TOP_LEVEL context
named for the method, ``connect`` opens a NESTED context, and every plugin
invocation in the chain is wrapped in a NESTED context named after the plugin
class. The factory is fetched once in ``__init__``; with the default
:class:`NullTelemetryFactory` (or any factory returning ``None`` contexts) the
spans collapse to guard checks, so dispatch behavior is unchanged.

NO plugin factory registry (each plugin registers its own factory); NO pipeline
cache (build-per-call).
"""

from __future__ import annotations

from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, List,
                    Optional, Set)

from aws_advanced_python_wrapper.aio.default_plugin import AsyncDefaultPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import \
    OldConnectionSuggestedAction
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.utils.notifications import (
        ConnectionEvent, HostEvent)
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
        TelemetryFactory


class AsyncPluginManager:
    """Builds the async plugin pipeline and dispatches ``connect`` /
    ``execute`` through it. Always appends :class:`AsyncDefaultPlugin` as
    the terminal plugin.
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties,
            plugins: Optional[List[AsyncPlugin]] = None) -> None:
        self._plugin_service = plugin_service
        self._props = props
        # Cache the telemetry factory once (mirrors sync PluginManager.__init__)
        # so per-call dispatch adds only guard checks when telemetry is off.
        self._telemetry_factory: TelemetryFactory = plugin_service.get_telemetry_factory()
        # Explicit list of plugins is what SP-1 accepts. SP-4+ will add a
        # factory-registry-based constructor overload so `plugins="failover,host_monitoring_v2"`
        # in connection props can build the list.
        user_plugins: List[AsyncPlugin] = list(plugins) if plugins else []
        self._plugins: List[AsyncPlugin] = [
            *user_plugins,
            AsyncDefaultPlugin(plugin_service),
        ]

    @property
    def num_plugins(self) -> int:
        return len(self._plugins)

    @property
    def plugins(self) -> List[AsyncPlugin]:
        return list(self._plugins)

    # ------------------------------------------------------------------
    # Public dispatch methods

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            plugin_to_skip: Optional[AsyncPlugin] = None) -> Any:
        # NESTED telemetry span around connect (sync plugin_service.py:1105-1116):
        # named "connect", closed in finally, no success flag. Per-plugin NESTED
        # spans are added inside _dispatch.
        context = self._telemetry_factory.open_telemetry_context(
            DbApiMethod.CONNECT.method_name, TelemetryTraceLevel.NESTED)
        try:
            return await self._dispatch(
                DbApiMethod.CONNECT,
                lambda plugin, next_func: plugin.connect(
                    target_driver_func,
                    driver_dialect,
                    host_info,
                    props,
                    is_initial_connection,
                    next_func,
                ),
                plugin_to_skip,
            )
        finally:
            if context is not None:
                context.close_context()

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            plugin_to_skip: Optional[AsyncPlugin] = None) -> Any:
        return await self._dispatch(
            DbApiMethod.FORCE_CONNECT,
            lambda plugin, next_func: plugin.force_connect(
                target_driver_func,
                driver_dialect,
                host_info,
                props,
                is_initial_connection,
                next_func,
            ),
            plugin_to_skip,
        )

    @staticmethod
    def _unwrap_conn(conn: Any) -> Any:
        """Reduce a connection-ish object to its underlying raw driver
        connection: a pool proxy -> its ``driver_connection``; anything else
        -> itself."""
        if conn is None:
            return None
        return getattr(conn, "driver_connection", conn)

    def _target_connection(self, target: object) -> Any:
        """The raw driver connection a cursor/connection target is bound to.

        Cursor: the raw cursor's ``connection`` (the conn it was created on).
        Connection wrapper: its current ``_target_conn``. Both are unwrapped
        to the underlying driver connection for a like-for-like comparison.

        Uses ``isinstance`` rather than ``getattr`` probing: the connection
        wrapper's ``__getattr__`` forwards unknown attributes to the driver
        connection, so a plain ``getattr(target, "_target_cursor")`` would
        wrongly resolve (and a MagicMock target would even fabricate it).
        """
        # Lazy import to avoid the wrapper<->plugin_manager import cycle.
        from aws_advanced_python_wrapper.aio.wrapper import (
            AsyncAwsWrapperConnection, AsyncAwsWrapperCursor)
        if isinstance(target, AsyncAwsWrapperCursor):
            return self._unwrap_conn(getattr(target._target_cursor, "connection", None))
        if isinstance(target, AsyncAwsWrapperConnection):
            return self._unwrap_conn(target._target_conn)
        return None

    async def execute(
            self,
            target: object,
            method: DbApiMethod,
            target_driver_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        # Old-connection guard (parity with sync plugin_service.execute:987).
        # A cursor/connection bound to a connection that is no longer the
        # current one -- e.g. an old cursor reused after an RWS reader/writer
        # swap or a failover -- must not silently run against the stale
        # connection. CLOSE methods are exempt (they clean up old objects).
        if method not in (DbApiMethod.CURSOR_CLOSE, DbApiMethod.CONNECTION_CLOSE):
            obj_conn = self._target_connection(target)
            current = self._unwrap_conn(self._plugin_service.current_connection)
            if obj_conn is not None and current is not None and obj_conn is not current:
                raise AwsWrapperError(Messages.get_formatted(
                    "PluginManager.MethodInvokedAgainstOldConnection", target))

        # TOP_LEVEL telemetry span around the whole call (sync plugin_service.py:
        # 1004-1026): named for the method, tagged with python_call, success set,
        # closed in finally. Guards keep this a no-op under the Null factory.
        context = self._telemetry_factory.open_telemetry_context(
            method.method_name, TelemetryTraceLevel.TOP_LEVEL)
        if context is not None:
            context.set_attribute("python_call", method.method_name)
        try:
            result = await self._dispatch(
                method,
                lambda plugin, next_func: plugin.execute(
                    target, method.method_name, next_func, *args, **kwargs),
                plugin_to_skip=None,
                terminal_call=target_driver_func,
            )
            if context is not None:
                context.set_success(True)
            return result
        except Exception:
            if context is not None:
                context.set_success(False)
            raise
        finally:
            if context is not None:
                context.close_context()

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        all_methods = DbApiMethod.ALL.method_name
        strategy_method = DbApiMethod.GET_HOST_INFO_BY_STRATEGY.method_name
        for plugin in self._plugins:
            subs = plugin.subscribed_methods
            if all_methods in subs or strategy_method in subs:
                if plugin.accepts_strategy(role, strategy):
                    return True
        return False

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        all_methods = DbApiMethod.ALL.method_name
        strategy_method = DbApiMethod.GET_HOST_INFO_BY_STRATEGY.method_name
        for plugin in self._plugins:
            subs = plugin.subscribed_methods
            if all_methods in subs or strategy_method in subs:
                host = plugin.get_host_info_by_strategy(role, strategy, host_list)
                if host is not None:
                    return host
        return None

    def notify_connection_changed(
            self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        """Notify every plugin that the current connection object changed.

        Mirrors sync ``PluginManager.notify_connection_changed``, including the
        aggregated :class:`OldConnectionSuggestedAction` return: PRESERVE wins
        over DISPOSE wins over NO_OPINION, so a plugin that still owns the old
        connection (e.g. RWS mid-switch caching its reader/writer) can stop the
        service from closing it. The host-monitoring (EFM) plugin resets its
        per-connection failure state in this hook; without the call, after
        failover swaps the connection the monitor keeps the OLD (dead) host's
        UNAVAILABLE flag set and re-raises "Host ... is unavailable" on the
        next query, even though the new connection is healthy
        (test_fail_from_reader_to_writer). Each plugin's hook is best-effort --
        a raising hook must not abort the switch (treated as NO_OPINION).
        """
        suggestions: Set[OldConnectionSuggestedAction] = set()
        for plugin in self._plugins:
            try:
                suggestion = plugin.notify_connection_changed(changes)
            except Exception:  # noqa: BLE001 - a notify hook must not break the switch
                continue
            if isinstance(suggestion, OldConnectionSuggestedAction):
                suggestions.add(suggestion)

        if OldConnectionSuggestedAction.PRESERVE in suggestions:
            return OldConnectionSuggestedAction.PRESERVE
        if OldConnectionSuggestedAction.DISPOSE in suggestions:
            return OldConnectionSuggestedAction.DISPOSE
        return OldConnectionSuggestedAction.NO_OPINION

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]) -> None:
        """Notify every plugin that the host list changed.

        Mirrors sync ``PluginManager.notify_host_list_changed``: plugins react
        to topology diffs (EFM stops monitors for deleted hosts, stale-DNS
        re-checks the cluster address, RWS re-caches by role). Dispatched
        best-effort to all plugins (the async plugin base provides a no-op
        default), matching ``notify_connection_changed`` above.
        """
        for plugin in self._plugins:
            try:
                plugin.notify_host_list_changed(changes)
            except Exception:  # noqa: BLE001 - a notify hook must not break the refresh
                pass

    # ------------------------------------------------------------------
    # Pipeline mechanics

    async def _execute_with_telemetry(
            self,
            plugin_name: str,
            func: Callable[[], Awaitable[Any]]) -> Any:
        """Wrap a single plugin invocation in a NESTED telemetry context named
        after the plugin class (sync ``_execute_with_telemetry``,
        plugin_service.py:1028-1034). The context is closed in ``finally`` so a
        raising plugin still cleans up; guards keep it a no-op under Null."""
        context = self._telemetry_factory.open_telemetry_context(
            plugin_name, TelemetryTraceLevel.NESTED)
        try:
            return await func()
        finally:
            if context is not None:
                context.close_context()

    async def _dispatch(
            self,
            method: DbApiMethod,
            plugin_call: Callable[[AsyncPlugin, Callable[..., Awaitable[Any]]], Awaitable[Any]],
            plugin_to_skip: Optional[AsyncPlugin],
            terminal_call: Optional[Callable[..., Awaitable[Any]]] = None) -> Any:
        """Build a chain over subscribed plugins and await the head.

        The pipeline walks plugins from first to last. Each subscribed plugin
        is wrapped so that ``plugin.execute(..., next_func)`` awaits
        ``next_func()`` to continue down the chain. The terminal ``next_func``
        is the target driver call (for ``execute``) or a no-op (for ``connect``
        -- the DefaultPlugin handles the actual driver call in its ``connect``
        override).
        """
        subscribed = self._subscribed_plugins(method, plugin_to_skip)
        if not subscribed:
            # No plugin wants to intercept this method; just run the terminal.
            if terminal_call is None:
                raise AwsWrapperError(Messages.get("PluginManager.PipelineNone"))
            return await terminal_call()

        # Build call chain back-to-front.
        async def _terminal() -> Any:
            if terminal_call is not None:
                return await terminal_call()
            # No terminal call: the last plugin (AsyncDefaultPlugin) is
            # responsible for producing the result itself.
            return None

        chain: Callable[..., Awaitable[Any]] = _terminal

        for plugin in reversed(subscribed):
            # Capture plugin + next in default args to pin the closure vars.
            next_in_chain = chain

            async def _step(
                    _plugin: AsyncPlugin = plugin,
                    _next: Callable[..., Awaitable[Any]] = next_in_chain) -> Any:
                # Each plugin invocation gets its own NESTED telemetry span named
                # after the plugin class (sync bakes this into _make_pipeline via
                # _execute_with_telemetry, plugin_service.py:1079-1095).
                return await self._execute_with_telemetry(
                    _plugin.__class__.__name__,
                    lambda: plugin_call(_plugin, _next))

            chain = _step

        return await chain()

    def _subscribed_plugins(
            self,
            method: DbApiMethod,
            plugin_to_skip: Optional[AsyncPlugin]) -> List[AsyncPlugin]:
        result: List[AsyncPlugin] = []
        all_methods_marker = DbApiMethod.ALL.method_name
        for plugin in self._plugins:
            if plugin_to_skip is not None and plugin_to_skip is plugin:
                continue
            subs: Set[str] = plugin.subscribed_methods
            # Sync parity (PluginManager._make_pipeline): even for
            # always_use_pipeline methods (CONNECT/FORCE_CONNECT) only
            # SUBSCRIBED plugins join the chain -- the flag forces the
            # pipeline to run, not unsubscribed plugins into it.
            if all_methods_marker in subs or method.method_name in subs:
                result.append(plugin)
        return result
