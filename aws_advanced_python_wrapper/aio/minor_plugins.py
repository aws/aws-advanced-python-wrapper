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

"""F3-B SP-8: remaining small async plugins.

Async counterparts for the sync "minor plugins":

* :class:`AsyncConnectTimePlugin` -- records time spent in connect()
* :class:`AsyncExecuteTimePlugin` -- records time spent in execute()
* :class:`AsyncDeveloperPlugin` -- optionally injects an exception into
  the pipeline on the next call; used for testing.
* :class:`AsyncAuroraConnectionTrackerPlugin` -- real tracker + writer-change
  invalidation. Lives in its own module
  (:mod:`aws_advanced_python_wrapper.aio.aurora_connection_tracker`); re-exported
  here so existing imports keep working.
* :class:`AsyncCustomEndpointPlugin` -- pass-through stub; full custom
  endpoint monitoring lands in its own SP.

Each plugin is kept intentionally small; none spawn background tasks.
"""

from __future__ import annotations

import asyncio
import time
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, List,
                    Optional, Set)

# Re-export: moved to its own module in Phase D.1. Keeping the import path stable
# so downstream users and the plugin factory don't care where the class lives.
from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncAuroraConnectionTrackerPlugin  # noqa: F401
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncConnectTimePlugin(AsyncPlugin):
    """Record wall-clock time spent in the connect phase.

    State is per-instance; applications can read ``total_connect_time_ns``
    to aggregate. ``plugin_service`` is optional so tests that never
    exercise telemetry can keep constructing the plugin with no args;
    production code (via the factory) always passes one.
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(
            self,
            plugin_service: Optional[AsyncPluginService] = None) -> None:
        self.total_connect_time_ns: int = 0
        self.connect_count: int = 0
        self._plugin_service = plugin_service
        # Telemetry counter -- matches sync ConnectTimePlugin intent.
        # NullTelemetryFactory returns a no-op; real factories may return
        # None, so every .inc() guards with ``is not None``.
        self._connect_counter = None
        if plugin_service is not None:
            tf = plugin_service.get_telemetry_factory()
            self._connect_counter = tf.create_counter(
                "connect_time.total.count")

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
        start_ns = time.perf_counter_ns()
        try:
            return await connect_func()
        finally:
            self.total_connect_time_ns += time.perf_counter_ns() - start_ns
            self.connect_count += 1
            if self._connect_counter is not None:
                self._connect_counter.inc()


class AsyncExecuteTimePlugin(AsyncPlugin):
    """Record wall-clock time spent in execute().

    Subscribes to everything network-bound; state is per-instance.
    ``plugin_service`` is optional so tests that never exercise telemetry
    can keep constructing the plugin with no args; production code (via
    the factory) always passes one.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    def __init__(
            self,
            plugin_service: Optional[AsyncPluginService] = None) -> None:
        self.total_execute_time_ns: int = 0
        self.execute_count: int = 0
        self._plugin_service = plugin_service
        self._execute_counter = None
        if plugin_service is not None:
            tf = plugin_service.get_telemetry_factory()
            self._execute_counter = tf.create_counter(
                "execute_time.total.count")

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        start_ns = time.perf_counter_ns()
        try:
            return await execute_func()
        finally:
            self.total_execute_time_ns += time.perf_counter_ns() - start_ns
            self.execute_count += 1
            if self._execute_counter is not None:
                self._execute_counter.inc()


class AsyncDeveloperPlugin(AsyncPlugin):
    """Test-only plugin that injects exceptions / callbacks into the pipeline.

    Mirrors the sync ``DeveloperPlugin`` + ``ExceptionSimulatorManager`` pair.
    Supports four injection modes, all stored as ``ClassVar`` so tests and
    debuggers can reach them without holding a plugin instance:

    * ``set_next_connect_exception`` — one-shot exception raised from the next
      :meth:`connect` call; cleared after firing.
    * ``set_connect_callback`` — callable invoked on every connect; may raise
      (the raise is propagated) or return normally. Stays installed until
      cleared explicitly.
    * ``set_next_method_exception`` — one-shot exception for the next
      :meth:`execute`; cleared after firing.
    * ``set_method_callback`` — callable invoked on every execute. Same
      semantics as the connect callback.

    Callbacks may be sync or async; if a callback returns a coroutine, the
    plugin awaits it. :meth:`clear` resets all four slots and is wired into
    the unit-test ``pytest_runtest_setup`` hook to prevent cross-test leaks
    (see ``tests/unit/conftest.py``).

    Kept as a test helper; not safe for production use.
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.ALL.method_name}

    _next_connect_exception: ClassVar[Optional[BaseException]] = None
    _connect_callback: ClassVar[Optional[Callable[..., Any]]] = None
    _next_method_exception: ClassVar[Optional[BaseException]] = None
    _method_callback: ClassVar[Optional[Callable[..., Any]]] = None

    # Retained for backwards compat with the one-shot execute-only API
    # shipped before this plugin grew the 4-mode surface. Delegates to
    # ``set_next_method_exception``; new callers should use the explicit name.
    def set_next_exception(self, exc: BaseException) -> None:
        AsyncDeveloperPlugin._next_method_exception = exc

    @classmethod
    def set_next_connect_exception(cls, exc: BaseException) -> None:
        cls._next_connect_exception = exc

    @classmethod
    def set_connect_callback(cls, cb: Callable[..., Any]) -> None:
        cls._connect_callback = cb

    @classmethod
    def set_next_method_exception(cls, exc: BaseException) -> None:
        cls._next_method_exception = exc

    @classmethod
    def set_method_callback(cls, cb: Callable[..., Any]) -> None:
        cls._method_callback = cb

    @classmethod
    def clear(cls) -> None:
        cls._next_connect_exception = None
        cls._connect_callback = None
        cls._next_method_exception = None
        cls._method_callback = None

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
        # Callback fires first; a raise here propagates and does NOT consume
        # the one-shot exception slot (callbacks are persistent, not one-shot).
        cb = AsyncDeveloperPlugin._connect_callback
        if cb is not None:
            result = cb(host_info, props)
            if asyncio.iscoroutine(result):
                await result
        exc = AsyncDeveloperPlugin._next_connect_exception
        if exc is not None:
            AsyncDeveloperPlugin._next_connect_exception = None
            raise exc
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        cb = AsyncDeveloperPlugin._method_callback
        if cb is not None:
            result = cb(method_name, args, kwargs)
            if asyncio.iscoroutine(result):
                await result
        exc = AsyncDeveloperPlugin._next_method_exception
        if exc is not None:
            AsyncDeveloperPlugin._next_method_exception = None
            raise exc
        return await execute_func()


class AsyncCustomEndpointPlugin(AsyncPlugin):
    """Placeholder for async custom-endpoint support.

    Sync custom endpoint plugin maintains a background monitor resolving
    Aurora custom endpoints to member instances. The async version follows
    the same pattern as AsyncClusterTopologyMonitor; 3.0.0 ships a
    pass-through so existing apps don't error when the plugin code is in
    their profile.
    """

    @property
    def subscribed_methods(self) -> Set[str]:
        # Subscribing to nothing means PluginManager skips this plugin for
        # every method; the class exists only so `plugins="custom_endpoint"`
        # doesn't fail when the async engine starts up.
        return set()


__all__ = [
    "AsyncConnectTimePlugin",
    "AsyncExecuteTimePlugin",
    "AsyncDeveloperPlugin",
    "AsyncAuroraConnectionTrackerPlugin",
    "AsyncCustomEndpointPlugin",
]


_unused_list: List[str] = []
