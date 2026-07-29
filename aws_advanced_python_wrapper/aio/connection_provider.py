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

"""Async counterpart of :mod:`aws_advanced_python_wrapper.connection_provider`.

Mirrors the sync file's three classes:
  * :class:`AsyncConnectionProvider` -- Protocol each provider satisfies.
  * :class:`AsyncDriverConnectionProvider` -- default pass-through that
    routes connects to the target async driver via the :class:`AsyncDriverDialect`.
  * :class:`AsyncConnectionProviderManager` -- registry holding an optional
    user-supplied provider + the default, with sync parity semantics.

The manager-level configuration methods (``set_connection_provider``,
``reset_provider``) remain sync because they're one-shot startup calls --
they use a :class:`threading.Lock` so the async API surface matches sync's.
"""

from __future__ import annotations

from threading import Lock
from types import MappingProxyType
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    Mapping, Optional, Protocol, Tuple)

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import (
    HighestWeightHostSelector, HostSelector, RandomHostSelector,
    RoundRobinHostSelector, WeightedRandomHostSelector)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole

logger = Logger(__name__)


class AsyncConnectionProvider(Protocol):
    """Protocol every async connection provider satisfies.

    Sync and async providers diverge only in ``connect``: sync returns
    the driver connection directly; async awaits it. The strategy/host
    filter methods remain sync because they consult in-memory registries.
    """

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(
            self,
            hosts: Tuple[HostInfo, ...],
            role: HostRole,
            strategy: str,
            props: Optional[Properties]) -> HostInfo:
        ...

    async def connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: AsyncDriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Any:
        ...


class AsyncDriverConnectionProvider(AsyncConnectionProvider):
    """Default async provider -- opens connections via the target driver.

    Reuses the sync :class:`DriverConnectionProvider` host-selector
    registry via its ``accepted_strategies()`` classmethod, so new
    selectors added on the sync side become available to async
    automatically and round-robin state stays shared.
    """

    _accepted_strategies: ClassVar[Dict[str, HostSelector]] = {
        "random": RandomHostSelector(),
        "round_robin": RoundRobinHostSelector(),
        "weighted_random": WeightedRandomHostSelector(),
        "highest_weight": HighestWeightHostSelector(),
    }

    @classmethod
    def accepted_strategies(cls) -> Mapping[str, HostSelector]:
        """Public read-only view of the selector registry."""
        return MappingProxyType(cls._accepted_strategies)

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        return True

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepted_strategies

    def get_host_info_by_strategy(
            self,
            hosts: Tuple[HostInfo, ...],
            role: HostRole,
            strategy: str,
            props: Optional[Properties]) -> HostInfo:
        selector: Optional[HostSelector] = self._accepted_strategies.get(strategy)
        if selector is not None:
            return selector.get_host(hosts, role, props)
        raise AwsWrapperError(
            Messages.get_formatted(
                "DriverConnectionProvider.UnsupportedStrategy", strategy))

    async def connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: AsyncDriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Any:
        prepared = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared)
        logger.debug(
            "DriverConnectionProvider.ConnectingToHost",
            host_info.host,
            PropertiesUtils.log_properties(PropertiesUtils.mask_properties(prepared)),
        )
        # Delegate to the dialect so per-driver connect handling (e.g. aiomysql's
        # full-connect timeout bounding) applies here too -- calling target_func
        # directly would bypass it and let a dead endpoint hang the handshake.
        return await driver_dialect.execute_connect(target_func, prepared, props)


class AsyncConnectionProviderManager:
    """Registry holding the optional user-supplied provider + the default.

    Semantic parity with sync's :class:`ConnectionProviderManager`:
      * ``set_connection_provider`` installs a custom provider (applied to
        any host the provider accepts).
      * Falls back to the default provider otherwise.
      * ``reset_provider`` / ``release_resources`` for lifecycle parity.

    Uses :class:`threading.Lock` for the class-level registry slot so
    calls from multiple event loops / setup threads don't race. The lock
    is held only around the registry read/write, never around the
    provider's ``connect`` (which is async and long-running).
    """

    _lock: ClassVar[Lock] = Lock()
    _conn_provider: ClassVar[Optional[AsyncConnectionProvider]] = None

    def __init__(
            self,
            default_provider: Optional[AsyncConnectionProvider] = None) -> None:
        self._default_provider: AsyncConnectionProvider = (
            default_provider if default_provider is not None
            else AsyncDriverConnectionProvider()
        )

    @property
    def default_provider(self) -> AsyncConnectionProvider:
        return self._default_provider

    @staticmethod
    def set_connection_provider(
            connection_provider: AsyncConnectionProvider) -> None:
        """Install a non-default async connection provider.

        The provider is consulted first; if it does not accept the host
        (``accepts_host_info`` returns False), the default provider is
        used instead.
        """
        with AsyncConnectionProviderManager._lock:
            AsyncConnectionProviderManager._conn_provider = connection_provider

    def get_connection_provider(
            self,
            host_info: HostInfo,
            props: Properties) -> AsyncConnectionProvider:
        """Return the provider to use for ``host_info``.

        Custom provider if installed and ``accepts_host_info`` returns
        True; default otherwise.
        """
        if AsyncConnectionProviderManager._conn_provider is None:
            return self._default_provider
        with AsyncConnectionProviderManager._lock:
            custom = AsyncConnectionProviderManager._conn_provider
            if custom is not None and custom.accepts_host_info(host_info, props):
                return custom
        return self._default_provider

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        accepts = False
        if AsyncConnectionProviderManager._conn_provider is not None:
            with AsyncConnectionProviderManager._lock:
                custom = AsyncConnectionProviderManager._conn_provider
                if custom is not None:
                    accepts = custom.accepts_strategy(role, strategy)
        if not accepts:
            accepts = self._default_provider.accepts_strategy(role, strategy)
        return accepts

    def get_host_info_by_strategy(
            self,
            hosts: Tuple[HostInfo, ...],
            role: HostRole,
            strategy: str,
            props: Optional[Properties]) -> HostInfo:
        if AsyncConnectionProviderManager._conn_provider is not None:
            with AsyncConnectionProviderManager._lock:
                custom = AsyncConnectionProviderManager._conn_provider
                if custom is not None and custom.accepts_strategy(role, strategy):
                    return custom.get_host_info_by_strategy(
                        hosts, role, strategy, props)
        return self._default_provider.get_host_info_by_strategy(
            hosts, role, strategy, props)

    @staticmethod
    def reset_provider() -> None:
        """Clear the non-default provider if one was installed."""
        if AsyncConnectionProviderManager._conn_provider is not None:
            with AsyncConnectionProviderManager._lock:
                AsyncConnectionProviderManager._conn_provider = None

    @staticmethod
    async def release_resources() -> None:
        """Release resources held by the installed provider.

        Async because a custom provider's ``release_resources`` may be
        async (e.g., a pool that awaits connection teardown).
        """
        if AsyncConnectionProviderManager._conn_provider is None:
            return
        with AsyncConnectionProviderManager._lock:
            provider = AsyncConnectionProviderManager._conn_provider
        if provider is None:
            return
        release = getattr(provider, "release_resources", None)
        if release is None:
            return
        result = release()
        if hasattr(result, "__await__"):
            try:
                await result
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass


__all__ = [
    "AsyncConnectionProvider",
    "AsyncDriverConnectionProvider",
    "AsyncConnectionProviderManager",
]
