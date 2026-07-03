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

"""``AsyncPlugin`` / ``AsyncConnectionProvider`` abstract base classes.

Per SP-0 decision D3, async plugins do NOT inherit from sync ``Plugin``.
Parallel class hierarchies avoid mixing ``def`` and ``async def`` methods
on one class.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, List,
                    Optional, Protocol, Set, Tuple, runtime_checkable)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.utils.notifications import (
        ConnectionEvent, HostEvent)
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncPlugin(ABC):
    """Async counterpart of :class:`Plugin`. Independent ABC, not a subclass."""

    @property
    @abstractmethod
    def subscribed_methods(self) -> Set[str]:
        """Methods this plugin wants to intercept in the async pipeline."""

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        """Establishes a connection via the async pipeline. Default passes through."""
        return await connect_func()

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable[..., Awaitable[Any]]) -> Any:
        """Bypass any custom connection provider; use the default async driver."""
        return await force_connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        """Intercept an async method call on a connection/cursor."""
        return await execute_func()

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]) -> None:
        return

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> None:
        """Notified when the current connection OBJECT changes -- e.g. failover
        or read/write-splitting swaps the underlying connection. Default no-op;
        plugins that hold per-connection state (the EFM monitor) override this
        to reset/re-point it."""
        return

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        """Default: this plugin does not support any strategy."""
        return False

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        """Default: this plugin does not participate in strategy-based selection."""
        return None


class AsyncConnectionProvider(Protocol):
    """Async counterpart of :class:`ConnectionProvider`."""

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(
            self,
            hosts: Tuple[HostInfo, ...],
            role: HostRole,
            strategy: str,
            props: Properties) -> HostInfo:
        ...

    async def connect(
            self,
            target_func: Callable,
            driver_dialect: AsyncDriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Any:
        ...


@runtime_checkable
class AsyncCanReleaseResources(Protocol):
    """Async counterpart of :class:`CanReleaseResources`.

    Providers and monitors implement this so :meth:`AsyncPluginService.release_resources`
    can await cleanup without duck-typing via ``hasattr``.
    """

    async def release_resources(self) -> None:
        ...
