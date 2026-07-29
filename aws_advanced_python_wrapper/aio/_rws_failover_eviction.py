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

"""Shared failover-eviction logic for the async read/write-splitting plugins.

Async parity with the sync ``AbstractReadWriteSplittingPlugin`` (PR #1117 /
#1229): when a ``FailoverError`` is raised while a command executes, the cached
reader/writer connections must be evicted -- otherwise an internal-pool
connection that failed over mid-query is returned to the pool stale, and a
later checkout hands back a connection bound to a now-demoted instance.

Both async RWS variants (:class:`AsyncReadWriteSplittingPlugin` and
:class:`AsyncSimpleReadWriteSplittingPlugin`) mix this in. It expects the host
class to define ``_plugin_service``, ``_reader_conn`` and ``_writer_conn``;
``_reader_host_info`` / ``_writer_host_info`` are cleared too when present (the
topology variant caches them; the simple variant uses fixed endpoint hosts).
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from aws_advanced_python_wrapper.errors import (FailoverError,
                                                FailoverFailedError)
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class _AsyncRwsFailoverEvictionMixin:
    """Failover-eviction helpers shared by the async RWS plugins."""

    # Declared on the concrete plugin classes; annotated here for type checkers.
    _plugin_service: Any
    _reader_conn: Any
    _writer_conn: Any
    # The topology RWS variant caches these; the simple variant uses fixed
    # endpoint hosts and never sets them. Declared as Any so each subclass may
    # annotate them as Optional[HostInfo] without a variance conflict.
    _reader_host_info: Any
    _writer_host_info: Any

    async def _execute_with_failover_eviction(
            self,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]]) -> Any:
        """Run ``execute_func`` and evict stale pooled conns on failover.

        Sync parity: ``AbstractReadWriteSplittingPlugin.execute``.
        ``FailoverFailedError`` -> no usable connection, so evict even the
        in-use one; any other ``FailoverError`` (success /
        transaction-resolution-unknown) means the wrapper reconnected, so evict
        only the idle cached connections and keep the in-use one.
        """
        try:
            return await execute_func()
        except FailoverFailedError:
            await self._close_connections(close_only_if_idle=False)
            raise
        except FailoverError:
            logger.debug(
                "ReadWriteSplittingPlugin.FailoverExceptionWhileExecutingCommand",
                method_name)
            await self._close_connections(close_only_if_idle=True)
            raise

    async def _close_connections(self, close_only_if_idle: bool = True) -> None:
        await self._close_connection(self._reader_conn, close_only_if_idle)
        await self._close_connection(self._writer_conn, close_only_if_idle)

    async def _close_connection(
            self,
            internal_conn: Any,
            close_only_if_idle: bool = True) -> None:
        if internal_conn is None:
            return
        current = self._plugin_service.current_connection
        driver_dialect = self._plugin_service.driver_dialect
        if close_only_if_idle and internal_conn is current:
            # In use and still usable (failover reconnected it) -> keep.
            return
        try:
            if not await driver_dialect.is_closed(internal_conn):
                result = internal_conn.close()
                if asyncio.iscoroutine(result):
                    await result
        except Exception:  # noqa: BLE001 - cleanup is best-effort
            pass
        finally:
            # Drop the cache slot so the dead connection is never reused.
            if internal_conn is self._writer_conn:
                self._writer_conn = None
                if getattr(self, "_writer_host_info", None) is not None:
                    self._writer_host_info = None
            if internal_conn is self._reader_conn:
                self._reader_conn = None
                if getattr(self, "_reader_host_info", None) is not None:
                    self._reader_host_info = None
