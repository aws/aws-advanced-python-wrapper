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

"""Where the key and metadata layer gets a connection to run its SQL on."""

from __future__ import annotations

from contextlib import contextmanager
from threading import local
from typing import (TYPE_CHECKING, Any, ContextManager, Iterator, Optional,
                    Protocol)

from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.retry_util import RetryUtil

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin import Plugin
    from aws_advanced_python_wrapper.plugin_service import PluginService


class ConnectionSource(Protocol):
    def connection(self) -> ContextManager[Any]:
        """Return a context manager that yields a DB-API connection."""
        ...


class CallerConnectionSource:
    """Runs every operation on a connection the caller owns.

    The connection is never closed, committed, or rolled back here, so a
    multi-step administrative operation stays inside the caller's transaction.
    """

    def __init__(self, conn: Any):
        self._conn = conn

    @contextmanager
    def connection(self) -> Iterator[Any]:
        yield self._conn


class PluginConnectionSource:
    """Opens a short-lived connection through the wrapper's connect pipeline
    and closes it: one per :meth:`connection` call, or one per
    :meth:`statement` scope on the thread.

    Metadata and key reads therefore never use the application's connection,
    and cannot block on or join the application's transaction. The connection
    runs in autocommit mode so a read never leaves a session idle in a
    transaction and a ``last_used_at`` update is not lost on close.
    """

    def __init__(self, plugin_service: PluginService, plugin_to_skip: Optional[Plugin] = None):
        self._plugin_service = plugin_service
        self._plugin_to_skip = plugin_to_skip
        self._scope = local()

    @contextmanager
    def statement(self) -> Iterator[None]:
        """Share one connection among the lookups of one statement on this thread.

        The connection opens at the first lookup inside the scope and closes when
        the outermost scope ends, also when a lookup fails, so a statement uses at
        most one short-lived connection. A scope that looks nothing up opens
        nothing.
        """
        depth = getattr(self._scope, "depth", 0)
        self._scope.depth = depth + 1
        if depth == 0:
            self._scope.conn = None
        try:
            yield
        finally:
            self._scope.depth = depth
            if depth == 0:
                conn, self._scope.conn = self._scope.conn, None
                if conn is not None:
                    RetryUtil.close_connection(self._plugin_service, conn)

    @contextmanager
    def connection(self) -> Iterator[Any]:
        if getattr(self._scope, "depth", 0):
            if self._scope.conn is None:
                self._scope.conn = self._open()
            yield self._scope.conn
            return
        conn = self._open()
        try:
            yield conn
        finally:
            RetryUtil.close_connection(self._plugin_service, conn)

    def _open(self) -> Any:
        service = self._plugin_service
        conn = service.force_connect(
            service.current_host_info, Properties(service.props.copy()), self._plugin_to_skip)
        try:
            service.driver_dialect.set_autocommit(conn, True)
        except BaseException:  # noqa: BLE001 - close the half-opened connection, then re-raise it as is
            RetryUtil.close_connection(service, conn)
            raise
        return conn
