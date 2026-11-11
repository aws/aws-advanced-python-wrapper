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

"""Async counterpart of :class:`ConnectionLifetimePlugin`.

Same contract as the sync plugin -- stamp each physical connection, and on
the first network-bound call after ``connection_lifetime_ms`` has elapsed
either close the connection or replace it with one on the host the
application originally asked for. Property parsing and the action enum are
shared with the sync module so the two cannot disagree on what a
configuration value means.
"""

from __future__ import annotations

import asyncio
from time import monotonic_ns
from typing import TYPE_CHECKING, Any, Callable, Optional, Set
from weakref import WeakKeyDictionary

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.connection_lifetime_plugin import (
    LIFECYCLE_METHODS, ConnectionLifetimeAction,
    parse_connection_lifetime_action, parse_connection_lifetime_ns)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                ConnectionLifetimeExpiredError,
                                                ConnectionLifetimeFailedError,
                                                ConnectionLifetimeSuccessError)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, OldConnectionSuggestedAction)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


class AsyncConnectionLifetimePlugin(AsyncPlugin):
    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties,
            clock_ns: Callable[[], int] = monotonic_ns):
        self._plugin_service = plugin_service
        self._props = props
        # Injected only by the tests, which need to move time without sleeping.
        self._clock_ns = clock_ns
        self._connection_lifetime_ns = parse_connection_lifetime_ns(props)
        self._action = parse_connection_lifetime_action(props)

        subscribed = set(plugin_service.network_bound_methods)
        subscribed.update({
            DbApiMethod.CONNECT.method_name,
            DbApiMethod.FORCE_CONNECT.method_name,
            DbApiMethod.NOTIFY_CONNECTION_CHANGED.method_name,
        })
        self._subscribed_methods = subscribed

        # See the sync plugin for why the keys are weak.
        self._connection_creation_times: WeakKeyDictionary = WeakKeyDictionary()
        self._original_host_info: Optional[HostInfo] = None
        self._timed_connection: Optional[Any] = None
        self._connection_creation_time_ns: Optional[int] = None
        self._terminal = False

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._subscribed_methods

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Any:
        return await self._connect(host_info, is_initial_connection, connect_func)

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Any:
        return await self._connect(host_info, is_initial_connection, force_connect_func)

    async def _connect(
            self,
            host_info: HostInfo,
            is_initial_connection: bool,
            connect_func: Callable) -> Any:
        # Sync parity (connection_lifetime_plugin.py): the reconnect host is
        # the application's own, captured from the first initial connect
        # because the plugin service does not yet carry it during
        # init_host_provider.
        if is_initial_connection and self._original_host_info is None:
            self._original_host_info = host_info

        connection = await connect_func()
        self._record_connection_creation_time(connection)
        return connection

    def _record_connection_creation_time(self, connection: Optional[Any]) -> None:
        if connection is None:
            return
        try:
            self._connection_creation_times[connection] = self._clock_ns()
        except TypeError:
            # Not weak-referenceable; stamped on first use instead.
            pass

    def notify_connection_changed(
            self, changes: Set[ConnectionEvent]) -> Optional[OldConnectionSuggestedAction]:
        if (ConnectionEvent.INITIAL_CONNECTION in changes
                or ConnectionEvent.CONNECTION_OBJECT_CHANGED in changes):
            self._select_current_connection_timestamp()
        return OldConnectionSuggestedAction.NO_OPINION

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable,
            *args: Any,
            **kwargs: Any) -> Any:
        if (method_name in LIFECYCLE_METHODS
                or not self._plugin_service.is_network_bound_method(method_name)):
            return await execute_func()

        if self._terminal:
            raise ConnectionLifetimeExpiredError(
                Messages.get("ConnectionLifetimePlugin.ConnectionClosed"))

        if not self._is_current_connection_expired():
            return await execute_func()

        # An open transaction finishes on the connection it started on, so
        # commit and rollback are not cut off mid-way. Deferring does not
        # extend the lifetime.
        if self._plugin_service.is_in_transaction:
            logger.debug("ConnectionLifetimePlugin.ActionDeferred", method_name)
            return await execute_func()

        if self._action == ConnectionLifetimeAction.CLOSE:
            await self._close_current_connection()
        else:
            await self._reconnect()

    def _select_current_connection_timestamp(self) -> None:
        current_connection = self._plugin_service.current_connection
        creation_time = None
        if current_connection is not None:
            creation_time = self._connection_creation_times.get(current_connection)
            if creation_time is None:
                creation_time = self._clock_ns()
                self._record_connection_creation_time(current_connection)

        self._timed_connection = current_connection
        self._connection_creation_time_ns = creation_time

    def _is_current_connection_expired(self) -> bool:
        current_connection = self._plugin_service.current_connection
        if current_connection is not self._timed_connection or self._connection_creation_time_ns is None:
            self._select_current_connection_timestamp()

        creation_time = self._connection_creation_time_ns
        # Inclusive boundary: a 1000 ms lifetime expires at exactly 1000 ms.
        return (creation_time is not None
                and self._clock_ns() - creation_time >= self._connection_lifetime_ns)

    async def _close_current_connection(self) -> None:
        self._terminal = True
        current_connection = self._plugin_service.current_connection
        close_failure: Optional[Exception] = None

        if current_connection is not None:
            self._connection_creation_times.pop(current_connection, None)
            try:
                await self._close(current_connection)
            except Exception as e:
                close_failure = e
                logger.warning("ConnectionLifetimePlugin.CloseConnectionFailed", e)

        error = ConnectionLifetimeExpiredError(
            Messages.get("ConnectionLifetimePlugin.ConnectionClosed"))
        if close_failure is not None:
            raise error from close_failure
        raise error

    async def _reconnect(self) -> None:
        previous_connection = self._plugin_service.current_connection
        replacement_connection = None
        try:
            reconnect_host_info = self._original_host_info
            if reconnect_host_info is None:
                reconnect_host_info = self._plugin_service.initial_connection_host_info
            if reconnect_host_info is None:
                raise AwsWrapperError(
                    Messages.get("ConnectionLifetimePlugin.OriginalHostUnavailable"))

            logger.debug("ConnectionLifetimePlugin.Reconnecting", reconnect_host_info.url)
            replacement_connection = await self._plugin_service.connect(
                reconnect_host_info, self._props, self)
            if replacement_connection is None:
                raise AwsWrapperError(
                    Messages.get("ConnectionLifetimePlugin.ReconnectReturnedNone"))
            replacement_creation_time = self._clock_ns()

            await self._plugin_service.set_current_connection(
                replacement_connection, reconnect_host_info)
            await self._forget_closed_connection(previous_connection)
            self._connection_creation_times[replacement_connection] = replacement_creation_time
            self._timed_connection = replacement_connection
            self._connection_creation_time_ns = replacement_creation_time
        except Exception as e:
            await self._cleanup_failed_replacement(replacement_connection, previous_connection)
            raise ConnectionLifetimeFailedError(
                Messages.get("ConnectionLifetimePlugin.UnableToReconnect")) from e

        raise ConnectionLifetimeSuccessError(
            Messages.get("ConnectionLifetimePlugin.ConnectionChanged"))

    async def _forget_closed_connection(self, connection: Optional[Any]) -> None:
        if connection is None:
            return

        try:
            closed = await self._plugin_service.driver_dialect.is_closed(connection)
        except Exception:
            # A connection that cannot report its state is unusable; do not retain it.
            closed = True

        if closed:
            self._connection_creation_times.pop(connection, None)

    async def _cleanup_failed_replacement(
            self,
            replacement_connection: Optional[Any],
            previous_connection: Optional[Any]) -> None:
        if (replacement_connection is not None
                and replacement_connection is self._plugin_service.current_connection
                and replacement_connection is not previous_connection):
            # The replacement was installed and the failure came after, so
            # neither connection can be relied on from here.
            self._terminal = True
            await self._close_after_failed_installation(replacement_connection)
            await self._close_after_failed_installation(previous_connection)
            return

        await self._close_unattached_replacement(replacement_connection)

    async def _close_after_failed_installation(self, connection: Optional[Any]) -> None:
        if connection is None:
            return

        self._connection_creation_times.pop(connection, None)
        try:
            await self._close(connection)
        except Exception as e:
            logger.warning("ConnectionLifetimePlugin.FailedInstallationConnectionCloseFailed", e)

    async def _close_unattached_replacement(self, replacement_connection: Optional[Any]) -> None:
        if (replacement_connection is None
                or replacement_connection is self._plugin_service.current_connection):
            return

        self._connection_creation_times.pop(replacement_connection, None)
        try:
            await self._close(replacement_connection)
        except Exception as e:
            logger.warning("ConnectionLifetimePlugin.ReplacementCloseFailed", e)

    @staticmethod
    async def _close(connection: Any) -> None:
        # aiomysql's close() is a coroutine, psycopg's async close() is too,
        # but a mock or a sync driver connection returns None -- mirrors
        # AsyncPluginService's own old-connection disposal.
        result = connection.close()
        if asyncio.iscoroutine(result):
            await result
