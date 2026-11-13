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

"""Bounds how long the wrapper keeps a physical connection.

The plugin stamps every physical connection it sees created, and before each
network-bound call checks whether the current one has outlived
``connection_lifetime_ms``. An expired connection is either closed or replaced
by a fresh one on the host the application originally asked for. There is no
background timer: an expired connection stays open until the next
network-bound call.
"""

from __future__ import annotations

from enum import Enum
from time import monotonic_ns
from typing import TYPE_CHECKING, Any, Callable, Optional, Set
from weakref import WeakKeyDictionary

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                ConnectionLifetimeExpiredError,
                                                ConnectionLifetimeFailedError,
                                                ConnectionLifetimeSuccessError)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

logger = Logger(__name__)

# Teardown calls never trigger the lifetime action, even though the driver
# dialects count them as network-bound. Closing an expired connection has to
# keep working -- raising here would mean a connection could not be disposed
# of, and would surface out of a ``with`` block's exit in place of whatever
# the body raised.
LIFECYCLE_METHODS = frozenset({
    DbApiMethod.CONNECTION_CLOSE.method_name,
    DbApiMethod.CONNECTION_IS_CLOSED.method_name,
    DbApiMethod.CURSOR_CLOSE.method_name,
})


class ConnectionLifetimeAction(Enum):
    CLOSE = "close"
    RECONNECT = "reconnect"


def parse_connection_lifetime_ns(props: Properties) -> int:
    """Read ``connection_lifetime_ms`` as nanoseconds, or raise.

    The property is required and has no default, so an absent, non-numeric or
    non-positive value is a configuration error worth failing the connect on
    rather than silently disabling the plugin.
    """
    raw = WrapperProperties.CONNECTION_LIFETIME_MS.get(props)
    try:
        lifetime_ms = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise AwsWrapperError(
            Messages.get_formatted("ConnectionLifetimePlugin.InvalidConnectionLifetime", raw))

    if lifetime_ms < 1:
        raise AwsWrapperError(
            Messages.get_formatted("ConnectionLifetimePlugin.InvalidConnectionLifetime", raw))

    return lifetime_ms * 1_000_000


def parse_connection_lifetime_action(props: Properties) -> ConnectionLifetimeAction:
    """Read ``connection_lifetime_action``, ignoring surrounding space and case."""
    configured = WrapperProperties.CONNECTION_LIFETIME_ACTION.get(props)
    try:
        return ConnectionLifetimeAction(str(configured).strip().lower())
    except ValueError:
        raise AwsWrapperError(
            Messages.get_formatted(
                "ConnectionLifetimePlugin.InvalidConnectionLifetimeAction", configured))


class ConnectionLifetimePlugin(Plugin):
    def __init__(
            self,
            plugin_service: PluginService,
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

        # Keyed on the connection object, so two physical connections that
        # compare equal are still timed separately. Weak keys keep a
        # connection the pipeline opened and then discarded from being held
        # alive by this map; a connection that cannot be weak-referenced is
        # simply not pre-stamped and gets its timestamp on first use instead.
        self._connection_creation_times: WeakKeyDictionary = WeakKeyDictionary()
        self._original_host_info: Optional[HostInfo] = None
        self._timed_connection: Optional[Connection] = None
        self._connection_creation_time_ns: Optional[int] = None
        self._terminal = False

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._subscribed_methods

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect(host_info, is_initial_connection, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(host_info, is_initial_connection, force_connect_func)

    def _connect(
            self,
            host_info: HostInfo,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        # The host to reconnect through is the one the application asked for,
        # not one a later routing, read/write-splitting or failover decision
        # landed on. It cannot be read from the plugin service in
        # init_host_provider: AwsWrapperConnection.__init__ only fills that
        # field in the refresh_host_list() that runs after the
        # init_host_provider pipeline, so it is still None inside the hook.
        # The first initial connect carries the same host, and this plugin is
        # the outermost one in the chain, so it sees that host before any
        # nested connect a routing plugin makes.
        #
        # Note this names the host the reconnect *asks* for. Plugins beneath
        # this one may route the replacement elsewhere -- initial_connection
        # substitutes a writer instance for a cluster endpoint on a
        # non-initial connect.
        if is_initial_connection and self._original_host_info is None:
            self._original_host_info = host_info

        connection = connect_func()
        self._record_connection_creation_time(connection)
        return connection

    def _record_connection_creation_time(self, connection: Optional[Connection]) -> None:
        if connection is None:
            return
        try:
            self._connection_creation_times[connection] = self._clock_ns()
        except TypeError:
            # Not weak-referenceable; _select_current_connection_timestamp
            # stamps it on first use.
            pass

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        if (ConnectionEvent.INITIAL_CONNECTION in changes
                or ConnectionEvent.CONNECTION_OBJECT_CHANGED in changes):
            self._select_current_connection_timestamp()
        return OldConnectionSuggestedAction.NO_OPINION

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        if (method_name in LIFECYCLE_METHODS
                or not self._plugin_service.is_network_bound_method(method_name)):
            return execute_func()

        if self._terminal:
            raise ConnectionLifetimeExpiredError(
                Messages.get("ConnectionLifetimePlugin.ConnectionClosed"))

        if not self._is_current_connection_expired():
            return execute_func()

        # Let an open transaction finish on the connection it started on, so
        # commit and rollback are not cut off mid-way. Deferring does not
        # extend the lifetime: the action runs on the first eligible call once
        # the transaction ends.
        if self._plugin_service.is_in_transaction:
            logger.debug("ConnectionLifetimePlugin.ActionDeferred", method_name)
            return execute_func()

        if self._action == ConnectionLifetimeAction.CLOSE:
            self._close_current_connection()
        else:
            self._reconnect()

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
        # The boundary is inclusive: a 1000 ms lifetime expires at exactly 1000 ms.
        return (creation_time is not None
                and self._clock_ns() - creation_time >= self._connection_lifetime_ns)

    def _close_current_connection(self) -> None:
        # Set before closing: once the physical connection is on its way out
        # the wrapper connection is unusable whether or not close() succeeds.
        self._terminal = True
        current_connection = self._plugin_service.current_connection
        close_failure: Optional[Exception] = None

        if current_connection is not None:
            self._connection_creation_times.pop(current_connection, None)
            try:
                current_connection.close()
            except Exception as e:
                close_failure = e
                logger.warning("ConnectionLifetimePlugin.CloseConnectionFailed", e)

        error = ConnectionLifetimeExpiredError(
            Messages.get("ConnectionLifetimePlugin.ConnectionClosed"))
        if close_failure is not None:
            raise error from close_failure
        raise error

    def _reconnect(self) -> None:
        previous_connection = self._plugin_service.current_connection
        replacement_connection = None
        try:
            reconnect_host_info = self._original_host_info
            if reconnect_host_info is None:
                # Fall back to whatever the plugin service still reports; a
                # pipeline that never ran an initial connect through this
                # plugin (a pooled connection handed over, say) has no
                # application host recorded.
                reconnect_host_info = self._plugin_service.initial_connection_host_info
            if reconnect_host_info is None:
                raise AwsWrapperError(
                    Messages.get("ConnectionLifetimePlugin.OriginalHostUnavailable"))

            logger.debug("ConnectionLifetimePlugin.Reconnecting", reconnect_host_info.url)
            replacement_connection = self._plugin_service.connect(
                reconnect_host_info, self._props, self)
            if replacement_connection is None:
                raise AwsWrapperError(
                    Messages.get("ConnectionLifetimePlugin.ReconnectReturnedNone"))
            replacement_creation_time = self._clock_ns()

            # Installs the replacement and disposes the old connection. The
            # timer is only reset once this has succeeded.
            self._plugin_service.set_current_connection(replacement_connection, reconnect_host_info)
            self._forget_closed_connection(previous_connection)
            self._connection_creation_times[replacement_connection] = replacement_creation_time
            self._timed_connection = replacement_connection
            self._connection_creation_time_ns = replacement_creation_time
        except Exception as e:
            self._cleanup_failed_replacement(replacement_connection, previous_connection)
            raise ConnectionLifetimeFailedError(
                Messages.get("ConnectionLifetimePlugin.UnableToReconnect")) from e

        raise ConnectionLifetimeSuccessError(
            Messages.get("ConnectionLifetimePlugin.ConnectionChanged"))

    def _forget_closed_connection(self, connection: Optional[Connection]) -> None:
        if connection is None:
            return

        try:
            closed = self._plugin_service.driver_dialect.is_closed(connection)
        except Exception:
            # A connection that cannot report its state is unusable; do not retain it.
            closed = True

        if closed:
            self._connection_creation_times.pop(connection, None)

    def _cleanup_failed_replacement(
            self,
            replacement_connection: Optional[Connection],
            previous_connection: Optional[Connection]) -> None:
        if (replacement_connection is not None
                and replacement_connection is self._plugin_service.current_connection
                and replacement_connection is not previous_connection):
            # The replacement was installed and the failure came after, so
            # neither connection can be relied on from here.
            self._terminal = True
            self._close_after_failed_installation(replacement_connection)
            self._close_after_failed_installation(previous_connection)
            return

        self._close_unattached_replacement(replacement_connection)

    def _close_after_failed_installation(self, connection: Optional[Connection]) -> None:
        if connection is None:
            return

        self._connection_creation_times.pop(connection, None)
        try:
            connection.close()
        except Exception as e:
            logger.warning("ConnectionLifetimePlugin.FailedInstallationConnectionCloseFailed", e)

    def _close_unattached_replacement(self, replacement_connection: Optional[Connection]) -> None:
        if (replacement_connection is None
                or replacement_connection is self._plugin_service.current_connection):
            return

        self._connection_creation_times.pop(replacement_connection, None)
        try:
            replacement_connection.close()
        except Exception as e:
            logger.warning("ConnectionLifetimePlugin.ReplacementCloseFailed", e)


class ConnectionLifetimePluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return ConnectionLifetimePlugin(plugin_service, props)
