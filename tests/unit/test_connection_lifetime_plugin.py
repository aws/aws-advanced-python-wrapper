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

from __future__ import annotations

from typing import Any, List, Optional

import pytest

from aws_advanced_python_wrapper.connection_lifetime_plugin import (
    ConnectionCreationTimes, ConnectionLifetimeAction,
    ConnectionLifetimePlugin, parse_connection_lifetime_action,
    parse_connection_lifetime_ns)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                ConnectionLifetimeExpiredError,
                                                ConnectionLifetimeFailedError,
                                                ConnectionLifetimeSuccessError)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.notifications import ConnectionEvent
from aws_advanced_python_wrapper.utils.properties import Properties

EXECUTE = DbApiMethod.CURSOR_EXECUTE.method_name
LIFETIME_MS = 1000
MS = 1_000_000


class FakeConnection:
    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class UnreferenceableConnection:
    # No __weakref__ slot, so weakref.ref() raises TypeError for it.
    __slots__ = ("name", "closed")

    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeClock:
    def __init__(self) -> None:
        self.now_ns = 0

    def __call__(self) -> int:
        return self.now_ns

    def advance_ms(self, ms: int) -> None:
        self.now_ns += ms * MS


class FakePluginService:
    def __init__(self, mocker) -> None:
        self.network_bound_methods = {EXECUTE, DbApiMethod.CONNECTION_CLOSE.method_name}
        self.current_connection: Optional[Any] = None
        self.current_host_info: Optional[HostInfo] = None
        self.is_in_transaction = False
        self.initial_connection_host_info: Optional[HostInfo] = None
        self.replacements: List[Any] = []
        self.connect_hosts: List[HostInfo] = []
        self.connect_error: Optional[Exception] = None
        self.driver_dialect = mocker.MagicMock()
        self.driver_dialect.is_closed.side_effect = lambda conn: conn.closed
        self.preserve_previous = False
        self.install_error: Optional[Exception] = None
        # Each evicted connection with whether it was already closed at the time.
        self.evicted: List[Any] = []
        self.connection_provider_manager = mocker.MagicMock()
        self.connection_provider_manager.evict_connection.side_effect = \
            lambda conn: self.evicted.append((conn, conn.closed))

    def get_connection_provider_manager(self) -> Any:
        return self.connection_provider_manager

    def is_network_bound_method(self, method_name: str) -> bool:
        return method_name in self.network_bound_methods

    def connect(self, host_info: HostInfo, props: Properties, plugin_to_skip: Any) -> Any:
        self.connect_hosts.append(host_info)
        if self.connect_error is not None:
            raise self.connect_error
        return self.replacements.pop(0)

    def set_current_connection(self, connection: Any, host_info: HostInfo) -> None:
        previous = self.current_connection
        self.current_connection = connection
        self.current_host_info = host_info
        if previous is not None and previous is not connection and not self.preserve_previous:
            previous.close()
        if self.install_error is not None:
            raise self.install_error


HOST = HostInfo("instance-1.xyz.us-east-1.rds.amazonaws.com")


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def plugin_service(mocker):
    return FakePluginService(mocker)


def make_plugin(plugin_service, clock, action: str = "reconnect") -> ConnectionLifetimePlugin:
    props = Properties({
        "connection_lifetime_ms": str(LIFETIME_MS),
        "connection_lifetime_action": action,
    })
    return ConnectionLifetimePlugin(plugin_service, props, clock_ns=clock)


def open_connection(plugin, plugin_service, connection) -> None:
    # Runs the initial connect through the plugin, then installs the result
    # the way the plugin service does.
    plugin.connect(None, None, HOST, Properties(), True, lambda: connection)
    plugin_service.current_connection = connection
    plugin.notify_connection_changed(set())


def execute(plugin, result: str = "rows") -> Any:
    return plugin.execute(None, EXECUTE, lambda: result)


@pytest.mark.parametrize("raw", [None, "", "abc", "0", "-5"])
def test_invalid_connection_lifetime_is_rejected(raw):
    props = Properties({} if raw is None else {"connection_lifetime_ms": raw})
    with pytest.raises(AwsWrapperError):
        parse_connection_lifetime_ns(props)


def test_connection_lifetime_is_read_as_nanoseconds():
    assert parse_connection_lifetime_ns(Properties({"connection_lifetime_ms": "250"})) == 250 * MS


@pytest.mark.parametrize("raw, expected", [
    (" CLOSE ", ConnectionLifetimeAction.CLOSE),
    ("Reconnect", ConnectionLifetimeAction.RECONNECT),
])
def test_connection_lifetime_action_ignores_case_and_space(raw, expected):
    assert parse_connection_lifetime_action(Properties({"connection_lifetime_action": raw})) == expected


def test_connection_lifetime_action_defaults_to_reconnect():
    assert parse_connection_lifetime_action(Properties()) == ConnectionLifetimeAction.RECONNECT


def test_invalid_connection_lifetime_action_is_rejected():
    with pytest.raises(AwsWrapperError):
        parse_connection_lifetime_action(Properties({"connection_lifetime_action": "drop"}))


def test_connection_before_lifetime_is_used(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = FakeConnection("first")
    open_connection(plugin, plugin_service, connection)

    clock.advance_ms(LIFETIME_MS - 1)

    assert execute(plugin) == "rows"
    assert not connection.closed


def test_lifetime_boundary_is_inclusive(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = FakeConnection("first")
    open_connection(plugin, plugin_service, connection)

    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)


def test_close_action_closes_connection_and_stays_closed(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = FakeConnection("first")
    open_connection(plugin, plugin_service, connection)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)
    assert connection.closed

    def must_not_run():
        raise AssertionError("execute_func ran on a closed wrapper connection")

    with pytest.raises(ConnectionLifetimeExpiredError):
        plugin.execute(None, EXECUTE, must_not_run)


def test_close_action_evicts_before_closing(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = FakeConnection("first")
    open_connection(plugin, plugin_service, connection)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)

    assert plugin_service.evicted == [(connection, False)]
    assert connection.closed


def test_close_action_chains_close_failure(plugin_service, clock, mocker):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = FakeConnection("first")
    close_error = RuntimeError("socket already gone")
    mocker.patch.object(connection, "close", side_effect=close_error)
    open_connection(plugin, plugin_service, connection)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeExpiredError) as raised:
        execute(plugin)
    assert raised.value.__cause__ is close_error


def test_lifecycle_methods_run_on_expired_connection(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    open_connection(plugin, plugin_service, FakeConnection("first"))
    clock.advance_ms(LIFETIME_MS)
    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)

    assert plugin.execute(None, DbApiMethod.CONNECTION_CLOSE.method_name, lambda: "closed") == "closed"


def test_non_network_bound_method_is_not_checked(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    open_connection(plugin, plugin_service, FakeConnection("first"))
    clock.advance_ms(LIFETIME_MS)

    assert plugin.execute(None, DbApiMethod.CONNECTION_COMMIT.method_name, lambda: "local") == "local"


def test_action_waits_for_open_transaction(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = FakeConnection("first")
    open_connection(plugin, plugin_service, connection)
    clock.advance_ms(LIFETIME_MS)

    plugin_service.is_in_transaction = True
    assert execute(plugin) == "rows"
    assert not connection.closed

    plugin_service.is_in_transaction = False
    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)
    assert connection.closed


def test_reconnect_action_replaces_connection_on_original_host(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    first = FakeConnection("first")
    replacement = FakeConnection("replacement")
    plugin_service.replacements.append(replacement)
    open_connection(plugin, plugin_service, first)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeSuccessError):
        execute(plugin)

    assert plugin_service.connect_hosts == [HOST]
    assert plugin_service.current_connection is replacement
    assert first.closed
    assert not replacement.closed


def test_reconnect_evicts_superseded_connection_before_closing(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    first = FakeConnection("first")
    plugin_service.replacements.append(FakeConnection("replacement"))
    open_connection(plugin, plugin_service, first)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeSuccessError):
        execute(plugin)

    assert plugin_service.evicted == [(first, False)]
    assert first.closed


def test_reconnect_leaves_preserved_connection_open(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    first = FakeConnection("first")
    plugin_service.replacements.append(FakeConnection("replacement"))
    plugin_service.preserve_previous = True
    open_connection(plugin, plugin_service, first)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeSuccessError):
        execute(plugin)

    # Eviction only marks the connection; the plugin that preserved it can keep using it.
    assert plugin_service.evicted == [(first, False)]
    assert not first.closed


def test_terminal_reconnect_failure_evicts_only_previous_connection(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    first = FakeConnection("first")
    replacement = FakeConnection("replacement")
    plugin_service.replacements.append(replacement)
    plugin_service.install_error = RuntimeError("session state could not be applied")
    open_connection(plugin, plugin_service, first)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeFailedError):
        execute(plugin)

    assert plugin_service.evicted == [(first, False)]
    assert first.closed
    assert replacement.closed
    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)


def test_reconnected_connection_gets_a_fresh_lifetime(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    plugin_service.replacements.append(FakeConnection("replacement"))
    open_connection(plugin, plugin_service, FakeConnection("first"))
    clock.advance_ms(LIFETIME_MS)
    with pytest.raises(ConnectionLifetimeSuccessError):
        execute(plugin)

    clock.advance_ms(LIFETIME_MS - 1)

    assert execute(plugin) == "rows"


def test_reconnect_without_initial_connect_uses_plugin_service_host(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    fallback_host = HostInfo("cluster.cluster-xyz.us-east-1.rds.amazonaws.com")
    plugin_service.initial_connection_host_info = fallback_host
    plugin_service.replacements.append(FakeConnection("replacement"))
    plugin_service.current_connection = FakeConnection("pooled")
    plugin.notify_connection_changed({ConnectionEvent.INITIAL_CONNECTION})
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeSuccessError):
        execute(plugin)
    assert plugin_service.connect_hosts == [fallback_host]


def test_reconnect_without_any_host_fails(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    plugin_service.current_connection = FakeConnection("pooled")
    plugin.notify_connection_changed({ConnectionEvent.INITIAL_CONNECTION})
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeFailedError):
        execute(plugin)
    assert plugin_service.connect_hosts == []


def test_failed_reconnect_keeps_previous_connection(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock)
    first = FakeConnection("first")
    connect_error = RuntimeError("host unreachable")
    plugin_service.connect_error = connect_error
    open_connection(plugin, plugin_service, first)
    clock.advance_ms(LIFETIME_MS)

    with pytest.raises(ConnectionLifetimeFailedError) as raised:
        execute(plugin)

    assert raised.value.__cause__ is connect_error
    assert plugin_service.current_connection is first
    assert not first.closed
    assert plugin_service.evicted == []


def test_connection_creation_times_ignore_unreferenceable_connections():
    times = ConnectionCreationTimes()
    connection = UnreferenceableConnection("no-weakref")

    times.record(connection, 5)

    assert times.get(connection) is None
    times.forget(connection)


def test_unreferenceable_connection_is_timed_from_first_use(plugin_service, clock):
    plugin = make_plugin(plugin_service, clock, "close")
    connection = UnreferenceableConnection("no-weakref")
    open_connection(plugin, plugin_service, connection)

    # The connect could not be stamped, so the lifetime starts here.
    clock.advance_ms(LIFETIME_MS)
    assert execute(plugin) == "rows"

    clock.advance_ms(LIFETIME_MS - 1)
    assert execute(plugin) == "rows"

    clock.advance_ms(1)
    with pytest.raises(ConnectionLifetimeExpiredError):
        execute(plugin)
    assert connection.closed
