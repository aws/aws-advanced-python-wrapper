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

from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional, Set
from unittest.mock import MagicMock, patch

import psycopg
import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.default_plugin import DefaultPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.pg_driver_dialect import PgDriverDialect
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginServiceImpl, PluginServiceManagerContainer)
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    AbstractReadWriteSplittingPlugin
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.wrapper import (AwsWrapperConnection,
                                                 AwsWrapperCursor)


def test_connection_basic():
    conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty wrapper_driver_dialect=psycopg"
    connection_mock: Connection = MagicMock(spec=psycopg.Connection)
    container_mock = MagicMock()
    connection_mock.connect.return_value = MagicMock(spec=psycopg.Connection)

    with patch.object(PluginServiceManagerContainer, "__new__", container_mock), \
            patch.object(PluginServiceImpl, "refresh_host_list"), \
            patch.object(PluginServiceImpl, "current_connection", None), \
            patch.object(PluginServiceImpl, "initial_connection_host_info", HostInfo("localhost")):
        AwsWrapperConnection.connect(connection_mock.connect, conninfo)

        connection_mock.connect.assert_called_with(host="localhost", dbname="postgres", user="postgres",
                                                   password="qwerty")


def _wrapper_with(plugin_service, plugin_manager, conn):
    """Drive AwsWrapperConnection.__init__ past its setup to the host-adoption
    step, which is what decides the connection's current host."""
    plugin_manager.connect.return_value = conn
    plugin_service.database_dialect.get_host_list_provider_supplier.return_value = \
        lambda _service, _props: MagicMock()
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    AwsWrapperConnection.__init__(
        wrapper, MagicMock(), MagicMock(), plugin_service, plugin_manager)
    return wrapper


def test_the_wrapper_adopts_the_host_a_plugin_routed_the_connect_to():
    """The initial connection strategy plugin substitutes an instance for a
    cluster endpoint. Binding the connection to the endpoint instead would
    leave every later host comparison naming the wrong host."""
    routed = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    conn = MagicMock(spec=psycopg.Connection)
    plugin_service = MagicMock()
    plugin_service.current_connection = None
    plugin_service.initial_connection_host_info = HostInfo("mydb.cluster-xyz.us-east-2.rds.amazonaws.com", 5432)
    plugin_service.take_routed_host_info.return_value = routed

    _wrapper_with(plugin_service, MagicMock(), conn)

    plugin_service.set_current_connection.assert_called_once_with(conn, routed)


def test_the_wrapper_falls_back_to_the_initial_host_when_nothing_routed():
    initial = HostInfo("mydb.cluster-xyz.us-east-2.rds.amazonaws.com", 5432)
    conn = MagicMock(spec=psycopg.Connection)
    plugin_service = MagicMock()
    plugin_service.current_connection = None
    plugin_service.initial_connection_host_info = initial
    plugin_service.take_routed_host_info.return_value = None

    _wrapper_with(plugin_service, MagicMock(), conn)

    plugin_service.set_current_connection.assert_called_once_with(conn, initial)


def test_connection_getattr_delegates_to_underlying_connection():
    # Driver-specific extension methods absent from the wrapper's PEP-249 surface
    # (e.g. psycopg's add_notice_handler, called by SQLAlchemy) must reach the
    # live underlying connection via __getattr__.
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    underlying = MagicMock()
    plugin_service = MagicMock()
    plugin_service.current_connection = underlying
    wrapper._plugin_service = plugin_service

    assert wrapper.add_notice_handler is underlying.add_notice_handler


def test_connection_getattr_delegates_driver_attrs_including_underscore():
    # Public AND single-underscore driver attrs delegate to the live underlying
    # connection (SQLAlchemy's psycopg adapter reaches for underscore members like
    # _close). Only dunders stay on the wrapper, and the recursion-critical
    # _plugin_service name is guarded so a miss before it is set raises cleanly.
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    underlying = MagicMock()
    plugin_service = MagicMock()
    plugin_service.current_connection = underlying
    wrapper._plugin_service = plugin_service

    # single-underscore driver attr delegates (regression: was wrongly blocked)
    assert wrapper._close is underlying._close
    # dunder stays on the wrapper, not delegated
    with pytest.raises(AttributeError):
        _ = wrapper.__totally_made_up_dunder__
    # the recursion-critical internal name is guarded when unset
    fresh = AwsWrapperConnection.__new__(AwsWrapperConnection)
    with pytest.raises(AttributeError):
        _ = fresh._plugin_service


def test_cursor_getattr_delegates_to_target_cursor():
    wrapper = AwsWrapperCursor.__new__(AwsWrapperCursor)
    target_cursor = MagicMock()
    wrapper._target_cursor = target_cursor

    assert wrapper.statusmessage is target_cursor.statusmessage


def test_cursor_getattr_delegates_driver_attrs_including_underscore():
    wrapper = AwsWrapperCursor.__new__(AwsWrapperCursor)
    target_cursor = MagicMock()
    wrapper._target_cursor = target_cursor

    # _close must delegate: SQLAlchemy's psycopg cursor adapter calls it.
    assert wrapper._close is target_cursor._close
    with pytest.raises(AttributeError):
        _ = wrapper.__totally_made_up_dunder__
    fresh = AwsWrapperCursor.__new__(AwsWrapperCursor)
    with pytest.raises(AttributeError):
        _ = fresh._target_cursor


def _make_sync_conn(target=None):
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    plugin_service = MagicMock()
    plugin_service.current_connection = target if target is not None else MagicMock()
    wrapper._plugin_service = plugin_service
    wrapper._plugin_manager = MagicMock()
    return wrapper, plugin_service.current_connection


def test_connection_execute_routes_through_cursor_not_raw_driver():
    # psycopg exposes conn.execute(); the wrapper must NOT forward it raw via
    # __getattr__ (that would bypass the plugin chain). It opens a wrapper cursor.
    wrapper, target = _make_sync_conn()
    fake_cursor = MagicMock()
    with patch.object(AwsWrapperConnection, "cursor", return_value=fake_cursor) as cursor_mock:
        result = wrapper.execute("SELECT 1", None)
    cursor_mock.assert_called_once()
    fake_cursor.execute.assert_called_once()
    assert result is fake_cursor
    target.execute.assert_not_called()


def test_connection_set_read_only_routes_through_plugin_manager():
    # Must go through the plugin-aware read_only setter (plugin chain +
    # session-state), not the raw driver set_read_only that __getattr__ would
    # forward -- otherwise the read-only state is lost across failover.
    wrapper, target = _make_sync_conn()
    wrapper.set_read_only(True)
    assert wrapper._plugin_manager.execute.called
    target.set_read_only.assert_not_called()


def test_connection_set_autocommit_routes_through_plugin_manager():
    wrapper, target = _make_sync_conn()
    wrapper.set_autocommit(True)
    assert wrapper._plugin_manager.execute.called
    target.set_autocommit.assert_not_called()


def test_connection_invalidate_prefers_target_invalidate():
    wrapper, target = _make_sync_conn()
    wrapper.release_resources = MagicMock()
    wrapper.invalidate()
    target.invalidate.assert_called_once()
    wrapper.release_resources.assert_called_once()


def test_connection_invalidate_falls_back_to_close():
    # A raw driver connection has no invalidate() -> fall back to close().
    wrapper, _ = _make_sync_conn(MagicMock(spec=psycopg.Connection))
    wrapper.close = MagicMock()
    wrapper.invalidate()
    wrapper.close.assert_called_once()


def test_connection_closed_forwards_target_and_defaults_false():
    wrapper, target = _make_sync_conn()
    target.closed = True
    assert wrapper.closed is True
    # a target lacking `closed` normalizes to False instead of AttributeError
    wrapper2, _ = _make_sync_conn(MagicMock(spec=[]))
    assert wrapper2.closed is False


def test_connection_prepare_threshold_and_prepared_max_forward():
    wrapper, target = _make_sync_conn()
    target.prepare_threshold = 7
    target.prepared_max = 11
    assert wrapper.prepare_threshold == 7
    assert wrapper.prepared_max == 11
    wrapper.prepare_threshold = 99
    wrapper.prepared_max = 100
    assert target.prepare_threshold == 99
    assert target.prepared_max == 100


class UpperRowsPlugin(Plugin):
    """Upper-cases the rows of the methods it subscribes to, and records the methods it handles."""

    def __init__(self, methods: Set[str], calls: List[str]):
        self._methods = methods
        self._calls = calls

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._methods

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._calls.append(method_name)
        if method_name == "Cursor.__iter__":
            return (row.upper() for row in execute_func())
        return execute_func().upper()


def _cursor_with_plugins(driver_cursor: Any, plugins: List[Plugin], current_connection: Any = None) -> AwsWrapperCursor:
    """A wrapper cursor over driver_cursor whose plugin manager runs the given plugins, then DefaultPlugin."""
    plugin_service = MagicMock()
    plugin_service.current_connection = driver_cursor.connection if current_connection is None else current_connection
    plugin_service.driver_dialect.get_connection_from_obj.side_effect = lambda obj: obj.connection
    plugin_service.driver_dialect.unwrap_connection.side_effect = lambda conn: conn
    plugin_service.driver_dialect.execute.side_effect = lambda method_name, func, *args, **kwargs: func()
    container = MagicMock()
    container.plugin_service = plugin_service
    manager = PluginManager.__new__(PluginManager)
    manager._container = container
    manager._telemetry_factory = MagicMock()
    manager._telemetry_in_use = False
    manager._function_cache = [None] * (DbApiMethod.ALL.id + 1)
    manager._plugins = plugins + [DefaultPlugin(plugin_service, MagicMock())]
    return AwsWrapperCursor(MagicMock(), plugin_service, manager, driver_cursor)


def test_cursor_loop_runs_the_iter_subscribers_once_per_loop():
    calls: List[str] = []
    driver_cursor = MagicMock()
    driver_cursor.__iter__.return_value = iter(["a", "b", "c"])
    # So a route that fetches row by row ends (and fails) instead of looping forever.
    driver_cursor.fetchone.return_value = None
    cursor = _cursor_with_plugins(driver_cursor, [UpperRowsPlugin({"Cursor.__iter__"}, calls)])

    assert [row for row in cursor] == ["A", "B", "C"]
    assert calls == ["Cursor.__iter__"]


def test_cursor_loop_does_not_run_the_next_subscribers():
    plugin = MagicMock(spec=Plugin)
    plugin.subscribed_methods = {"Cursor.__next__"}
    plugin.execute.side_effect = lambda target, method_name, execute_func, *args, **kwargs: execute_func()
    driver_cursor = MagicMock()
    driver_cursor.__iter__.return_value = iter(["a", "b", "c"])
    driver_cursor.fetchone.return_value = None
    cursor = _cursor_with_plugins(driver_cursor, [plugin])

    assert [row for row in cursor] == ["a", "b", "c"]
    plugin.execute.assert_not_called()


@pytest.mark.parametrize("network_bound", [True, False])
def test_cursor_loop_without_subscribers_returns_the_driver_iterator(network_bound):
    # A method the dialect doesn't count as network-bound skips the pipeline when nothing subscribes to it.
    rows = iter(["a", "b"])
    driver_cursor = MagicMock()
    driver_cursor.__iter__.return_value = rows
    cursor = _cursor_with_plugins(driver_cursor, [])
    cursor._plugin_service.is_network_bound_method.return_value = network_bound

    assert iter(cursor) is rows
    assert list(cursor) == ["a", "b"]


def test_cursor_loop_after_a_break_continues_with_the_next_row():
    driver_cursor = MagicMock()
    driver_cursor.__iter__.return_value = iter(["a", "b", "c"])
    cursor = _cursor_with_plugins(driver_cursor, [UpperRowsPlugin({"Cursor.__iter__"}, [])])

    for first in cursor:
        break

    assert first == "A"
    assert list(cursor) == ["B", "C"]
    assert list(cursor) == []


def test_cursor_loop_over_a_cursor_of_an_old_connection_raises():
    # After failover, say: the connection the driver cursor belongs to is no longer the current one.
    driver_cursor = MagicMock()
    driver_cursor.__iter__.return_value = iter(["a"])
    cursor = _cursor_with_plugins(driver_cursor, [], current_connection=MagicMock())

    with pytest.raises(AwsWrapperError):
        for _ in cursor:
            pass


def test_cursor_execute_returns_the_wrapper_cursor_when_the_driver_returns_its_own():
    # psycopg's Cursor.execute returns the cursor, so a fetch chained onto it must go through the plugins too.
    calls: List[str] = []
    driver_cursor = MagicMock()
    driver_cursor.execute.return_value = driver_cursor
    driver_cursor.fetchone.return_value = "row"
    cursor = _cursor_with_plugins(driver_cursor, [UpperRowsPlugin({DbApiMethod.CURSOR_FETCHONE.method_name}, calls)])

    assert cursor.execute("SELECT 1") is cursor
    assert cursor.execute("SELECT 1").fetchone() == "ROW"
    assert calls == [DbApiMethod.CURSOR_FETCHONE.method_name]


@pytest.mark.parametrize("driver_result", [None, object()])
def test_cursor_execute_passes_any_other_driver_result_through(driver_result):
    # mysql-connector's Cursor.execute returns None.
    driver_cursor = MagicMock()
    driver_cursor.execute.return_value = driver_result

    assert _cursor_with_plugins(driver_cursor, []).execute("SELECT 1") is driver_result


def test_next_on_the_cursor_returns_the_next_row_through_the_next_subscribers():
    # psycopg's Cursor.execute returns the cursor, which is an iterator, so next(cursor.execute(query)) is the first row.
    calls: List[str] = []
    driver_cursor = MagicMock()
    driver_cursor.execute.return_value = driver_cursor
    driver_cursor.fetchone.return_value = "row"
    cursor = _cursor_with_plugins(driver_cursor, [UpperRowsPlugin({DbApiMethod.CURSOR_NEXT.method_name}, calls)])

    assert next(cursor.execute("SELECT 1")) == "ROW"
    assert calls == [DbApiMethod.CURSOR_NEXT.method_name]


def test_next_on_an_exhausted_cursor_raises_stop_iteration():
    driver_cursor = MagicMock()
    driver_cursor.fetchone.return_value = None
    cursor = _cursor_with_plugins(driver_cursor, [])

    with pytest.raises(StopIteration):
        next(cursor)


def test_next_on_the_cursor_returns_an_empty_row():
    # A query with no columns (SELECT FROM t on PostgreSQL) has empty rows; only None ends the rows.
    driver_cursor = MagicMock()
    driver_cursor.fetchone.return_value = ()
    cursor = _cursor_with_plugins(driver_cursor, [])

    assert next(cursor) == ()


class _ServerSideRows:
    """A server-side cursor's result of five rows; each read is a round trip that returns the rows after the server's position."""

    def __init__(self) -> None:
        self.connection = MagicMock()
        self.fetches = 0
        self._rows = ["a", "b", "c", "d", "e"]
        self._position = 0

    def _fetch(self, size: int) -> List[str]:
        self.fetches += 1
        rows = self._rows[self._position:self._position + size]
        self._position += len(rows)
        return rows

    def fetchone(self) -> Optional[str]:
        rows = self._fetch(1)
        return rows[0] if rows else None


class _SelfIteratingServerCursor(_ServerSideRows):
    """Shaped like psycopg 3.3's ServerCursor: it is its own iterator, and its next() serves a page of 3 rows it read ahead."""

    def __init__(self) -> None:
        super().__init__()
        self._page: List[str] = []

    def __iter__(self) -> _SelfIteratingServerCursor:
        return self

    def __next__(self) -> str:
        if not self._page:
            self._page = self._fetch(3)
        if not self._page:
            raise StopIteration
        return self._page.pop(0)


class _GeneratorServerCursor(_ServerSideRows):
    """Shaped like psycopg 3.2's ServerCursor: each iter() starts a generator that reads a page of 3 rows at a time; no next()."""

    def __iter__(self) -> Iterator[str]:
        while True:
            page = self._fetch(3)
            yield from page
            if len(page) < 3:
                return


class _LostConnectionError(Exception):
    pass


class _FetchoneIteratingCursor:
    """Shaped like mysql-connector's cursor: iter() calls fetchone for each row, and next() turns a driver error into the end of the rows."""

    def __init__(self) -> None:
        self.connection = MagicMock()

    def fetchone(self) -> Any:
        raise _LostConnectionError("Lost connection to the server during query")

    def __iter__(self) -> Iterator[Any]:
        return iter(self.fetchone, None)

    def __next__(self) -> Any:
        try:
            return self.fetchone()
        except _LostConnectionError:
            raise StopIteration from None


def test_next_after_a_partial_loop_returns_the_row_after_the_loop_row():
    # The loop read a page ahead, so the server is already past the row after the loop's.
    calls: List[str] = []
    driver_cursor = _SelfIteratingServerCursor()
    cursor = _cursor_with_plugins(driver_cursor, [UpperRowsPlugin({DbApiMethod.CURSOR_NEXT.method_name}, calls)])

    for first in cursor:
        break

    assert first == "a"
    assert next(cursor) == "B"
    assert calls == [DbApiMethod.CURSOR_NEXT.method_name]


def test_next_reads_a_cursor_that_is_its_own_iterator_a_page_at_a_time():
    driver_cursor = _SelfIteratingServerCursor()
    cursor = _cursor_with_plugins(driver_cursor, [])

    assert [next(cursor) for _ in range(5)] == ["a", "b", "c", "d", "e"]
    assert driver_cursor.fetches == 2


def test_next_passes_a_driver_error_to_the_next_subscribers():
    # A lost connection must reach the plugins (failover, say) instead of ending the rows.
    calls: List[str] = []
    cursor = _cursor_with_plugins(_FetchoneIteratingCursor(), [UpperRowsPlugin({DbApiMethod.CURSOR_NEXT.method_name}, calls)])

    with pytest.raises(_LostConnectionError):
        next(cursor)
    assert calls == [DbApiMethod.CURSOR_NEXT.method_name]


def test_next_on_a_cursor_with_a_generator_iterator_keeps_every_row():
    # Each iter() on this cursor starts a generator that reads a page, so next() must not take its row from a new one.
    cursor = _cursor_with_plugins(_GeneratorServerCursor(), [])

    assert [next(cursor) for _ in range(5)] == ["a", "b", "c", "d", "e"]


class _SelfIteratingRowsCursor:
    """Shaped like psycopg 3.3's Cursor with a row factory that can return None (scalar_row on a NULL): it is its own iterator."""

    def __init__(self, rows: List[Optional[str]]) -> None:
        self.connection = MagicMock()
        self._rows = iter(rows)

    def __iter__(self) -> _SelfIteratingRowsCursor:
        return self

    def __next__(self) -> Optional[str]:
        return next(self._rows)


def test_next_on_a_cursor_that_is_its_own_iterator_returns_a_none_row():
    cursor = _cursor_with_plugins(_SelfIteratingRowsCursor(["a", None, "c"]), [])

    assert [next(cursor) for _ in range(3)] == ["a", None, "c"]
    with pytest.raises(StopIteration):
        next(cursor)


def test_next_when_a_next_plugin_returns_none_without_reading_raises_stop_iteration():
    # A plugin that answers None without reading a row ends the rows; else next() would return None forever.
    plugin = MagicMock(spec=Plugin)
    plugin.subscribed_methods = {DbApiMethod.CURSOR_NEXT.method_name}
    plugin.execute.return_value = None
    driver_cursor = _SelfIteratingServerCursor()
    cursor = _cursor_with_plugins(driver_cursor, [plugin])

    with pytest.raises(StopIteration):
        next(cursor)
    assert driver_cursor.fetches == 0


class _WrapErrorsPlugin(Plugin):
    """Raises AwsWrapperError from any error of Cursor.__next__, as failover does with an error that isn't a lost connection."""

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.CURSOR_NEXT.method_name}

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        try:
            return execute_func()
        except Exception as e:
            raise AwsWrapperError(str(e)) from e


def test_next_at_the_end_of_a_cursor_that_is_its_own_iterator_raises_stop_iteration():
    # The driver's end of rows must not reach the plugins as an error: failover would raise AwsWrapperError from it.
    cursor = _cursor_with_plugins(_SelfIteratingServerCursor(), [_WrapErrorsPlugin()])

    assert [next(cursor) for _ in range(5)] == ["a", "b", "c", "d", "e"]
    with pytest.raises(StopIteration):
        next(cursor)


def _fetchone_rows_cursor() -> MagicMock:
    """A driver cursor that isn't its own iterator, so next() takes its row from fetchone."""
    driver_cursor = MagicMock()
    driver_cursor.fetchone.return_value = "row"
    return driver_cursor


def test_next_at_the_end_of_a_fetchone_cursor_raises_stop_iteration():
    driver_cursor = _fetchone_rows_cursor()
    driver_cursor.fetchone.side_effect = ["row", None]
    cursor = _cursor_with_plugins(driver_cursor, [_WrapErrorsPlugin()])

    assert next(cursor) == "row"
    with pytest.raises(StopIteration):
        next(cursor)


@pytest.mark.parametrize("new_driver_cursor", [_fetchone_rows_cursor, lambda: _SelfIteratingRowsCursor(["row"])], ids=["fetchone", "own_iterator"])
@pytest.mark.parametrize(
    "methods",
    [PgDriverDialect(Properties()).network_bound_methods,
     MySQLDriverDialect(Properties()).network_bound_methods,
     AbstractReadWriteSplittingPlugin._SUBSCRIBED_METHODS],
    ids=["pg", "mysql", "read_write_splitting"])
def test_next_runs_the_plugins_subscribed_through_the_network_bound_methods(methods, new_driver_cursor):
    # Failover, host monitoring and the connection tracker subscribe to the dialect's network-bound methods, and read/write
    # splitting to its own list, so each must hold Cursor.__next__ for those plugins to see next().
    calls: List[str] = []
    cursor = _cursor_with_plugins(new_driver_cursor(), [UpperRowsPlugin(methods, calls)])

    assert next(cursor) == "ROW"
    assert calls == [DbApiMethod.CURSOR_NEXT.method_name]


@pytest.mark.parametrize("new_driver_cursor", [_fetchone_rows_cursor, lambda: _SelfIteratingRowsCursor(["row"])], ids=["fetchone", "own_iterator"])
def test_next_does_not_run_the_fetchone_subscribers(new_driver_cursor):
    calls: List[str] = []
    cursor = _cursor_with_plugins(new_driver_cursor(), [UpperRowsPlugin({DbApiMethod.CURSOR_FETCHONE.method_name}, calls)])

    assert next(cursor) == "row"
    assert calls == []
