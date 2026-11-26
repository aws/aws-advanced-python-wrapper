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

import asyncio
from typing import (Any, Awaitable, Callable, Coroutine, List, Optional, Set,
                    Tuple)
from unittest.mock import AsyncMock, MagicMock

import aiomysql
import psycopg
import pytest

from aws_advanced_python_wrapper.aio import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

_QUERY = "SELECT x FROM t ORDER BY i"
_CONNINFO = "host=h user=u password=p dbname=d port=5432"
_CURSOR_NAME = "rows_cursor"
_FETCHONE = DbApiMethod.CURSOR_FETCHONE.method_name
_ALL_METHODS = DbApiMethod.ALL.method_name
_DEFAULT = "default"
_MAX_ROWS = 1000

_CLIENT = "client"
_SERVER = "server"
_AIOMYSQL = "aiomysql"
_SEPARATE_ITERATOR = "separate_iterator"
_PSYCOPG_KINDS = [_CLIENT, _SERVER]
_ALL_KINDS = [_CLIENT, _SERVER, _AIOMYSQL]

_NO_PLUGINS = "no_plugins"
_CUSTOM_PLUGINS = "custom_plugins"
_DEFAULT_PLUGINS = "default_plugins"
_CHAINS = [_NO_PLUGINS, _CUSTOM_PLUGINS, _DEFAULT_PLUGINS]

_NONE_ROWS = {
    "none_first": [None, 2, 3],
    "none_middle": [1, None, 3],
    "none_last": [1, 2, None],
    "all_none": [None, None, None],
}
_MIXED_ROWS = [1, None, 3, None, 5, 6, None, 8, 9, 10]
_PLAIN_ROWS = list(range(1, 11))
_LONG_ROWS = [None if i % 7 == 0 else i for i in range(1, 251)]
_LONG_PLAIN_ROWS = list(range(1, 251))
_TUPLE_ROWS = [(1, None), (None, None), (3, "c")]
_LONG_TUPLE_ROWS = [(i, None) for i in range(1, 11)]
_DICT_ROWS = [{"i": 1, "x": None}, {"i": 2, "x": "b"}]


class _FakeDatabase:
    """The result every query returns, and the row whose read fails."""

    def __init__(
            self,
            rows: List[Any],
            error_index: Optional[int] = None,
            error: Optional[Callable[[], Exception]] = None) -> None:
        self.rows = rows
        self.executes: List[str] = []
        self.fetch_round_trips = 0
        self._error_index = error_index
        self._error = error

    def check_read(self, start: int, end: int) -> None:
        if self._error is None or self._error_index is None:
            return
        if start <= self._error_index < end:
            raise self._error()


class _FakeClientCursorBase:
    """psycopg.AsyncCursor without iteration (psycopg/cursor_async.py:241-286):
    execute loads the whole result, and every fetch method reads from one position."""

    def __init__(self, connection: Any, db: _FakeDatabase) -> None:
        self.connection = connection
        self.arraysize = 1
        self.closed = False
        self._db = db
        self._rows: Optional[List[Any]] = None
        self._pos = 0

    @property
    def description(self) -> Optional[List[Tuple[str]]]:
        return None if self._rows is None else [("x",)]

    @property
    def rowcount(self) -> int:
        return -1 if self._rows is None else len(self._rows)

    @property
    def rownumber(self) -> Optional[int]:
        return None if self._rows is None else self._pos

    async def execute(self, query: str, params: Any = None, **kwargs: Any) -> _FakeClientCursorBase:
        self._db.executes.append(query)
        self._rows = list(self._db.rows)
        self._pos = 0
        return self

    async def fetchone(self) -> Any:
        if self._pos >= len(self._result()):
            return None
        return self._read(1)[0]

    async def fetchmany(self, size: Optional[int] = 0) -> List[Any]:
        return self._read(size or self.arraysize)

    async def fetchall(self) -> List[Any]:
        return self._read(len(self._result()))

    async def close(self) -> None:
        self.closed = True

    def _result(self) -> List[Any]:
        if self.closed:
            raise psycopg.InterfaceError("the cursor is closed")
        if self._rows is None:
            raise psycopg.ProgrammingError("no result available")
        return self._rows

    def _read(self, count: int) -> List[Any]:
        rows = self._result()
        end = min(self._pos + count, len(rows))
        self._db.check_read(self._pos, end)
        records = rows[self._pos:end]
        self._pos = end
        return records


class _FakePsycopgCursor(_FakeClientCursorBase):
    """psycopg.AsyncCursor is its own async iterator (psycopg/cursor_async.py:288-298)."""

    def __aiter__(self) -> _FakePsycopgCursor:
        return self

    async def __anext__(self) -> Any:
        if self._pos >= len(self._result()):
            raise StopAsyncIteration("no more records to return")
        return self._read(1)[0]


class _FakeSeparateIteratorCursor(_FakeClientCursorBase):
    """A driver cursor whose __aiter__ returns a different object over the same position."""

    def __aiter__(self) -> _FakeRowIterator:
        return _FakeRowIterator(self)


class _FakeRowIterator:
    def __init__(self, cursor: _FakeClientCursorBase) -> None:
        self._cursor = cursor

    def __aiter__(self) -> _FakeRowIterator:
        return self

    async def __anext__(self) -> Any:
        if self._cursor.rownumber == self._cursor.rowcount:
            raise StopAsyncIteration
        return (await self._cursor.fetchmany(1))[0]


class _FakePsycopgServerCursor:
    """psycopg.AsyncServerCursor (psycopg/_server_cursor_async.py:107-147): each fetch
    method is one FETCH round trip, and __anext__ serves rows from a page of itersize
    rows, so a fetch method after a loop continues after that page."""

    def __init__(self, connection: Any, db: _FakeDatabase, name: str) -> None:
        self.connection = connection
        self.name = name
        self.itersize = 100
        self.arraysize = 1
        self.closed = False
        self._db = db
        self._rows: Optional[List[Any]] = None
        self._server_pos = 0
        self._pos = 0
        self._iter_rows: Optional[List[Any]] = None
        self._page_pos = 0

    @property
    def description(self) -> Optional[List[Tuple[str]]]:
        return None if self._rows is None else [("x",)]

    @property
    def rowcount(self) -> int:
        return -1

    @property
    def rownumber(self) -> Optional[int]:
        return None if self._rows is None else self._pos

    async def execute(self, query: str, params: Any = None, **kwargs: Any) -> _FakePsycopgServerCursor:
        self._db.executes.append(query)
        self._rows = list(self._db.rows)
        self._server_pos = 0
        self._pos = 0
        self._iter_rows = None
        self._page_pos = 0
        return self

    async def fetchone(self) -> Any:
        records = self._fetch(1)
        if not records:
            return None
        self._pos += 1
        return records[0]

    async def fetchmany(self, size: Optional[int] = 0) -> List[Any]:
        records = self._fetch(size or self.arraysize)
        self._pos += len(records)
        return records

    async def fetchall(self) -> List[Any]:
        records = self._fetch(None)
        self._pos += len(records)
        return records

    def __aiter__(self) -> _FakePsycopgServerCursor:
        return self

    async def __anext__(self) -> Any:
        if self._iter_rows is None or self._page_pos >= len(self._iter_rows) >= self.itersize:
            self._iter_rows = self._fetch(self.itersize)
            self._page_pos = 0
        if self._page_pos >= len(self._iter_rows):
            raise StopAsyncIteration("no more records to return")
        record = self._iter_rows[self._page_pos]
        self._page_pos += 1
        self._pos += 1
        return record

    async def close(self) -> None:
        self.closed = True

    def _fetch(self, count: Optional[int]) -> List[Any]:
        if self.closed:
            raise psycopg.InterfaceError("the cursor is closed")
        if self._rows is None:
            raise psycopg.ProgrammingError("no result available")
        self._db.fetch_round_trips += 1
        if count is None:
            end = len(self._rows)
        else:
            end = min(self._server_pos + count, len(self._rows))
        self._db.check_read(self._server_pos, end)
        records = self._rows[self._server_pos:end]
        self._server_pos = end
        return records


class _FakeAiomysqlCursor:
    """aiomysql.Cursor (aiomysql/cursors.py:365-420, 502-510): the fetch methods
    return futures, and __anext__ ends at the first None from fetchone."""

    def __init__(self, connection: Any, db: _FakeDatabase) -> None:
        self.connection = connection
        self.arraysize = 1
        self.closed = False
        self._db = db
        self._rows: Optional[List[Any]] = None
        self._rownumber = 0

    @property
    def description(self) -> Optional[Tuple[Tuple[str, ...], ...]]:
        return None if self._rows is None else (("x",),)

    @property
    def rowcount(self) -> int:
        return -1 if self._rows is None else len(self._rows)

    @property
    def rownumber(self) -> int:
        return self._rownumber

    async def execute(self, query: str, args: Any = None) -> int:
        self._db.executes.append(query)
        self._rows = list(self._db.rows)
        self._rownumber = 0
        return len(self._rows)

    def fetchone(self) -> asyncio.Future[Any]:
        if self._rownumber >= len(self._executed_rows()):
            return _resolved_future(None)
        return _resolved_future(self._read(1)[0])

    def fetchmany(self, size: Optional[int] = None) -> asyncio.Future[Any]:
        return _resolved_future(self._read(size or self.arraysize))

    def fetchall(self) -> asyncio.Future[Any]:
        return _resolved_future(self._read(len(self._executed_rows())))

    def __aiter__(self) -> _FakeAiomysqlCursor:
        return self

    async def __anext__(self) -> Any:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def close(self) -> None:
        self.closed = True

    def _executed_rows(self) -> List[Any]:
        if self._rows is None:
            raise aiomysql.ProgrammingError("execute() first")
        return self._rows

    def _read(self, count: int) -> List[Any]:
        rows = self._executed_rows()
        end = min(self._rownumber + count, len(rows))
        self._db.check_read(self._rownumber, end)
        records = rows[self._rownumber:end]
        self._rownumber = end
        return records


class _PassThroughPlugin(AsyncPlugin):
    @property
    def subscribed_methods(self) -> Set[str]:
        return {_ALL_METHODS}


class _RowRecorderPlugin(AsyncPlugin):
    def __init__(self, events: List[Tuple[str, Any]], subscribed: str) -> None:
        self._events = events
        self._subscribed = subscribed

    @property
    def subscribed_methods(self) -> Set[str]:
        return {self._subscribed}

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        result = await execute_func()
        if method_name == _FETCHONE:
            self._events.append(("plugin", result))
        return result


class _RowChangingPlugin(AsyncPlugin):
    def __init__(self, change: Callable[[Any], Any]) -> None:
        self._change = change

    @property
    def subscribed_methods(self) -> Set[str]:
        return {_FETCHONE}

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        return self._change(await execute_func())


class _GuardingPlugin(AsyncPlugin):
    def __init__(self) -> None:
        self.errors: List[BaseException] = []

    @property
    def subscribed_methods(self) -> Set[str]:
        return {_FETCHONE}

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        try:
            return await execute_func()
        except BaseException as error:
            self.errors.append(error)
            raise


class _StoppingPlugin(AsyncPlugin):
    def __init__(self) -> None:
        self.stop = False

    @property
    def subscribed_methods(self) -> Set[str]:
        return {_FETCHONE}

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if self.stop:
            return None
        return await execute_func()


def _resolved_future(value: Any) -> asyncio.Future[Any]:
    future = asyncio.get_running_loop().create_future()
    future.set_result(value)
    return future


async def _resolved(value: Any) -> Any:
    return value


def _plugins(chain: str) -> Any:
    if chain == _CUSTOM_PLUGINS:
        return [_PassThroughPlugin()]
    if chain == _DEFAULT_PLUGINS:
        return None
    return ""


def _make_fake_conn(kind: str, db: _FakeDatabase) -> MagicMock:
    conn = MagicMock()
    conn.close = AsyncMock()
    conn.fileno = MagicMock(side_effect=OSError("fake conn has no real fd"))
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    conn.closed = False
    conn.autocommit = True
    conn.driver_connection = conn

    def _cursor(*args: Any, **kwargs: Any) -> Any:
        # aiomysql's Connection.cursor() returns an awaitable, psycopg's returns the cursor.
        if kind == _AIOMYSQL:
            return _resolved(_FakeAiomysqlCursor(conn, db))
        if kwargs.get("name"):
            return _FakePsycopgServerCursor(conn, db, kwargs["name"])
        if kind == _SEPARATE_ITERATOR:
            return _FakeSeparateIteratorCursor(conn, db)
        return _FakePsycopgCursor(conn, db)

    conn.cursor = MagicMock(side_effect=_cursor)
    return conn


async def _open_raw(kind: str, db: _FakeDatabase) -> Any:
    if kind == _SERVER:
        cursor: Any = _FakePsycopgServerCursor(None, db, _CURSOR_NAME)
    elif kind == _AIOMYSQL:
        cursor = _FakeAiomysqlCursor(None, db)
    elif kind == _SEPARATE_ITERATOR:
        cursor = _FakeSeparateIteratorCursor(None, db)
    else:
        cursor = _FakePsycopgCursor(None, db)
    await cursor.execute(_QUERY)
    return cursor


async def _open_wrapper(kind: str, db: _FakeDatabase, plugins: Any = "") -> Any:
    conn = _make_fake_conn(kind, db)

    async def _target(**kwargs: Any) -> Any:
        return conn

    if kind == _AIOMYSQL:
        _target.__module__ = "aiomysql.connection"
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_target, host="h", port="3306", user="u", password="p", dbname="d",
            wrapper_dialect="mysql", plugins=plugins)
    else:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_target, conninfo=_CONNINFO, wrapper_dialect="pg", plugins=plugins)
    if kind == _SERVER:
        cursor = wrapper.cursor(name=_CURSOR_NAME)
    else:
        cursor = wrapper.cursor()
    await cursor.execute(_QUERY)
    return cursor


async def _loop(cursor: Any) -> List[Any]:
    rows: List[Any] = []
    async for row in cursor:
        rows.append(row)
        assert len(rows) <= _MAX_ROWS
    return rows


async def _anext_until_end(cursor: Any) -> List[Any]:
    rows: List[Any] = []
    while len(rows) <= _MAX_ROWS:
        try:
            rows.append(await anext(cursor))
        except StopAsyncIteration:
            return rows
    raise AssertionError("the cursor never ended")


async def _loop_until(cursor: Any, count: int) -> List[Any]:
    rows: List[Any] = []
    async for row in cursor:
        rows.append(row)
        if len(rows) == count:
            break
    return rows


async def _loop_until_error(cursor: Any) -> Tuple[List[Any], Optional[Exception]]:
    rows: List[Any] = []
    try:
        async for row in cursor:
            rows.append(row)
    except Exception as error:
        return rows, error
    return rows, None


async def _fetchone_until_error(cursor: Any) -> Exception:
    for _ in range(_MAX_ROWS):
        try:
            await cursor.fetchone()
        except Exception as error:
            return error
    raise AssertionError("fetchone never raised")


async def _fetchone_then_loop(cursor: Any) -> List[Any]:
    return [await cursor.fetchone(), await _loop(cursor)]


async def _fetchmany_then_loop(cursor: Any) -> List[Any]:
    return [await cursor.fetchmany(2), await _loop(cursor)]


async def _break_then_fetchone(cursor: Any) -> List[Any]:
    return [await _loop_until(cursor, 3), await cursor.fetchone()]


async def _break_then_fetchmany(cursor: Any) -> List[Any]:
    return [await _loop_until(cursor, 3), await cursor.fetchmany(5)]


async def _break_then_loop(cursor: Any) -> List[Any]:
    return [await _loop_until(cursor, 3), await _loop(cursor)]


async def _break_fetchone_then_loop(cursor: Any) -> List[Any]:
    return [await _loop_until(cursor, 3), await cursor.fetchone(), await _loop(cursor)]


async def _anext_then_fetchall(cursor: Any) -> List[Any]:
    return [await anext(cursor), await cursor.fetchall()]


_READERS = [pytest.param(_loop, id="async_for"), pytest.param(_anext_until_end, id="anext")]
_SEQUENCES = {
    "fetchone_then_loop": _fetchone_then_loop,
    "fetchmany_then_loop": _fetchmany_then_loop,
    "break_then_fetchone": _break_then_fetchone,
    "break_then_fetchmany": _break_then_fetchmany,
    "break_then_loop": _break_then_loop,
    "break_fetchone_then_loop": _break_fetchone_then_loop,
    "anext_then_fetchall": _anext_then_fetchall,
}
_SEQUENCE_DATA = {
    "client-mixed": (_CLIENT, _MIXED_ROWS),
    "client-plain": (_CLIENT, _PLAIN_ROWS),
    "server-long": (_SERVER, _LONG_ROWS),
    "server-long_plain": (_SERVER, _LONG_PLAIN_ROWS),
    "aiomysql-tuples": (_AIOMYSQL, _LONG_TUPLE_ROWS),
}


def _run_wrapper(
        kind: str,
        rows: List[Any],
        read: Callable[[Any], Coroutine[Any, Any, Any]],
        plugins: Any = "") -> Any:
    async def _body() -> Any:
        return await read(await _open_wrapper(kind, _FakeDatabase(rows), plugins))

    return asyncio.run(_body())


def _run_raw(kind: str, rows: List[Any], read: Callable[[Any], Coroutine[Any, Any, Any]]) -> Any:
    async def _body() -> Any:
        return await read(await _open_raw(kind, _FakeDatabase(rows)))

    return asyncio.run(_body())


def _plugin_results_before_each_row(events: List[Tuple[str, Any]]) -> List[List[Any]]:
    groups: List[List[Any]] = []
    current: List[Any] = []
    for source, value in events:
        if source == "plugin":
            current.append(value)
            continue
        groups.append(current)
        current = []
    return groups


@pytest.mark.parametrize("chain", _CHAINS)
@pytest.mark.parametrize("read", _READERS)
@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
@pytest.mark.parametrize("rows", list(_NONE_ROWS.values()), ids=list(_NONE_ROWS))
def test_loop_yields_none_rows(rows, kind, read, chain):
    assert _run_wrapper(kind, rows, read, _plugins(chain)) == rows


@pytest.mark.parametrize("read", _READERS)
@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
def test_loop_long_result_none_in_each_page(kind, read):
    assert _run_wrapper(kind, _LONG_ROWS, read) == _LONG_ROWS


@pytest.mark.parametrize("read", _READERS)
@pytest.mark.parametrize("kind", _ALL_KINDS)
def test_loop_empty_result_ends(kind, read):
    assert _run_wrapper(kind, [], read) == []


@pytest.mark.parametrize("read", _READERS)
@pytest.mark.parametrize(("kind", "rows"), [
    pytest.param(_CLIENT, _TUPLE_ROWS, id="client-tuples"),
    pytest.param(_SERVER, _TUPLE_ROWS, id="server-tuples"),
    pytest.param(_CLIENT, _LONG_PLAIN_ROWS, id="client-long_plain"),
    pytest.param(_SERVER, _LONG_PLAIN_ROWS, id="server-long_plain"),
    pytest.param(_AIOMYSQL, _TUPLE_ROWS, id="aiomysql-tuples"),
    pytest.param(_AIOMYSQL, _DICT_ROWS, id="aiomysql-dicts"),
])
def test_loop_rows_without_none_unchanged(kind, rows, read):
    assert _run_wrapper(kind, rows, read) == rows


@pytest.mark.parametrize("read", _READERS)
@pytest.mark.parametrize("rows", [
    pytest.param([1, None, 3], id="none_middle"),
    pytest.param([None, 2, 3], id="none_first"),
    pytest.param([1, 2, 3], id="plain"),
])
def test_loop_separate_iterator_yields_rows_not_none(rows, read):
    result = _run_wrapper(_SEPARATE_ITERATOR, rows, read)

    assert [row for row in result if row is not None] == [row for row in rows if row is not None]
    assert result.count(None) <= rows.count(None)


@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
def test_anext_default_after_last_row(kind):
    async def _read(cursor: Any) -> List[Any]:
        return [await anext(cursor, _DEFAULT) for _ in range(4)]

    assert _run_wrapper(kind, [None, 2, None], _read) == [None, 2, None, _DEFAULT]


@pytest.mark.parametrize("kind", _ALL_KINDS)
def test_anext_after_end_raises_again(kind):
    async def _read(cursor: Any) -> List[Any]:
        rows = await _loop(cursor)
        with pytest.raises(StopAsyncIteration):
            await anext(cursor)
        with pytest.raises(StopAsyncIteration):
            await anext(cursor)
        rows.append(await anext(cursor, _DEFAULT))
        return rows

    assert _run_wrapper(kind, _TUPLE_ROWS, _read) == _TUPLE_ROWS + [_DEFAULT]


@pytest.mark.parametrize("kind", _ALL_KINDS)
def test_anext_empty_result_returns_default(kind):
    async def _read(cursor: Any) -> Any:
        return await anext(cursor, _DEFAULT)

    assert _run_wrapper(kind, [], _read) == _DEFAULT


@pytest.mark.parametrize("sequence", list(_SEQUENCES.values()), ids=list(_SEQUENCES))
@pytest.mark.parametrize(("kind", "rows"), list(_SEQUENCE_DATA.values()), ids=list(_SEQUENCE_DATA))
def test_mixed_fetches_match_raw(kind, rows, sequence):
    assert _run_wrapper(kind, rows, sequence) == _run_raw(kind, rows, sequence)


def _error_for(kind: str) -> Callable[[], Exception]:
    if kind == _AIOMYSQL:
        return lambda: aiomysql.DataError(1366, "Incorrect value")
    if kind == _SERVER:
        return lambda: psycopg.errors.DivisionByZero("division by zero")
    return lambda: psycopg.DataError("invalid value")


def _loop_and_fetchone_errors(
        kind: str,
        rows: List[Any],
        error_index: int,
        chain: str) -> Tuple[List[Any], Optional[Exception], Exception]:
    async def _body() -> Tuple[List[Any], Optional[Exception], Exception]:
        error = _error_for(kind)
        loop_cursor = await _open_wrapper(kind, _FakeDatabase(rows, error_index, error), _plugins(chain))
        read, loop_error = await _loop_until_error(loop_cursor)
        fetchone_cursor = await _open_wrapper(kind, _FakeDatabase(rows, error_index, error), _plugins(chain))
        return read, loop_error, await _fetchone_until_error(fetchone_cursor)

    return asyncio.run(_body())


@pytest.mark.parametrize("chain", _CHAINS)
@pytest.mark.parametrize("kind", [_CLIENT, _AIOMYSQL])
def test_loop_driver_error_raised(kind, chain):
    read, loop_error, fetchone_error = _loop_and_fetchone_errors(kind, _TUPLE_ROWS, 1, chain)

    assert type(loop_error) is type(fetchone_error)
    assert read == _TUPLE_ROWS[:1]


@pytest.mark.parametrize("chain", _CHAINS)
def test_server_loop_driver_error_raised(chain):
    read, loop_error, fetchone_error = _loop_and_fetchone_errors(_SERVER, _TUPLE_ROWS, 1, chain)

    assert type(loop_error) is type(fetchone_error)
    assert read == _TUPLE_ROWS[:len(read)]


@pytest.mark.parametrize("chain", _CHAINS)
@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
def test_loop_driver_error_after_none_row_raised(kind, chain):
    rows = [1, None, 3]
    read, loop_error, fetchone_error = _loop_and_fetchone_errors(kind, rows, 2, chain)

    assert type(loop_error) is type(fetchone_error)
    assert read == rows[:len(read)]


def _run_recorded_loop(kind: str, rows: List[Any], subscribed: str) -> Tuple[List[Any], List[Tuple[str, Any]]]:
    events: List[Tuple[str, Any]] = []

    async def _read(cursor: Any) -> List[Any]:
        read: List[Any] = []
        async for row in cursor:
            events.append(("body", row))
            read.append(row)
            assert len(read) <= _MAX_ROWS
        return read

    read = _run_wrapper(kind, rows, _read, [_RowRecorderPlugin(events, subscribed)])
    return read, events


@pytest.mark.parametrize("subscribed", [_FETCHONE, _ALL_METHODS], ids=["fetchone", "all_methods"])
@pytest.mark.parametrize(("kind", "rows"), [
    pytest.param(_CLIENT, [1, None, 3], id="client-none_middle"),
    pytest.param(_SERVER, [1, None, 3], id="server-none_middle"),
    pytest.param(_CLIENT, _TUPLE_ROWS, id="client-tuples"),
    pytest.param(_SERVER, _TUPLE_ROWS, id="server-tuples"),
    pytest.param(_AIOMYSQL, _TUPLE_ROWS, id="aiomysql-tuples"),
])
def test_plugin_sees_each_row_before_loop_body(kind, rows, subscribed):
    read, events = _run_recorded_loop(kind, rows, subscribed)
    groups = _plugin_results_before_each_row(events)

    assert read == rows
    assert all(groups)
    assert [group[-1] for group in groups] == rows


def _changed(row: Any) -> Any:
    if row is None:
        return None
    return ("changed", row)


@pytest.mark.parametrize(("kind", "rows"), [
    pytest.param(_CLIENT, [1, None, 3], id="client-none_middle"),
    pytest.param(_SERVER, [1, None, 3], id="server-none_middle"),
    pytest.param(_CLIENT, _TUPLE_ROWS, id="client-tuples"),
    pytest.param(_SERVER, _TUPLE_ROWS, id="server-tuples"),
    pytest.param(_AIOMYSQL, _TUPLE_ROWS, id="aiomysql-tuples"),
])
def test_plugin_changed_row_yielded(kind, rows):
    result = _run_wrapper(kind, rows, _loop, [_RowChangingPlugin(_changed)])

    assert result == [_changed(row) for row in rows]


@pytest.mark.parametrize(("kind", "rows"), [
    pytest.param(_CLIENT, [1, None, 3], id="client-none_middle"),
    pytest.param(_SERVER, [1, None, 3], id="server-none_middle"),
    pytest.param(_CLIENT, _TUPLE_ROWS, id="client-tuples"),
    pytest.param(_SERVER, _TUPLE_ROWS, id="server-tuples"),
    pytest.param(_AIOMYSQL, _TUPLE_ROWS, id="aiomysql-tuples"),
])
def test_plugin_guarding_next_step_sees_no_exception(kind, rows):
    plugin = _GuardingPlugin()

    result = _run_wrapper(kind, rows, _loop, [plugin])

    assert result == rows
    assert plugin.errors == []


@pytest.mark.parametrize(("kind", "rows"), [
    pytest.param(_CLIENT, [1, None, 3, 4], id="client-none_second"),
    pytest.param(_SERVER, [1, None, 3, 4], id="server-none_second"),
    pytest.param(_CLIENT, [1, 2, 3, 4], id="client-plain"),
    pytest.param(_SERVER, [1, 2, 3, 4], id="server-plain"),
    pytest.param(_AIOMYSQL, _LONG_TUPLE_ROWS, id="aiomysql-tuples"),
])
def test_plugin_returning_none_ends_loop(kind, rows):
    plugin = _StoppingPlugin()

    async def _read(cursor: Any) -> List[Any]:
        read: List[Any] = []
        async for row in cursor:
            read.append(row)
            if len(read) == 2:
                plugin.stop = True
            assert len(read) <= _MAX_ROWS
        return read

    assert _run_wrapper(kind, rows, _read, [plugin]) == rows[:2]


@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
def test_plugin_none_for_read_row_yielded(kind):
    plugin = _RowChangingPlugin(lambda row: None if row == 2 else row)

    assert _run_wrapper(kind, [1, 2, 3], _loop, [plugin]) == [1, None, 3]


@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
def test_fetchone_none_row_and_end_unchanged(kind):
    async def _read(cursor: Any) -> List[Any]:
        return [await cursor.fetchone() for _ in range(3)]

    assert _run_wrapper(kind, [1, None], _read) == [1, None, None]


@pytest.mark.parametrize("kind", _PSYCOPG_KINDS)
def test_fetchmany_fetchall_none_rows_unchanged(kind):
    async def _read(cursor: Any) -> List[Any]:
        return [await cursor.fetchmany(2), await cursor.fetchall(), await cursor.fetchall()]

    rows = [1, None, 3, None]

    assert _run_wrapper(kind, rows, _read) == _run_raw(kind, rows, _read)


def test_server_loop_fetch_round_trips_not_increased():
    db = _FakeDatabase(_LONG_PLAIN_ROWS)

    async def _body() -> Tuple[List[Any], int]:
        cursor = await _open_wrapper(_SERVER, db)
        before = db.fetch_round_trips
        read = await _loop(cursor)
        return read, db.fetch_round_trips - before

    read, round_trips = asyncio.run(_body())

    assert read == _LONG_PLAIN_ROWS
    assert round_trips <= len(_LONG_PLAIN_ROWS) + 1


@pytest.mark.parametrize("read", _READERS)
def test_client_loop_sends_no_query(read):
    db = _FakeDatabase([1, None, 3])

    async def _body() -> List[str]:
        cursor = await _open_wrapper(_CLIENT, db)
        before = list(db.executes)
        await read(cursor)
        return db.executes[len(before):]

    assert asyncio.run(_body()) == []
