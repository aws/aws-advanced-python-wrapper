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

"""F3-B SP-2: AsyncAwsWrapperConnection + AsyncAwsWrapperCursor.

Tests exercise the wrapper with a mock ``psycopg.AsyncConnection`` so no
database is required. Covers:
  - ``connect`` factory routes through the plugin pipeline and stores
    the opened connection on the service.
  - ``cursor()`` returns an ``AsyncAwsWrapperCursor`` wrapping the driver cursor.
  - Cursor operations route through ``AsyncPluginManager.execute``.
  - Connection operations (close/commit/rollback) route through the pipeline.
  - ``__getattr__`` forwards unknown attrs to the underlying driver conn/cursor.
  - Async context manager protocol closes on exit.
  - Target-driver validation (``target`` must be a callable).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Set
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
    AsyncPsycopgDriverDialect
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aio.wrapper import (AsyncAwsWrapperConnection,
                                                     AsyncAwsWrapperCursor)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Plugin fixtures ----------------------------------------------------


class RecorderPlugin(AsyncPlugin):
    """Records every method seen through the execute pipeline."""

    def __init__(self, log: List[str]) -> None:
        self.log = log

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.ALL.method_name}

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: Any,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self.log.append("connect:enter")
        result = await connect_func()
        self.log.append("connect:exit")
        return result

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        self.log.append(f"execute:{method_name}")
        return await execute_func()


# ---- Mock driver setup --------------------------------------------------


def _build_mock_psycopg_connect(returned_conn: Any) -> Callable[..., Awaitable[Any]]:
    """Build an awaitable that returns ``returned_conn``. Mimics
    :func:`psycopg.AsyncConnection.connect`."""

    async def _connect(**kwargs: Any) -> Any:
        return returned_conn

    return _connect


def _make_mock_async_conn() -> MagicMock:
    """Build a MagicMock shaped like a psycopg.AsyncConnection."""
    conn = MagicMock()
    conn.close = AsyncMock()
    # wrapper.close() releases plugin-service resources (sync parity), whose
    # abort_connection probes conn.fileno(); a MagicMock would fabricate a
    # non-int fd and push abort into its close() fallback, double-closing the
    # mock. A raising fileno makes abort a no-op, like a real conn whose
    # socket is already gone.
    conn.fileno = MagicMock(side_effect=OSError("mock conn has no real fd"))
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    conn.closed = False
    conn.autocommit = True
    # A raw driver connection unwraps to itself (it is not a pool proxy); the
    # plugin manager's old-connection guard unwraps via ``driver_connection``,
    # and a bare MagicMock would otherwise auto-fabricate a *different* object.
    conn.driver_connection = conn

    def _cursor(*args: Any, **kwargs: Any) -> MagicMock:
        cur = _make_mock_async_cursor()
        # psycopg/aiomysql cursors expose ``.connection`` as the conn they were
        # created on; the old-connection guard compares it to current_connection.
        cur.connection = conn
        return cur

    conn.cursor = MagicMock(side_effect=_cursor)
    return conn


def _make_mock_async_cursor() -> MagicMock:
    cur = MagicMock()
    cur.execute = AsyncMock(return_value=None)
    cur.executemany = AsyncMock(return_value=None)
    cur.fetchone = AsyncMock(return_value=("row",))
    cur.fetchmany = AsyncMock(return_value=[("a",), ("b",)])
    cur.fetchall = AsyncMock(return_value=[("r1",), ("r2",), ("r3",)])
    cur.close = AsyncMock()
    cur.description = [("col",)]
    cur.rowcount = 3
    cur.arraysize = 1
    return cur


# ---- Tests --------------------------------------------------------------


def test_connect_rejects_missing_target():
    async def _body() -> None:
        with pytest.raises(AwsWrapperError):
            await AsyncAwsWrapperConnection.connect()

    asyncio.run(_body())


def test_connect_rejects_non_callable_target():
    async def _body() -> None:
        with pytest.raises(AwsWrapperError):
            await AsyncAwsWrapperConnection.connect("not-a-callable")

    asyncio.run(_body())


def test_connect_opens_via_plugin_pipeline_and_returns_wrapper():
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=example.com user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )

    wrapper_conn = asyncio.run(_body())
    assert isinstance(wrapper_conn, AsyncAwsWrapperConnection)
    assert wrapper_conn.target_connection is raw_conn
    # Pipeline ordering: RecorderPlugin wraps AsyncDefaultPlugin.
    assert log == ["connect:enter", "connect:exit"]
    # Connection bound to the plugin service.
    assert wrapper_conn._plugin_service.current_connection is raw_conn


def test_connect_passes_host_and_port_from_props():
    raw_conn = _make_mock_async_conn()
    captured_kwargs: List[dict] = []

    async def _target(**kwargs: Any) -> Any:
        captured_kwargs.append(kwargs)
        return raw_conn

    async def _body() -> None:
        await AsyncAwsWrapperConnection.connect(
            target=_target,
            conninfo="host=h.example user=u password=p dbname=db port=6543",
        )

    asyncio.run(_body())
    assert captured_kwargs, "target_func was never invoked"
    kw = captured_kwargs[0]
    assert kw["host"] == "h.example"
    assert kw["port"] == "6543"
    assert kw["user"] == "u"
    assert kw["dbname"] == "db"


def test_cursor_is_sync_and_returns_async_cursor_wrapper():
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    cur = wrapper.cursor()
    assert isinstance(cur, AsyncAwsWrapperCursor)
    assert cur.connection is wrapper


def test_cursor_execute_routes_through_plugin_pipeline():
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        cur = wrapper.cursor()
        await cur.execute("SELECT 1")
        await cur.fetchone()
        await cur.fetchall()
        await cur.close()

    asyncio.run(_body())
    assert log == [
        "connect:enter",
        "connect:exit",
        "execute:Cursor.execute",
        "execute:Cursor.fetchone",
        "execute:Cursor.fetchall",
        "execute:Cursor.close",
    ]


def _connected_wrapper(raw_conn: Any) -> AsyncAwsWrapperConnection:
    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    return asyncio.run(_body())


def test_is_closed_psycopg_uses_closed_attr():
    # psycopg.AsyncConnection exposes a sync `closed` bool. The wrapper's
    # is_closed property mirrors the sync wrapper so parity tests can assert
    # `conn.is_closed is False/True` without it falling through __getattr__ to
    # the raw conn (which lacks `is_closed`).
    raw_conn = _make_mock_async_conn()  # has .closed = False
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.is_closed is False
    raw_conn.closed = True
    assert wrapper.is_closed is True


def test_connect_selects_psycopg_dialect_for_psycopg_target():
    from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
        AsyncPsycopgDriverDialect
    wrapper = _connected_wrapper(_make_mock_async_conn())  # default mock target
    assert isinstance(
        wrapper._plugin_service.driver_dialect, AsyncPsycopgDriverDialect)


def test_connect_selects_aiomysql_dialect_for_aiomysql_target():
    # Regression for the broad MySQL-async failures: the wrapper must pick the
    # aiomysql driver dialect when the target connect callable is aiomysql's
    # (module 'aiomysql.*'), NOT hardcode psycopg. Using psycopg's dialect for
    # MySQL left a string port (aiomysql '%d format' crash) and mismatched
    # cursor/transaction semantics across every env.
    from aws_advanced_python_wrapper.aio.driver_dialect.aiomysql import \
        AsyncAiomysqlDriverDialect
    raw_conn = _make_mock_async_conn()
    target = _build_mock_psycopg_connect(raw_conn)
    target.__module__ = "aiomysql.connection"  # tag the target as aiomysql

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=target, host="h", port="3306", user="u",
            password="p", dbname="d")
        assert isinstance(
            wrapper._plugin_service.driver_dialect, AsyncAiomysqlDriverDialect)

    asyncio.run(_body())


def test_is_closed_aiomysql_uses_open_attr():
    # aiomysql Connection exposes `open` (inverse of closed), not `closed`.
    raw_conn = _make_mock_async_conn()
    del raw_conn.closed
    raw_conn.open = True
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.is_closed is False
    raw_conn.open = False
    assert wrapper.is_closed is True


def test_read_only_normalizes_psycopg_none_to_false():
    # psycopg exposes read_only as a tri-state (None == unset/server default).
    # A bare passthrough would return None and fail `assert conn.read_only is
    # False`; the wrapper normalizes to a plain bool.
    raw_conn = _make_mock_async_conn()
    del raw_conn._aws_read_only  # psycopg-shape: no aiomysql intent stash
    raw_conn.read_only = None
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.read_only is False
    raw_conn.read_only = True
    assert wrapper.read_only is True


def test_set_read_only_on_closed_connection_raises():
    # set_read_only on a closed connection must raise AwsWrapperError, not let
    # the RWS plugin silently open a fresh reader.
    raw_conn = _make_mock_async_conn()
    wrapper = _connected_wrapper(raw_conn)
    raw_conn.closed = True

    async def _body() -> None:
        with pytest.raises(AwsWrapperError):
            await wrapper.set_read_only(True)

    asyncio.run(_body())


def test_set_read_only_in_user_transaction_does_not_rollback_and_propagates():
    # When the USER is in a transaction (tracked by the plugin service before
    # this op), set_read_only(True) must NOT silently roll it back -- the driver
    # (psycopg) rejects the mid-txn change and that error must propagate to the
    # caller (regression for test_set_read_only_true_in_transaction).
    raw_conn = _make_mock_async_conn()
    wrapper = _connected_wrapper(raw_conn)
    svc = wrapper._plugin_service
    svc._is_in_transaction = True  # user txn started by a PRIOR op
    svc._driver_dialect.is_in_transaction = AsyncMock(return_value=True)
    svc._driver_dialect.set_read_only = AsyncMock(
        side_effect=Exception("can't change 'read_only' now: INTRANS"))
    svc._session_state_service.setup_pristine_readonly = AsyncMock()
    svc._session_state_service.set_read_only = MagicMock()

    async def _body() -> None:
        with pytest.raises(Exception):
            await wrapper.set_read_only(True)
        raw_conn.rollback.assert_not_called()  # user txn NOT rolled back

    asyncio.run(_body())


def test_set_read_only_rolls_back_transient_non_user_transaction():
    # A transient (non-user) transaction -- left by an RWS switch probe or
    # SQLAlchemy's pool-reset -- IS rolled back so the read_only flip can apply.
    # No user txn is tracked, but the driver reports INTRANS.
    raw_conn = _make_mock_async_conn()
    wrapper = _connected_wrapper(raw_conn)
    svc = wrapper._plugin_service
    svc._is_in_transaction = False  # no user txn
    svc._driver_dialect.is_in_transaction = AsyncMock(return_value=True)
    svc._driver_dialect.set_read_only = AsyncMock()
    svc._session_state_service.setup_pristine_readonly = AsyncMock()
    svc._session_state_service.set_read_only = MagicMock()

    async def _body() -> None:
        await wrapper.set_read_only(False)
        raw_conn.rollback.assert_awaited()  # transient txn rolled back
        svc._driver_dialect.set_read_only.assert_awaited_once()

    asyncio.run(_body())


def test_execute_on_old_cursor_after_switch_raises():
    # Old-connection guard (parity with sync): a cursor created on the original
    # connection must not silently run after the wrapper's current connection
    # switched (RWS reader/writer swap or failover) -> AwsWrapperError.
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    raw_conn = _make_mock_async_conn()
    wrapper = _connected_wrapper(raw_conn)
    cur = wrapper.cursor()  # bound to raw_conn

    new_conn = _make_mock_async_conn()
    svc = wrapper._plugin_service
    svc._driver_dialect.transfer_session_state = AsyncMock()
    svc._session_state_service.apply_current_session_state = AsyncMock()

    async def _body() -> None:
        await svc.set_current_connection(new_conn, HostInfo("new-host", 5432))
        with pytest.raises(AwsWrapperError):
            await cur.execute("SELECT 1")
        # A freshly-created cursor (bound to the new current conn) works fine.
        fresh = wrapper.cursor()
        await fresh.execute("SELECT 1")

    asyncio.run(_body())


def test_read_only_falls_back_to_aiomysql_stash():
    # aiomysql has no native read_only flag; the async aiomysql driver dialect
    # stashes intent on _aws_read_only. The wrapper reads it back when the
    # native attr is absent/None.
    raw_conn = _make_mock_async_conn()
    del raw_conn.read_only
    raw_conn._aws_read_only = True
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.read_only is True


def test_autocommit_getter_returns_sync_bool_psycopg():
    # Parity with the sync wrapper / read_only: a plain bool, no await needed
    # (psycopg exposes autocommit as a sync bool property).
    raw_conn = _make_mock_async_conn()  # autocommit = True (bool)
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.autocommit is True
    raw_conn.autocommit = False
    assert wrapper.autocommit is False


def test_autocommit_getter_aiomysql_uses_get_autocommit():
    # aiomysql exposes autocommit as a setter *method*; read via get_autocommit().
    raw_conn = _make_mock_async_conn()
    raw_conn.autocommit = MagicMock(name="autocommit-setter")  # callable
    raw_conn.get_autocommit = MagicMock(return_value=False)
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.autocommit is False


def test_wrapper_target_follows_connection_switch():
    # Core failover / RWS fix: when the plugin service switches the current
    # connection, the owning wrapper's cached target connection must follow,
    # so subsequent cursor() / commit() hit the NEW connection -- not the old,
    # often-closed one. Without the registration + rebind, RWS never redirects
    # and failover retries hit "the connection is closed".
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    raw_conn = _make_mock_async_conn()
    wrapper = _connected_wrapper(raw_conn)
    assert wrapper.target_connection is raw_conn

    new_conn = _make_mock_async_conn()
    svc = wrapper._plugin_service
    # Stub the session-state transfer machinery; we're testing the rebind.
    svc._driver_dialect.transfer_session_state = AsyncMock()
    svc._session_state_service.apply_current_session_state = AsyncMock()

    async def _switch() -> None:
        await svc.set_current_connection(new_conn, HostInfo("new-host", 5432))

    asyncio.run(_switch())

    assert wrapper.target_connection is new_conn
    # New cursors bind to the switched connection.
    wrapper.cursor()
    new_conn.cursor.assert_called()


def test_connection_commit_rollback_close_route_through_pipeline():
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        await wrapper.commit()
        await wrapper.rollback()
        await wrapper.close()

    asyncio.run(_body())
    commit_calls = [e for e in log if e == "execute:Connection.commit"]
    rollback_calls = [e for e in log if e == "execute:Connection.rollback"]
    close_calls = [e for e in log if e == "execute:Connection.close"]
    assert len(commit_calls) == 1
    assert len(rollback_calls) == 1
    assert len(close_calls) == 1
    raw_conn.commit.assert_awaited_once()
    raw_conn.rollback.assert_awaited_once()
    raw_conn.close.assert_awaited_once()


def test_connection_async_context_manager_closes_on_exit():
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        # plugins="" -> bare connection (no default plugins), isolating the
        # context-manager close path from default-plugin activity.
        async with await AsyncAwsWrapperConnection.connect(
                target=_build_mock_psycopg_connect(raw_conn),
                conninfo="host=h user=u password=p dbname=d port=5432",
                plugins="",
        ) as conn:
            assert isinstance(conn, AsyncAwsWrapperConnection)

    asyncio.run(_body())
    raw_conn.close.assert_awaited_once()


def test_connect_applies_default_plugins_when_unset():
    # Parity with the sync wrapper: when neither an explicit plugin list nor a
    # ``plugins`` property is given, the default plugin list is applied.
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    conn = asyncio.run(_body())
    # DEFAULT_PLUGINS: initial_connection, aurora_connection_tracker,
    # failover_v2, host_monitoring_v2.
    assert conn._plugin_manager.num_plugins >= 4


def test_connect_explicit_blank_plugins_loads_none():
    # ``plugins=""`` is distinct from unset: it means "no plugins".
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )

    conn = asyncio.run(_body())
    # Only the always-present built-in AsyncDefaultPlugin; no user/default
    # plugins (vs >=5 when defaults are applied for the unset case above).
    assert conn._plugin_manager.num_plugins == 1


def test_connect_rolls_back_lingering_nonautocommit_transaction():
    # Connect-time topology/plugin queries can leave a non-autocommit
    # connection in a transaction -- on a NON-Aurora target a failed Aurora
    # query leaves psycopg's txn ABORTED. connect() must roll it back before
    # handing the connection to the caller, otherwise the first real query
    # dies with InFailedSqlTransaction (regression once default plugins
    # auto-load on plain Postgres).
    import psycopg
    raw_conn = _make_mock_async_conn()
    raw_conn.autocommit = False  # not autocommit -> a txn can linger
    raw_conn.info.transaction_status = psycopg.pq.TransactionStatus.INERROR

    async def _body() -> None:
        await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )

    asyncio.run(_body())
    raw_conn.rollback.assert_awaited()


def test_connect_does_not_roll_back_autocommit_connection():
    # An autocommit connection has no lingering transaction, so connect() must
    # NOT issue a spurious rollback (guards the gate above).
    raw_conn = _make_mock_async_conn()  # autocommit=True by default

    async def _body() -> None:
        await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )

    asyncio.run(_body())
    raw_conn.rollback.assert_not_awaited()


def test_connection_getattr_forwards_to_raw_conn():
    raw_conn = _make_mock_async_conn()
    raw_conn.info = "pgconn-info-sentinel"

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    assert wrapper.info == "pgconn-info-sentinel"


def test_cursor_getattr_forwards_to_target_cursor():
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperCursor:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )
        return wrapper.cursor()

    cur = asyncio.run(_body())
    # The mock async cursor has a description attribute on the target.
    assert cur.description == [("col",)]
    assert cur.rowcount == 3


def test_cursor_async_context_manager_closes_on_exit():
    raw_conn = _make_mock_async_conn()
    # Capture the single cursor mock the conn will hand out.
    mock_cursor = _make_mock_async_cursor()
    raw_conn.cursor = MagicMock(return_value=mock_cursor)

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )
        async with wrapper.cursor() as cur:
            assert isinstance(cur, AsyncAwsWrapperCursor)
        mock_cursor.close.assert_awaited_once()

    asyncio.run(_body())


def test_psycopg_driver_dialect_is_dialect_recognizes_psycopg_connect():
    import psycopg

    dialect = AsyncPsycopgDriverDialect()
    assert dialect.is_dialect(psycopg.AsyncConnection.connect) is True

    def _other_connect() -> None:  # pragma: no cover - identity-only check
        pass

    # is_dialect still returns True as a default for unknown callables
    # (matches sync DriverDialect base behavior). Verifying it doesn't
    # raise, not that it's False.
    result = dialect.is_dialect(_other_connect)
    assert result in (True, False)


def test_psycopg_driver_dialect_lifecycle_ops_against_mock():
    """Exercise the dialect's async ops using a mock `AsyncConnection` shape."""

    async def _body() -> None:
        dialect = AsyncPsycopgDriverDialect()
        conn = _make_mock_async_conn()
        # Install a fake transaction_status on conn.info
        import psycopg
        conn.info = MagicMock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE

        assert await dialect.is_closed(conn) is False
        assert await dialect.is_in_transaction(conn) is False
        assert await dialect.get_autocommit(conn) is True
        conn.set_autocommit = AsyncMock()
        await dialect.set_autocommit(conn, False)
        conn.set_autocommit.assert_awaited_once_with(False)

        conn.read_only = False
        assert await dialect.is_read_only(conn) is False
        conn.set_read_only = AsyncMock()
        await dialect.set_read_only(conn, True)
        conn.set_read_only.assert_awaited_once_with(True)

        assert await dialect.can_execute_query(conn) is True
        # A reachable-but-unusable fd (non-int / negative) pushes abort into
        # its close() fallback; the shared mock raises from fileno() (abort
        # no-op), so restore the fallback shape for this assertion.
        conn.fileno = MagicMock(return_value=-1)
        await dialect.abort_connection(conn)
        conn.close.assert_awaited_once()

    asyncio.run(_body())


def test_psycopg_driver_dialect_network_bound_methods_covers_core():
    dialect = AsyncPsycopgDriverDialect()
    nb = dialect.network_bound_methods
    assert DbApiMethod.CONNECT.method_name in nb
    assert DbApiMethod.CURSOR_EXECUTE.method_name in nb
    assert DbApiMethod.CONNECTION_COMMIT.method_name in nb


def test_connect_populates_plugin_service_slots():
    """AsyncAwsWrapperConnection.connect populates database_dialect,
    host_list_provider, plugin_manager, and initial_connection_host_info
    on the plugin service."""
    import asyncio
    from unittest.mock import MagicMock

    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection
    from aws_advanced_python_wrapper.database_dialect import PgDatabaseDialect

    async def _fake_target(**kwargs):
        mock = MagicMock(spec=["close", "cursor"])
        mock.close = MagicMock()
        return mock

    conn = asyncio.run(
        AsyncAwsWrapperConnection.connect(
            target=_fake_target,
            host="localhost",
            dbname="test",
            user="u",
            password="p",
        )
    )

    # The plugin service slots should be populated
    svc = conn._plugin_service
    assert isinstance(svc.database_dialect, PgDatabaseDialect), \
        f"database_dialect was {svc.database_dialect!r}"
    assert svc.host_list_provider is not None
    assert svc.plugin_manager is not None
    assert svc.initial_connection_host_info is not None
    assert svc.initial_connection_host_info.host == "localhost"


# ---- Phase I.1: Cursor PEP 249 surface ---------------------------------


def _make_wrapper_and_cursor() -> tuple:
    """Build a wrapper connection + cursor backed by mocks, returning
    ``(wrapper, cursor, mock_target_cursor)`` so tests can assert the
    mock was called."""
    raw_conn = _make_mock_async_conn()
    target_cur = _make_mock_async_cursor()
    # Cursor reports the conn it was created on (psycopg/aiomysql semantics) so
    # the plugin manager's old-connection guard sees a match for valid ops.
    target_cur.connection = raw_conn
    raw_conn.cursor = MagicMock(return_value=target_cur)

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    cur = wrapper.cursor()
    return wrapper, cur, target_cur


def test_cursor_lastrowid_passthrough():
    _, cur, target_cur = _make_wrapper_and_cursor()
    target_cur.lastrowid = 42
    assert cur.lastrowid == 42


def test_cursor_scroll_sync_target_calls_through():
    _, cur, target_cur = _make_wrapper_and_cursor()
    # Sync scroll returns None (not a coroutine). The wrapper should still
    # await the pipeline and return the sync value.
    target_cur.scroll = MagicMock(return_value=None)

    async def _body() -> Any:
        return await cur.scroll(5, "relative")

    result = asyncio.run(_body())
    assert result is None
    target_cur.scroll.assert_called_once_with(5, "relative")


def test_cursor_scroll_async_target_awaits_coroutine():
    _, cur, target_cur = _make_wrapper_and_cursor()
    # Target's scroll is async -- wrapper must await the coroutine it
    # returns rather than treating it as the final value.
    target_cur.scroll = AsyncMock(return_value="scrolled")

    async def _body() -> Any:
        return await cur.scroll(3, "absolute")

    result = asyncio.run(_body())
    assert result == "scrolled"
    target_cur.scroll.assert_awaited_once_with(3, "absolute")


def test_cursor_callproc_calls_through():
    _, cur, target_cur = _make_wrapper_and_cursor()
    target_cur.callproc = MagicMock(return_value=(1, 2))

    async def _body() -> Any:
        return await cur.callproc("sp", (1, 2))

    result = asyncio.run(_body())
    assert result == (1, 2)
    target_cur.callproc.assert_called_once_with("sp", (1, 2))


def test_cursor_nextset_calls_through():
    _, cur, target_cur = _make_wrapper_and_cursor()
    target_cur.nextset = MagicMock(return_value=True)

    async def _body() -> Any:
        return await cur.nextset()

    result = asyncio.run(_body())
    assert result is True
    target_cur.nextset.assert_called_once_with()


def test_cursor_setinputsizes_is_sync_passthrough():
    _, cur, target_cur = _make_wrapper_and_cursor()
    target_cur.setinputsizes = MagicMock()
    # Sync method -- no await, just direct call on the wrapper.
    cur.setinputsizes([10, 20, 30])
    target_cur.setinputsizes.assert_called_once_with([10, 20, 30])


def test_cursor_setoutputsize_is_sync_passthrough():
    _, cur, target_cur = _make_wrapper_and_cursor()
    target_cur.setoutputsize = MagicMock()
    cur.setoutputsize(100, 0)
    target_cur.setoutputsize.assert_called_once_with(100, 0)


# ---- Phase I.2: Connection autocommit + isolation_level -----------------


def test_connection_autocommit_getter_reads_target_connection_directly():
    """The autocommit getter reads the target connection's autocommit
    SYNCHRONOUSLY (parity with the sync wrapper / read_only), not via the async
    dialect's coroutine get_autocommit -- so ``conn.autocommit is False`` works
    without an await."""
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    raw_conn.autocommit = True
    assert wrapper.autocommit is True
    raw_conn.autocommit = False
    assert wrapper.autocommit is False


def test_connection_set_autocommit_awaits_driver_dialect():
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    fake_dialect = MagicMock()
    fake_dialect.set_autocommit = AsyncMock()
    wrapper._plugin_service._driver_dialect = fake_dialect

    async def _set() -> None:
        await wrapper.set_autocommit(True)

    asyncio.run(_set())
    fake_dialect.set_autocommit.assert_awaited_once_with(raw_conn, True)


def test_connection_isolation_level_roundtrip():
    """``isolation_level`` getter reads the target's attribute; setter
    delegates to the target's ``set_isolation_level`` if present, else
    falls back to attribute assignment."""
    raw_conn = _make_mock_async_conn()
    raw_conn.isolation_level = "READ COMMITTED"

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    assert wrapper.isolation_level == "READ COMMITTED"

    # Case 1: target exposes async set_isolation_level -- must be awaited.
    raw_conn.set_isolation_level = AsyncMock()

    async def _set_async() -> None:
        await wrapper.set_isolation_level("SERIALIZABLE")

    asyncio.run(_set_async())
    raw_conn.set_isolation_level.assert_awaited_once_with("SERIALIZABLE")

    # Case 2: target has no set_isolation_level -- wrapper falls back to
    # attribute assignment.
    raw_conn2 = _make_mock_async_conn()
    # MagicMock auto-creates attrs, so we need a spec'd mock that raises
    # AttributeError for set_isolation_level to force the fallback path.
    raw_conn2 = MagicMock(spec=["close", "commit", "rollback", "cursor",
                                "autocommit", "isolation_level"])
    raw_conn2.close = AsyncMock()
    raw_conn2.commit = AsyncMock()
    raw_conn2.rollback = AsyncMock()
    raw_conn2.autocommit = True
    raw_conn2.isolation_level = None
    raw_conn2.cursor = MagicMock(return_value=_make_mock_async_cursor())

    async def _body2() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn2),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper2 = asyncio.run(_body2())

    async def _set_fallback() -> None:
        await wrapper2.set_isolation_level("REPEATABLE READ")

    asyncio.run(_set_fallback())
    assert raw_conn2.isolation_level == "REPEATABLE READ"


def test_async_connection_getattr_delegates_driver_attrs_including_underscore():
    # Public AND single-underscore driver attrs are delegated to the underlying
    # connection (SQLAlchemy's psycopg async adapter reaches for underscore
    # members). Only dunders stay on the wrapper, and the _target_conn name is
    # guarded so a miss before __init__ sets it raises instead of recursing.
    wrapper = AsyncAwsWrapperConnection.__new__(AsyncAwsWrapperConnection)
    target = MagicMock()
    wrapper._target_conn = target

    assert wrapper.pgconn is target.pgconn
    # single-underscore driver attr delegates (regression: was wrongly blocked)
    assert wrapper._close is target._close
    # dunder stays on the wrapper, not delegated
    with pytest.raises(AttributeError):
        _ = wrapper.__totally_made_up_dunder__
    # the internal target name is guarded against recursion when unset
    fresh = AsyncAwsWrapperConnection.__new__(AsyncAwsWrapperConnection)
    with pytest.raises(AttributeError):
        _ = fresh._target_conn


def test_async_cursor_getattr_delegates_driver_attrs_including_underscore():
    wrapper = AsyncAwsWrapperCursor.__new__(AsyncAwsWrapperCursor)
    target = MagicMock()
    wrapper._target_cursor = target

    assert wrapper.statusmessage is target.statusmessage
    # _close must delegate: SQLAlchemy AsyncAdapt_psycopg_cursor.close() calls
    # self._cursor._close() on the wrapped DBAPI cursor.
    assert wrapper._close is target._close
    with pytest.raises(AttributeError):
        _ = wrapper.__totally_made_up_dunder__
    fresh = AsyncAwsWrapperCursor.__new__(AsyncAwsWrapperCursor)
    with pytest.raises(AttributeError):
        _ = fresh._target_cursor


# ---- Parity fixes: TPC routing, async iteration, routed getters, --------
# ---- connect telemetry span, close-releases-resources -------------------


def test_tpc_methods_route_through_plugin_pipeline():
    # Sync parity (wrapper.py:240-258): all five PEP 249 TPC methods route
    # through the plugin manager with their CONNECTION_TPC_* DbApiMethods.
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()
    raw_conn.tpc_begin = AsyncMock()
    raw_conn.tpc_prepare = AsyncMock()
    raw_conn.tpc_commit = AsyncMock()
    raw_conn.tpc_rollback = AsyncMock()
    raw_conn.tpc_recover = AsyncMock(return_value=["xid-1"])

    async def _body() -> Any:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        await wrapper.tpc_begin("xid-1")
        await wrapper.tpc_prepare()
        await wrapper.tpc_commit("xid-1")
        await wrapper.tpc_rollback("xid-1")
        return await wrapper.tpc_recover()

    recovered = asyncio.run(_body())
    assert recovered == ["xid-1"]
    assert [e for e in log if e.startswith("execute:Connection.tpc_")] == [
        "execute:Connection.tpc_begin",
        "execute:Connection.tpc_prepare",
        "execute:Connection.tpc_commit",
        "execute:Connection.tpc_rollback",
        "execute:Connection.tpc_recover",
    ]
    raw_conn.tpc_begin.assert_awaited_once_with("xid-1")
    raw_conn.tpc_prepare.assert_awaited_once_with()
    raw_conn.tpc_commit.assert_awaited_once_with("xid-1")
    raw_conn.tpc_rollback.assert_awaited_once_with("xid-1")
    raw_conn.tpc_recover.assert_awaited_once_with()


def test_tpc_methods_probe_sync_driver_shape():
    # A driver whose tpc_* methods are sync (return a value, not a coroutine)
    # works through the same probe-and-await pattern used for close/scroll.
    raw_conn = _make_mock_async_conn()
    raw_conn.tpc_recover = MagicMock(return_value=["xid-sync"])
    wrapper = _connected_wrapper(raw_conn)

    async def _body() -> Any:
        return await wrapper.tpc_recover()

    assert asyncio.run(_body()) == ["xid-sync"]
    raw_conn.tpc_recover.assert_called_once_with()


def test_cursor_async_for_iterates_via_plugin_routed_fetchone():
    # Async-idiomatic port of sync cursor __iter__ (wrapper.py:432-433):
    # ``async for`` pulls rows through the plugin-routed fetchone() and stops
    # on None (StopAsyncIteration).
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()
    target_cur = _make_mock_async_cursor()
    target_cur.connection = raw_conn
    target_cur.fetchone = AsyncMock(side_effect=[("r1",), ("r2",), None])
    raw_conn.cursor = MagicMock(return_value=target_cur)

    async def _body() -> List[Any]:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        rows: List[Any] = []
        async for row in wrapper.cursor():
            rows.append(row)
        return rows

    rows = asyncio.run(_body())
    assert rows == [("r1",), ("r2",)]
    # Three fetchone dispatches (two rows + the terminating None), all routed.
    assert log.count("execute:Cursor.fetchone") == 3


def test_get_read_only_routes_through_plugin_pipeline():
    # Sync parity (wrapper.py:106-111): the read-only GETTER routes
    # CONNECTION_IS_READ_ONLY through the plugin chain. The sync property
    # `.read_only` stays a direct driver read (documented); the routed read
    # is the coroutine get_read_only().
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()
    raw_conn.read_only = True

    async def _body() -> bool:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        return await wrapper.get_read_only()

    assert asyncio.run(_body()) is True
    assert "execute:Connection.is_read_only" in log


def test_get_autocommit_routes_through_plugin_pipeline():
    # Sync parity (wrapper.py:131-136): the autocommit GETTER routes
    # CONNECTION_AUTOCOMMIT through the plugin chain.
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()
    raw_conn.autocommit = False

    async def _body() -> bool:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        return await wrapper.get_autocommit()

    assert asyncio.run(_body()) is False
    assert "execute:Connection.autocommit" in log


class _RecordingTelemetryContext:
    def __init__(self, name: Any, trace_level: Any) -> None:
        self.name = name
        self.trace_level = trace_level
        self.closed = False
        self.success: Any = None
        self.exception: Any = None

    def set_success(self, success: bool) -> None:
        self.success = success

    def set_exception(self, exception: Exception) -> None:
        self.exception = exception

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def close_context(self) -> None:
        self.closed = True


class _RecordingTelemetryFactory:
    def __init__(self) -> None:
        self.contexts: List[_RecordingTelemetryContext] = []

    def open_telemetry_context(
            self, name: Any, trace_level: Any) -> _RecordingTelemetryContext:
        ctx = _RecordingTelemetryContext(name, trace_level)
        self.contexts.append(ctx)
        return ctx

    def post_copy(self, context: Any, trace_level: Any) -> None:
        pass

    def create_counter(self, name: str) -> Any:
        return None

    def create_gauge(self, name: str, callback: Any) -> Any:
        return None

    def in_use(self) -> bool:
        return True


def test_connect_opens_and_closes_top_level_telemetry_context(monkeypatch):
    # Sync parity (wrapper.py:172-195): connect opens a TOP_LEVEL context
    # named after the wrapper module and closes it in finally. On success no
    # success flag is set (sync only sets it on failure).
    from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
        TelemetryTraceLevel
    created: List[_RecordingTelemetryFactory] = []

    def _factory(props: Any) -> _RecordingTelemetryFactory:
        f = _RecordingTelemetryFactory()
        created.append(f)
        return f

    monkeypatch.setattr(
        "aws_advanced_python_wrapper.aio.wrapper.DefaultTelemetryFactory",
        _factory)
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )

    asyncio.run(_body())
    assert created, "DefaultTelemetryFactory was never constructed"
    top = created[0].contexts[0]
    assert top.name == "aws_advanced_python_wrapper.aio.wrapper"
    assert top.trace_level == TelemetryTraceLevel.TOP_LEVEL
    assert top.closed is True
    assert top.success is None
    assert top.exception is None


def test_connect_telemetry_context_records_failure_and_closes(monkeypatch):
    # Failure path: the exception is recorded on the context, success is set
    # False, and the context is still closed (finally).
    created: List[_RecordingTelemetryFactory] = []

    def _factory(props: Any) -> _RecordingTelemetryFactory:
        f = _RecordingTelemetryFactory()
        created.append(f)
        return f

    monkeypatch.setattr(
        "aws_advanced_python_wrapper.aio.wrapper.DefaultTelemetryFactory",
        _factory)

    async def _boom_target(**kwargs: Any) -> Any:
        raise ValueError("boom")

    async def _body() -> None:
        await AsyncAwsWrapperConnection.connect(
            target=_boom_target,
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(_body())
    top = created[0].contexts[0]
    assert top.closed is True
    assert top.success is False
    assert isinstance(top.exception, ValueError)


def test_close_releases_plugin_service_resources():
    # Sync parity (wrapper.py:197-200): close() runs CONNECTION_CLOSE through
    # the pipeline, then releases plugin-service resources. plugins="" keeps
    # default-plugin background machinery (topology monitor teardown closes)
    # out of the close count.
    raw_conn = _make_mock_async_conn()
    release = AsyncMock()

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )
        wrapper._plugin_service.release_resources = release  # type: ignore[method-assign]
        await wrapper.close()

    asyncio.run(_body())
    release.assert_awaited_once()
    raw_conn.close.assert_awaited_once()


def test_aexit_releases_plugin_service_resources():
    # The async context-manager exit path goes through close() and therefore
    # also releases resources (sync parity: __exit__ at wrapper.py:335-338).
    raw_conn = _make_mock_async_conn()
    release = AsyncMock()

    async def _body() -> None:
        async with await AsyncAwsWrapperConnection.connect(
                target=_build_mock_psycopg_connect(raw_conn),
                conninfo="host=h user=u password=p dbname=d port=5432",
                plugins="",
        ) as conn:
            conn._plugin_service.release_resources = release  # type: ignore[method-assign]

    asyncio.run(_body())
    release.assert_awaited_once()
    raw_conn.close.assert_awaited_once()


def test_close_swallows_release_resources_errors():
    # release_resources is best-effort: a misbehaving release must not turn a
    # successful close() into a failure.
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="",
        )
        wrapper._plugin_service.release_resources = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("release blew up"))
        await wrapper.close()  # must not raise

    asyncio.run(_body())
    raw_conn.close.assert_awaited_once()
