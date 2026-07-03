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

"""F3-B SP-9: SQLAlchemy async dialect registration.

Verifies the async dialect class, async resolution via the sync dialect's
``get_async_dialect_cls`` hook (single ``postgresql+aws_wrapper_psycopg`` URL
serving both sync and async), and the ``wrapper_plugins`` -> ``plugins`` URL
translation.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.dialects.postgresql.psycopg import PGDialectAsync_psycopg
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine

import aws_advanced_python_wrapper.aio.psycopg as aio_wrapper_psycopg
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.sqlalchemy_dialects.pg_async import (
    AwsWrapperAsyncPsycopgAdaptDBAPI, AwsWrapperPGPsycopgAsyncDialect)

# ---- Class-shape tests --------------------------------------------------


def test_async_dialect_subclasses_pgdialectasync_psycopg():
    assert issubclass(AwsWrapperPGPsycopgAsyncDialect, PGDialectAsync_psycopg)


def test_async_dialect_is_async_flag():
    assert AwsWrapperPGPsycopgAsyncDialect.is_async is True


def test_async_dialect_driver_attr():
    # Same driver name as the sync dialect: async is reached via the sync
    # dialect's get_async_dialect_cls, not a distinct URL (mirrors stock
    # psycopg, where both report driver="psycopg").
    assert AwsWrapperPGPsycopgAsyncDialect.driver == "aws_wrapper_psycopg"


def test_async_dialect_import_dbapi_returns_adapter_wrapping_aio_submodule():
    adapter = AwsWrapperPGPsycopgAsyncDialect.import_dbapi()
    assert isinstance(adapter, AwsWrapperAsyncPsycopgAdaptDBAPI)
    # The adapter exposes the wrapped aio submodule via ``.psycopg`` for
    # parity with SA's own PsycopgAdaptDBAPI attribute name.
    assert adapter.psycopg is aio_wrapper_psycopg
    # PEP 249 surface (Error, apilevel, paramstyle, ...) copied onto adapter.
    assert adapter.Error is aio_wrapper_psycopg.Error
    assert adapter.apilevel == "2.0"


def test_async_dialect_import_dbapi_sets_exec_status_on_cursor_class():
    """Regression guard for AttributeError at
    sqlalchemy/dialects/postgresql/psycopg.py:679 ("'NoneType' object
    has no attribute 'TUPLES_OK'").

    SA's native ``PGDialectAsync_psycopg.import_dbapi`` has a side
    effect: it sets ``AsyncAdapt_psycopg_cursor._psycopg_ExecStatus``
    to ``psycopg.pq.ExecStatus``. SA's async cursor.execute()
    dereferences ``self._psycopg_ExecStatus.TUPLES_OK`` on every call.
    Our dialect overrides ``import_dbapi`` wholesale, so without
    mirroring that side effect the class attribute stays at its
    default ``None`` and first cursor.execute() crashes.

    This test resets the attribute to None, calls our
    ``import_dbapi``, and confirms the assignment happens and matches
    the real ``psycopg.pq.ExecStatus`` (identity check).
    """
    from psycopg.pq import ExecStatus
    from sqlalchemy.dialects.postgresql.psycopg import \
        AsyncAdapt_psycopg_cursor

    # Blank slate: ensure the assignment must happen now, not rely
    # on a prior test's leftover.
    AsyncAdapt_psycopg_cursor._psycopg_ExecStatus = None
    try:
        AwsWrapperPGPsycopgAsyncDialect.import_dbapi()
        assert AsyncAdapt_psycopg_cursor._psycopg_ExecStatus is ExecStatus
        # Sanity: the thing SA's execute() actually reads is present.
        assert AsyncAdapt_psycopg_cursor._psycopg_ExecStatus.TUPLES_OK \
            is ExecStatus.TUPLES_OK
    finally:
        # Restore to the real value so downstream tests don't see None.
        AsyncAdapt_psycopg_cursor._psycopg_ExecStatus = ExecStatus


# ---- Async resolution via get_async_dialect_cls -------------------------
# psycopg3 is a single DBAPI doing both sync and async, so there is no
# separate async registry key. A single ``postgresql+aws_wrapper_psycopg``
# URL serves both: ``create_engine`` uses the sync dialect, while
# ``create_async_engine`` resolves the async dialect via the sync dialect's
# ``get_async_dialect_cls`` hook (``URL.get_dialect(_is_async=True)``).


def test_sync_dialect_get_async_dialect_cls_returns_async():
    from aws_advanced_python_wrapper.sqlalchemy_dialects.pg import \
        AwsWrapperPGPsycopgDialect
    url = make_url("postgresql+aws_wrapper_psycopg://u:p@h:5432/db")
    assert AwsWrapperPGPsycopgDialect.get_async_dialect_cls(url) \
        is AwsWrapperPGPsycopgAsyncDialect


def test_url_get_dialect_async_resolves_async_class():
    url = make_url(
        "postgresql+aws_wrapper_psycopg://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    # create_async_engine drives this path with _is_async=True.
    assert url.get_dialect(_is_async=True) is AwsWrapperPGPsycopgAsyncDialect


def test_url_get_dialect_sync_resolves_sync_class():
    from aws_advanced_python_wrapper.sqlalchemy_dialects.pg import \
        AwsWrapperPGPsycopgDialect
    url = make_url(
        "postgresql+aws_wrapper_psycopg://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    # create_engine drives this path with _is_async=False (the default).
    assert url.get_dialect() is AwsWrapperPGPsycopgDialect


# ---- URL kwargs passthrough --------------------------------------------


def test_async_url_query_args_flow_through_to_async_wrapper_connect(mocker):
    """URL query args (incl. ``wrapper_plugins`` alias) reach
    ``AsyncAwsWrapperConnection.connect`` unaltered except for the
    alias rename."""
    fake_raw_conn = MagicMock()
    fake_raw_conn.close = AsyncMock()

    mock_connect = mocker.patch.object(
        AsyncAwsWrapperConnection,
        "connect",
        new_callable=AsyncMock,
        return_value=fake_raw_conn,
    )

    async def _body() -> None:
        engine = create_async_engine(
            "postgresql+aws_wrapper_psycopg://u:p@h:5432/db"
            "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
        )
        try:
            async with engine.connect():
                pass
        except Exception:
            # The MagicMock conn may not satisfy every SA probe; we care only
            # that AsyncAwsWrapperConnection.connect was invoked with the right
            # kwargs.
            pass
        finally:
            await engine.dispose()

    asyncio.run(_body())

    assert mock_connect.called, "AsyncAwsWrapperConnection.connect was never invoked"
    _args, kwargs = mock_connect.call_args
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover,efm"
    assert "wrapper_plugins" not in kwargs, (
        "dialect should have renamed wrapper_plugins -> plugins before the connect call"
    )


def test_async_dialect_create_connect_args_renames_wrapper_plugins():
    """Unit-level check: create_connect_args renames the alias even when
    invoked directly (no engine involved)."""
    dialect = AwsWrapperPGPsycopgAsyncDialect()
    url = make_url(
        "postgresql+aws_wrapper_psycopg://u:p@h:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover"
    )
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover"
    assert "wrapper_plugins" not in kwargs


# ---- _type_info_fetch unwrap (async) -----------------------------------


def test_async_dialect_type_info_fetch_unwraps_target_connection(mocker):
    """Regression guard for

        TypeError: expected Connection or AsyncConnection,
                   got AsyncAwsWrapperConnection

    at ``psycopg/_typeinfo.py:90``. ``psycopg.TypeInfo.fetch`` strictly
    isinstance-checks its first argument. SA's native
    ``_type_info_fetch`` passes ``adapted.driver_connection`` which in
    our setup is the wrapper proxy, not the native psycopg
    AsyncConnection. Our dialect override unwraps via
    ``AsyncAwsWrapperConnection.target_connection`` before calling
    TypeInfo.fetch.
    """
    # Build the 3-layer shape SA's async path constructs:
    #   sa_connection (engine-level)
    #     .connection = AsyncAdapt_psycopg_connection (SA adapter)
    #       .driver_connection = AsyncAwsWrapperConnection (our wrapper)
    #         .target_connection = psycopg.AsyncConnection (native)
    native_conn = MagicMock(name="native_psycopg_AsyncConnection")
    wrapper = MagicMock(name="AsyncAwsWrapperConnection")
    wrapper.target_connection = native_conn

    adapted = MagicMock(name="AsyncAdapt_psycopg_connection")
    adapted.driver_connection = wrapper
    # await_ unwraps the awaitable passed to it synchronously in the
    # test -- matches SA's ``staticmethod(await_only)`` shape.
    adapted.await_ = lambda coro: "type-info-sentinel"

    sa_connection = MagicMock()
    sa_connection.connection = adapted

    fetch_mock = mocker.patch(
        "psycopg.types.TypeInfo.fetch", return_value="raw-fetch-result")

    dialect = AwsWrapperPGPsycopgAsyncDialect()
    result = dialect._type_info_fetch(sa_connection, "hstore")

    # await_ returned the sentinel (confirming the result flows through).
    assert result == "type-info-sentinel"
    # TypeInfo.fetch was called with the NATIVE, not our wrapper.
    fetch_mock.assert_called_once()
    called_arg = fetch_mock.call_args.args[0]
    assert called_arg is native_conn, (
        "TypeInfo.fetch must receive the native psycopg connection, "
        "not our AsyncAwsWrapperConnection proxy")
    # And the second arg is the type name.
    assert fetch_mock.call_args.args[1] == "hstore"


# ---- do_execute sync-contract (SA-creator ResourceClosedError) ----------


def test_async_failover_rewrap_do_execute_is_synchronous():
    """Regression guard for the ``sqlalchemy_creator_*`` ResourceClosedError.

    SQLAlchemy calls ``dialect.do_execute(...)`` SYNCHRONOUSLY inside a
    greenlet (the async work is bridged inside SA's ``AsyncAdapt_*_cursor
    .execute``, itself a sync method using ``await_only``). If our async
    mixin's ``do_execute`` were ``async def`` it would only build a coroutine
    SA never awaits -- the query would never run, the cursor would have no
    result, ``description`` would be ``None`` and SA raises ResourceClosedError
    from ``dialect.initialize``'s ``SELECT version()``. So these MUST be sync.
    """
    import inspect

    from aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling import \
        _AsyncFailoverSuccessRewrapMixin
    assert not inspect.iscoroutinefunction(
        _AsyncFailoverSuccessRewrapMixin.do_execute)
    assert not inspect.iscoroutinefunction(
        _AsyncFailoverSuccessRewrapMixin.do_executemany)


def test_async_failover_rewrap_runs_parent_and_rewraps_failover_success():
    import pytest

    from aws_advanced_python_wrapper.errors import FailoverSuccessError
    from aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling import \
        _AsyncFailoverSuccessRewrapMixin

    class _Target(Exception):
        pass

    calls = []

    class _Parent:
        def do_execute(self, cursor, statement, parameters, context=None):
            calls.append((statement, parameters))

    class _Dialect(_AsyncFailoverSuccessRewrapMixin, _Parent):
        _failover_success_target_cls = _Target

    # Synchronous call actually invokes the parent => the query runs.
    _Dialect().do_execute(MagicMock(), "select 1", None)
    assert calls == [("select 1", None)]

    class _ParentRaises:
        def do_execute(self, *a, **k):
            raise FailoverSuccessError("failover")

    class _DialectRaises(_AsyncFailoverSuccessRewrapMixin, _ParentRaises):
        _failover_success_target_cls = _Target

    # FailoverSuccessError from the driver is rewrapped to the target class.
    with pytest.raises(_Target):
        _DialectRaises().do_execute(MagicMock(), "select 1", None)


def test_async_dialect_type_info_fetch_falls_through_without_wrapper(mocker):
    """If ``driver_connection`` is already a native psycopg AsyncConnection
    (no wrapper in the middle), pass it through unchanged -- don't break
    SA configurations that bypass our wrapper."""
    class _NativeLike:
        pass
    native = _NativeLike()

    adapted = MagicMock()
    adapted.driver_connection = native
    adapted.await_ = lambda coro: "ok"

    sa_connection = MagicMock()
    sa_connection.connection = adapted

    fetch_mock = mocker.patch(
        "psycopg.types.TypeInfo.fetch", return_value="raw")

    AwsWrapperPGPsycopgAsyncDialect()._type_info_fetch(
        sa_connection, "hstore")

    called_arg = fetch_mock.call_args.args[0]
    assert called_arg is native
