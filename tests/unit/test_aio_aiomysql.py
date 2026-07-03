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

"""Task 2: aiomysql driver dialect + submodule + SA dialect."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiomysql

import aws_advanced_python_wrapper.aio.aiomysql as aio_aiomysql
from aws_advanced_python_wrapper.aio.driver_dialect.aiomysql import \
    AsyncAiomysqlDriverDialect
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Driver dialect ---------------------------------------------------


def test_driver_dialect_is_dialect_recognizes_aiomysql_connect():
    d = AsyncAiomysqlDriverDialect()
    assert d.is_dialect(aiomysql.connect) is True


def test_driver_dialect_is_dialect_rejects_other_callables():
    d = AsyncAiomysqlDriverDialect()
    assert d.is_dialect(lambda: None) is False


def test_driver_dialect_prepare_connect_info_renames_database_to_db():
    d = AsyncAiomysqlDriverDialect()
    props = Properties({
        "database": "mydb", "user": "u", "password": "p",
    })
    prepared = d.prepare_connect_info(HostInfo("h", 3306), props)
    assert prepared["host"] == "h"
    # Port must be an int: aiomysql/pymysql formats it with "%d" and a string
    # raises "%d format: a real number is required, not str". The pooled
    # provider's creator calls prepare_connect_info directly, so the cast must
    # live here (not only in connect()).
    assert prepared["port"] == 3306
    assert prepared["user"] == "u"
    assert prepared["password"] == "p"
    assert prepared["db"] == "mydb"
    assert "database" not in prepared


def test_driver_dialect_casts_string_port_from_props_when_host_has_no_port():
    # Regression for the MySQL env-1/env-2 async failures (every
    # test_*_connection_async): when the port arrives as a STRING in props and
    # host_info has NO explicit port, the connection-provider path calls
    # prepare_connect_info directly (bypassing connect()'s coercion). The port
    # must still be cast to int -- aiomysql formats it with "%d" and a string
    # raises "%d format: a real number is required, not str" from connection.py.
    d = AsyncAiomysqlDriverDialect()
    props = Properties({"port": "3306", "user": "u", "password": "p"})
    prepared = d.prepare_connect_info(HostInfo("h"), props)  # host_info: no port
    assert prepared["port"] == 3306
    assert isinstance(prepared["port"], int)


def test_driver_dialect_preserves_db_key_if_already_present():
    d = AsyncAiomysqlDriverDialect()
    props = Properties({"db": "explicit", "database": "ignored"})
    prepared = d.prepare_connect_info(HostInfo("h", 3306), props)
    # Explicit `db=` wins over `database=`; `database` is a known wrapper
    # property and gets stripped during prepare_connect_info.
    assert prepared["db"] == "explicit"
    assert "database" not in prepared


def test_driver_dialect_network_bound_methods_covers_core():
    d = AsyncAiomysqlDriverDialect()
    nb = d.network_bound_methods
    assert DbApiMethod.CURSOR_EXECUTE.method_name in nb
    assert DbApiMethod.CONNECT.method_name in nb
    assert DbApiMethod.CONNECTION_COMMIT.method_name in nb


def test_driver_dialect_capabilities():
    d = AsyncAiomysqlDriverDialect()
    assert d.supports_connect_timeout() is True
    assert d.supports_socket_timeout() is False
    assert d.supports_tcp_keepalive() is False
    assert d.supports_abort_connection() is False


def test_driver_dialect_connect_coerces_port_to_int():
    async def _body() -> None:
        d = AsyncAiomysqlDriverDialect()
        props = Properties({"host": "h", "port": "3306", "user": "u"})
        captured: dict = {}

        async def _fake_connect(**kwargs: object) -> object:
            captured.update(kwargs)
            return "conn-sentinel"

        result = await d.connect(HostInfo("h", 3306), props, _fake_connect)
        assert result == "conn-sentinel"
        assert isinstance(captured["port"], int)
        assert captured["port"] == 3306
    asyncio.run(_body())


def test_driver_dialect_lifecycle_ops_against_mock_conn():
    async def _body() -> None:
        d = AsyncAiomysqlDriverDialect()
        conn = MagicMock()
        # aiomysql's Connection exposes ``closed`` (no ``open``); is_closed /
        # can_execute_query read that.
        conn.closed = False
        conn.get_autocommit = MagicMock(return_value=True)
        # is_in_transaction now reads MySQL's SERVER_STATUS_IN_TRANS via
        # aiomysql's get_transaction_status() (not the autocommit heuristic).
        conn.get_transaction_status = MagicMock(return_value=False)
        conn.autocommit = AsyncMock()
        conn.ping = AsyncMock()

        assert await d.is_closed(conn) is False
        assert await d.is_in_transaction(conn) is False  # not in a transaction
        assert await d.can_execute_query(conn) is True
        assert await d.get_autocommit(conn) is True

        await d.set_autocommit(conn, False)
        conn.autocommit.assert_awaited_once_with(False)

        # Ping happy path.
        assert await d.ping(conn) is True
        conn.ping.assert_awaited_once_with(reconnect=False)

        # Abort uses sync close.
        await d.abort_connection(conn)
        conn.close.assert_called_once()
    asyncio.run(_body())


def test_driver_dialect_ping_returns_false_on_failure():
    async def _body() -> None:
        d = AsyncAiomysqlDriverDialect()
        conn = MagicMock()
        conn.open = True
        conn.ping = AsyncMock(side_effect=RuntimeError("net fail"))
        assert await d.ping(conn) is False
    asyncio.run(_body())


def test_driver_dialect_set_read_only_issues_set_transaction_sql():
    async def _body() -> None:
        d = AsyncAiomysqlDriverDialect()
        cur = MagicMock()
        cur.execute = AsyncMock()
        cur.__aenter__ = AsyncMock(return_value=cur)
        cur.__aexit__ = AsyncMock(return_value=None)
        conn = MagicMock()
        conn.cursor = MagicMock(return_value=cur)

        await d.set_read_only(conn, True)
        cur.execute.assert_awaited_once_with(
            "SET SESSION TRANSACTION READ ONLY"
        )
        assert conn._aws_read_only is True

        await d.set_read_only(conn, False)
        assert conn._aws_read_only is False

        # is_read_only reads the stashed flag.
        assert await d.is_read_only(conn) is False
        conn._aws_read_only = True
        assert await d.is_read_only(conn) is True
    asyncio.run(_body())


# ---- Submodule --------------------------------------------------------


def test_aio_aiomysql_submodule_has_pep249_surface():
    for name in ("Error", "OperationalError", "InterfaceError",
                 "DatabaseError", "Date", "Binary",
                 "STRING", "NUMBER", "apilevel", "paramstyle"):
        assert hasattr(aio_aiomysql, name), f"missing {name}"
    assert aio_aiomysql.apilevel == "2.0"


def test_aio_aiomysql_submodule_connect_is_coroutine():
    import inspect
    assert inspect.iscoroutinefunction(aio_aiomysql.connect)


def test_aio_aiomysql_submodule_getattr_falls_through_to_real_module():
    # aiomysql.Cursor should be accessible via the submodule.
    assert aio_aiomysql.Cursor is aiomysql.Cursor


# ---- SA dialect --------------------------------------------------------


def test_sa_dialect_subclasses_mysqldialect_aiomysql():
    from sqlalchemy.dialects.mysql.aiomysql import MySQLDialect_aiomysql

    from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql_async import \
        AwsWrapperMySQLAiomysqlAsyncDialect
    assert issubclass(
        AwsWrapperMySQLAiomysqlAsyncDialect, MySQLDialect_aiomysql
    )
    assert AwsWrapperMySQLAiomysqlAsyncDialect.is_async is True
    assert AwsWrapperMySQLAiomysqlAsyncDialect.driver == "aws_wrapper_aiomysql"


def test_sa_dialect_import_dbapi_returns_adapter():
    from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql_async import (
        AwsWrapperAsyncAiomysqlAdaptDBAPI, AwsWrapperMySQLAiomysqlAsyncDialect)
    adapter = AwsWrapperMySQLAiomysqlAsyncDialect.import_dbapi()
    assert isinstance(adapter, AwsWrapperAsyncAiomysqlAdaptDBAPI)
    assert adapter.aiomysql is aio_aiomysql
    assert adapter.Error is aio_aiomysql.Error


def test_sa_dialect_registry_resolves():
    from sqlalchemy.dialects import registry

    from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql_async import \
        AwsWrapperMySQLAiomysqlAsyncDialect

    # aiomysql is an async-only DBAPI, so the driver name conveys async on its
    # own -- no ``_async`` suffix, matching stock ``mysql+aiomysql``.
    cls = registry.load("mysql.aws_wrapper_aiomysql")
    assert cls is AwsWrapperMySQLAiomysqlAsyncDialect


def test_sa_url_resolves_to_our_dialect():
    from sqlalchemy.engine.url import make_url

    from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql_async import \
        AwsWrapperMySQLAiomysqlAsyncDialect
    url = make_url(
        "mysql+aws_wrapper_aiomysql://u:p@h:3306/db?wrapper_dialect=aurora-mysql"
    )
    assert url.get_dialect(_is_async=True) is AwsWrapperMySQLAiomysqlAsyncDialect


def test_sa_dialect_renames_wrapper_plugins_url_alias():
    from sqlalchemy.engine.url import make_url

    from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql_async import \
        AwsWrapperMySQLAiomysqlAsyncDialect
    d = AwsWrapperMySQLAiomysqlAsyncDialect()
    url = make_url(
        "mysql+aws_wrapper_aiomysql://u:p@h:3306/db"
        "?wrapper_dialect=aurora-mysql&wrapper_plugins=failover"
    )
    _args, kwargs = d.create_connect_args(url)
    assert kwargs.get("wrapper_dialect") == "aurora-mysql"
    assert kwargs.get("plugins") == "failover"
    assert "wrapper_plugins" not in kwargs
