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

"""Async DialectUtils tests."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.dialect_utils import AsyncDialectUtils
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.hostinfo import HostRole


def _mk_conn(reader_row):
    """Build an async-cursor-ish mock. reader_row is what fetchone returns."""
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=reader_row)
    cursor.close = MagicMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    return conn, cursor


def test_get_host_role_returns_reader_when_query_returns_true():
    conn, cursor = _mk_conn(reader_row=(True,))
    dd = MagicMock()
    role = asyncio.run(AsyncDialectUtils.get_host_role(
        conn, dd, "SELECT pg_is_in_recovery()"))
    assert role == HostRole.READER
    cursor.execute.assert_awaited_once_with("SELECT pg_is_in_recovery()")


def test_get_host_role_returns_writer_when_query_returns_false():
    conn, cursor = _mk_conn(reader_row=(False,))
    role = asyncio.run(AsyncDialectUtils.get_host_role(
        conn, MagicMock(), "SELECT pg_is_in_recovery()"))
    assert role == HostRole.WRITER


def test_get_host_role_raises_on_timeout():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.close = MagicMock()

    async def _slow_execute(*args, **kwargs):
        await asyncio.sleep(1.0)

    cursor.execute = _slow_execute
    conn.cursor = MagicMock(return_value=cursor)

    with pytest.raises(QueryTimeoutError):
        asyncio.run(AsyncDialectUtils.get_host_role(
            conn, MagicMock(), "SELECT 1", timeout_sec=0.05))


def test_get_host_role_raises_on_empty_result():
    conn, cursor = _mk_conn(reader_row=None)
    with pytest.raises(AwsWrapperError):
        asyncio.run(AsyncDialectUtils.get_host_role(
            conn, MagicMock(), "SELECT 1"))


def test_get_host_role_raises_on_exec_error():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=RuntimeError("boom"))
    cursor.close = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)

    with pytest.raises(AwsWrapperError):
        asyncio.run(AsyncDialectUtils.get_host_role(
            conn, MagicMock(), "SELECT 1"))


def test_get_host_role_handles_async_cursor_creation():
    """aiomysql returns a coroutine from conn.cursor()."""
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(True,))
    cursor.close = MagicMock()

    async def _async_cursor():
        return cursor

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=_async_cursor())

    role = asyncio.run(AsyncDialectUtils.get_host_role(
        conn, MagicMock(), "SELECT 1"))
    assert role == HostRole.READER


# ---- transaction-status preservation (read_only-INTRANS regression) ------


def test_get_host_role_rolls_back_transient_probe_txn():
    """A probe on a NOT-in-transaction connection must roll back afterward so
    the connection isn't left INTRANS -- an autocommit=False conn would
    otherwise have an open txn from the SELECT, blocking a later set_read_only
    (test_sqlalchemy_creator_read_write_splitting). Mirrors sync's
    preserve_transaction_status."""
    conn, cursor = _mk_conn(reader_row=(False,))
    conn.rollback = AsyncMock()
    dd = MagicMock()
    dd.is_in_transaction = AsyncMock(return_value=False)
    role = asyncio.run(AsyncDialectUtils.get_host_role(conn, dd, "q"))
    assert role == HostRole.WRITER
    conn.rollback.assert_awaited_once()


def test_get_host_role_preserves_existing_transaction():
    """If the connection is already mid-transaction, the probe must NOT roll
    back -- don't disturb the caller's open transaction."""
    conn, cursor = _mk_conn(reader_row=(True,))
    conn.rollback = AsyncMock()
    dd = MagicMock()
    dd.is_in_transaction = AsyncMock(return_value=True)
    role = asyncio.run(AsyncDialectUtils.get_host_role(conn, dd, "q"))
    assert role == HostRole.READER
    conn.rollback.assert_not_awaited()


def test_get_instance_id_rolls_back_transient_probe_txn():
    conn, cursor = _mk_conn(reader_row=("inst-1",))
    conn.rollback = AsyncMock()
    dd = MagicMock()
    dd.is_in_transaction = AsyncMock(return_value=False)
    iid = asyncio.run(AsyncDialectUtils.get_instance_id(conn, dd, "q"))
    assert iid == "inst-1"
    conn.rollback.assert_awaited_once()
