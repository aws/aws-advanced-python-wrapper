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

"""Async variant of :class:`DialectUtils`.

Runs dialect probe queries (is_reader, instance_id) against async
connections. Wraps each probe in ``asyncio.wait_for`` for a per-call
timeout, and performs a best-effort in_transaction check to avoid
polluting the caller's transaction state.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect


logger = Logger(__name__)


class AsyncDialectUtils:
    """Async counterpart of :class:`DialectUtils`."""

    @staticmethod
    async def get_host_role(
            conn: Any,
            driver_dialect: AsyncDriverDialect,
            is_reader_query: str,
            timeout_sec: float = 5.0) -> HostRole:
        """Query ``conn`` to determine whether it's bound to a reader
        or writer. Mirrors sync DialectUtils.get_host_role at
        database_dialect.py:1045-1059.

        Runs the dialect's ``is_reader_query`` via an async cursor with
        ``asyncio.wait_for`` for the timeout. Probe runs best-effort;
        an empty result or transaction failure raises AwsWrapperError.
        """
        was_in_txn = await AsyncDialectUtils._safe_in_transaction(
            conn, driver_dialect)
        try:
            result = await asyncio.wait_for(
                AsyncDialectUtils._execute_is_reader_query(conn, is_reader_query),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError as e:
            raise QueryTimeoutError(
                Messages.get("DialectUtils.GetHostRoleTimeout")) from e
        except Exception as e:
            raise AwsWrapperError(
                Messages.get("DialectUtils.ErrorGettingHostRole")) from e
        finally:
            await AsyncDialectUtils._restore_idle(conn, was_in_txn)

        if result is None:
            raise AwsWrapperError(
                Messages.get("DialectUtils.ErrorGettingHostRole"))
        is_reader = bool(result[0])
        return HostRole.READER if is_reader else HostRole.WRITER

    @staticmethod
    async def get_instance_id(
            conn: Any,
            driver_dialect: AsyncDriverDialect,
            host_id_query: str,
            timeout_sec: float = 5.0) -> Optional[str]:
        """Run the dialect's ``host_id_query`` to learn the Aurora
        instance identifier ``conn`` is bound to. Returns ``None`` on
        empty result or query failure (best-effort; callers fall back
        to topology resolution by host_id). Mirrors sync
        DialectUtils.get_instance_id at database_dialect.py.
        """
        was_in_txn = await AsyncDialectUtils._safe_in_transaction(
            conn, driver_dialect)
        try:
            result = await asyncio.wait_for(
                AsyncDialectUtils._execute_scalar_query(conn, host_id_query),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            logger.debug("AsyncDialectUtils.GetInstanceIdTimeout", timeout_sec)
            return None
        except Exception as e:  # noqa: BLE001 - identification is best-effort
            logger.debug("AsyncDialectUtils.GetInstanceIdError", e)
            return None
        finally:
            await AsyncDialectUtils._restore_idle(conn, was_in_txn)
        if not result:
            return None
        first = result[0]
        if first is None:
            return None
        return str(first)

    @staticmethod
    async def _safe_in_transaction(
            conn: Any, driver_dialect: AsyncDriverDialect) -> bool:
        """Best-effort 'is the connection mid-transaction?' probe. On error,
        return True so :meth:`_restore_idle` won't roll back a state we
        couldn't read."""
        try:
            return await driver_dialect.is_in_transaction(conn)
        except Exception:  # noqa: BLE001
            return True

    @staticmethod
    async def _restore_idle(conn: Any, was_in_txn: bool) -> None:
        """Roll back the transient transaction a probe query opened, so the
        connection returns to its pre-probe idle state.

        A SELECT probe (is_reader / host_id) on an autocommit=False connection
        opens a transaction (psycopg status INTRANS). Leaving it open then
        makes a subsequent ``set_read_only`` / ``set_autocommit`` on that
        connection raise ProgrammingError("can't change ... now: ... INTRANS")
        -- exactly what breaks the RWS reader-switch + failover session-state
        transfer (test_sqlalchemy_creator_read_write_splitting). Mirrors sync
        ``DialectUtils.get_host_role`` running under
        ``preserve_transaction_status_with_timeout``. No-op when the connection
        was already in a transaction (don't disturb the caller's txn).
        """
        if was_in_txn:
            return
        try:
            result = conn.rollback()
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - best-effort restore
            pass

    @staticmethod
    async def _execute_scalar_query(
            conn: Any, query: str) -> Optional[tuple]:
        """Execute a scalar-returning query, handling sync/coroutine
        cursor shapes identically to ``_execute_is_reader_query``.
        """
        cursor_ret = conn.cursor()
        if asyncio.iscoroutine(cursor_ret):
            cursor = await cursor_ret
        else:
            cursor = cursor_ret
        try:
            await cursor.execute(query)
            return await cursor.fetchone()
        finally:
            close = getattr(cursor, "close", None)
            if close is not None:
                try:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:  # noqa: BLE001 - cursor close best-effort
                    pass

    @staticmethod
    async def _execute_is_reader_query(
            conn: Any,
            is_reader_query: str) -> Optional[tuple]:
        """Open an async cursor, run the is_reader_query, return the row.

        Handles both psycopg's sync-returning ``conn.cursor()`` and
        aiomysql's coroutine-returning ``conn.cursor()``.
        """
        cursor_ret = conn.cursor()
        if asyncio.iscoroutine(cursor_ret):
            cursor = await cursor_ret
        else:
            cursor = cursor_ret
        try:
            await cursor.execute(is_reader_query)
            return await cursor.fetchone()
        finally:
            close = getattr(cursor, "close", None)
            if close is not None:
                try:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:  # noqa: BLE001 - cursor close best-effort
                    pass


__all__ = ["AsyncDialectUtils"]
