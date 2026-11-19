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

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Optional
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.utils.decorators import (
    is_connection_abandoned, mark_connection_abandoned,
    preserve_transaction_status_with_timeout, timeout)


class _Conn:
    """Minimal weak-referenceable, hashable stand-in for a DBAPI connection."""
    pass


def _dialect(abort_releases: Optional[threading.Event] = None):
    dialect = MagicMock()
    dialect.is_in_transaction.return_value = False
    dialect.unwrap_connection.side_effect = lambda c: c
    if abort_releases is not None:
        # abort_connection unblocks the worker (models socket shutdown on PG /
        # mysql-pure, where the recv returns promptly).
        dialect.abort_connection.side_effect = lambda c: abort_releases.set()
    else:
        # abort_connection cannot interrupt the worker (models the mysql C-ext,
        # whose abort_connection is a no-op).
        dialect.abort_connection.side_effect = lambda c: None
    return dialect


def test_mark_and_check_abandoned():
    conn = _Conn()
    future: Future = Future()
    assert is_connection_abandoned(conn) is False
    mark_connection_abandoned(conn, future)
    assert is_connection_abandoned(conn) is True
    assert is_connection_abandoned(None) is False

    future.set_result(None)
    assert is_connection_abandoned(conn) is False


def test_preserve_transaction_drainable_is_not_abandoned():
    # When abort_connection unblocks the worker, the drain completes and the
    # connection is left closeable (not marked abandoned).
    executor = ThreadPoolExecutor(max_workers=1)
    conn = _Conn()
    released = threading.Event()

    def slow():
        released.wait(timeout=5)
        return "done"

    wrapped = preserve_transaction_status_with_timeout(
        executor, 0.1, _dialect(abort_releases=released), conn)(slow)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
        assert is_connection_abandoned(conn) is False
    finally:
        released.set()
        executor.shutdown(wait=True)


def test_preserve_transaction_undrainable_is_abandoned():
    # When abort_connection cannot interrupt the worker, the drain times out and
    # the connection is marked abandoned so close paths leave it for the worker.
    executor = ThreadPoolExecutor(max_workers=1)
    conn = _Conn()
    release = threading.Event()

    def stuck():
        release.wait(timeout=5)
        return "done"

    wrapped = preserve_transaction_status_with_timeout(
        executor, 0.1, _dialect(abort_releases=None), conn)(stuck)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
        assert is_connection_abandoned(conn) is True
    finally:
        release.set()
        executor.shutdown(wait=True)


def test_timeout_undrainable_is_abandoned():
    executor = ThreadPoolExecutor(max_workers=1)
    conn = _Conn()
    release = threading.Event()

    def stuck():
        release.wait(timeout=5)
        return "done"

    wrapped = timeout(executor, 0.1, _dialect(abort_releases=None), conn)(stuck)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
        assert is_connection_abandoned(conn) is True
    finally:
        release.set()
        executor.shutdown(wait=True)


def test_timeout_without_conn_does_not_mark():
    # The bare timeout form (no conn/dialect) cannot abort or mark anything.
    executor = ThreadPoolExecutor(max_workers=1)
    release = threading.Event()

    def stuck():
        release.wait(timeout=5)
        return "done"

    wrapped = timeout(executor, 0.1)(stuck)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
    finally:
        release.set()
        executor.shutdown(wait=True)


def test_timeout_undrainable_closes_connection_when_worker_finishes(uninterruptible_call):
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="TestTimeoutExecutor")
    conn = uninterruptible_call.conn

    wrapped = timeout(executor, 0.1, _dialect(abort_releases=None), conn)(uninterruptible_call.run)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
        assert uninterruptible_call.close_threads == []

        uninterruptible_call.finish()
        assert uninterruptible_call.closed.wait(5) is True
    finally:
        uninterruptible_call.finish()
        executor.shutdown(wait=True)

    assert uninterruptible_call.calls_while_running == []
    assert uninterruptible_call.close_threads == [uninterruptible_call.worker_thread]
    assert is_connection_abandoned(conn) is False


def test_timeout_drainable_does_not_close_connection(mocker):
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="TestTimeoutExecutor")
    conn = mocker.MagicMock()
    released = threading.Event()

    wrapped = timeout(executor, 0.1, _dialect(abort_releases=released), conn)(lambda: released.wait(timeout=5))
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
    finally:
        released.set()
        executor.shutdown(wait=True)

    conn.close.assert_not_called()
    assert is_connection_abandoned(conn) is False


def test_preserve_transaction_abandoned_raises_without_running(mocker, uninterruptible_call):
    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="TestTimeoutExecutor")
    conn = uninterruptible_call.conn
    dialect = _dialect(abort_releases=None)
    query = mocker.MagicMock()

    try:
        with pytest.raises(FuturesTimeoutError):
            timeout(executor, 0.1, dialect, conn)(uninterruptible_call.run)()
        dialect.is_in_transaction.reset_mock()

        with pytest.raises(FuturesTimeoutError):
            preserve_transaction_status_with_timeout(executor, 0.1, dialect, conn)(query)(conn)

        query.assert_not_called()
        dialect.is_in_transaction.assert_not_called()
    finally:
        uninterruptible_call.finish()
        executor.shutdown(wait=True)
