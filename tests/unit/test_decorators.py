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
import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Optional
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.errors import QueryTimeoutError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils import decorators as decorators_mod
from aws_advanced_python_wrapper.utils.decorators import (
    is_connection_abandoned, mark_connection_abandoned,
    preserve_transaction_status_with_timeout, timeout)
from aws_advanced_python_wrapper.utils.properties import Properties


class _Conn:
    """Minimal weak-referenceable, hashable stand-in for a DBAPI connection."""
    pass


def _dialect(abort_releases: Optional[threading.Event] = None):
    dialect = MagicMock()
    dialect.is_in_transaction.return_value = False
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
    assert is_connection_abandoned(conn) is False
    mark_connection_abandoned(conn)
    assert is_connection_abandoned(conn) is True
    assert is_connection_abandoned(None) is False


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


def _saturate_pool(executor: ThreadPoolExecutor, hold_sec: float) -> None:
    """Occupy every worker so a later submit sits in the queue."""
    workers = executor._max_workers
    started = threading.Barrier(workers + 1)

    def occupy():
        started.wait(timeout=2)
        time.sleep(hold_sec)

    for _ in range(workers):
        executor.submit(occupy)
    started.wait(timeout=2)


def test_timeout_queued_fast_call_does_not_use_submit_clock():
    # Issue #1278: future.result(timeout=...) starts at submit, so queue time
    # counts as query time. A fast call sitting behind a saturated pool must
    # still succeed once a worker runs it.
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        _saturate_pool(executor, hold_sec=0.5)

        def fast():
            return "ok"

        assert timeout(executor, 0.2)(fast)() == "ok"
    finally:
        executor.shutdown(wait=True)


def test_preserve_transaction_queued_fast_call_does_not_use_submit_clock():
    executor = ThreadPoolExecutor(max_workers=1)
    conn = _Conn()
    try:
        _saturate_pool(executor, hold_sec=0.5)

        def fast():
            return "ok"

        wrapped = preserve_transaction_status_with_timeout(
            executor, 0.2, _dialect(), conn)(fast)
        assert wrapped() == "ok"
    finally:
        executor.shutdown(wait=True)


def test_timeout_slow_call_still_times_out_after_start():
    executor = ThreadPoolExecutor(max_workers=1)

    def slow():
        time.sleep(0.6)
        return "done"

    wrapped = timeout(executor, 0.2)(slow)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
    finally:
        executor.shutdown(wait=True)


def test_execute_queued_fast_call_does_not_raise_query_timeout():
    # DriverDialect.execute maps the decorator TimeoutError to QueryTimeoutError.
    # Saturating DriverDialectExecutor makes a fast execute time out on main.
    dialect = DriverDialect(Properties({"socket_timeout": "0.2"}))
    occupy_pool = ThreadPoolExecutor(max_workers=1)
    dialect._thread_pool = occupy_pool
    try:
        _saturate_pool(occupy_pool, hold_sec=0.5)
        assert dialect.execute(
            DbApiMethod.CURSOR_EXECUTE.method_name, lambda: "ok") == "ok"
    finally:
        occupy_pool.shutdown(wait=True)


def test_queue_bound_expiry_cancels_queued_work_and_skips_conn(monkeypatch):
    # Issue #1278 follow-up: when the queue-wait bound expires while the call
    # is still queued, the work must never run and the connection must not be
    # aborted/touched after the caller has already timed out.
    monkeypatch.setattr(decorators_mod, "_queue_wait_bound_sec", lambda timeout_sec: 0.15)
    executor = ThreadPoolExecutor(max_workers=1)
    conn = _Conn()
    occupy_hold = threading.Event()
    occupy_started = threading.Event()
    ran = threading.Event()
    dialect = _dialect()

    def occupy():
        occupy_started.set()
        occupy_hold.wait(timeout=5)

    def queued():
        ran.set()
        return "ran"

    executor.submit(occupy)
    assert occupy_started.wait(timeout=2)
    wrapped = timeout(executor, 0.2, dialect, conn)(queued)
    try:
        with pytest.raises(FuturesTimeoutError):
            wrapped()
        assert ran.is_set() is False
        dialect.abort_connection.assert_not_called()
        occupy_hold.set()
        time.sleep(0.2)
        assert ran.is_set() is False
    finally:
        occupy_hold.set()
        executor.shutdown(wait=True)


def test_queue_bound_expiry_race_starts_then_drains(monkeypatch):
    # Bound expires in the same window the worker starts: cancel() is False,
    # so abort/drain must run (the call is in-flight on conn).
    monkeypatch.setattr(decorators_mod, "_queue_wait_bound_sec", lambda timeout_sec: 0.15)

    class _CancelStartsExecutor:
        def submit(self, fn, *args, **kwargs):
            future = Future()

            def run():
                try:
                    result = fn(*args, **kwargs)
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)

            def cancel_starts_then_fails() -> bool:
                if future.set_running_or_notify_cancel():
                    threading.Thread(target=run, daemon=True).start()
                    # Let invoke() pass try_start before we report cancel() == False.
                    time.sleep(0.1)
                return False

            future.cancel = cancel_starts_then_fails  # type: ignore[method-assign]
            return future

    hold = threading.Event()
    ran = threading.Event()
    conn = _Conn()
    dialect = _dialect(abort_releases=hold)

    def work():
        ran.set()
        hold.wait(timeout=5)
        return "done"

    try:
        with pytest.raises(FuturesTimeoutError):
            timeout(_CancelStartsExecutor(), 0.2, dialect, conn)(work)()
        assert ran.wait(timeout=2)
        dialect.abort_connection.assert_called()
    finally:
        hold.set()


def test_execute_slow_call_still_raises_query_timeout():
    dialect = DriverDialect(Properties({"socket_timeout": "0.2"}))
    pool = ThreadPoolExecutor(max_workers=1)
    dialect._thread_pool = pool

    def slow():
        time.sleep(0.6)
        return "done"

    try:
        with pytest.raises(QueryTimeoutError):
            dialect.execute(DbApiMethod.CURSOR_EXECUTE.method_name, slow)
    finally:
        pool.shutdown(wait=True)
