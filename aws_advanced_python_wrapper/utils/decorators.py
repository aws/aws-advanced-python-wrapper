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

import functools
import threading
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple
from weakref import WeakSet

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection


# Connections whose offloaded operation could not be drained after
# ``abort_connection`` -- e.g. the mysql C-extension, whose abort_connection is a
# no-op and cannot interrupt an in-flight query. Closing such a connection while
# the worker thread is still using it is a cross-thread use-after-free in the
# driver (libpq / libmysqlclient) that crashes the whole process with SIGSEGV.
# These connections are marked here so that connection-close paths skip them: the
# still-running worker holds the last reference, so the connection is finalized
# safely on the worker's own thread once it finishes. A connection leaked until GC
# is strictly better than a process crash (close-during-query) or a hang (waiting
# forever for a worker that cannot be interrupted). The WeakSet self-empties as the
# abandoned connections are garbage-collected.
_abandoned_connections: WeakSet = WeakSet()
_abandoned_connections_lock = threading.Lock()


def mark_connection_abandoned(conn: Connection) -> None:
    """Record that ``conn`` has an in-flight worker that could not be interrupted."""
    if conn is None:
        return
    try:
        with _abandoned_connections_lock:
            _abandoned_connections.add(conn)
    except TypeError:
        # Not weak-referenceable/hashable; cannot track -- caller falls back to
        # its existing close behavior.
        pass


def is_connection_abandoned(conn: Optional[Connection]) -> bool:
    """Return True if ``conn`` was marked abandoned and must not be closed yet."""
    if conn is None:
        return False
    try:
        with _abandoned_connections_lock:
            return conn in _abandoned_connections
    except TypeError:
        return False


def _queue_wait_bound_sec(timeout_sec: float) -> float:
    # Queue delay is not charged against the operation timeout, but a
    # saturated pool must not stall the caller for a minute. Bound queue wait
    # to the larger of 2x the timeout and timeout+5s: a 5s socket_timeout
    # waits at most 10s for a worker (not 60s), while a 0.2s timeout still
    # has 5.2s to get scheduled.
    timeout_sec = float(timeout_sec)
    return max(timeout_sec * 2.0, timeout_sec + 5.0)


class _WorkGate:
    """Handshake so a worker cannot start ``func`` after the caller gives up."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._abandoned = False
        self.started = threading.Event()

    def try_start(self) -> bool:
        with self._lock:
            if self._abandoned:
                return False
            self.started.set()
            return True

    def mark_abandoned(self) -> None:
        with self._lock:
            self._abandoned = True

    def abandon_if_not_started(self) -> bool:
        """Return True if the worker already started; otherwise prevent start."""
        with self._lock:
            if self.started.is_set():
                return True
            self._abandoned = True
            return False


def _submit_timed_call(
        executor: Executor, func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Tuple[Future, _WorkGate]:
    """Submit ``func``; the timeout clock starts only after ``try_start``."""
    gate = _WorkGate()

    def invoke() -> Any:
        if not gate.try_start():
            return None
        return func(*args, **kwargs)

    return executor.submit(invoke), gate


def _give_up_if_still_queued(future: Future, gate: _WorkGate) -> None:
    """Cancel queued work, or leave an in-flight call for abort/drain.

    ``future.cancel()`` is True only while the task is still queued. If it
    returns False the worker has started (or just finished); the caller
    treats that as started and must abort/drain. The gate stops ``func``
    from running if the worker is still in the handshake.
    """
    if future.cancel():
        gate.mark_abandoned()
        return
    gate.abandon_if_not_started()


def _drain_timed_out_worker(
        future: Future,
        timeout_sec: float,
        driver_dialect: Optional[DriverDialect],
        conn: Optional[Connection]) -> None:
    if conn is None or driver_dialect is None:
        return
    driver_dialect.abort_connection(conn)
    try:
        future.result(timeout=timeout_sec)
    except FuturesTimeoutError:
        # The worker still has not finished: abort_connection could not
        # interrupt it (e.g. the mysql C-extension, where it is a no-op).
        # Closing this connection now would free it under the running
        # query and crash the process, so mark it abandoned -- close
        # paths skip it and the worker frees it when it finishes.
        mark_connection_abandoned(conn)
    except Exception:  # noqa: BLE001 - worker raised its own error; it has drained
        pass


def preserve_transaction_status_with_timeout(executor: Executor, timeout_sec, driver_dialect: DriverDialect, conn: Connection):
    """
    Timeout decorator, timeout in seconds
    """

    def preserve_transaction_status_with_timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):

            initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)

            future, gate = _submit_timed_call(executor, func, *args, **kwargs)
            try:
                if not gate.started.wait(timeout=_queue_wait_bound_sec(timeout_sec)):
                    _give_up_if_still_queued(future, gate)
                    raise FuturesTimeoutError()
                result = future.result(timeout=timeout_sec)
            except FuturesTimeoutError:
                # The query is still running on the worker thread. Interrupt its
                # socket so it unwinds, then WAIT for the worker to finish before
                # propagating -- otherwise the caller may close/reuse this
                # connection while the worker is still reading it, a cross-thread
                # use-after-free in the driver (env-4 SIGSEGV). Skip abort when
                # the call never left the executor queue (cancelled above).
                if gate.started.is_set():
                    _drain_timed_out_worker(future, timeout_sec, driver_dialect, conn)
                raise

            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                # this condition is True when autocommit is False and the query started a new transaction.
                conn.commit()

            return result

        return func_wrapper

    return preserve_transaction_status_with_timeout_decorator


def timeout(executor: Executor, timeout_sec, driver_dialect: Optional[DriverDialect] = None,
            conn: Optional[Connection] = None):
    """
    Timeout decorator, timeout in seconds.

    ``conn`` is the connection the offloaded operation runs on, and
    ``driver_dialect`` is the dialect that knows how to interrupt it. When both
    are given and the operation times out, the connection's socket is shut down
    (via ``driver_dialect.abort_connection``) and the worker thread is awaited
    before the timeout propagates -- otherwise the operation keeps running on the
    worker thread and a subsequent close/reuse of ``conn`` (e.g. a failover handler
    closing the old connection while an app/EFM query is still in flight on it)
    races the abandoned operation: a cross-thread use-after-free in
    libpq/libmysqlclient that crashes the process (env-4 SIGSEGV).
    """

    def timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):

            future, gate = _submit_timed_call(executor, func, *args, **kwargs)
            try:
                if not gate.started.wait(timeout=_queue_wait_bound_sec(timeout_sec)):
                    _give_up_if_still_queued(future, gate)
                    raise FuturesTimeoutError()
                # raises TimeoutError on timeout, measured from worker start
                return future.result(timeout=timeout_sec)
            except FuturesTimeoutError:
                if gate.started.is_set():
                    _drain_timed_out_worker(future, timeout_sec, driver_dialect, conn)
                raise

        return func_wrapper

    return timeout_decorator
