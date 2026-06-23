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
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection


def preserve_transaction_status_with_timeout(executor: Executor, timeout_sec, driver_dialect: DriverDialect, conn: Connection):
    """
    Timeout decorator, timeout in seconds
    """

    def preserve_transaction_status_with_timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):

            initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)

            future = executor.submit(func, *args, **kwargs)

            try:
                result = future.result(timeout=timeout_sec)
            except FuturesTimeoutError:
                # The query is still running on the worker thread. Interrupt its
                # socket so it unwinds, then WAIT for the worker to finish before
                # propagating -- otherwise the caller may close/reuse this
                # connection while the worker is still reading it, a cross-thread
                # use-after-free in the driver (env-4 SIGSEGV).
                driver_dialect.abort_connection(conn)
                try:
                    future.result(timeout=timeout_sec)
                except Exception:  # noqa: BLE001 - already timing out; just drain it
                    pass
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

            future = executor.submit(func, *args, **kwargs)

            try:
                # raises TimeoutError on timeout
                return future.result(timeout=timeout_sec)
            except FuturesTimeoutError:
                if conn is not None and driver_dialect is not None:
                    driver_dialect.abort_connection(conn)
                    try:
                        future.result(timeout=timeout_sec)
                    except Exception:  # noqa: BLE001 - already timing out; just drain it
                        pass
                raise

        return func_wrapper

    return timeout_decorator
