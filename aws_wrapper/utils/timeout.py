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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from aws_wrapper.generic_target_driver_dialect import TargetDriverDialect
    from aws_wrapper.pep249 import Connection


def preserve_transaction_status_with_timeout(executor: Executor, timeout_sec, target_driver_dialect: TargetDriverDialect, conn: Connection):
    """
    Timeout decorator, timeout in seconds
    """

    def preserve_transaction_status_with_timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):

            initial_transaction_status: bool = target_driver_dialect.is_in_transaction(conn)

            future = executor.submit(func, *args, **kwargs)

            # raises TimeoutError on timeout
            result = future.result(timeout=timeout_sec)

            if not initial_transaction_status and target_driver_dialect.is_in_transaction(conn):
                # this condition is True when autocommit is False and the query started a new transaction.
                conn.commit()

            return result

        return func_wrapper

    return preserve_transaction_status_with_timeout_decorator
