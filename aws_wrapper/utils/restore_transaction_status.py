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

from aws_wrapper.generic_target_driver_dialect import TargetDriverDialect

from aws_wrapper.host_list_provider import HostListProviderService

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection


def restore_transaction_status(conn: Connection):
    """
    Restore transaction status decorator
    """

    def restore_transaction_status_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            target_driver_dialect = HostListProviderService.target_driver_dialect
            initial_transaction_status: bool = TargetDriverDialect.is_in_transaction(conn)

            func(*args, **kwargs)

            if not initial_transaction_status and target_driver_dialect.is_in_transaction(conn):
                # this condition is True when autocommit is False and the query started a new transaction.
                conn.commit()

        return func_wrapper

    return restore_transaction_status_decorator
