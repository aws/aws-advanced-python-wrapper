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

"""``aws_advanced_python_wrapper.aio.psycopg`` submodule contract tests.

Mirrors the sync ``test_psycopg_submodule.py``: the async submodule must
expose the full PEP 249 surface via ``_dbapi.install`` and route
``connect`` through ``AsyncAwsWrapperConnection`` bound to psycopg's
async driver connect.
"""

from __future__ import annotations

import asyncio
import inspect

import psycopg

from aws_advanced_python_wrapper.aio import psycopg as aio_psycopg
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection


def test_submodule_connect_routes_through_async_wrapper_connection(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AsyncAwsWrapperConnection, "connect", return_value="sentinel_connection"
    )
    result = asyncio.run(aio_psycopg.connect(
        "host=h user=u dbname=d", wrapper_dialect="aurora-pg"))
    assert result == "sentinel_connection"
    args, kwargs = mock_wrapper_connect.call_args
    assert args[0].__func__ is psycopg.AsyncConnection.connect.__func__
    assert args[1] == "host=h user=u dbname=d"
    assert kwargs == {"wrapper_dialect": "aurora-pg"}


def test_submodule_connect_is_a_coroutine_function():
    assert inspect.iscoroutinefunction(aio_psycopg.connect)


def test_submodule_exposes_pep249_surface():
    # The _dbapi.install surface sync exposes must exist here too.
    for name in ("Error", "DatabaseError", "OperationalError", "ProgrammingError",
                 "InterfaceError", "Warning", "apilevel", "threadsafety",
                 "paramstyle", "Date", "Time", "Timestamp", "Binary",
                 "STRING", "NUMBER", "DATETIME", "ROWID"):
        assert hasattr(aio_psycopg, name), f"missing PEP 249 export: {name}"


def test_submodule_getattr_falls_through_to_driver():
    # PEP 562 fallthrough: names not defined on the submodule resolve on the
    # real psycopg module (SQLAlchemy's async dialect probes these).
    assert aio_psycopg.AsyncConnection is psycopg.AsyncConnection
