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

"""PEP 249-style async DBAPI module bound to psycopg v3.

Async counterpart of :mod:`aws_advanced_python_wrapper.psycopg`. Enables
SQLAlchemy's ``create_async_engine`` creator-pattern today (SP-2) and the
URL-based dialect path in SP-9::

    from sqlalchemy.ext.asyncio import create_async_engine
    from aws_advanced_python_wrapper.aio.psycopg import connect

    engine = create_async_engine(
        "postgresql+aws_wrapper_psycopg://",
        async_creator=lambda: connect(
            "host=... user=... dbname=...",
            wrapper_dialect="aurora-pg",
        ),
    )

Module-level attributes are populated via :func:`_dbapi.install` mirroring
the sync submodule's pattern. PEP 562 ``__getattr__`` forwards missing
attrs to the real :mod:`psycopg` module so SA's PG dialect introspection
(``adapters``, ``__version__``, ``pq``, ...) keeps working.
"""

from __future__ import annotations

import sys
from typing import Any

import psycopg as _psycopg
from psycopg import AsyncConnection as _PGAsyncConnection

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection


async def connect(conninfo: str = "", **kwargs: Any) -> AsyncAwsWrapperConnection:
    """PEP 249 async ``connect``, target-driver-bound to psycopg v3.

    Equivalent to::

        await AsyncAwsWrapperConnection.connect(
            psycopg.AsyncConnection.connect, conninfo, **kwargs)
    """
    return await AsyncAwsWrapperConnection.connect(
        _PGAsyncConnection.connect, conninfo, **kwargs)


def __getattr__(name: str) -> Any:
    """Forward missing attributes to the underlying :mod:`psycopg` module.

    PEP 562 module-level ``__getattr__``. Only fires for names NOT defined
    here (including names populated by :func:`_dbapi.install`), so our
    PEP 249 exports (``Error``, ``Date``, ``STRING``, ...) and our own
    definitions take precedence. SA's ``PGDialectAsync_psycopg`` probes
    the DBAPI module for psycopg-specific state (``adapters``, ``pq``,
    ...); forwarding lets it see the real driver for those reads while
    keeping our wrapper's ``connect`` for the connection path.
    """
    try:
        return getattr(_psycopg, name)
    except AttributeError:
        raise AttributeError(
            f"module 'aws_advanced_python_wrapper.aio.psycopg' has no "
            f"attribute {name!r}"
        ) from None


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
