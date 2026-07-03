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

"""PEP 249-style async DBAPI module bound to aiomysql.

Async counterpart of :mod:`aws_advanced_python_wrapper.mysql_connector`.
Enables SQLAlchemy's ``create_async_engine`` with the custom dialect
``mysql+aws_wrapper_aiomysql``.

Module-level attributes are populated via :func:`_dbapi.install`;
PEP 562 ``__getattr__`` forwards missing attrs to the real
:mod:`aiomysql` module so SA's MySQL-async dialect introspection keeps
working.
"""

from __future__ import annotations

import sys
from typing import Any

import aiomysql as _aiomysql

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection


async def connect(conninfo: str = "", **kwargs: Any) -> AsyncAwsWrapperConnection:
    """PEP 249 async ``connect``, target-driver-bound to aiomysql.

    Equivalent to::

        await AsyncAwsWrapperConnection.connect(
            aiomysql.connect, conninfo, **kwargs)
    """
    return await AsyncAwsWrapperConnection.connect(
        _aiomysql.connect, conninfo, **kwargs)


def __getattr__(name: str) -> Any:
    """Forward missing attributes to the underlying :mod:`aiomysql` module.

    PEP 562 module-level ``__getattr__``. Fires only for names NOT
    populated by :func:`_dbapi.install`, so PEP 249 exports take
    precedence. SA's MySQL-aiomysql dialect probes the DBAPI module for
    aiomysql-specific state (cursor class, error types, etc.);
    forwarding lets it see the real driver.
    """
    try:
        return getattr(_aiomysql, name)
    except AttributeError:
        raise AttributeError(
            f"module 'aws_advanced_python_wrapper.aio.aiomysql' has no "
            f"attribute {name!r}"
        ) from None


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
