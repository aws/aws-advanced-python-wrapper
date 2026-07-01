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

"""PEP 249 DBAPI module bound to mysql-connector-python.

Enables SQLAlchemy's creator-pattern:

    from sqlalchemy import create_engine
    from aws_advanced_python_wrapper.mysql_connector import connect

    engine = create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: connect(
            "host=... user=... database=...",
            wrapper_dialect="aurora-mysql",
        ),
    )
"""

from __future__ import annotations

import sys
from typing import Any

import mysql.connector as _mysql_connector
from mysql.connector import connect as _mysql_connect

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def __getattr__(name: str) -> Any:
    """Forward missing attributes to the underlying mysql.connector module.

    See ``aws_advanced_python_wrapper/psycopg.py`` for the rationale.
    """
    try:
        return getattr(_mysql_connector, name)
    except AttributeError:
        raise AttributeError(
            f"module 'aws_advanced_python_wrapper.mysql_connector' has no attribute {name!r}"
        ) from None


def connect(conninfo: str = "", **kwargs: Any) -> AwsWrapperConnection:
    """PEP 249 `connect`, target-driver-bound to mysql-connector-python.

    Equivalent to::

        AwsWrapperConnection.connect(mysql.connector.connect, conninfo, **kwargs)
    """
    return AwsWrapperConnection.connect(_mysql_connect, conninfo, **kwargs)


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
