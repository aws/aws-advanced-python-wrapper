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

"""Deprecated alias for the pre-3.1.0 MySQL SQLAlchemy dialect module path.

``SqlAlchemyOrmMysqlDialect`` was renamed to
:class:`~aws_advanced_python_wrapper.sqlalchemy_dialects.mysql.AwsWrapperMySQLConnectorDialect`
and moved to :mod:`aws_advanced_python_wrapper.sqlalchemy_dialects` in 3.1.0.

URL-based configuration was never affected: the registered driver name
``aws_wrapper_mysqlconnector`` is unchanged, so
``mysql+aws_wrapper_mysqlconnector://`` resolves in both releases. This shim
exists for the narrower case of code that imported the dialect class directly —
to subclass it, or to register it by hand — which would otherwise break with
``ModuleNotFoundError`` on upgrade.

One behavioural difference is worth knowing, though it is not a loss of
capability. 3.0.0's ``create_connect_args`` hard-coded
``plugins = "aurora_connection_tracker,failover_v2"`` whenever ``wrapper_plugins``
was absent from the URL. The replacement class does not set ``plugins`` at all,
so the wrapper's own default chain applies instead. For mysql-connector that
default is ``initial_connection,aurora_connection_tracker,failover_v2`` — a
superset of what 3.0.0 injected — so failover remains enabled by default. An
application that relied on the old value *exactly* (for instance to keep
``initial_connection`` out of the chain) should now pass ``wrapper_plugins`` in
the URL, or ``plugins`` via ``connect_args``, explicitly.
"""

import warnings

from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql import \
    AwsWrapperMySQLConnectorDialect

warnings.warn(
    "aws_advanced_python_wrapper.sqlalchemy.mysql_orm_dialect."
    "SqlAlchemyOrmMysqlDialect is deprecated and will be removed in the next "
    "major version. Use aws_advanced_python_wrapper.sqlalchemy_dialects.mysql."
    "AwsWrapperMySQLConnectorDialect instead. URL-based configuration needs no "
    "change: mysql+aws_wrapper_mysqlconnector:// is unchanged. Note that the "
    "replacement no longer injects plugins=aurora_connection_tracker,"
    "failover_v2; the wrapper's default chain applies instead, which for "
    "mysql-connector is a superset of it.",
    DeprecationWarning,
    stacklevel=2,
)

#: Deprecated alias. Kept so 3.0.0 imports resolve; prefer
#: ``AwsWrapperMySQLConnectorDialect``.
SqlAlchemyOrmMysqlDialect = AwsWrapperMySQLConnectorDialect

__all__ = ["SqlAlchemyOrmMysqlDialect"]
