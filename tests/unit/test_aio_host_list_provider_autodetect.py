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

"""Tests for dialect-driven selection of the async host list provider.

Each dialect names its provider through
``get_async_host_list_provider_supplier``, which the async wrapper invokes
during connect. These tests call the suppliers the way the wrapper does,
pinning the dialect -> provider mapping and the per-dialect topology SQL
and default port that come with it.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

import pytest

from aws_advanced_python_wrapper.aio.driver_dialect.aiomysql import \
    AsyncAiomysqlDriverDialect
from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
    AsyncPsycopgDriverDialect
from aws_advanced_python_wrapper.aio.host_list_provider import (
    AsyncAuroraHostListProvider, AsyncGlobalAuroraHostListProvider,
    AsyncMultiAzHostListProvider, AsyncStaticHostListProvider)
from aws_advanced_python_wrapper.database_dialect import (
    AuroraMysqlDialect, AuroraPgDialect, GlobalAuroraMysqlDialect,
    GlobalAuroraPgDialect, MultiAzClusterMysqlDialect, MultiAzClusterPgDialect,
    MysqlDatabaseDialect, PgDatabaseDialect, RdsMysqlDialect, RdsPgDialect,
    UnknownDatabaseDialect)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties

MYSQL_PORT = 3306
PG_PORT = 5432


class _FakeProviderService:
    """Minimal AsyncHostListProviderService for provider construction.

    The suppliers only read ``driver_dialect`` while building; the seed
    host and target driver function are resolved lazily, when the topology
    monitor opens its own connection.
    """

    def __init__(self, driver_dialect: Any, is_dialect_confirmed: bool = True) -> None:
        self.driver_dialect = driver_dialect
        self.is_dialect_confirmed = is_dialect_confirmed
        self.current_connection: Optional[Any] = None
        self.current_host_info: Optional[HostInfo] = None
        self.initial_connection_host_info: Optional[HostInfo] = None
        self.database_dialect: Optional[Any] = None
        self.host_list_provider: Optional[Any] = None

    def is_static_host_list_provider(self) -> bool:
        return isinstance(self.host_list_provider, AsyncStaticHostListProvider)

    def get_target_driver_func(self) -> Optional[Callable]:
        return None


def _build(dialect: Any, driver_dialect: Any = None, props: Optional[Properties] = None,
           service: Optional[Any] = None) -> Any:
    """Invoke a dialect's supplier the way AsyncAwsWrapperConnection.connect does."""
    if service is None:
        service = _FakeProviderService(
            driver_dialect if driver_dialect is not None else AsyncPsycopgDriverDialect())
    return dialect.get_async_host_list_provider_supplier()(
        service, props if props is not None else Properties())


@pytest.mark.parametrize("dialect", [
    PgDatabaseDialect(),
    MysqlDatabaseDialect(),
    RdsPgDialect(),
    RdsMysqlDialect(),
    UnknownDatabaseDialect(),
])
def test_no_topology_dialects_return_static(dialect) -> None:
    # These dialects have no topology query, so there is nothing for a
    # topology provider to run. Mirrors the sync suppliers, which all
    # return ConnectionStringHostListProvider for these dialects.
    assert isinstance(_build(dialect), AsyncStaticHostListProvider)


def test_aurora_pg_dialect_selects_aurora_provider() -> None:
    provider = _build(AuroraPgDialect())
    assert type(provider) is AsyncAuroraHostListProvider
    assert provider._topology_query == AuroraPgDialect().topology_query
    assert provider._default_port == PG_PORT


def test_aurora_mysql_dialect_selects_aurora_provider_with_mysql_sql() -> None:
    # Regression: before the dialect supplied the provider, an Aurora MySQL
    # connection got the provider's PostgreSQL-default topology query and
    # port 5432, so discovery returned nothing until a later reconfigure.
    provider = _build(AuroraMysqlDialect(), AsyncAiomysqlDriverDialect())
    assert type(provider) is AsyncAuroraHostListProvider
    assert provider._topology_query == AuroraMysqlDialect().topology_query
    assert provider._default_port == MYSQL_PORT


def test_global_aurora_pg_dialect_selects_global_provider() -> None:
    provider = _build(
        GlobalAuroraPgDialect(),
        props=Properties({
            "global_cluster_instance_host_patterns": (
                "us-east-1:global-db-us-east-1.cluster-xyz.us-east-1.rds.amazonaws.com"),
        }))
    assert isinstance(provider, AsyncGlobalAuroraHostListProvider)
    assert provider._topology_query == GlobalAuroraPgDialect().topology_query


def test_global_aurora_mysql_dialect_selects_global_provider() -> None:
    provider = _build(GlobalAuroraMysqlDialect(), AsyncAiomysqlDriverDialect())
    assert isinstance(provider, AsyncGlobalAuroraHostListProvider)
    assert provider._topology_query == GlobalAuroraMysqlDialect().topology_query
    assert provider._default_port == MYSQL_PORT


def test_multi_az_pg_dialect_selects_multi_az_provider() -> None:
    dialect = MultiAzClusterPgDialect()
    provider = _build(dialect)
    assert isinstance(provider, AsyncMultiAzHostListProvider)
    assert provider._topology_query == dialect.topology_query
    assert provider._writer_host_query == dialect.writer_id_query
    assert provider._host_id_query == dialect.host_id_query
    # The PG writer query returns a single column.
    assert provider._writer_host_column_index == 0


def test_multi_az_mysql_dialect_carries_writer_column_index() -> None:
    # SHOW REPLICA STATUS carries the writer host at column 39; losing that
    # index silently reads the wrong column and no writer is ever found.
    dialect = MultiAzClusterMysqlDialect()
    provider = _build(dialect, AsyncAiomysqlDriverDialect())
    assert isinstance(provider, AsyncMultiAzHostListProvider)
    assert provider._writer_host_column_index == dialect._WRITER_HOST_COLUMN_INDEX
    assert provider._default_port == MYSQL_PORT


def test_provider_receives_provider_service_for_dialect_confirmation() -> None:
    # The provider must be able to see is_dialect_confirmed, otherwise it
    # would query topology with SQL from an unconfirmed dialect guess.
    service = _FakeProviderService(AsyncPsycopgDriverDialect())
    provider = _build(AuroraPgDialect(), service=service)
    assert isinstance(provider, AsyncAuroraHostListProvider)
    assert provider._provider_service is service
