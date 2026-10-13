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

"""Tests for the async dialect / host-list-provider debug logging.

These logs are how a failover investigation establishes which dialect and
which provider a connection actually ended up with, so they have to render
real values. A message whose placeholder count disagrees with its call site
silently logs a template fragment -- ``Messages.get_formatted`` does not
raise on a count mismatch -- so the rendering is asserted here rather than
left to a manual read of the properties file.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import (AuroraMysqlDialect,
                                                          AuroraPgDialect,
                                                          DialectCode,
                                                          RdsMysqlDialect)
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

# Every key added for dialect / provider visibility, with the argument count
# its call site passes.
KEYS_AND_ARG_COUNTS = [
    ("AsyncAwsWrapperConnection.HostListProviderInitialized", 4),
    ("AsyncPluginService.DatabaseDialectUpdated", 2),
    ("AsyncPluginService.DatabaseDialectConfirmed", 1),
    ("AsyncPluginService.DatabaseDialectUnconfirmed", 1),
    ("AsyncPluginService.HostListProviderReplaced", 3),
    ("AsyncPluginService.HostListProviderStopFailed", 1),
    ("HostListProvider.MonitorConnectionNoSeedHost", 1),
    ("HostListProvider.MonitorConnectionNoTargetDriverFunc", 1),
]


@pytest.mark.parametrize("key,arg_count", KEYS_AND_ARG_COUNTS)
def test_message_placeholder_count_matches_call_site(key: str, arg_count: int) -> None:
    template = Messages.get(key)
    assert template.count("{}") == arg_count, (
        f"{key} has {template.count('{}')} placeholders but its call site "
        f"passes {arg_count} arguments")
    rendered = Messages.get_formatted(key, *[f"arg{i}" for i in range(arg_count)])
    assert "{}" not in rendered


def _mysql_service(dialect: Any, dialect_code: Any) -> AsyncPluginServiceImpl:
    from aws_advanced_python_wrapper.database_dialect import \
        DatabaseDialectManager

    dd = MagicMock()
    dd._driver_name = "aiomysql"
    dd.is_in_transaction = AsyncMock(return_value=False)
    props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com", "port": "3306"})
    svc = AsyncPluginServiceImpl(props, dd)
    svc.database_dialect = dialect

    # A manager mid-resolution, which is the state the wrapper hands over:
    # a pattern-based guess live verification may still replace.
    manager = DatabaseDialectManager(props)
    manager._dialect = dialect
    manager._dialect_code = dialect_code
    manager._can_update = True
    manager._confirmed = False
    svc.database_dialect_manager = manager
    return svc


def _aurora_verify_connection(is_aurora: bool = True) -> Any:
    """A connection satisfying the Aurora rules, and answering the Global
    Aurora candidate's queries with "no" so the walk settles on Aurora."""
    responses = {
        "aurora_version": [("aurora_version", "3.0")] if is_aurora else [],
        # Not a global database, and not Multi-AZ.
        "AURORA_GLOBAL_DB_STATUS": [],
        "AURORA_GLOBAL_DB_INSTANCE_STATUS": [],
        "replica_host_status": [],
    }
    state = {"query": ""}

    def rows() -> list:
        for fragment, response in responses.items():
            if fragment in state["query"]:
                return list(response)
        return []

    async def execute(query: str, *args: Any, **kwargs: Any) -> None:
        state["query"] = query

    cursor = MagicMock(name="cursor")
    cursor.execute = AsyncMock(side_effect=execute)
    cursor.fetchall = AsyncMock(side_effect=lambda: rows())
    cursor.fetchone = AsyncMock(side_effect=lambda: next(iter(rows()), None))
    cursor.close = MagicMock()
    conn = MagicMock(name="conn")
    conn.cursor = MagicMock(return_value=cursor)
    return conn


def _run_and_capture(svc: AsyncPluginServiceImpl, conn: Any, caplog) -> str:
    with caplog.at_level(
            logging.DEBUG, logger="aws_advanced_python_wrapper.aio.plugin_service"):
        asyncio.run(svc.update_database_dialect(conn))
    return caplog.text


def test_dialect_upgrade_and_provider_replacement_are_logged(caplog) -> None:
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncStaticHostListProvider

    svc = _mysql_service(RdsMysqlDialect(), DialectCode.RDS_MYSQL)
    svc.host_list_provider = AsyncStaticHostListProvider(svc.props)

    text = _run_and_capture(svc, _aurora_verify_connection(), caplog)

    # Both dialect names must appear, not a bare template.
    assert "RdsMysqlDialect -> AuroraMysqlDialect" in text
    assert "AsyncStaticHostListProvider -> AsyncAuroraHostListProvider" in text
    assert "{}" not in text


def test_unchanged_dialect_logs_confirmation_not_upgrade(caplog) -> None:
    # A cluster endpoint already resolved to AuroraMysqlDialect by pattern:
    # verification confirms it, nothing is rebuilt.
    svc = _mysql_service(AuroraMysqlDialect(), DialectCode.AURORA_MYSQL)

    text = _run_and_capture(svc, _aurora_verify_connection(), caplog)

    assert "Database dialect confirmed" in text
    assert "AuroraMysqlDialect" in text
    assert "->" not in text
    assert "{}" not in text


def _resolved_cluster_id(props: Properties) -> str:
    """The cluster id the connect-time log reports.

    Mirrors the resolution in ``AsyncAwsWrapperConnection._connect_pipeline``:
    the explicit property wins, else the identifier carried by an RDS cluster
    endpoint. Deliberately NOT the provider's ``get_cluster_id()``, which is a
    cache key and degrades to "aurora:<host>:<port>" -- the endpoint restated.
    """
    cluster_id = (props.get(WrapperProperties.CLUSTER_ID.name)
                  or RdsUtils().get_cluster_id(props.get("host", "")))
    return cluster_id or "<none>"


@pytest.mark.parametrize("host,expected", [
    ("db.cluster-xyz.us-east-2.rds.amazonaws.com", "db"),
    # A reader endpoint belongs to the same cluster.
    ("db.cluster-ro-xyz.us-east-2.rds.amazonaws.com", "db"),
    # An instance endpoint carries no cluster identity, and must not be
    # reported as though it were one.
    ("instance-1.xyz.us-east-2.rds.amazonaws.com", "<none>"),
    ("localhost", "<none>"),
])
def test_logged_cluster_id_is_the_cluster_not_the_endpoint(host: str, expected: str) -> None:
    props = Properties({"host": host, "port": "5432"})
    resolved = _resolved_cluster_id(props)
    assert resolved == expected
    # Whatever is reported, it is never the endpoint or the cache key built
    # from it.
    assert not resolved.startswith("aurora:")
    assert host not in resolved


def test_explicit_cluster_id_property_wins() -> None:
    props = Properties({
        "host": "db.cluster-xyz.us-east-2.rds.amazonaws.com",
        "port": "5432",
        WrapperProperties.CLUSTER_ID.name: "my-cluster",
    })
    assert _resolved_cluster_id(props) == "my-cluster"


def test_provider_initialization_log_renders_dialect_and_cluster_id(caplog) -> None:
    """The connect-time log is what confirms which provider a connection got."""
    from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
        AsyncPsycopgDriverDialect

    class _Service:
        def __init__(self) -> None:
            self.driver_dialect = AsyncPsycopgDriverDialect()
            self.is_dialect_confirmed = True
            self.initial_connection_host_info = None
            self.current_host_info = None

        def get_target_driver_func(self) -> Optional[Any]:
            return None

    props = Properties({"host": "db.cluster-xyz.us-east-2.rds.amazonaws.com", "port": "5432"})
    dialect = AuroraPgDialect()
    provider = dialect.get_async_host_list_provider_supplier()(_Service(), props)

    logger_name = "aws_advanced_python_wrapper.aio.wrapper"
    with caplog.at_level(logging.DEBUG, logger=logger_name):
        from aws_advanced_python_wrapper.utils.log import Logger
        Logger(logger_name).debug(
            "AsyncAwsWrapperConnection.HostListProviderInitialized",
            type(provider).__name__,
            _resolved_cluster_id(props),
            type(dialect).__name__,
            True)

    assert "AsyncAuroraHostListProvider" in caplog.text
    assert "AuroraPgDialect" in caplog.text
    assert "cluster id: db)" in caplog.text
    # The endpoint-derived cache key must not leak into the line.
    assert "aurora:db.cluster-xyz" not in caplog.text
    assert "confirmed: True" in caplog.text
    assert "{}" not in caplog.text
