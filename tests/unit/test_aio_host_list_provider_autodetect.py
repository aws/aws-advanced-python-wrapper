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

"""Tests for auto-detection of MultiAz/GlobalAurora host-list providers (N.1)."""

from __future__ import annotations

from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
    AsyncPsycopgDriverDialect
from aws_advanced_python_wrapper.aio.host_list_provider import (
    AsyncAuroraHostListProvider, AsyncGlobalAuroraHostListProvider,
    AsyncMultiAzHostListProvider, AsyncStaticHostListProvider)
from aws_advanced_python_wrapper.aio.wrapper import _build_host_list_provider
from aws_advanced_python_wrapper.utils.properties import Properties


def test_no_topology_plugins_returns_static() -> None:
    dd = AsyncPsycopgDriverDialect()
    provider = _build_host_list_provider(Properties(), dd)
    assert isinstance(provider, AsyncStaticHostListProvider)


def test_topology_plugins_without_dialect_return_aurora() -> None:
    dd = AsyncPsycopgDriverDialect()
    provider = _build_host_list_provider(
        Properties({"plugins": "failover"}), dd)
    assert isinstance(provider, AsyncAuroraHostListProvider)
    # Subclasses like MultiAz are a "real" Aurora-derived type; verify
    # we got the plain base not a subclass.
    assert type(provider) is AsyncAuroraHostListProvider


def test_gdb_failover_plugin_returns_topology_provider() -> None:
    # Regression: gdb_failover MUST be treated as a topology-requiring plugin.
    # It was omitted from _TOPOLOGY_REQUIRING_PLUGINS (gdb_rw was present but
    # gdb_failover was not), so a gdb_failover connection got the static
    # provider -- a single seed host, no topology query, no cluster topology
    # monitor. Writer failover then had only the seed host and looped on the
    # demoted old writer until timeout (never discovering the new writer), on
    # both PG and MySQL. A topology provider (with monitor) is required.
    dd = AsyncPsycopgDriverDialect()
    provider = _build_host_list_provider(
        Properties({"plugins": "gdb_failover"}), dd)
    assert not isinstance(provider, AsyncStaticHostListProvider)
    assert isinstance(provider, AsyncAuroraHostListProvider)


def test_multi_az_dialect_selects_multi_az_provider() -> None:
    dd = AsyncPsycopgDriverDialect()

    class MultiAzClusterPgDialect:
        _WRITER_HOST_QUERY = "SELECT writer FROM multi_az"
        _TOPOLOGY_QUERY = "SELECT id, host, port FROM multi_az"
        _HOST_ID_QUERY = "SELECT aurora_db_instance_identifier()"

    provider = _build_host_list_provider(
        Properties({"plugins": "failover"}), dd, MultiAzClusterPgDialect())
    assert isinstance(provider, AsyncMultiAzHostListProvider)


def test_multi_az_dialect_missing_queries_falls_back_to_aurora() -> None:
    """Defensive: if a dialect has the MultiAz naming but lacks the
    required query strings, fall back to plain Aurora rather than
    raising on init."""
    dd = AsyncPsycopgDriverDialect()

    class MultiAzClusterPgDialect:
        pass  # No _WRITER_HOST_QUERY, etc.

    provider = _build_host_list_provider(
        Properties({"plugins": "failover"}), dd, MultiAzClusterPgDialect())
    assert type(provider) is AsyncAuroraHostListProvider


def test_global_aurora_dialect_selects_global_provider() -> None:
    dd = AsyncPsycopgDriverDialect()

    class GlobalAuroraPgDialect:
        topology_query = "SELECT aurora_global_db_instance_identifier()"

    provider = _build_host_list_provider(
        Properties({
            "plugins": "failover",
            "global_cluster_instance_host_patterns": (
                "us-east-1=global-db-us-east-1.cluster-xyz.us-east-1.rds.amazonaws.com"),
        }),
        dd,
        GlobalAuroraPgDialect(),
    )
    assert isinstance(provider, AsyncGlobalAuroraHostListProvider)


def test_plain_aurora_dialect_selects_aurora_provider() -> None:
    dd = AsyncPsycopgDriverDialect()

    class AuroraPgDialect:
        pass

    provider = _build_host_list_provider(
        Properties({"plugins": "failover"}), dd, AuroraPgDialect())
    assert type(provider) is AsyncAuroraHostListProvider
