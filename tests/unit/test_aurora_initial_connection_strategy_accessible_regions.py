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

from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import \
    AuroraInitialConnectionStrategyPlugin
from aws_advanced_python_wrapper.database_dialect import (
    GlobalAuroraPgDialect, MysqlDatabaseDialect)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType

WRITER_HOME = HostInfo("writer.cluster-xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_HOME = HostInfo("reader1.xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_OUT = HostInfo("reader2.xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.READER)
WRITER_OUT = HostInfo("writer.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)

ALL_HOSTS = (WRITER_HOME, READER_HOME, READER_OUT, WRITER_OUT)


@pytest.fixture
def plugin_service_mock():
    mock = MagicMock()
    mock.all_hosts = ALL_HOSTS
    mock.database_dialect = GlobalAuroraPgDialect()
    return mock


def _make_plugin(plugin_service_mock, accessible_regions=None):
    props = Properties()
    if accessible_regions is not None:
        WrapperProperties.GDB_ACCESSIBLE_REGIONS.set(props, accessible_regions)
    return AuroraInitialConnectionStrategyPlugin(plugin_service_mock, props)


class TestFilterByAccessibleRegions:
    def test_no_restriction_returns_all(self, plugin_service_mock):
        plugin = _make_plugin(plugin_service_mock)
        result = plugin._filter_by_accessible_regions(ALL_HOSTS)
        assert list(result) == list(ALL_HOSTS)

    def test_filters_by_region(self, plugin_service_mock):
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        result = plugin._filter_by_accessible_regions(ALL_HOSTS)
        assert result == [WRITER_HOME, READER_HOME]

    def test_non_global_dialect_returns_all(self, plugin_service_mock):
        plugin_service_mock.database_dialect = MysqlDatabaseDialect()
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        result = plugin._filter_by_accessible_regions(ALL_HOSTS)
        # Non-global dialect's default filter_available_hosts is a no-op.
        assert list(result) == list(ALL_HOSTS)

    def test_none_dialect_returns_all(self, plugin_service_mock):
        plugin_service_mock.database_dialect = None
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        result = plugin._filter_by_accessible_regions(ALL_HOSTS)
        assert list(result) == list(ALL_HOSTS)


class TestFindWriter:
    """`_find_writer` is unfiltered by design; accessible-region filtering is
    applied by the caller."""

    def test_returns_first_writer_unfiltered(self, plugin_service_mock):
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        # Passed the raw host list, it returns the first writer regardless of region.
        assert plugin._find_writer(ALL_HOSTS) == WRITER_HOME

    def test_returns_out_of_region_writer_when_not_pre_filtered(self, plugin_service_mock):
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        # An out-of-region writer is still returned — filtering is the caller's job.
        assert plugin._find_writer((READER_HOME, WRITER_OUT)) == WRITER_OUT

    def test_returns_none_when_no_writer(self, plugin_service_mock):
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        assert plugin._find_writer((READER_HOME, READER_OUT)) is None


class TestGetCandidateHostWriter:
    """The SUBSTITUTE_WITH_WRITER branch of `_get_candidate_host` filters by
    accessible regions before picking the writer."""

    def _candidate(self, plugin, original_host):
        from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import \
            InstanceSubstitutionStrategy
        return plugin._get_candidate_host(
            original_host,
            RdsUrlType.RDS_WRITER_CLUSTER,
            InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER)

    def test_writer_in_accessible_region_returned(self, plugin_service_mock):
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        assert self._candidate(plugin, WRITER_HOME) == WRITER_HOME

    def test_writer_filtered_out_returns_none(self, plugin_service_mock):
        # Only the out-of-region writer exists; it is filtered out before selection.
        plugin_service_mock.all_hosts = (READER_HOME, WRITER_OUT)
        plugin = _make_plugin(plugin_service_mock, "us-west-1")
        assert self._candidate(plugin, WRITER_OUT) is None
