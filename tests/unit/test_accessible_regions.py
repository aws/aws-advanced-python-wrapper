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

from aws_advanced_python_wrapper.database_dialect import (
    GlobalAuroraMysqlDialect, GlobalAuroraPgDialect, MysqlDatabaseDialect)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.accessible_regions import parse
from aws_advanced_python_wrapper.utils.properties import Properties


class TestParseAccessibleRegions:
    def test_returns_none_when_property_not_set(self):
        props = Properties()
        assert parse(props) is None

    def test_returns_none_when_empty_string(self):
        props = Properties()
        props["gdb_accessible_regions"] = ""
        assert parse(props) is None

    def test_returns_none_when_whitespace_only(self):
        props = Properties()
        props["gdb_accessible_regions"] = "   "
        assert parse(props) is None

    def test_parses_single_region(self):
        props = Properties()
        props["gdb_accessible_regions"] = "us-east-1"
        result = parse(props)
        assert result == frozenset({"us-east-1"})

    def test_parses_multiple_regions(self):
        props = Properties()
        props["gdb_accessible_regions"] = "us-east-1,us-west-2,eu-central-1"
        result = parse(props)
        assert result == frozenset({"us-east-1", "us-west-2", "eu-central-1"})

    def test_normalizes_to_lowercase(self):
        props = Properties()
        props["gdb_accessible_regions"] = "US-EAST-1,Us-West-2"
        result = parse(props)
        assert result == frozenset({"us-east-1", "us-west-2"})

    def test_trims_whitespace(self):
        props = Properties()
        props["gdb_accessible_regions"] = " us-east-1 , us-west-2 "
        result = parse(props)
        assert result == frozenset({"us-east-1", "us-west-2"})

    def test_skips_empty_entries(self):
        props = Properties()
        props["gdb_accessible_regions"] = "us-east-1,,us-west-2,"
        result = parse(props)
        assert result == frozenset({"us-east-1", "us-west-2"})

    def test_returns_frozenset(self):
        props = Properties()
        props["gdb_accessible_regions"] = "us-east-1"
        result = parse(props)
        assert isinstance(result, frozenset)


class TestDialectFilterAvailableHosts:
    @staticmethod
    def _make_host(host: str, role: HostRole = HostRole.READER) -> HostInfo:
        return HostInfo(host=host, role=role)

    def _sample_hosts(self):
        return [
            self._make_host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER),
            self._make_host("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER),
            self._make_host("instance3.cluster-xyz.us-west-2.rds.amazonaws.com", HostRole.READER),
            self._make_host("instance4.cluster-ro-xyz.eu-central-1.rds.amazonaws.com", HostRole.READER),
        ]

    def test_global_aurora_mysql_filters_by_region(self):
        dialect = GlobalAuroraMysqlDialect()
        regions = frozenset({"us-east-1", "us-west-2"})
        hosts = self._sample_hosts()

        filtered = dialect.filter_available_hosts(hosts, regions)

        assert len(filtered) == 3
        for h in filtered:
            assert "eu-central-1" not in h.host

    def test_global_aurora_pg_filters_by_region(self):
        dialect = GlobalAuroraPgDialect()
        regions = frozenset({"us-east-1"})
        hosts = self._sample_hosts()

        filtered = dialect.filter_available_hosts(hosts, regions)

        assert len(filtered) == 2
        for h in filtered:
            assert "us-east-1" in h.host

    def test_returns_all_when_no_restriction(self):
        dialect = GlobalAuroraMysqlDialect()
        hosts = self._sample_hosts()

        assert dialect.filter_available_hosts(hosts, None) == hosts

    def test_returns_all_when_empty_frozenset(self):
        dialect = GlobalAuroraMysqlDialect()
        hosts = self._sample_hosts()

        assert dialect.filter_available_hosts(hosts, frozenset()) == hosts

    def test_non_global_dialect_returns_all(self):
        dialect = MysqlDatabaseDialect()
        hosts = self._sample_hosts()
        regions = frozenset({"us-east-1"})

        result = dialect.filter_available_hosts(hosts, regions)
        assert result == hosts

    def test_returns_list_for_tuple_input(self):
        # The method accepts any Sequence and always returns a list, so the
        # return type matches its annotation even for tuple input.
        dialect = GlobalAuroraMysqlDialect()
        hosts = tuple(self._sample_hosts())

        result = dialect.filter_available_hosts(hosts, None)
        assert isinstance(result, list)
        assert result == list(hosts)

    def test_case_insensitive_region_matching(self):
        dialect = GlobalAuroraMysqlDialect()
        regions = frozenset({"us-east-1"})
        hosts = [
            self._make_host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER),
        ]

        filtered = dialect.filter_available_hosts(hosts, regions)
        assert len(filtered) == 1

    def test_excludes_hosts_without_parseable_region(self):
        dialect = GlobalAuroraMysqlDialect()
        regions = frozenset({"us-east-1"})
        hosts = [
            self._make_host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER),
            self._make_host("custom-domain.example.com", HostRole.READER),
        ]

        filtered = dialect.filter_available_hosts(hosts, regions)
        assert len(filtered) == 1
        assert "us-east-1" in filtered[0].host
