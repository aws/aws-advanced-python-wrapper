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

import psycopg
import pytest

from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.gdb_read_write_splitting_plugin import \
    GdbReadWriteSplittingPlugin
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

HOME_REGION = "us-west-1"
OUT_REGION = "us-east-1"

WRITER_HOME = HostInfo("writer.cluster-xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_HOME = HostInfo("reader1.xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_OUT = HostInfo("reader2.xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.READER)
WRITER_OUT = HostInfo("writer.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)


@pytest.fixture
def plugin_service_mock():
    mock = MagicMock()
    mock.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
    mock.current_host_info = WRITER_HOME
    mock.current_connection = MagicMock(spec=psycopg.Connection)
    mock.is_in_transaction = False
    return mock


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def gdb_rw_plugin(plugin_service_mock, props):
    plugin = GdbReadWriteSplittingPlugin(plugin_service_mock, props)
    # Baseline: home region set, no accessible-region restriction, no
    # home-region reader/writer restriction unless a test enables it.
    plugin._home_region = HOME_REGION
    return plugin


class TestInitSettingsAccessibleRegions:
    def test_home_region_not_accessible_raises(self, plugin_service_mock):
        props = Properties()
        WrapperProperties.GDB_RW_HOME_REGION.set(props, HOME_REGION)
        WrapperProperties.GDB_ACCESSIBLE_REGIONS.set(props, OUT_REGION)
        plugin = GdbReadWriteSplittingPlugin(plugin_service_mock, props)

        with pytest.raises(ReadWriteSplittingError):
            plugin._init_settings(WRITER_HOME, props)

    def test_home_region_accessible_ok(self, plugin_service_mock):
        props = Properties()
        WrapperProperties.GDB_RW_HOME_REGION.set(props, HOME_REGION)
        WrapperProperties.GDB_ACCESSIBLE_REGIONS.set(props, f"{HOME_REGION},{OUT_REGION}")
        plugin = GdbReadWriteSplittingPlugin(plugin_service_mock, props)

        plugin._init_settings(WRITER_HOME, props)

        assert plugin._accessible_regions == frozenset({HOME_REGION, OUT_REGION})


class TestIsInAccessibleRegion:
    def test_no_restriction_allows_all(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = None
        assert gdb_rw_plugin._is_in_accessible_region(WRITER_OUT) is True

    def test_filters_by_region(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = frozenset({HOME_REGION})
        assert gdb_rw_plugin._is_in_accessible_region(WRITER_HOME) is True
        assert gdb_rw_plugin._is_in_accessible_region(READER_OUT) is False


class TestInitializeWriterConnection:
    def test_inaccessible_writer_raises(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_rw_plugin._get_writer_host_info = MagicMock(return_value=WRITER_OUT)

        with pytest.raises(ReadWriteSplittingError):
            gdb_rw_plugin._initialize_writer_connection()

    def test_accessible_writer_delegates_to_super(self, gdb_rw_plugin, monkeypatch):
        gdb_rw_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_rw_plugin._restrict_writer_to_home_region = False
        gdb_rw_plugin._get_writer_host_info = MagicMock(return_value=WRITER_HOME)
        called = {}
        monkeypatch.setattr(
            "aws_advanced_python_wrapper.read_write_splitting_plugin."
            "ReadWriteSplittingPlugin._initialize_writer_connection",
            lambda self: called.setdefault("super", True),
        )

        gdb_rw_plugin._initialize_writer_connection()

        assert called.get("super") is True


class TestSetWriterConnection:
    def test_inaccessible_writer_closes_and_raises(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_rw_plugin._close_connection = MagicMock()
        conn = MagicMock(spec=psycopg.Connection)

        with pytest.raises(ReadWriteSplittingError):
            gdb_rw_plugin._set_writer_connection(conn, WRITER_OUT)

        gdb_rw_plugin._close_connection.assert_called_once_with(conn)


class TestGetReaderHostCandidates:
    def test_filters_out_inaccessible_readers(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_rw_plugin._restrict_reader_to_home_region = False

        candidates = gdb_rw_plugin._get_reader_host_candidates()

        assert WRITER_HOME in candidates
        assert READER_HOME in candidates
        assert READER_OUT not in candidates

    def test_no_fallback_when_all_readers_inaccessible(self, gdb_rw_plugin):
        # Hard restriction: no fallback to the unfiltered host list.
        gdb_rw_plugin._accessible_regions = frozenset({"eu-central-1"})
        gdb_rw_plugin._restrict_reader_to_home_region = False

        candidates = gdb_rw_plugin._get_reader_host_candidates()

        assert candidates == []

    def test_unrestricted_returns_all_hosts(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = None
        gdb_rw_plugin._restrict_reader_to_home_region = False

        candidates = gdb_rw_plugin._get_reader_host_candidates()

        assert candidates == [WRITER_HOME, READER_HOME, READER_OUT]

    def test_home_region_restriction_applied_after_accessible_filter(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = frozenset({HOME_REGION, OUT_REGION})
        gdb_rw_plugin._restrict_reader_to_home_region = True

        candidates = gdb_rw_plugin._get_reader_host_candidates()

        # READER_OUT is accessible but not in the home region, so it is dropped
        # by the home-region restriction that runs on the post-filter set.
        assert READER_HOME in candidates
        assert READER_OUT not in candidates

    def test_home_region_restriction_no_home_readers_raises(self, gdb_rw_plugin):
        gdb_rw_plugin._accessible_regions = frozenset({OUT_REGION})
        gdb_rw_plugin._restrict_reader_to_home_region = True

        with pytest.raises(ReadWriteSplittingError):
            gdb_rw_plugin._get_reader_host_candidates()
