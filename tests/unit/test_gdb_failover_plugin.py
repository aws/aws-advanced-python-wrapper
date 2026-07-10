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

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverFailedError,
                                                FailoverSuccessError)
from aws_advanced_python_wrapper.gdb_failover_plugin import (
    GdbFailoverPlugin, GdbFailoverPluginFactory)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.gdb_failover_mode import GdbFailoverMode
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType

# Region-tagged endpoints used across the tests.
HOME_REGION = "us-west-1"
OUT_REGION = "us-east-1"

WRITER_HOME = HostInfo("writer.cluster-xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_HOME = HostInfo("reader1.xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_OUT = HostInfo("reader2.xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.READER)
WRITER_OUT = HostInfo("writer.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)


@pytest.fixture
def plugin_service_mock():
    mock = MagicMock()
    mock.network_bound_methods = {"*"}
    mock.current_host_info = WRITER_HOME
    mock.current_connection = MagicMock(spec=psycopg.Connection)
    mock.driver_dialect.network_bound_methods = {"Connection.execute", "Connection.commit"}
    mock.driver_dialect.is_closed.return_value = False
    mock.is_network_exception.return_value = True
    mock.is_in_transaction = False
    mock.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
    mock.all_hosts = mock.hosts
    mock.get_telemetry_factory.return_value.open_telemetry_context.return_value = None
    return mock


@pytest.fixture
def properties():
    props = Properties()
    WrapperProperties.FAILOVER_TIMEOUT_SEC.set(props, "60")
    WrapperProperties.TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.set(props, "false")
    return props


@pytest.fixture
def gdb_plugin(plugin_service_mock, properties):
    return GdbFailoverPlugin(plugin_service_mock, properties)


def _host_list_provider_with_host(host: str):
    hlps = MagicMock()
    hlps.initial_connection_host_info = HostInfo(host, 5432, HostRole.WRITER)
    return hlps


class TestGlobalDbFailoverMode:
    def test_from_value_hyphenated(self):
        assert GdbFailoverMode.from_value("strict-writer") == GdbFailoverMode.STRICT_WRITER
        assert GdbFailoverMode.from_value("home-reader-or-writer") == GdbFailoverMode.HOME_READER_OR_WRITER

    def test_from_value_underscored(self):
        assert GdbFailoverMode.from_value("strict_any_reader") == GdbFailoverMode.STRICT_ANY_READER

    def test_from_value_case_insensitive(self):
        assert GdbFailoverMode.from_value("STRICT-WRITER") == GdbFailoverMode.STRICT_WRITER

    def test_from_value_none_or_empty(self):
        assert GdbFailoverMode.from_value(None) is None
        assert GdbFailoverMode.from_value("   ") is None

    def test_from_value_invalid(self):
        with pytest.raises(AwsWrapperError):
            GdbFailoverMode.from_value("bogus-mode")


class TestGdbFailoverPluginFactory:
    def test_factory_returns_plugin(self, plugin_service_mock, properties):
        plugin = GdbFailoverPluginFactory.get_instance(plugin_service_mock, properties)
        assert isinstance(plugin, GdbFailoverPlugin)


class TestGdbFailoverInitMode:
    def test_init_mode_uses_explicit_home_region_and_modes(self, gdb_plugin, properties):
        WrapperProperties.FAILOVER_HOME_REGION.set(properties, HOME_REGION)
        WrapperProperties.ACTIVE_HOME_FAILOVER_MODE.set(properties, "strict-writer")
        WrapperProperties.INACTIVE_HOME_FAILOVER_MODE.set(properties, "strict-any-reader")
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host(
            "gdb.global-xyz.global.rds.amazonaws.com")

        gdb_plugin._init_failover_mode()

        assert gdb_plugin._home_region == HOME_REGION
        assert gdb_plugin._active_home_failover_mode == GdbFailoverMode.STRICT_WRITER
        assert gdb_plugin._inactive_home_failover_mode == GdbFailoverMode.STRICT_ANY_READER

    def test_init_mode_parses_region_from_endpoint(self, gdb_plugin):
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host(WRITER_HOME.host)

        gdb_plugin._init_failover_mode()

        assert gdb_plugin._home_region == "us-west-1"
        # Writer cluster endpoint default is STRICT_WRITER.
        assert gdb_plugin._active_home_failover_mode == GdbFailoverMode.STRICT_WRITER
        assert gdb_plugin._inactive_home_failover_mode == GdbFailoverMode.STRICT_WRITER

    def test_init_mode_default_for_reader_endpoint(self, gdb_plugin):
        reader_cluster = "db.cluster-ro-xyz.us-west-1.rds.amazonaws.com"
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host(reader_cluster)

        gdb_plugin._init_failover_mode()

        assert gdb_plugin._active_home_failover_mode == GdbFailoverMode.HOME_READER_OR_WRITER
        assert gdb_plugin._inactive_home_failover_mode == GdbFailoverMode.HOME_READER_OR_WRITER

    def test_init_mode_missing_region_raises(self, gdb_plugin):
        # IP address endpoint has no region and no home region property set.
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host("10.0.0.1")

        with pytest.raises(AwsWrapperError):
            gdb_plugin._init_failover_mode()

    def test_init_mode_idempotent(self, gdb_plugin):
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host(WRITER_HOME.host)
        gdb_plugin._init_failover_mode()
        gdb_plugin._rds_url_type = RdsUrlType.RDS_READER_CLUSTER  # sentinel
        gdb_plugin._init_failover_mode()
        # Second call returns early without recomputing.
        assert gdb_plugin._rds_url_type == RdsUrlType.RDS_READER_CLUSTER


class TestGdbFailover:
    def test_failover_refresh_failed(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._active_home_failover_mode = GdbFailoverMode.STRICT_WRITER
        gdb_plugin._inactive_home_failover_mode = GdbFailoverMode.STRICT_WRITER
        gdb_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = False

        with pytest.raises(FailoverFailedError):
            gdb_plugin._failover()

    def test_failover_no_writer_found(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._active_home_failover_mode = GdbFailoverMode.STRICT_WRITER
        gdb_plugin._inactive_home_failover_mode = GdbFailoverMode.STRICT_WRITER
        gdb_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        gdb_plugin._plugin_service.all_hosts = [READER_HOME, READER_OUT]

        with pytest.raises(FailoverFailedError):
            gdb_plugin._failover()

    def test_failover_in_home_uses_active_mode(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._active_home_failover_mode = GdbFailoverMode.STRICT_WRITER
        gdb_plugin._inactive_home_failover_mode = GdbFailoverMode.STRICT_ANY_READER
        gdb_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        gdb_plugin._plugin_service.all_hosts = [WRITER_HOME, READER_HOME, READER_OUT]
        captured = {}

        def fake_failover_with_mode(mode, writer_candidate, end_time):
            captured["mode"] = mode

        gdb_plugin._failover_with_mode = MagicMock(side_effect=fake_failover_with_mode)
        gdb_plugin._throw_failover_success_exception = MagicMock(side_effect=FailoverSuccessError())

        with pytest.raises(FailoverSuccessError):
            gdb_plugin._failover()

        assert captured["mode"] == GdbFailoverMode.STRICT_WRITER

    def test_failover_out_of_home_uses_inactive_mode(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._active_home_failover_mode = GdbFailoverMode.STRICT_WRITER
        gdb_plugin._inactive_home_failover_mode = GdbFailoverMode.STRICT_ANY_READER
        gdb_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        # New writer is in the out-of-home region.
        gdb_plugin._plugin_service.all_hosts = [WRITER_OUT, READER_HOME, READER_OUT]
        captured = {}

        gdb_plugin._failover_with_mode = MagicMock(side_effect=lambda mode, w, e: captured.update(mode=mode))
        gdb_plugin._throw_failover_success_exception = MagicMock(side_effect=FailoverSuccessError())

        with pytest.raises(FailoverSuccessError):
            gdb_plugin._failover()

        assert captured["mode"] == GdbFailoverMode.STRICT_ANY_READER


class TestGdbFailoverWithMode:
    def test_strict_writer_dispatch(self, gdb_plugin):
        gdb_plugin._failover_to_writer = MagicMock()
        gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_WRITER, WRITER_HOME, 0.0)
        gdb_plugin._failover_to_writer.assert_called_once()

    def test_strict_home_reader_filters_home_readers(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._plugin_service.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_HOME_READER, WRITER_HOME, 0.0)

        assert captured["role"] == HostRole.READER
        assert captured["hosts"] == [READER_HOME]

    def test_strict_out_of_home_reader_filters_out_readers(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._plugin_service.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_OUT_OF_HOME_READER, WRITER_HOME, 0.0)

        assert captured["role"] == HostRole.READER
        assert captured["hosts"] == [READER_OUT]

    def test_strict_any_reader_selects_all_readers(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._plugin_service.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_ANY_READER, WRITER_HOME, 0.0)

        assert captured["role"] == HostRole.READER
        assert READER_HOME in captured["hosts"]
        assert READER_OUT in captured["hosts"]
        assert len(captured["hosts"]) == 2

    def test_home_reader_or_writer_includes_writer_and_home_readers(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._plugin_service.hosts = [WRITER_OUT, READER_HOME, READER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.HOME_READER_OR_WRITER, WRITER_OUT, 0.0)

        assert captured["role"] is None
        assert WRITER_OUT in captured["hosts"]
        assert READER_HOME in captured["hosts"]
        assert READER_OUT not in captured["hosts"]
        assert len(captured["hosts"]) == 2

    def test_any_reader_or_writer_includes_all(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._plugin_service.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.ANY_READER_OR_WRITER, WRITER_HOME, 0.0)

        assert captured["role"] is None
        assert len(captured["hosts"]) == 3
        for host in (WRITER_HOME, READER_HOME, READER_OUT):
            assert host in captured["hosts"]


class TestGdbFailoverUnsupportedMethods:
    def test_failover_reader_unsupported(self, gdb_plugin):
        with pytest.raises(AwsWrapperError):
            gdb_plugin._failover_reader()

    def test_failover_writer_unsupported(self, gdb_plugin):
        with pytest.raises(AwsWrapperError):
            gdb_plugin._failover_writer()


class TestGdbFailoverAccessibleRegions:
    def test_init_mode_home_region_not_accessible_raises(self, gdb_plugin, properties):
        WrapperProperties.GDB_ACCESSIBLE_REGIONS.set(properties, OUT_REGION)
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host(WRITER_HOME.host)

        with pytest.raises(AwsWrapperError):
            gdb_plugin._init_failover_mode()

    def test_init_mode_home_region_accessible_ok(self, gdb_plugin, properties):
        WrapperProperties.GDB_ACCESSIBLE_REGIONS.set(properties, f"{HOME_REGION},{OUT_REGION}")
        gdb_plugin._host_list_provider_service = _host_list_provider_with_host(WRITER_HOME.host)

        gdb_plugin._init_failover_mode()

        assert gdb_plugin._home_region == HOME_REGION
        assert gdb_plugin._accessible_regions == frozenset({HOME_REGION, OUT_REGION})

    def test_is_in_accessible_region_no_restriction_allows_all(self, gdb_plugin):
        gdb_plugin._accessible_regions = None
        assert gdb_plugin._is_in_accessible_region(WRITER_OUT) is True

    def test_is_in_accessible_region_filters(self, gdb_plugin):
        gdb_plugin._accessible_regions = frozenset({HOME_REGION})
        assert gdb_plugin._is_in_accessible_region(WRITER_HOME) is True
        assert gdb_plugin._is_in_accessible_region(READER_OUT) is False

    def test_strict_writer_inaccessible_region_raises_and_counts(self, gdb_plugin):
        gdb_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_plugin._failover_to_writer = MagicMock()
        gdb_plugin._failover_writer_triggered_counter = MagicMock()
        gdb_plugin._failover_writer_failed_counter = MagicMock()

        with pytest.raises(FailoverFailedError):
            gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_WRITER, WRITER_OUT, 0.0)

        gdb_plugin._failover_to_writer.assert_not_called()
        gdb_plugin._failover_writer_triggered_counter.inc.assert_called_once()
        gdb_plugin._failover_writer_failed_counter.inc.assert_called_once()

    def test_strict_writer_accessible_region_dispatches(self, gdb_plugin):
        gdb_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_plugin._failover_to_writer = MagicMock()

        gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_WRITER, WRITER_HOME, 0.0)

        gdb_plugin._failover_to_writer.assert_called_once()

    def test_strict_any_reader_excludes_inaccessible(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_plugin._plugin_service.hosts = [WRITER_HOME, READER_HOME, READER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.STRICT_ANY_READER, WRITER_HOME, 0.0)

        assert captured["hosts"] == [READER_HOME]

    def test_any_reader_or_writer_excludes_inaccessible(self, gdb_plugin):
        gdb_plugin._home_region = HOME_REGION
        gdb_plugin._accessible_regions = frozenset({HOME_REGION})
        gdb_plugin._plugin_service.hosts = [WRITER_HOME, READER_HOME, READER_OUT, WRITER_OUT]
        captured = {}
        gdb_plugin._failover_to_allowed_host = MagicMock(
            side_effect=lambda supplier, role, end: captured.update(hosts=supplier(), role=role))

        gdb_plugin._failover_with_mode(GdbFailoverMode.ANY_READER_OR_WRITER, WRITER_HOME, 0.0)

        assert WRITER_HOME in captured["hosts"]
        assert READER_HOME in captured["hosts"]
        assert READER_OUT not in captured["hosts"]
        assert WRITER_OUT not in captured["hosts"]
