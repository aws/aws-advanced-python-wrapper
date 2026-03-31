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

import psycopg  # type: ignore
import pytest  # type: ignore

from aws_advanced_python_wrapper.cluster_topology_monitor import \
    GlobalAuroraTopologyMonitor
from aws_advanced_python_wrapper.database_dialect import GlobalAuroraPgDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_list_provider import (
    GlobalAuroraHostListProvider, GlobalAuroraTopologyUtils)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def clear_caches():
    services_container.get_storage_service().clear_all()
    services_container.get_monitor_service().stop_all()


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_provider_service(mocker):
    service_mock = mocker.MagicMock()
    service_mock.database_dialect = GlobalAuroraPgDialect()
    return service_mock


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def global_props():
    return Properties({
        "host": "gdb-cluster.global-xyz.global.rds.amazonaws.com",
        "global_cluster_instance_host_patterns":
            "us-east-2:?.cluster-id.us-east-2.rds.amazonaws.com:5432,"
            "ap-south-1:?.cluster-id.ap-south-1.rds.amazonaws.com:5432"
    })


@pytest.fixture
def global_topology_utils(global_props):
    return GlobalAuroraTopologyUtils(GlobalAuroraPgDialect(), global_props)


class TestGlobalAuroraHostListProvider:
    def test_init_stores_global_topology_utils(self, mock_provider_service, mock_plugin_service, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)

        assert provider._global_topology_utils is global_topology_utils

    def test_init_settings_parses_instance_templates(self, mock_provider_service, mock_plugin_service, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()

        assert len(provider._instance_templates_by_region) == 2
        assert "us-east-2" in provider._instance_templates_by_region
        assert "ap-south-1" in provider._instance_templates_by_region

    def test_init_settings_raises_error_without_patterns(self, mock_provider_service, mock_plugin_service, global_topology_utils):
        props = Properties({"host": "gdb-cluster.global-xyz.global.rds.amazonaws.com"})
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, props, global_topology_utils)

        with pytest.raises(AwsWrapperError):
            provider._initialize()

    def test_get_or_create_monitor_returns_global_monitor(self, mock_provider_service, mock_plugin_service, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()

        monitor = provider._get_or_create_monitor()

        assert isinstance(monitor, GlobalAuroraTopologyMonitor)

    def test_get_or_create_monitor_passes_instance_templates(
            self, mocker, mock_provider_service, mock_plugin_service, global_props, global_topology_utils):
        mock_monitor_init = mocker.patch(
            'aws_advanced_python_wrapper.host_list_provider.GlobalAuroraTopologyMonitor')
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()
        provider._get_or_create_monitor()

        # Verify instance_templates_by_region was passed as last argument
        assert mock_monitor_init.called
        call_args = mock_monitor_init.call_args[0]
        assert call_args[-1] == provider._instance_templates_by_region

    def test_get_current_topology_calls_query_with_regions(
            self, mocker, mock_provider_service, mock_plugin_service, mock_conn, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()

        mock_query = mocker.patch.object(
            global_topology_utils, 'query_for_topology_with_regions',
            return_value=(HostInfo("host1", role=HostRole.WRITER),))

        result = provider.get_current_topology(mock_conn, HostInfo("initial-host"))

        mock_query.assert_called_once_with(mock_conn, provider._instance_templates_by_region)
        assert len(result) == 1

    def test_get_current_topology_returns_empty_tuple_on_none(
            self, mocker, mock_provider_service, mock_plugin_service, mock_conn, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()

        mocker.patch.object(global_topology_utils, 'query_for_topology_with_regions', return_value=None)

        result = provider.get_current_topology(mock_conn, HostInfo("initial-host"))

        assert result == ()

    def test_get_current_topology_returns_empty_tuple_on_empty_list(
            self, mocker, mock_provider_service, mock_plugin_service, mock_conn, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()

        mocker.patch.object(global_topology_utils, 'query_for_topology_with_regions', return_value=())

        result = provider.get_current_topology(mock_conn, HostInfo("initial-host"))

        assert result == ()

    def test_instance_templates_by_region_contains_correct_hosts(
            self, mock_provider_service, mock_plugin_service, global_props, global_topology_utils):
        provider = GlobalAuroraHostListProvider(
            mock_provider_service, mock_plugin_service, global_props, global_topology_utils)
        provider._initialize()

        us_east_template = provider._instance_templates_by_region["us-east-2"]
        ap_south_template = provider._instance_templates_by_region["ap-south-1"]

        assert "us-east-2" in us_east_template.host
        assert "ap-south-1" in ap_south_template.host
        assert us_east_template.port == 5432
        assert ap_south_template.port == 5432
