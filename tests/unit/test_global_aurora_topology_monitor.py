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

from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from aws_advanced_python_wrapper.cluster_topology_monitor import \
    GlobalAuroraTopologyMonitor
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_list_provider import \
    GlobalAuroraTopologyUtils
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def plugin_service_mock():
    mock = MagicMock()
    mock.force_connect.return_value = MagicMock()
    mock.driver_dialect = MagicMock()
    return mock


@pytest.fixture
def global_topology_utils_mock():
    mock = MagicMock(spec=GlobalAuroraTopologyUtils)
    mock.query_for_topology_with_regions.return_value = (
        HostInfo("writer.us-east-2.com", 5432, HostRole.WRITER),
        HostInfo("reader1.us-east-2.com", 5432, HostRole.READER),
        HostInfo("reader2.ap-south-1.com", 5432, HostRole.READER)
    )
    mock.get_region.return_value = "us-east-2"
    return mock


@pytest.fixture
def instance_templates_by_region():
    return {
        "us-east-2": HostInfo("?.cluster-id.us-east-2.rds.amazonaws.com", 5432),
        "ap-south-1": HostInfo("?.cluster-id.ap-south-1.rds.amazonaws.com", 5432)
    }


@pytest.fixture
def monitor_properties():
    props = Properties()
    WrapperProperties.TOPOLOGY_REFRESH_MS.set(props, "1000")
    WrapperProperties.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.set(props, "100")
    return props


@pytest.fixture
def global_monitor(plugin_service_mock, global_topology_utils_mock, monitor_properties, instance_templates_by_region):
    cluster_id = "test-global-cluster"
    initial_host = HostInfo("writer.us-east-2.com", 5432, HostRole.WRITER)
    instance_template = HostInfo("?.cluster-id.us-east-2.rds.amazonaws.com", 5432)
    refresh_rate_ns = 1000 * 1_000_000
    high_refresh_rate_ns = 100 * 1_000_000

    with patch('threading.Thread'):
        monitor = GlobalAuroraTopologyMonitor(
            plugin_service_mock, global_topology_utils_mock, cluster_id,
            initial_host, monitor_properties, instance_template,
            refresh_rate_ns, high_refresh_rate_ns, instance_templates_by_region
        )
        monitor._stop.set()
        return monitor


class TestGlobalAuroraTopologyMonitor:
    def test_init_stores_instance_templates_by_region(self, global_monitor, instance_templates_by_region):
        assert global_monitor._instance_templates_by_region == instance_templates_by_region

    def test_init_stores_global_topology_utils(self, global_monitor, global_topology_utils_mock):
        assert global_monitor._global_topology_utils == global_topology_utils_mock

    def test_query_for_topology_calls_query_with_regions(self, global_monitor, global_topology_utils_mock, instance_templates_by_region):
        mock_conn = MagicMock()

        result = global_monitor._query_for_topology(mock_conn)

        global_topology_utils_mock.query_for_topology_with_regions.assert_called_once_with(
            mock_conn, instance_templates_by_region)
        assert len(result) == 3

    def test_query_for_topology_returns_empty_tuple_on_none(self, global_monitor, global_topology_utils_mock):
        mock_conn = MagicMock()
        global_topology_utils_mock.query_for_topology_with_regions.return_value = None

        result = global_monitor._query_for_topology(mock_conn)

        assert result == ()

    def test_get_instance_template_returns_region_specific_template(self, global_monitor, global_topology_utils_mock, instance_templates_by_region):
        mock_conn = MagicMock()
        global_topology_utils_mock.get_region.return_value = "ap-south-1"

        result = global_monitor._get_instance_template("instance-id", mock_conn)

        assert result == instance_templates_by_region["ap-south-1"]
        global_topology_utils_mock.get_region.assert_called_once_with("instance-id", mock_conn)

    def test_get_instance_template_falls_back_to_default(self, global_monitor, global_topology_utils_mock):
        mock_conn = MagicMock()
        global_topology_utils_mock.get_region.return_value = None

        result = global_monitor._get_instance_template("instance-id", mock_conn)

        assert result == global_monitor._instance_template

    def test_get_instance_template_raises_error_for_unknown_region(self, global_monitor, global_topology_utils_mock):
        mock_conn = MagicMock()
        global_topology_utils_mock.get_region.return_value = "eu-west-1"

        with pytest.raises(AwsWrapperError) as exc_info:
            global_monitor._get_instance_template("instance-id", mock_conn)

        assert "eu-west-1" in str(exc_info.value)

    def test_get_instance_template_uses_us_east_2_template(self, global_monitor, global_topology_utils_mock, instance_templates_by_region):
        mock_conn = MagicMock()
        global_topology_utils_mock.get_region.return_value = "us-east-2"

        result = global_monitor._get_instance_template("instance-id", mock_conn)

        assert result == instance_templates_by_region["us-east-2"]
        assert "us-east-2" in result.host
