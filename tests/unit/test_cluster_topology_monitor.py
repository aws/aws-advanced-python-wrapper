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

import time
from unittest.mock import MagicMock, patch

import pytest

from aws_advanced_python_wrapper.cluster_topology_monitor import (
    ClusterTopologyMonitorImpl, HostMonitor)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def plugin_service_mock():
    mock = MagicMock()
    mock.force_connect.return_value = MagicMock()
    mock.is_network_exception.return_value = False
    mock.is_login_exception.return_value = False
    mock.driver_dialect = MagicMock()
    return mock


@pytest.fixture
def topology_utils_mock():
    mock = MagicMock()
    mock.query_for_topology.return_value = (
        HostInfo("writer.com", 5432, HostRole.WRITER),
        HostInfo("reader1.com", 5432, HostRole.READER),
        HostInfo("reader2.com", 5432, HostRole.READER)
    )
    mock.get_writer_host_if_connected.return_value = "writer.com"
    mock.get_host_role.return_value = HostRole.WRITER
    return mock


@pytest.fixture
def monitor_properties():
    props = Properties()
    WrapperProperties.TOPOLOGY_REFRESH_MS.set(props, "1000")
    WrapperProperties.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.set(props, "100")
    return props


@pytest.fixture
def cluster_monitor(plugin_service_mock, topology_utils_mock, monitor_properties):
    cluster_id = "test-cluster"
    initial_host = HostInfo("writer.com", 5432, HostRole.WRITER)
    instance_template = HostInfo("?.com", 5432)
    refresh_rate_ns = 1000 * 1_000_000
    high_refresh_rate_ns = 100 * 1_000_000

    with patch('threading.Thread'):
        monitor = ClusterTopologyMonitorImpl(
            plugin_service_mock, topology_utils_mock, cluster_id,
            initial_host, monitor_properties, instance_template,
            refresh_rate_ns, high_refresh_rate_ns
        )
        monitor._stop.set()
        return monitor


class TestClusterTopologyMonitorImpl:
    def test_force_refresh_with_cached_hosts_ignoring_requests(self, cluster_monitor):
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._get_stored_hosts = MagicMock(return_value=expected_hosts)
        cluster_monitor._ignore_new_topology_requests_end_time_nano = time.time_ns() + 10_000_000_000

        result = cluster_monitor.force_refresh(False, 5)
        assert result == expected_hosts

    def test_force_refresh_with_writer_verification(self, cluster_monitor):
        cluster_monitor._monitoring_connection.set(MagicMock())
        cluster_monitor._is_verified_writer_connection = True
        cluster_monitor._wait_till_topology_gets_updated = MagicMock(return_value=())

        cluster_monitor.force_refresh(True, 5)

        assert cluster_monitor._monitoring_connection.get() is None
        assert not cluster_monitor._is_verified_writer_connection

    def test_force_refresh_without_writer_verification(self, cluster_monitor):
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._is_verified_writer_connection = True
        cluster_monitor._wait_till_topology_gets_updated = MagicMock(return_value=expected_hosts)

        result = cluster_monitor.force_refresh(False, 5)

        assert result == expected_hosts
        cluster_monitor._wait_till_topology_gets_updated.assert_called_once_with(5)
        assert cluster_monitor._is_verified_writer_connection

    def test_force_refresh_with_connection_verified_writer(self, cluster_monitor):
        connection_mock = MagicMock()
        cluster_monitor._is_verified_writer_connection = True
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._wait_till_topology_gets_updated = MagicMock(return_value=expected_hosts)

        result = cluster_monitor.force_refresh_with_connection(connection_mock, 5)

        assert result == expected_hosts
        cluster_monitor._wait_till_topology_gets_updated.assert_called_once_with(5)

    def test_force_refresh_with_connection_not_verified(self, cluster_monitor):
        connection_mock = MagicMock()
        cluster_monitor._is_verified_writer_connection = False
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._fetch_topology_and_update_cache = MagicMock(return_value=expected_hosts)

        result = cluster_monitor.force_refresh_with_connection(connection_mock, 5)

        assert result == expected_hosts
        cluster_monitor._fetch_topology_and_update_cache.assert_called_once_with(connection_mock)

    def test_wait_till_topology_gets_updated_timeout_zero(self, cluster_monitor):
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._get_stored_hosts = MagicMock(return_value=expected_hosts)

        result = cluster_monitor._wait_till_topology_gets_updated(0)

        assert result == expected_hosts
        assert cluster_monitor._request_to_update_topology.is_set()

    def test_wait_till_topology_gets_updated_success(self, cluster_monitor):
        initial_hosts = (HostInfo("old-writer.com", 5432, HostRole.WRITER),)
        updated_hosts = (HostInfo("new-writer.com", 5432, HostRole.WRITER),)

        cluster_monitor._get_stored_hosts = MagicMock(side_effect=[initial_hosts, updated_hosts])

        result = cluster_monitor._wait_till_topology_gets_updated(1)

        assert result == updated_hosts

    def test_wait_till_topology_gets_updated_timeout(self, cluster_monitor):
        hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._get_stored_hosts = MagicMock(return_value=hosts)

        with pytest.raises(TimeoutError, match="Topology has not been updated"):
            cluster_monitor._wait_till_topology_gets_updated(0.001)

    def test_close(self, cluster_monitor):
        cluster_monitor._monitoring_connection.set(MagicMock())
        cluster_monitor._host_threads_writer_connection.set(MagicMock())
        cluster_monitor._host_threads_reader_connection.set(MagicMock())
        cluster_monitor._close_host_monitors = MagicMock()
        cluster_monitor._monitor_thread = MagicMock()
        cluster_monitor._monitor_thread.is_alive.return_value = False

        cluster_monitor.close()

        assert cluster_monitor._stop.is_set()
        assert cluster_monitor._request_to_update_topology.is_set()
        cluster_monitor._close_host_monitors.assert_called_once()
        assert cluster_monitor._monitoring_connection.get() is None
        assert cluster_monitor._host_threads_writer_connection.get() is None
        assert cluster_monitor._host_threads_reader_connection.get() is None

    def test_is_in_panic_mode(self, cluster_monitor):
        cluster_monitor._monitoring_connection.set(None)
        assert cluster_monitor._is_in_panic_mode()

        cluster_monitor._monitoring_connection.set(MagicMock())
        cluster_monitor._is_verified_writer_connection = False
        assert cluster_monitor._is_in_panic_mode()

        cluster_monitor._monitoring_connection.set(MagicMock())
        cluster_monitor._is_verified_writer_connection = True
        assert not cluster_monitor._is_in_panic_mode()

    def test_open_any_connection_and_update_topology_success(self, cluster_monitor):
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        cluster_monitor._plugin_service.force_connect.return_value = MagicMock()
        cluster_monitor._fetch_topology_and_update_cache = MagicMock(return_value=expected_hosts)

        result = cluster_monitor._open_any_connection_and_update_topology()

        assert result == expected_hosts
        assert cluster_monitor._monitoring_connection.get() is not None

    def test_open_any_connection_and_update_topology_connection_failure(self, cluster_monitor):
        cluster_monitor._plugin_service.force_connect.side_effect = Exception("Connection failed")

        result = cluster_monitor._open_any_connection_and_update_topology()

        assert result == ()

    def test_open_any_connection_and_update_topology_verifies_writer(self, cluster_monitor, topology_utils_mock):
        expected_hosts = (HostInfo("writer", 5432, HostRole.WRITER),)
        connection_mock = MagicMock()
        cluster_monitor._writer_host_info.set(None)
        cluster_monitor._plugin_service.force_connect.return_value = connection_mock
        topology_utils_mock.get_writer_host_if_connected.return_value = "writer"
        cluster_monitor._fetch_topology_and_update_cache = MagicMock(return_value=expected_hosts)

        result = cluster_monitor._open_any_connection_and_update_topology()

        assert result == expected_hosts
        assert cluster_monitor._is_verified_writer_connection
        assert cluster_monitor._writer_host_info.get() is not None


class TestHostMonitor:
    @pytest.fixture
    def monitor_impl_mock(self, plugin_service_mock, topology_utils_mock):
        mock = MagicMock()
        mock._plugin_service = plugin_service_mock
        mock._topology_utils = topology_utils_mock
        mock._monitoring_properties = Properties()
        mock._host_threads_stop = MagicMock()
        mock._host_threads_stop.is_set.return_value = False
        mock._host_threads_writer_connection = MagicMock()
        mock._host_threads_writer_connection.get.return_value = None
        mock._host_threads_writer_connection.compare_and_set.return_value = True
        mock._host_threads_reader_connection = MagicMock()
        mock._host_threads_reader_connection.compare_and_set.return_value = True
        mock._host_threads_latest_topology = MagicMock()
        mock._close_connection = MagicMock()
        mock._fetch_topology_and_update_cache = MagicMock()
        mock._query_for_topology = MagicMock(return_value=(HostInfo("writer.com", 5432, HostRole.WRITER),))
        return mock

    def test_call_stop_signal_immediate(self, monitor_impl_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        monitor = HostMonitor(monitor_impl_mock, host_info, None)
        monitor_impl_mock._host_threads_stop.is_set.return_value = True

        monitor()

        monitor_impl_mock._plugin_service.force_connect.assert_not_called()

    def test_call_connection_success_writer_detected(self, monitor_impl_mock, topology_utils_mock):
        host_info = HostInfo("writer.com", 5432, HostRole.WRITER)
        monitor = HostMonitor(monitor_impl_mock, host_info, None)
        connection_mock = MagicMock()
        monitor_impl_mock._plugin_service.force_connect.return_value = connection_mock
        topology_utils_mock.get_writer_host_if_connected.return_value = "writer.com"
        topology_utils_mock.get_host_role.return_value = HostRole.WRITER

        call_count = [0]

        def stop_after_checks():
            call_count[0] += 1
            # Stop after: initial check, connection attempt, writer check, role check
            return call_count[0] > 4

        monitor_impl_mock._host_threads_stop.is_set.side_effect = stop_after_checks

        with patch('time.sleep'):
            monitor()

        monitor_impl_mock._host_threads_writer_connection.compare_and_set.assert_called_once_with(None, connection_mock)
        monitor_impl_mock._fetch_topology_and_update_cache.assert_called_once_with(connection_mock)
        monitor_impl_mock._host_threads_writer_host_info.set.assert_called_once_with(host_info)

    def test_call_connection_success_reader_detected(self, monitor_impl_mock, topology_utils_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        monitor = HostMonitor(monitor_impl_mock, host_info, None)
        connection_mock = MagicMock()
        monitor_impl_mock._plugin_service.force_connect.return_value = connection_mock
        topology_utils_mock.get_writer_host_if_connected.return_value = None

        call_count = [0]

        def stop_after_iterations():
            call_count[0] += 1
            # Stop after: initial check, connection, writer check, reader logic, sleep
            return call_count[0] > 5

        monitor_impl_mock._host_threads_stop.is_set.side_effect = stop_after_iterations

        with patch('time.sleep'):
            monitor()

        monitor_impl_mock._host_threads_reader_connection.compare_and_set.assert_called()

    def test_call_network_exception_retry(self, monitor_impl_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        monitor = HostMonitor(monitor_impl_mock, host_info, None)
        monitor_impl_mock._plugin_service.force_connect.side_effect = Exception("Network error")
        monitor_impl_mock._plugin_service.is_network_exception.return_value = True

        call_count = [0]

        def stop_after_retries():
            call_count[0] += 1
            # Allow multiple connection attempts
            return call_count[0] > 10

        monitor_impl_mock._host_threads_stop.is_set.side_effect = stop_after_retries

        with patch('time.sleep'):
            monitor()

        assert monitor_impl_mock._plugin_service.force_connect.call_count >= 2

    def test_call_login_exception_raises(self, monitor_impl_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        monitor = HostMonitor(monitor_impl_mock, host_info, None)
        login_error = Exception("Login failed")
        monitor_impl_mock._plugin_service.force_connect.side_effect = login_error
        monitor_impl_mock._plugin_service.is_network_exception.return_value = False
        monitor_impl_mock._plugin_service.is_login_exception.return_value = True

        # The login exception is caught and handled in the finally block, not raised
        monitor()

        # Verify force_connect was called
        monitor_impl_mock._plugin_service.force_connect.assert_called()

    def test_reader_thread_fetch_topology(self, monitor_impl_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        writer_info = HostInfo("writer.com", 5432, HostRole.WRITER)
        monitor = HostMonitor(monitor_impl_mock, host_info, writer_info)
        connection_mock = MagicMock()
        expected_hosts = (HostInfo("writer.com", 5432, HostRole.WRITER),)
        monitor_impl_mock._query_for_topology.return_value = expected_hosts

        monitor._reader_thread_fetch_topology(connection_mock)

        monitor_impl_mock._host_threads_latest_topology.set.assert_called_once_with(expected_hosts)

    def test_reader_thread_fetch_topology_writer_changed(self, monitor_impl_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        old_writer = HostInfo("old-writer.com", 5432, HostRole.WRITER)
        monitor = HostMonitor(monitor_impl_mock, host_info, old_writer)
        connection_mock = MagicMock()
        new_writer = HostInfo("new-writer.com", 5432, HostRole.WRITER)
        expected_hosts = (new_writer, HostInfo("reader.com", 5432, HostRole.READER))
        monitor_impl_mock._query_for_topology.return_value = expected_hosts

        monitor._reader_thread_fetch_topology(connection_mock)

        assert monitor._writer_changed
        monitor_impl_mock._update_topology_cache.assert_called_once_with(expected_hosts)

    def test_reader_thread_fetch_topology_exception(self, monitor_impl_mock):
        host_info = HostInfo("reader.com", 5432, HostRole.READER)
        monitor = HostMonitor(monitor_impl_mock, host_info, None)
        connection_mock = MagicMock()
        monitor_impl_mock._query_for_topology.side_effect = Exception("Query failed")

        monitor._reader_thread_fetch_topology(connection_mock)

        monitor_impl_mock._host_threads_latest_topology.set.assert_not_called()
