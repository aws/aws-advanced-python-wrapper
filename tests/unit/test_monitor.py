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

from concurrent.futures import ThreadPoolExecutor, wait
from time import perf_counter_ns, sleep

import psycopg
import pytest

from aws_wrapper.host_monitoring_plugin import (Monitor, MonitoringContext,
                                                MonitoringThreadContainer)
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.utils.properties import Properties, WrapperProperties


@pytest.fixture
def host_info():
    return HostInfo("localhost")


@pytest.fixture
def props():
    return Properties({WrapperProperties.MONITOR_DISPOSAL_TIME_MS.name: "250", "monitoring-some_prop": "some_value"})


@pytest.fixture
def monitoring_conn_props(props):
    return Properties({WrapperProperties.MONITOR_DISPOSAL_TIME_MS.name: "250", "some_prop": "some_value"})


@pytest.fixture
def mock_monitoring_context(mocker):
    context = mocker.MagicMock()
    context.failure_detection_interval_ms = 500
    return context


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_driver_dialect(mocker):
    mock_driver_dialect = mocker.MagicMock()
    mock_driver_dialect.is_closed.return_value = False
    return mock_driver_dialect


@pytest.fixture
def mock_plugin_service(mocker, mock_conn, mock_driver_dialect):
    plugin_service = mocker.MagicMock()
    plugin_service.force_connect.return_value = mock_conn
    plugin_service.driver_dialect = mock_driver_dialect
    return plugin_service


@pytest.fixture
def mock_monitor_service(mocker):
    service = mocker.MagicMock()
    container = MonitoringThreadContainer()

    def release_monitor(monitor_to_release):
        container.release_monitor(monitor_to_release)

    service.notify_unused.side_effect = release_monitor
    return service


@pytest.fixture
def monitor(mock_plugin_service, mock_monitor_service, host_info, props):
    return Monitor(
        mock_plugin_service,
        host_info,
        props,
        mock_monitor_service)


@pytest.fixture(autouse=True)
def release_container():
    yield
    while MonitoringThreadContainer._instance is not None:
        MonitoringThreadContainer.release_instance()


def test_start_monitoring(monitor, mock_monitoring_context):
    current_time = perf_counter_ns()
    assert 0 != monitor._context_last_used_ns

    monitor.start_monitoring(mock_monitoring_context)
    mock_monitoring_context.set_monitor_start_time_ns.assert_called_once()
    assert monitor._context_last_used_ns > current_time
    assert mock_monitoring_context == monitor._new_contexts.get_nowait()


def test_stop_monitoring(monitor, mock_monitoring_context):
    current_time = perf_counter_ns()
    assert 0 != monitor._context_last_used_ns

    monitor.stop_monitoring(None)
    assert 0 != monitor._context_last_used_ns

    monitor.stop_monitoring(mock_monitoring_context)
    assert mock_monitoring_context.is_active is False
    assert monitor._context_last_used_ns > current_time


def test_clear_contexts(mocker, monitor):
    mock_new_context = mocker.MagicMock()
    mock_active_context = mocker.MagicMock()
    monitor._new_contexts.put(mock_new_context)
    monitor._active_contexts.put(mock_active_context)

    monitor.clear_contexts()
    assert monitor._new_contexts.empty()
    assert monitor._active_contexts.empty()
    assert 0 == monitor._new_contexts.unfinished_tasks
    assert 0 == monitor._active_contexts.unfinished_tasks


def test_run_host_available(
        mocker,
        monitor,
        host_info,
        monitoring_conn_props,
        mock_monitor_service,
        mock_plugin_service,
        mock_conn,
        mock_driver_dialect):
    remove_delays()
    host_alias = "host-1"
    container = MonitoringThreadContainer()
    container._monitor_map.put_if_absent(host_alias, monitor)
    container._tasks_map.put_if_absent(monitor, mocker.MagicMock())

    executor = ThreadPoolExecutor()
    context = MonitoringContext(monitor, mock_conn, mock_driver_dialect, 10, 1, 3)
    monitor.start_monitoring(context)

    future = executor.submit(monitor.run)
    sleep(0.1)  # Allow some time for the monitor to loop
    monitor.stop_monitoring(context)
    wait([future], 3)

    mock_plugin_service.force_connect.assert_called_once_with(host_info, monitoring_conn_props, None)
    mock_driver_dialect.abort_connection.assert_not_called()
    mock_monitor_service.notify_unused.assert_called_once()
    mock_conn.close.assert_called_once()
    assert context._is_host_unavailable is False
    assert monitor._is_stopped.is_set()
    assert container._monitor_map.get(host_alias) is None
    assert container._tasks_map.get(monitor) is None


def test_run_host_unavailable(
        mocker,
        monitor,
        host_info,
        monitoring_conn_props,
        mock_monitor_service,
        mock_plugin_service,
        mock_conn,
        mock_driver_dialect):
    remove_delays()
    executor = ThreadPoolExecutor()
    context = MonitoringContext(monitor, mock_conn, mock_driver_dialect, 30, 10, 3)

    mocker.patch("aws_wrapper.host_monitoring_plugin.Monitor._execute_conn_check", side_effect=TimeoutError())
    monitor.start_monitoring(context)
    future = executor.submit(monitor.run)
    wait([future], 3)

    mock_plugin_service.force_connect.assert_called_once_with(host_info, monitoring_conn_props, None)
    mock_driver_dialect.abort_connection.assert_called_once()
    mock_monitor_service.notify_unused.assert_called_once()
    mock_conn.close.assert_called_once()
    assert context._is_host_unavailable is True
    assert monitor._is_stopped.is_set()


def test_run__no_contexts(mocker, mock_monitor_service, monitor):
    host_alias = "host-1"
    container = MonitoringThreadContainer()
    container._monitor_map.put_if_absent(host_alias, monitor)
    container._tasks_map.put_if_absent(monitor, mocker.MagicMock())

    # Monitor should exit because there are no contexts
    monitor.run()

    assert container._monitor_map.get(host_alias) is None
    assert container._tasks_map.get(monitor) is None
    MonitoringThreadContainer.release_instance()


def test_check_connection_status__valid_then_invalid(mocker, monitor):
    mock_execute_conn_check = mocker.patch(
        "aws_wrapper.host_monitoring_plugin.Monitor._execute_conn_check", side_effect=[None, TimeoutError()])

    status = monitor._check_host_status(30)  # Initiate a monitoring connection
    assert status.is_available
    status = monitor._check_host_status(30)
    assert status.is_available
    status = monitor._check_host_status(30)
    assert not status.is_available
    assert 2 == mock_execute_conn_check.call_count


def test_check_connection_status__conn_check_throws_exception(mocker, monitor):
    mocker.patch("aws_wrapper.host_monitoring_plugin.Monitor._execute_conn_check", side_effect=Exception())

    status = monitor._check_host_status(30)  # Initiate a monitoring connection
    assert status.is_available
    status = monitor._check_host_status(30)
    assert not status.is_available


def remove_delays():
    Monitor._INACTIVE_SLEEP_MS = 0
    Monitor._MIN_HOST_CHECK_TIMEOUT_MS = 0
