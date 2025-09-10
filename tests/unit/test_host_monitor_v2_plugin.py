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

from queue import Queue
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_monitoring_v2_plugin import (
    HostMonitoringV2Plugin, HostMonitorV2, MonitoringContext)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249 import Error
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryCounter

FAILURE_DETECTION_TIME_MS = 1000
FAILURE_DETECTION_INTERVAL_MS = 5000
FAILURE_DETECTION_COUNT = 3


@pytest.fixture
def mock_driver_dialect(mocker):
    driver_dialect_mock = mocker.MagicMock()
    driver_dialect_mock.supports_socket_timeout.return_value = True
    driver_dialect_mock.is_closed.return_value = True
    return driver_dialect_mock


@pytest.fixture
def mock_plugin_service(mocker, mock_driver_dialect, mock_conn, host_info):
    service_mock = mocker.MagicMock()
    service_mock.current_connection = mock_conn
    service_mock.current_host_info = host_info
    type(service_mock).driver_dialect = mocker.PropertyMock(return_value=mock_driver_dialect)
    return service_mock


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_execute_func(mocker):
    return mocker.MagicMock()


@pytest.fixture
def monitoring_context(mock_conn):
    return MonitoringContext(mock_conn)


@pytest.fixture
def mock_monitor_service(mocker, monitoring_context):
    monitor_service_mock = mocker.MagicMock()
    monitor_service_mock.start_monitoring.return_value = monitoring_context
    return monitor_service_mock


@pytest.fixture
def mock_telemetry_counter():
    return MagicMock(spec=TelemetryCounter)


@pytest.fixture
def host_info():
    return HostInfo("my_database.cluster-xyz.us-east-2.rds.amazonaws.com")


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def host_monitor(
        mock_plugin_service,
        host_info,
        props,
        mock_telemetry_counter):
    return HostMonitorV2(
        plugin_service=mock_plugin_service,
        host_info=host_info,
        props=props,
        failure_detection_time_ms=FAILURE_DETECTION_TIME_MS,
        failure_detection_interval_ms=FAILURE_DETECTION_INTERVAL_MS,
        failure_detection_count=FAILURE_DETECTION_COUNT,
        aborted_connection_counter=mock_telemetry_counter
    )


@pytest.fixture
def plugin(mock_plugin_service, props, mock_monitor_service):
    return init_plugin(mock_plugin_service, props, mock_monitor_service)


def init_plugin(plugin_service, props, mock_monitor_service):
    plugin = HostMonitoringV2Plugin(plugin_service, props)
    plugin._monitor_service = mock_monitor_service
    return plugin


def test_init_no_query_timeout(mock_plugin_service, mock_driver_dialect, props):
    mock_driver_dialect.supports_abort_connection.return_value = False

    with pytest.raises(AwsWrapperError):
        HostMonitoringV2Plugin(mock_plugin_service, props)


def test_execute_null_connection_info(
        mocker, plugin, mock_plugin_service, mock_monitor_service, props, mock_execute_func, mock_conn):
    mock_plugin_service.current_connection = None
    with pytest.raises(AwsWrapperError):
        plugin.execute(mocker.MagicMock(), "Cursor.execute", mock_execute_func, "SELECT 1")
    mock_execute_func.assert_not_called()

    mock_plugin_service.current_connection = mock_conn
    mock_plugin_service.current_host_info = None
    with pytest.raises(AwsWrapperError):
        plugin.execute(mocker.MagicMock(), "Cursor.execute", mock_execute_func, "SELECT 1")
    mock_execute_func.assert_not_called()


def test_execute_monitoring_disabled(mocker, mock_plugin_service, props, mock_monitor_service, mock_execute_func):
    WrapperProperties.FAILURE_DETECTION_ENABLED.set(props, "False")
    plugin = init_plugin(mock_plugin_service, props, mock_monitor_service)
    plugin.execute(mocker.MagicMock(), "Cursor.execute", mock_execute_func, "SELECT 1")

    mock_monitor_service.start_monitoring.assert_not_called()
    mock_monitor_service.stop_monitoring.assert_not_called()
    mock_execute_func.assert_called_once()


def test_execute_non_network_method(mocker, plugin, mock_execute_func):
    mock_monitor_service = mocker.MagicMock()
    mock_monitor_service.network_bound_methods = {"foo"}
    plugin.execute(mocker.MagicMock(), "Connection.cancel", mock_execute_func)

    mock_monitor_service.start_monitoring.assert_not_called()
    mock_monitor_service.stop_monitoring.assert_not_called()
    mock_execute_func.assert_called_once()


def test_execute_monitoring_enabled(mocker, plugin, mock_monitor_service, mock_execute_func):
    plugin.execute(mocker.MagicMock(), "Cursor.execute", mock_execute_func, "SELECT 1")

    mock_monitor_service.start_monitoring.assert_called_once()
    mock_monitor_service.stop_monitoring.assert_called_once()
    mock_execute_func.assert_called_once()


def test_connect(mocker, plugin, host_info, props, mock_conn, mock_plugin_service):
    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    connection = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)
    mock_plugin_service.fill_aliases.assert_called_once()
    assert mock_conn == connection


def test_get_monitoring_host_info_errors(mocker, plugin, mock_plugin_service):
    mock_plugin_service.identify_connection.return_value = None

    with pytest.raises(AwsWrapperError):
        plugin._get_monitoring_host_info()

    expected_exception = Error()
    mock_plugin_service.identify_connection.return_value = mocker.MagicMock()
    mock_plugin_service.identify_connection.side_effect = expected_exception
    with pytest.raises(AwsWrapperError) as exc_info:
        plugin._get_monitoring_host_info()

    assert expected_exception == exc_info.value.__cause__


def test_release_resources(plugin, mock_monitor_service):
    plugin.release_resources()
    mock_monitor_service.release_resources.assert_called_once()
    assert plugin._monitor_service is None


def test_set_node_unhealthy(monitoring_context):
    assert monitoring_context._host_unhealthy.get() is False
    monitoring_context.set_host_unhealthy()
    assert monitoring_context._host_unhealthy.get() is True


def test_should_abort_when_healthy(monitoring_context):
    assert monitoring_context.should_abort() is False


def test_should_abort_when_unhealthy(monitoring_context):
    monitoring_context.set_host_unhealthy()
    assert monitoring_context.should_abort() is True


def test_should_abort_when_inactive(monitoring_context):
    monitoring_context.set_host_unhealthy()
    monitoring_context.set_inactive()
    assert monitoring_context.should_abort() is False


def test_set_inactive(monitoring_context):
    monitoring_context.set_inactive()
    assert monitoring_context.get_connection() is None
    assert monitoring_context.is_active() is False


def test_get_connection_when_active(monitoring_context, mock_conn):
    assert monitoring_context.get_connection() is mock_conn


def test_get_connection_when_inactive(monitoring_context):
    monitoring_context.set_inactive()
    assert monitoring_context.get_connection() is None


def test_is_active_when_active(monitoring_context):
    assert monitoring_context.is_active() is True


def test_is_active_when_inactive(monitoring_context):
    monitoring_context.set_inactive()
    assert monitoring_context.is_active() is False


def test_can_dispose_none_empty_active_context(host_monitor):
    assert host_monitor.can_dispose() is True

    host_monitor._active_contexts.put(MagicMock())
    assert host_monitor.can_dispose() is False


def test_can_dispose_none_new_contexts_context(host_monitor):
    assert host_monitor.can_dispose() is True

    host_monitor._new_contexts.compute_if_absent(1, lambda key: Queue())
    assert host_monitor.can_dispose() is False


def test_is_stopped(host_monitor):
    assert host_monitor.is_stopped is False
    host_monitor._is_stopped.set(True)
    assert host_monitor.is_stopped is True


def test_stop(host_monitor):
    host_monitor.stop()
    assert host_monitor.is_stopped is True


@patch.object(HostMonitorV2, '_is_host_available')
def test_check_connection_status_new_connection(
        mock_is_host_available,
        host_monitor,
        mock_plugin_service,
        mock_conn):
    mock_plugin_service.force_connect.return_value = mock_conn
    mock_is_host_available.return_value = True

    result = host_monitor.check_connection_status()

    assert result is True
    mock_plugin_service.force_connect.assert_called_once()
    assert host_monitor._monitoring_connection == mock_conn
    mock_is_host_available.assert_not_called()


@patch.object(HostMonitorV2, '_execute_conn_check')
def test_is_host_available_success(mock_execute, host_monitor, mock_conn):
    mock_execute.return_value = True
    assert host_monitor._is_host_available(mock_conn, 1.0) is True


@patch.object(HostMonitorV2, '_execute_conn_check')
def test_is_host_available_timeout(mock_execute, host_monitor, mock_conn):
    mock_execute.side_effect = TimeoutError()
    assert host_monitor._is_host_available(mock_conn, 1.0) is False


def test_execute_conn_check(mocker, host_monitor, mock_conn, mock_plugin_service):
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    host_monitor._execute_conn_check(mock_conn, 1.0)

    mock_plugin_service.driver_dialect.execute.assert_called_once()
    mock_cursor.fetchone.assert_called_once()


def test_update_node_health_status_multiple_failures(host_monitor):
    host_monitor._failure_count = 2
    host_monitor._invalid_host_start_time_ns = 10 ** 9

    host_monitor._update_host_health_status(False, 10 ** 9, 11 * 10 ** 9)

    assert host_monitor._is_unhealthy is True


def test_update_node_health_status_recovery(host_monitor):
    host_monitor._failure_count = 1
    host_monitor._invalid_host_start_time_ns = 10 ** 9

    host_monitor._update_host_health_status(True, 2 * 10 ** 9, 2.1 * 10 ** 9)

    assert host_monitor._failure_count == 0
    assert host_monitor._invalid_host_start_time_ns == 0
    assert host_monitor._is_unhealthy is False


def test_abort_connection_closed(host_monitor, mock_conn, mock_driver_dialect):
    mock_driver_dialect.is_closed.return_value = True
    host_monitor.abort_connection(mock_conn)
    mock_driver_dialect.abort_connection.assert_not_called()


def test_abort_connection_success(host_monitor, mock_conn, mock_driver_dialect):
    mock_driver_dialect.is_closed.return_value = False
    host_monitor.abort_connection(mock_conn)
    mock_driver_dialect.abort_connection.assert_called_once()


@patch('aws_advanced_python_wrapper.host_monitoring_v2_plugin.logger')
def test_abort_connection_failure(mock_logger, host_monitor, mock_conn, mock_driver_dialect):
    mock_driver_dialect.is_closed.return_value = False
    mock_driver_dialect.abort_connection.side_effect = AwsWrapperError("Error")
    host_monitor.abort_connection(mock_conn)
    mock_logger.debug.assert_called()
