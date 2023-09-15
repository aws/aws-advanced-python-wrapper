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
import psycopg
import pytest

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.host_monitoring_plugin import HostMonitoringPlugin
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.pep249 import Error
from aws_wrapper.utils.notifications import HostEvent
from aws_wrapper.utils.properties import Properties, WrapperProperties


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
    type(service_mock).target_driver_dialect = mocker.PropertyMock(return_value=mock_driver_dialect)
    return service_mock


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_execute_func(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_context(mocker):
    context_mock = mocker.MagicMock()
    context_mock.is_host_unavailable.return_value = False
    return context_mock


@pytest.fixture
def mock_monitor_service(mocker, mock_context):
    monitor_service_mock = mocker.MagicMock()
    monitor_service_mock.start_monitoring.return_value = mock_context
    return monitor_service_mock


@pytest.fixture
def host_info():
    return HostInfo("my_database.cluster-xyz.us-east-2.rds.amazonaws.com")


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def plugin(mock_plugin_service, props, mock_monitor_service):
    return init_plugin(mock_plugin_service, props, mock_monitor_service)


def init_plugin(plugin_service, props, mock_monitor_service):
    plugin = HostMonitoringPlugin(plugin_service, props)
    plugin._monitor_service = mock_monitor_service
    return plugin


def test_init_no_query_timeout(mock_plugin_service, mock_driver_dialect, props):
    mock_driver_dialect.supports_socket_timeout.return_value = False
    mock_driver_dialect.supports_tcp_keepalive.return_value = False

    with pytest.raises(AwsWrapperError):
        HostMonitoringPlugin(mock_plugin_service, props)


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


def test_execute_cleanup__error_checking_connection_status(
        mocker, plugin, mock_monitor_service, mock_execute_func, mock_context, mock_conn, mock_driver_dialect):
    mock_context.is_host_unavailable.return_value = True
    expected_exception = Error("Error checking connection status")
    mock_driver_dialect.is_closed.side_effect = expected_exception

    with pytest.raises(Error) as exc_info:
        plugin.execute(mocker.MagicMock(), "Cursor.execute", mock_execute_func, "SELECT 1")
    assert expected_exception == exc_info.value


def test_execute_cleanup__connection_not_closed(
        mocker, plugin, mock_monitor_service, mock_context, mock_conn, mock_execute_func, mock_driver_dialect):
    mock_context.is_host_unavailable.return_value = True
    mock_driver_dialect.is_closed.return_value = False

    with pytest.raises(AwsWrapperError):
        plugin.execute(mocker.MagicMock(), "Cursor.execute", mock_execute_func, "SELECT 1")


def test_connect(mocker, plugin, host_info, props, mock_conn, mock_plugin_service):
    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    connection = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)
    mock_plugin_service.fill_aliases.assert_called_once()
    assert mock_conn == connection


@pytest.mark.parametrize("host_events", [["host-1", HostEvent.HOST_DELETED], ["host-1", HostEvent.WENT_DOWN]])
def test_notify_host_list_changed(
        mocker, plugin, host_info, props, mock_conn, mock_plugin_service, mock_monitor_service, host_events):
    mock_host_info1 = mocker.MagicMock()
    mock_host_info2 = mocker.MagicMock()
    mock_host_info1.url = "instance-1.xyz.us-east-2.rds.amazonaws.com"
    mock_host_info2.url = "instance-2.xyz.us-east-2.rds.amazonaws.com"
    aliases1 = frozenset({"alias1", "alias2"})
    aliases2 = frozenset({"alias3", "alias4"})
    mock_host_info1.all_aliases = aliases1
    mock_host_info2.all_aliases = aliases2

    mock_plugin_service.current_host_info = mock_host_info1
    plugin.notify_host_list_changed(host_events)
    mock_monitor_service.stop_monitoring_host.assert_called_with(aliases1)

    mock_plugin_service.current_host_info = mock_host_info2
    plugin.notify_host_list_changed(host_events)
    mock_monitor_service.stop_monitoring_host.assert_called_with(aliases2)


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
