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

from aws_advanced_python_wrapper.fastest_response_strategy_plugin import (
    MAX_VALUE, FastestResponseStrategyPlugin, HostResponseTimeService)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def writer_host():
    return HostInfo("instance-0", 5432, HostRole.WRITER,  HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host1() -> HostInfo:
    return HostInfo("instance-1", 5432, HostRole.READER,  HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host2():
    return HostInfo("instance-2", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host3():
    return HostInfo("instance-3", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def default_hosts(writer_host, reader_host2, reader_host3, reader_host1):
    return [writer_host, reader_host1, reader_host2, reader_host3]


@pytest.fixture
def mock_driver_dialect(mocker):
    driver_dialect_mock = mocker.MagicMock()
    driver_dialect_mock.is_closed.return_value = True
    return driver_dialect_mock


@pytest.fixture
def mock_plugin_service(mocker, mock_driver_dialect, mock_conn, host_info, default_hosts):
    service_mock = mocker.MagicMock()
    service_mock.current_connection = mock_conn
    service_mock.current_host_info = host_info
    service_mock.all_hosts = default_hosts

    type(service_mock).driver_dialect = mocker.PropertyMock(return_value=mock_driver_dialect)
    return service_mock


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def reader_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_execute_func(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_host_response_time_service(mocker):
    host_response_time_service_mock = mocker.MagicMock()
    return host_response_time_service_mock


@pytest.fixture
def host_info():
    return HostInfo(host="instance1", role=HostRole.READER)


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def plugin(mock_plugin_service, props, mock_host_response_time_service):
    return init_plugin(mock_plugin_service, props, mock_host_response_time_service)


def init_plugin(plugin_service, props, mock_host_response_time_service):
    plugin = FastestResponseStrategyPlugin(plugin_service, props)
    plugin._host_response_time_service = mock_host_response_time_service
    return plugin


def test_connect(mocker, plugin, host_info, props, mock_conn, mock_plugin_service):
    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    connection = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)
    mock_plugin_service.refresh_host_list.assert_called_once()
    assert mock_conn == connection


@pytest.mark.parametrize(
    "host_events",
    [{"instance-1.xyz.us-east-2.rds.amazonaws.com": {HostEvent.HOST_DELETED}},
     {"instance-1.xyz.us-east-2.rds.amazonaws.com": {HostEvent.WENT_DOWN}}])
def test_notify_host_list_changed(
        mocker, plugin, mock_plugin_service, host_events, mock_host_response_time_service, default_hosts):
    mock_host_info = mocker.MagicMock()
    mock_host_info.url = "instance-1.xyz.us-east-2.rds.amazonaws.com"
    mock_plugin_service.current_host_info = mock_host_info
    plugin.notify_host_list_changed(host_events)
    mock_host_response_time_service.set_hosts.assert_called_once_with(default_hosts)


def test_get_host_info_by_strategy_calculated_host(plugin, reader_host2, mock_host_response_time_service):
    mock_host_response_time_service.get_response_time.side_effect = [MAX_VALUE, 4000, 5000, 4000]

    # calculated host
    result_host = plugin.get_host_info_by_strategy(HostRole.READER, "fastest_response")
    assert result_host.host == reader_host2.host

    # cached host
    result_host = plugin.get_host_info_by_strategy(HostRole.READER, "fastest_response")
    assert result_host.host == reader_host2.host


def test_host_response_time_set_hosts(mock_plugin_service, reader_host1, reader_host2):
    hosts = (reader_host1, reader_host2)

    target = HostResponseTimeService(mock_plugin_service, props, 10_000)
    target.hosts = (reader_host1,)
    target.set_hosts(hosts)

    assert target.hosts == hosts
