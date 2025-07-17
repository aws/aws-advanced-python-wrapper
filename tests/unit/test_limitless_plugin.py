#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from unittest.mock import PropertyMock

import psycopg
import pytest

from aws_advanced_python_wrapper.database_dialect import (DatabaseDialect,
                                                          MysqlDatabaseDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.limitless_plugin import LimitlessPlugin
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def mock_driver_dialect(mocker):
    driver_dialect_mock = mocker.MagicMock()
    driver_dialect_mock.is_closed.return_value = False
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
def mock_limitless_router_service(mocker):
    limitless_router_service_mock = mocker.MagicMock()
    return limitless_router_service_mock


@pytest.fixture
def host_info():
    return HostInfo(host="host-info", role=HostRole.READER)


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def plugin(mock_plugin_service, props, mock_limitless_router_service):
    plugin = LimitlessPlugin(mock_plugin_service, props)
    plugin._limitless_router_service = mock_limitless_router_service
    return plugin


def test_connect(mocker, plugin, host_info, props, mock_conn, mock_limitless_router_service):
    def replace_context_connection(invocation):
        context = invocation._connection_plugin._context
        context._connection = mock_conn
        return None

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = None
    plugin._limitless_router_service.establish_connection.side_effect = replace_context_connection

    connection = plugin.connect(mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)
    mock_connect_func.assert_not_called()
    mock_limitless_router_service.start_monitoring.assert_called_once_with(host_info, props)
    mock_limitless_router_service.establish_connection.assert_called_once()
    assert mock_conn == connection


def test_connect_none_connection(mocker, plugin, host_info, props, mock_conn, mock_limitless_router_service):
    def replace_context_connection_to_none(invocation):
        context = invocation._connection_plugin._context
        context._connection = None
        return None

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn
    plugin._limitless_router_service.establish_connection.side_effect = replace_context_connection_to_none

    with pytest.raises(Exception) as e_info:
        plugin.connect(mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)

    mock_connect_func.assert_not_called()
    mock_limitless_router_service.start_monitoring.assert_called_once_with(host_info, props)
    mock_limitless_router_service.establish_connection.assert_called_once()
    assert e_info.type == AwsWrapperError
    assert str(e_info.value) == Messages.get_formatted("LimitlessPlugin.FailedToConnectToHost", host_info.host)


def test_connect_unsupported_dialect(mocker, plugin, host_info, props, mock_conn, mock_plugin_service,
                                     mock_limitless_router_service):
    unsupported_dialect: DatabaseDialect = MysqlDatabaseDialect()
    mock_plugin_service.database_dialect = unsupported_dialect

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    with pytest.raises(Exception) as e_info:
        plugin.connect(mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)

    assert e_info.type == UnsupportedOperationError
    assert str(e_info.value) == Messages.get_formatted("LimitlessPlugin.UnsupportedDialectOrDatabase", type(unsupported_dialect).__name__)


def test_connect_supported_dialect_after_refresh(
    mocker, plugin, host_info, props, mock_conn, mock_plugin_service, mock_limitless_router_service, mock_driver_dialect
):
    unsupported_dialect: DatabaseDialect = MysqlDatabaseDialect()
    type(mock_plugin_service).database_dialect = PropertyMock(side_effect=[unsupported_dialect, mock_driver_dialect])

    def replace_context_connection(invocation):
        context = invocation._connection_plugin._context
        context._connection = mock_conn
        return None

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn
    plugin._limitless_router_service.establish_connection.side_effect = replace_context_connection

    connection = plugin.connect(mocker.MagicMock(), mocker.MagicMock(), host_info, props, True, mock_connect_func)
    mock_connect_func.assert_not_called()
    mock_limitless_router_service.start_monitoring.assert_called_once_with(host_info, props)
    mock_limitless_router_service.establish_connection.assert_called_once()
    assert mock_conn == connection
