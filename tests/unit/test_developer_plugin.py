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

import psycopg
import pytest

from aws_advanced_python_wrapper.developer_plugin import \
    ExceptionSimulatorManager
from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginServiceImpl, PluginServiceManagerContainer)
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_connect_callback(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_driver_dialect(mocker, mock_conn):
    driver_dialect = mocker.MagicMock()
    driver_dialect.get_connection_from_obj.return_value = mock_conn
    driver_dialect.unwrap_connection.return_value = mock_conn
    return driver_dialect


@pytest.fixture
def mock_dialect_manager(mocker, mock_driver_dialect):
    dialect_manager = mocker.MagicMock()
    dialect_manager.get_pool_connection_driver_dialect.return_value = mock_driver_dialect
    return dialect_manager


@pytest.fixture
def container():
    return PluginServiceManagerContainer()


@pytest.fixture
def plugin_service(mocker, container, props, mock_dialect_manager, mock_driver_dialect):
    return PluginServiceImpl(container, props, mocker.MagicMock(), mock_dialect_manager, mock_driver_dialect)


@pytest.fixture
def props():
    return Properties({"host": "localhost", "plugins": "dev"})


@pytest.fixture
def plugin_manager(container, props):
    return PluginManager(container, props)


@pytest.fixture
def mock_connect_func(mocker):
    mock_connect_func = mocker.MagicMock()
    return mock_connect_func


@pytest.fixture
def conn_callback_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture(autouse=True)
def setup_container(container, plugin_service, plugin_manager):
    container.plugin_service = plugin_service
    container.plugin_manager = plugin_manager


def test_raise_exception(mocker, plugin_service, plugin_manager):
    exception: RuntimeError = RuntimeError("exception to raise")
    conn = AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)

    conn.cursor()

    ExceptionSimulatorManager.raise_exception_on_next_method(exception)
    with pytest.raises(RuntimeError, match="exception to raise"):
        conn.cursor()

    conn.cursor()


def test_raise_exception_for_method_name(mocker, plugin_service, plugin_manager):
    exception: RuntimeError = RuntimeError("exception to raise")
    conn = AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)

    conn.cursor()

    ExceptionSimulatorManager.raise_exception_on_next_method(exception, "Connection.cursor")
    with pytest.raises(RuntimeError, match="exception to raise"):
        conn.cursor()

    conn.cursor()


def test_raise_exception_for_wrong_method_name(mocker, plugin_service, plugin_manager):
    exception: RuntimeError = RuntimeError("exception to raise")
    conn = AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)

    conn.cursor()

    ExceptionSimulatorManager.raise_exception_on_next_method(exception, "Connection.commit")
    conn.cursor()


def test_raise_exception_on_connect(mocker, plugin_service, plugin_manager):
    exception: Exception = Exception("exception to raise")
    exception_simulator_manager = ExceptionSimulatorManager()
    exception_simulator_manager.raise_exception_on_next_connect(exception)
    with pytest.raises(Exception, match="exception to raise"):
        AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)

    AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)


def test_no_exception_on_connect_with_callback(mocker, mock_connect_callback, plugin_service, plugin_manager):
    exception_simulator_manager = ExceptionSimulatorManager()
    mock_connect_callback.get_exception_to_raise.return_value = None

    exception_simulator_manager.set_connect_callback(mock_connect_callback)

    AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)


def test_raise_exception_on_connect_with_callback(mocker, mock_connect_callback, plugin_service, plugin_manager):
    exception: Exception = Exception("exception to raise")
    exception_simulator_manager = ExceptionSimulatorManager()
    ExceptionSimulatorManager.connect_callback = mocker.MagicMock()
    mock_connect_callback.get_exception_to_raise.return_value = None

    exception_simulator_manager.raise_exception_on_next_connect(exception)
    exception_simulator_manager.set_connect_callback(mock_connect_callback)

    with pytest.raises(Exception, match="exception to raise"):
        AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)

    AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)
