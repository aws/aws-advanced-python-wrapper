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

from aws_advanced_python_wrapper.connection_provider import (
    ConnectionProviderManager, DriverConnectionProvider)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def connection_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def default_provider_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def set_provider_mock(mocker):
    # In Python 3.12+, isinstance checks with Protocols are stricter
    # Create a real instance that implements the protocol, then wrap release_resources
    # with a mock to allow method call tracking
    class MockConnectionProviderWithRelease:
        """Mock connection provider that implements CanReleaseResources protocol."""
        def accepts_host_info(self, host_info, props):
            return True
        
        def accepts_strategy(self, role, strategy):
            return True
        
        def get_host_info_by_strategy(self, hosts, role, strategy, props):
            return hosts[0] if hosts else None
        
        def connect(self, target_func, driver_dialect, database_dialect, host_info, props):
            return None
        
        def release_resources(self):
            pass
    
    # Create a real instance
    provider = MockConnectionProviderWithRelease()
    # Replace release_resources with a MagicMock so we can assert it was called
    provider.release_resources = mocker.MagicMock()
    return provider


@pytest.fixture
def host_info():
    return HostInfo("localhost")


@pytest.fixture
def properties():
    return Properties()


@pytest.fixture
def mock_driver_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_database_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture(autouse=True)
def reset_provider():
    ConnectionProviderManager._conn_provider = None
    yield
    ConnectionProviderManager._conn_provider = None


def test_provider_accepts_all_host_infos(connection_mock, mock_driver_dialect):
    connection_mock.connect.return_value = "Test"
    connection_provider = DriverConnectionProvider()

    host_info = HostInfo("localhost")
    properties = Properties()
    assert connection_provider.accepts_host_info(host_info, properties) is True

    host_info2 = HostInfo("abc123")
    assert connection_provider.accepts_host_info(host_info2, properties) is True


def test_provider_accepts_random_strategy(connection_mock, mock_driver_dialect):
    connection_mock.connect.return_value = "Test"
    connection_provider = DriverConnectionProvider()

    assert connection_provider.accepts_strategy(HostRole.READER, "random") is True
    assert connection_provider.accepts_strategy(HostRole.READER, "other") is False


def test_provider_returns_host_info(connection_mock, mock_driver_dialect):
    connection_mock.connect.return_value = "Test"
    connection_provider = DriverConnectionProvider()

    host_info_list = [HostInfo("localhost", role=HostRole.WRITER), HostInfo("other", role=HostRole.READER)]
    properties = Properties({"test_prop": 5})
    host_info = connection_provider.get_host_info_by_strategy(host_info_list, HostRole.WRITER, "random", properties)

    assert host_info.host == "localhost"


def test_provider_returns_connection(connection_mock, mock_driver_dialect, mock_database_dialect):
    host_info = HostInfo("localhost", 1234)
    properties = Properties({"test_prop": 5})

    connection_mock.connect.return_value = "Test"
    mock_driver_dialect.prepare_connect_info.return_value = {"test_prop": 5, "host": "localhost", "port": "1234"}
    connection_provider = DriverConnectionProvider()

    connection_provider.connect(
        connection_mock.connect, mock_driver_dialect, mock_database_dialect, host_info, properties)

    mock_driver_dialect.prepare_connect_info.assert_called_with(host_info, properties)
    connection_mock.connect.assert_called_with(test_prop=5, host="localhost", port="1234")


def test_manager_provides_default_provider(connection_mock, default_provider_mock, host_info, properties):
    connection_provider_manager = ConnectionProviderManager(default_provider_mock)
    provider = connection_provider_manager.get_connection_provider(host_info, properties)

    assert provider == default_provider_mock


def test_manager_provides_set_provider(connection_mock, default_provider_mock, set_provider_mock, host_info,
                                       properties):
    connection_provider_manager = ConnectionProviderManager(default_provider_mock)
    ConnectionProviderManager.set_connection_provider(set_provider_mock)
    provider = connection_provider_manager.get_connection_provider(host_info, properties)

    assert provider == set_provider_mock


def test_manager_defaults_when_host_not_supported(connection_mock, default_provider_mock, set_provider_mock, host_info,
                                                  properties):
    set_provider_mock.accepts_host_info.return_value = False

    connection_provider_manager = ConnectionProviderManager(default_provider_mock)
    ConnectionProviderManager.set_connection_provider(set_provider_mock)
    provider = connection_provider_manager.get_connection_provider(host_info, properties)

    assert provider == default_provider_mock


def test_manager_accepts_strategy(connection_mock, default_provider_mock, set_provider_mock):
    set_provider_mock.accepts_strategy.return_value = True
    default_provider_mock.accepts_strategy.return_value = True

    connection_provider_manager = ConnectionProviderManager(default_provider_mock)
    ConnectionProviderManager.set_connection_provider(set_provider_mock)
    result = connection_provider_manager.accepts_strategy(HostRole.WRITER, "random")
    assert result

    set_provider_mock.accepts_strategy.return_value = False
    result = connection_provider_manager.accepts_strategy(HostRole.WRITER, "random")
    assert result

    default_provider_mock.accepts_strategy.return_value = False
    result = connection_provider_manager.accepts_strategy(HostRole.WRITER, "random")
    assert not result


def test_manager_get_host_info_by_strategy(connection_mock, default_provider_mock, set_provider_mock):
    set_provider_mock.accepts_strategy.return_value = True

    host_info_list = [HostInfo("localhost", role=HostRole.WRITER), HostInfo("other", role=HostRole.WRITER)]
    properties = Properties({"test_prop": 5})
    set_provider_mock.get_host_info_by_strategy.return_value = host_info_list[0]
    default_provider_mock.get_host_info_by_strategy.return_value = host_info_list[1]

    connection_provider_manager = ConnectionProviderManager(default_provider_mock)
    ConnectionProviderManager.set_connection_provider(set_provider_mock)
    host_info = connection_provider_manager.get_host_info_by_strategy(host_info_list, HostRole.WRITER, "random", properties)
    assert host_info.host == "localhost"

    set_provider_mock.accepts_strategy.return_value = False
    host_info = connection_provider_manager.get_host_info_by_strategy(host_info_list, HostRole.WRITER, "random", properties)
    assert host_info.host == "other"


def test_release_resources(connection_mock, default_provider_mock, set_provider_mock):
    connection_provider_manager = ConnectionProviderManager(default_provider_mock)
    ConnectionProviderManager.set_connection_provider(set_provider_mock)
    ConnectionProviderManager.release_resources()

    set_provider_mock.release_resources.assert_called_once()
