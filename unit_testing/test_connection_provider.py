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


from unittest import TestCase
from unittest.mock import MagicMock

from aws_wrapper.connection_provider import (ConnectionProvider,
                                             ConnectionProviderManager,
                                             DriverConnectionProvider)
from aws_wrapper.hostspec import HostRole, HostSpec
from aws_wrapper.utils.properties import Properties


class TestDriverConnectionProvider(TestCase):
    def test_provider_accepts_all_host_specs(self):
        connection_mock = MagicMock()
        connection_mock.connect.return_value = "Test"
        connection_provider = DriverConnectionProvider(connection_mock.connect)

        host_spec = HostSpec("localhost")
        properties = Properties()
        connection_provider.accepts_host_spec(host_spec, properties)

        host_spec2 = HostSpec("abc123")
        connection_provider.accepts_host_spec(host_spec2, properties)

    def test_provider_accepts_random_strategy(self):
        connection_mock = MagicMock()
        connection_mock.connect.return_value = "Test"
        connection_provider = DriverConnectionProvider(connection_mock.connect)

        assert connection_provider.accepts_strategy(HostRole.READER, "random") is True
        assert connection_provider.accepts_strategy(HostRole.READER, "other") is False

    def test_provider_returns_host_spec(self):
        connection_mock = MagicMock()
        connection_mock.connect.return_value = "Test"
        connection_provider = DriverConnectionProvider(connection_mock.connect)

        host_spec_list = [HostSpec("localhost", role=HostRole.WRITER), HostSpec("other", role=HostRole.READER)]
        host_spec = connection_provider.get_host_spec_by_strategy(host_spec_list, HostRole.WRITER, "random")

        assert host_spec.host == "localhost"

    def test_provider_returns_connection(self):
        connection_mock = MagicMock()
        connection_mock.connect.return_value = "Test"
        connection_provider = DriverConnectionProvider(connection_mock.connect)

        host_spec = HostSpec("localhost", 1234)
        properties = Properties({"test_prop": 5})
        connection_provider.connect(host_spec, properties)

        connection_mock.connect.assert_called_with(test_prop=5, host="localhost", port="1234")


class TestConnectionProviderManager(TestCase):

    host_spec = HostSpec("localhost")
    properties = Properties()

    def test_manager_provides_default_provider(self):
        default_provider_mock: ConnectionProvider = MagicMock()

        connection_provider_manager = ConnectionProviderManager(default_provider_mock)
        provider = connection_provider_manager.get_connection_provider(self.host_spec, self.properties)

        assert provider == default_provider_mock

    def test_manager_provides_set_provider(self):
        default_provider_mock: ConnectionProvider = MagicMock()
        set_provider_mock: ConnectionProvider = MagicMock()

        connection_provider_manager = ConnectionProviderManager(default_provider_mock, set_provider_mock)
        provider = connection_provider_manager.get_connection_provider(self.host_spec, self.properties)

        assert provider == set_provider_mock

    def test_manager_defaults_when_host_not_supported(self):
        default_provider_mock: ConnectionProvider = MagicMock()
        set_provider_mock: ConnectionProvider = MagicMock()
        set_provider_mock.accepts_host_spec.return_value = False

        connection_provider_manager = ConnectionProviderManager(default_provider_mock, set_provider_mock)
        provider = connection_provider_manager.get_connection_provider(self.host_spec, self.properties)

        assert provider == default_provider_mock

    def test_manager_accepts_strategy(self):
        default_provider_mock: ConnectionProvider = MagicMock()
        set_provider_mock: ConnectionProvider = MagicMock()
        set_provider_mock.accepts_strategy.return_value = True
        default_provider_mock.accepts_strategy.return_value = True

        connection_provider_manager = ConnectionProviderManager(default_provider_mock, set_provider_mock)
        result = connection_provider_manager.accepts_strategy(HostRole.WRITER, "random")
        assert result

        set_provider_mock.accepts_strategy.return_value = False
        result = connection_provider_manager.accepts_strategy(HostRole.WRITER, "random")
        assert result

        default_provider_mock.accepts_strategy.return_value = False
        result = connection_provider_manager.accepts_strategy(HostRole.WRITER, "random")
        assert not result

    def test_manager_get_host_spec_by_strategy(self):
        default_provider_mock: ConnectionProvider = MagicMock()
        set_provider_mock: ConnectionProvider = MagicMock()

        set_provider_mock.accepts_strategy.return_value = True

        host_spec_list = [HostSpec("localhost", role=HostRole.WRITER), HostSpec("other", role=HostRole.WRITER)]
        set_provider_mock.get_host_spec_by_strategy.return_value = host_spec_list[0]
        default_provider_mock.get_host_spec_by_strategy.return_value = host_spec_list[1]

        connection_provider_manager = ConnectionProviderManager(default_provider_mock, set_provider_mock)
        host_spec = connection_provider_manager.get_host_spec_by_strategy(host_spec_list, HostRole.WRITER, "random")
        assert host_spec.host == "localhost"

        set_provider_mock.accepts_strategy.return_value = False
        host_spec = connection_provider_manager.get_host_spec_by_strategy(host_spec_list, HostRole.WRITER, "random")
        assert host_spec.host == "other"
