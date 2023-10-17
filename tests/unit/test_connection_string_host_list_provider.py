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
#  limitations under the License.s

import pytest

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_list_provider import \
    ConnectionStringHostListProvider
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_provider_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def props():
    return Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})


def test_get_host_role(mock_provider_service, mock_cursor, props):
    provider = ConnectionStringHostListProvider(mock_provider_service, props)

    with pytest.raises(AwsWrapperError):
        provider.get_host_role("ConnectionStringHostListProvider.ErrorDoesNotSupportHostRole")


def test_identify_connection_no_dialect(mock_provider_service, props):
    provider = ConnectionStringHostListProvider(mock_provider_service, props)

    with pytest.raises(AwsWrapperError):
        provider.identify_connection("ConnectionStringHostListProvider.ErrorDoesNotSupportIdentifyConnection")


def test_refresh(mock_provider_service, props):
    provider = ConnectionStringHostListProvider(mock_provider_service, props)
    expected_host = HostInfo(props.get("host"))
    hosts = provider.refresh()

    assert 1 == len(hosts)
    assert expected_host == hosts[0]
    assert expected_host == mock_provider_service.initial_connection_host_info
    assert provider._is_initialized is True

    hosts = provider.refresh()
    assert 1 == len(hosts)
