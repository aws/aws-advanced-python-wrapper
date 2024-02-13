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

import pytest

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.plugin_service import PluginService
from aws_advanced_python_wrapper.states.session_state_service import \
    SessionStateServiceImpl
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock(spec=PluginService)


@pytest.fixture
def mock_connection(mocker):
    return mocker.MagicMock(spec=AwsWrapperConnection)


@pytest.fixture
def mock_new_connection(mocker):
    return mocker.MagicMock(spec=AwsWrapperConnection)


@pytest.fixture
def session_state_service(mock_plugin_service, props):
    return SessionStateServiceImpl(mock_plugin_service, props)


@pytest.fixture
def mock_driver_dialect(mocker):
    return mocker.MagicMock(spec=DriverDialect)


@pytest.fixture(autouse=True)
def setUp(mock_plugin_service, mock_connection, mock_driver_dialect):
    mock_plugin_service.current_connection = mock_connection
    mock_plugin_service.driver_dialect = mock_driver_dialect


@pytest.mark.parametrize("pristine_value, value, should_reset", [
    (False, False, False),
    (True, False, True),
    (False, True, True),
    (True, True, False)
])
def test_reset_connection_readonly(mock_connection, mock_new_connection, mock_plugin_service, mock_driver_dialect, session_state_service,
                                   pristine_value, value, should_reset):
    mock_plugin_service.driver_dialect.is_read_only.return_value = pristine_value
    assert session_state_service.get_readonly() is None
    session_state_service.setup_pristine_readonly()
    session_state_service.set_read_only(value)
    assert session_state_service.get_readonly() == value

    session_state_service.begin()
    session_state_service.apply_pristine_session_state(mock_new_connection)
    session_state_service.complete()

    if should_reset:
        mock_plugin_service.driver_dialect.set_read_only.assert_called_with(mock_new_connection, pristine_value)
    else:
        assert not mock_plugin_service.driver_dialect.set_read_only.called


@pytest.mark.parametrize("pristine_value, value", [
    (False, False),
    (True, False),
    (False, True),
    (True, True)
])
def test_transfer_to_new_connection_readonly(mock_connection, mock_new_connection, mock_plugin_service, session_state_service, pristine_value, value):
    mock_connection.is_read_only = pristine_value
    mock_new_connection.is_read_only = pristine_value
    session_state_service.set_read_only(value)
    assert session_state_service.get_readonly() == value

    session_state_service.begin()
    session_state_service.apply_current_session_state(mock_new_connection)
    session_state_service.complete()

    mock_plugin_service.driver_dialect.set_read_only.assert_called_with(mock_new_connection, value)


@pytest.mark.parametrize("pristine_value, value", [
    (False, False),
    (True, False),
    (False, True),
    (True, True)
])
def test_transfer_to_new_connection_autocommit(mock_connection, mock_new_connection, mock_plugin_service, session_state_service, pristine_value,
                                               value):
    mock_connection.autocommit = pristine_value
    mock_new_connection.autocommit = pristine_value
    session_state_service.set_autocommit(value)
    assert session_state_service.get_autocommit() == value

    session_state_service.begin()
    session_state_service.apply_current_session_state(mock_new_connection)
    session_state_service.complete()

    mock_plugin_service.driver_dialect.set_autocommit.assert_called_with(mock_new_connection, value)
