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

from unittest.mock import MagicMock, PropertyMock

from aws_wrapper.wrapper import AwsWrapperConnection

conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty"
connection_mock = MagicMock()
connection_mock.connect.return_value = connection_mock


def test_cursor_execute_no_plugins():
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = cursor_mock

    cursor_mock.fetchone.return_value = 1

    connection_mock.cursor.return_value = cursor_mock

    plugin_service_mock = MagicMock()
    plugin_manager_mock = MagicMock()
    type(plugin_manager_mock).num_plugins = PropertyMock(return_value=0)

    awsconn = AwsWrapperConnection(plugin_service_mock, plugin_manager_mock, connection_mock)

    new_cursor = awsconn.cursor()

    new_cursor.execute("SELECT 1")

    result = new_cursor.fetchone()

    cursor_mock.fetchone.assert_called()

    assert result == 1
