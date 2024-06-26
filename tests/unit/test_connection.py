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

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import psycopg

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.plugin_service import (
    PluginServiceImpl, PluginServiceManagerContainer)
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def test_connection_basic():
    conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty wrapper_driver_dialect=psycopg"
    connection_mock: Connection = MagicMock(spec=psycopg.Connection)
    container_mock = MagicMock()
    connection_mock.connect.return_value = MagicMock(spec=psycopg.Connection)

    with patch.object(PluginServiceManagerContainer, "__new__", container_mock), \
            patch.object(PluginServiceImpl, "refresh_host_list"), \
            patch.object(PluginServiceImpl, "current_connection", None), \
            patch.object(PluginServiceImpl, "initial_connection_host_info", HostInfo("localhost")):
        AwsWrapperConnection.connect(connection_mock.connect, conninfo)

        connection_mock.connect.assert_called_with(host="localhost", dbname="postgres", user="postgres",
                                                   password="qwerty")
