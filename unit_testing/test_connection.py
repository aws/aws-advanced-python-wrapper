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

from unittest.mock import MagicMock

from aws_wrapper.wrapper import AwsWrapperConnection

conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty"
connection_mock = MagicMock()
connection_mock.connect.return_value = "Test"


def test_connection_basic():

    AwsWrapperConnection.connect(
        conninfo,
        connection_mock.connect)

    connection_mock.connect.assert_called_with(host="localhost", dbname="postgres", user="postgres", password="qwerty")


def test_connection_no_plugins():

    AwsWrapperConnection.connect(
        conninfo,
        connection_mock.connect, plugins="")

    connection_mock.connect.assert_called_with(host="localhost", dbname="postgres", user="postgres", password="qwerty")

# def test_connection_str_callable():
