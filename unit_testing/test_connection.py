"""
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
"""

import aws_wrapper

conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty"


def test_connection_basic(mocker):
    connection_mock = mocker.MagicMock()
    connection_mock.connect.return_value = "Test"

    aws_wrapper.AwsWrapperConnection.connect(
        conninfo,
        connection_mock.connect)

    connection_mock.connect.assert_called_with(conninfo)


# test_connection_kwargs
# test_connection_function_cache
# test_connection_str_callable
