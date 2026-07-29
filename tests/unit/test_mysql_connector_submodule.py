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

import mysql.connector

from aws_advanced_python_wrapper import mysql_connector as wrapper_mysql
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def test_submodule_connect_routes_through_awswrapperconnection(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel_connection"
    )
    result = wrapper_mysql.connect(
        "host=h user=u database=d", wrapper_dialect="aurora-mysql"
    )
    assert result == "sentinel_connection"
    args, kwargs = mock_wrapper_connect.call_args
    assert args[0] is mysql.connector.connect
    assert args[1] == "host=h user=u database=d"
    assert kwargs == {"wrapper_dialect": "aurora-mysql"}


def test_submodule_connect_passes_all_kwargs_through(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel"
    )
    wrapper_mysql.connect(
        "host=h", wrapper_dialect="aurora-mysql",
        plugins="failover", use_pure=False,
    )
    _, kwargs = mock_wrapper_connect.call_args
    assert kwargs == {
        "wrapper_dialect": "aurora-mysql",
        "plugins": "failover",
        "use_pure": False,
    }


def test_submodule_has_full_pep249_surface():
    for name in ("Error", "OperationalError", "InterfaceError",
                 "DatabaseError", "Date", "Binary",
                 "STRING", "NUMBER", "apilevel", "paramstyle"):
        assert hasattr(wrapper_mysql, name), f"missing {name}"
    assert wrapper_mysql.apilevel == "2.0"
    assert wrapper_mysql.paramstyle == "pyformat"


def test_submodule_error_is_same_class_as_top_level():
    import aws_advanced_python_wrapper as aaw
    assert wrapper_mysql.Error is aaw.Error
    assert wrapper_mysql.OperationalError is aaw.OperationalError
