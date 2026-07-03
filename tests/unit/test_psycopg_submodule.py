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

from aws_advanced_python_wrapper import psycopg as wrapper_psycopg
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def test_submodule_connect_routes_through_awswrapperconnection(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel_connection"
    )
    result = wrapper_psycopg.connect(
        "host=h user=u dbname=d", wrapper_dialect="aurora-pg"
    )
    assert result == "sentinel_connection"
    args, kwargs = mock_wrapper_connect.call_args
    # Bound classmethod objects aren't identity-stable across lookups — compare
    # the underlying function instead.
    assert args[0].__func__ is psycopg.Connection.connect.__func__
    assert args[1] == "host=h user=u dbname=d"
    assert kwargs == {"wrapper_dialect": "aurora-pg"}


def test_submodule_connect_passes_all_kwargs_through(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel"
    )
    wrapper_psycopg.connect(
        "host=h", wrapper_dialect="aurora-pg", plugins="failover,efm", autocommit=True
    )
    _, kwargs = mock_wrapper_connect.call_args
    assert kwargs == {
        "wrapper_dialect": "aurora-pg",
        "plugins": "failover,efm",
        "autocommit": True,
    }


def test_submodule_has_full_pep249_surface():
    for name in ("Error", "OperationalError", "InterfaceError",
                 "DatabaseError", "Date", "Binary",
                 "STRING", "NUMBER", "apilevel", "paramstyle"):
        assert hasattr(wrapper_psycopg, name), f"missing {name}"
    assert wrapper_psycopg.apilevel == "2.0"
    assert wrapper_psycopg.paramstyle == "pyformat"


def test_submodule_error_is_same_class_as_top_level():
    import aws_advanced_python_wrapper as aaw
    assert wrapper_psycopg.Error is aaw.Error
    assert wrapper_psycopg.OperationalError is aaw.OperationalError
