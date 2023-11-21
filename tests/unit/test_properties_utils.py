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

import pytest

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils)


@pytest.mark.parametrize(
    "expected, conn_info, kwargs",
    [pytest.param(Properties({"user": "postgres", "password": "kwargs_password"}),
                  "user=postgres password=conninfo_password",
                  dict(password="kwargs_password")),
     pytest.param(Properties({"user": "postgres", "password": "kwargs_password"}),
                  "   user  =  postgres    password=conninfo_password ",
                  dict(password="kwargs_password")),
     pytest.param(Properties({"user": "postgres"}), "", dict(user="postgres")),
     pytest.param(Properties({"user": "some_user", "password": "some_pass", "host": "some_host", "port": "1111",
                              "database": "some_db", "connect_timeout": "11", "some_prop": "some_value"}),
                  "postgresql://some_user:some_pass@some_host:1111/some_db?connect_timeout=11",
                  dict(some_prop="some_value")),
     pytest.param(Properties({"database": "some_db", "host": "some_host", "port": "1111", "user": "postgres"}),
                  "postgresql:///some_db?host=some_host&port=1111",
                  dict(user="postgres")),
     pytest.param(Properties({"database": "some_db", "host": "[2001:db8::1234]", "user": "postgres"}),
                  "postgresql://[2001:db8::1234]/some_db",
                  dict(user="postgres")),
     pytest.param(Properties({"database": "some_db", "host": "/var/lib/postgresql", "user": "postgres"}),
                  "postgresql:///some_db?host=/var/lib/postgresql",
                  dict(user="postgres")),
     pytest.param(Properties({"database": "some_db", "host": "%2Fvar%2Flib%2Fpostgresql", "user": "postgres"}),
                  "postgresql://%2Fvar%2Flib%2Fpostgresql/some_db",
                  dict(user="postgres")),
     pytest.param(Properties({"host": "[2001:db8::1234]", "port": "1111", "user": "some_user",
                              "connect_timeout": "11"}),
                  "postgresql://some_user@[2001:db8::1234]:1111?connect_timeout=11",
                  {}),
     pytest.param(Properties({"host": "localhost"}), "postgresql://localhost", {}),
     pytest.param(Properties({"user": "postgres"}), "user=postgres", {})])
def test_parse_properties(expected, conn_info, kwargs):
    assert PropertiesUtils.parse_properties(conn_info, **kwargs) == expected


def test_parse_properties__multiple_hosts():
    with pytest.raises(AwsWrapperError):
        PropertiesUtils.parse_properties("postgresql://host1:123,host2:456/somedb")


@pytest.mark.parametrize("expected, test_props",
                         [pytest.param(Properties({"user": "postgres", "password": "conninfo_password"}),
                                       Properties(user="postgres", plugins="pluginCode", password="conninfo_password")),
                          pytest.param(Properties({"user": "postgres", "password": "conninfo_password"}),
                                       Properties(user="postgres", password="conninfo_password")),
                          pytest.param(Properties(), Properties())])
def test_remove_wrapper_props(expected, test_props):
    props_copy = test_props.copy()
    PropertiesUtils.remove_wrapper_props(props_copy)
    assert expected == props_copy


@pytest.mark.parametrize("expected, test_props",
                         [pytest.param(Properties({"user": "postgres", "password": "***"}),
                                       Properties({"user": "postgres", "password": "conninfo_password"})),
                          pytest.param(Properties(), Properties())])
def test_masked_props(expected, test_props):
    props_copy = test_props.copy()
    props_copy = PropertiesUtils.mask_properties(props_copy)
    assert expected == props_copy
