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
                                                          PropertiesUtils,
                                                          WrapperProperty)


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
     pytest.param(Properties({"database": "some_db", "host": "/var/lib/postgresql",
                              "options": "-c synchronous_commit=off", "user": "postgres"}),
                  "postgres:///some_db?host=/var/lib/postgresql&options=-c%20synchronous_commit%3Doff",
                  dict(user="postgres")),
     pytest.param(Properties({"database": "some_db", "host": "/var/lib/postgresql", "user": "postgres"}),
                  "postgres://%2Fvar%2Flib%2Fpostgresql/some_db",
                  dict(user="postgres")),
     pytest.param(Properties({"host": "[2001:db8::1234]", "port": "1111", "user": "some_user",
                              "connect_timeout": "11"}),
                  "postgres://some_user@[2001:db8::1234]:1111?connect_timeout=11",
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


@pytest.mark.parametrize("expected, test_props",
                         [pytest.param(Properties({"user": "postgres",
                                                   "test_property": 1}),
                                       Properties({"user": "postgres",
                                                   "monitoring-test_property": 1,
                                                   "test_property": 2})),
                          pytest.param(Properties(), Properties())])
def test_create_monitoring_properties(expected, test_props):
    props_copy = test_props.copy()
    props_copy = PropertiesUtils.create_monitoring_properties(props_copy)
    assert expected == props_copy


@pytest.mark.parametrize("expected, props, type_class", [
    # Int type tests
    (123, Properties({"test_prop": "123"}), int),
    (-1, Properties(), int),
    (456, Properties({"test_prop": 456}), int),

    # Float type tests
    (12.5, Properties({"test_prop": "12.5"}), float),
    (-1.0, Properties(), float),
    (3.14, Properties({"test_prop": 3.14}), float),

    # Bool type tests
    (True, Properties({"test_prop": "true"}), bool),
    (True, Properties({"test_prop": "TRUE"}), bool),
    (False, Properties({"test_prop": "false"}), bool),
    (True, Properties({"test_prop": True}), bool),
    (False, Properties({"test_prop": False}), bool),
    (False, Properties(), bool),

    # String type tests
    ("test_value", Properties({"test_prop": "test_value"}), str),
    (None, Properties(), str),
    ("", Properties({"test_prop": ""}), str),
])
def test_get_type(expected, props, type_class):
    wrapper_prop = WrapperProperty("test_prop", "Test property")
    result = wrapper_prop.get_type(props, type_class)
    assert result == expected


def test_get_type_with_default():
    wrapper_prop = WrapperProperty("test_prop", "Test property", "default_value")
    props = Properties()
    result = wrapper_prop.get_type(props, str)
    assert result == "default_value"


@pytest.mark.parametrize("expected, props", [
    (123, Properties({"test_prop": "123"})),
    (-1, Properties()),
    (456, Properties({"test_prop": 456})),
])
def test_get_int(expected, props):
    wrapper_prop = WrapperProperty("test_prop", "Test property")
    result = wrapper_prop.get_int(props)
    assert result == expected


@pytest.mark.parametrize("expected, props", [
    (12.5, Properties({"test_prop": "12.5"})),
    (-1.0, Properties()),
    (3.14, Properties({"test_prop": 3.14})),
])
def test_get_float(expected, props):
    wrapper_prop = WrapperProperty("test_prop", "Test property")
    result = wrapper_prop.get_float(props)
    assert result == expected


@pytest.mark.parametrize("expected, props", [
    (True, Properties({"test_prop": "true"})),
    (True, Properties({"test_prop": "TRUE"})),
    (False, Properties({"test_prop": "false"})),
    (True, Properties({"test_prop": True})),
    (False, Properties({"test_prop": False})),
    (False, Properties()),
])
def test_get_bool(expected, props):
    wrapper_prop = WrapperProperty("test_prop", "Test property")
    result = wrapper_prop.get_bool(props)
    assert result == expected
