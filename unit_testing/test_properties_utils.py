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

from unittest import TestCase

from parameterized import param, parameterized

from aws_wrapper.utils.properties import Properties, PropertiesUtils


class TestPropertiesUtils(TestCase):

    @parameterized.expand([
        param(Properties({"user": "postgres", "password": "kwargs_password"}),
              "user=postgres password=conninfo_password",
              password="kwargs_password"),
        param(Properties({"user": "postgres"}),
              "",
              user="postgres"),
        param(Properties({"user": "postgres"}),
              "user=postgres"),
    ])
    def test_parse_properties(self, expected, conninfo, **kwargs):
        assert expected == PropertiesUtils.parse_properties(conninfo, **kwargs)

    @parameterized.expand([
        param(Properties({"user": "postgres", "password": "conninfo_password"}),
              Properties(user="postgres", plugins="pluginCode", password="conninfo_password")),
        param(Properties({"user": "postgres", "password": "conninfo_password"}),
              Properties(user="postgres", password="conninfo_password")),
        param(Properties(), Properties())
    ])
    def test_remove_wrapper_props(self, expected, props):
        props_copy = props.copy()
        PropertiesUtils.remove_wrapper_props(props_copy)
        assert expected == props_copy
