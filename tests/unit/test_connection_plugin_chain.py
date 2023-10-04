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

if TYPE_CHECKING:
    from aws_wrapper.plugin import Plugin

from typing import List

import pytest

from aws_wrapper.connect_time_plugin import ConnectTimePlugin
from aws_wrapper.connection_plugin_chain import get_plugins
from aws_wrapper.default_plugin import DefaultPlugin
from aws_wrapper.execute_time_plugin import ExecuteTimePlugin
from aws_wrapper.failover_plugin import FailoverPlugin
from aws_wrapper.host_monitoring_plugin import HostMonitoringPlugin
from aws_wrapper.iam_plugin import IamAuthPlugin
from aws_wrapper.utils.properties import Properties


@pytest.fixture
def plugin_service_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def plugin_manager_service_mock(mocker):
    return mocker.MagicMock()


def test_sort_plugins(plugin_service_mock, plugin_manager_service_mock):
    props = Properties(plugins="iam,host_monitoring,failover")
    plugins: List[Plugin] = get_plugins(plugin_service_mock, plugin_manager_service_mock, props)

    assert plugins is not None
    assert 4 == len(plugins)

    assert isinstance(plugins[0], FailoverPlugin)
    assert isinstance(plugins[1], HostMonitoringPlugin)
    assert isinstance(plugins[2], IamAuthPlugin)
    assert isinstance(plugins[3], DefaultPlugin)


def test_preserve_plugin_order(plugin_service_mock, plugin_manager_service_mock):
    props = Properties(plugins="iam,host_monitoring,failover", auto_sort_wrapper_plugin_order=False)
    plugins: List[Plugin] = get_plugins(plugin_service_mock, plugin_manager_service_mock, props)

    assert plugins is not None
    assert 4 == len(plugins)

    assert isinstance(plugins[0], IamAuthPlugin)
    assert isinstance(plugins[1], HostMonitoringPlugin)
    assert isinstance(plugins[2], FailoverPlugin)
    assert isinstance(plugins[3], DefaultPlugin)


def test_sort_plugins_with_stick_to_prior(plugin_service_mock, plugin_manager_service_mock):
    props = Properties(plugins="iam,execute_time,connect_time,host_monitoring,failover")
    plugins: List[Plugin] = get_plugins(plugin_service_mock, plugin_manager_service_mock, props)

    assert plugins is not None
    assert 6 == len(plugins)

    assert isinstance(plugins[0], FailoverPlugin)
    assert isinstance(plugins[1], HostMonitoringPlugin)
    assert isinstance(plugins[2], IamAuthPlugin)
    assert isinstance(plugins[3], ExecuteTimePlugin)
    assert isinstance(plugins[4], ConnectTimePlugin)
    assert isinstance(plugins[5], DefaultPlugin)
