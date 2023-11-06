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

from typing import TYPE_CHECKING, List

import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.plugin import PluginFactory

from aws_advanced_python_wrapper.driver_configuration_profiles import \
    DriverConfigurationProfiles
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginServiceManagerContainer)
from aws_advanced_python_wrapper.utils.properties import Properties
from benchmarks.benchmark_plugin import BenchmarkPluginFactory

host_info = HostInfo(host="host", port=1234)


@pytest.fixture
def props_without_plugins():
    return Properties({"plugins": ""})


@pytest.fixture
def props_with_plugins():
    factories: List[PluginFactory] = []
    for _ in range(10):
        factories.append(BenchmarkPluginFactory())
    DriverConfigurationProfiles.add_or_replace_profile("benchmark", factories)
    return Properties({"profile_name": "benchmark"})


@pytest.fixture
def statement_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def driver_dialect_mock(mocker):
    dialect: DriverDialect = mocker.MagicMock()
    dialect.get_connection_from_obj.return_value = None
    return dialect


@pytest.fixture
def plugin_service_mock(mocker, driver_dialect_mock):
    service_mock = mocker.MagicMock()
    service_mock.driver_dialect = driver_dialect_mock
    service_mock.current_host_info = host_info
    return service_mock


@pytest.fixture
def plugin_service_manager_container_mock(mocker, plugin_service_mock):
    container_mock: PluginServiceManagerContainer = mocker.MagicMock()
    container_mock.plugin_service = plugin_service_mock
    return container_mock


@pytest.fixture
def plugin_manager_with_no_plugins(plugin_service_manager_container_mock, props_without_plugins):
    manager = PluginManager(plugin_service_manager_container_mock, props_without_plugins)
    return manager


@pytest.fixture
def plugin_manager_with_plugins(plugin_service_manager_container_mock, props_with_plugins):
    manager = PluginManager(plugin_service_manager_container_mock, props_with_plugins)
    return manager


def init_plugin_manager(plugin_service_manager_container, props):
    manager = PluginManager(plugin_service_manager_container, props)
    return manager


def test_init_plugin_manager_with_no_plugins(
        benchmark, plugin_service_manager_container_mock, props_without_plugins):

    result = benchmark(init_plugin_manager, plugin_service_manager_container_mock, props_without_plugins)
    assert result is not None


def test_init_plugin_manager_with_plugins(
        benchmark, plugin_service_manager_container_mock, props_with_plugins):

    result = benchmark(init_plugin_manager, plugin_service_manager_container_mock, props_with_plugins)
    assert result is not None


def connect(mocker, plugin_manager, props):
    conn = plugin_manager.connect(mocker.MagicMock(), mocker.MagicMock(), host_info, props, True)
    return conn


def test_connect_with_no_plugins(benchmark, mocker, plugin_manager_with_no_plugins, props_without_plugins):
    result = benchmark(connect, mocker, plugin_manager_with_no_plugins, props_without_plugins)
    assert result is not None


def test_connect_with_plugins(benchmark, mocker, plugin_manager_with_plugins, props_with_plugins):
    result = benchmark(connect, mocker, plugin_manager_with_plugins, props_with_plugins)
    assert result is not None


def execute(mocker, plugin_manager, statement):
    result = plugin_manager.execute(mocker.MagicMock(), "Statement.execute", statement)
    return result


def test_execute_with_no_plugins(benchmark, mocker, plugin_manager_with_no_plugins, statement_mock):
    result = benchmark(execute, mocker, plugin_manager_with_no_plugins, statement_mock)
    assert result is not None


def test_execute_with_plugins(benchmark, mocker, plugin_manager_with_plugins, statement_mock):
    result = benchmark(execute, mocker, plugin_manager_with_plugins, statement_mock)
    assert result is not None


def init_host_provider(mocker, plugin_manager, props):
    plugin_manager.init_host_provider(props, mocker.MagicMock())


def test_init_host_provider_with_no_plugins(benchmark, mocker, plugin_manager_with_no_plugins, props_without_plugins):
    benchmark(init_host_provider, mocker, plugin_manager_with_no_plugins, props_without_plugins)


def test_init_host_provider_with_plugins(benchmark, mocker, plugin_manager_with_plugins, props_with_plugins):
    benchmark(init_host_provider, mocker, plugin_manager_with_plugins, props_with_plugins)


def notify_connection_changed(mocker, plugin_manager):
    result = plugin_manager.notify_connection_changed(mocker.MagicMock())
    return result


def test_notify_connection_changed_with_no_plugins(benchmark, mocker, plugin_manager_with_no_plugins):
    result = benchmark(notify_connection_changed, mocker, plugin_manager_with_no_plugins)
    assert result is not None


def test_notify_connection_changed_with_plugins(benchmark, mocker, plugin_manager_with_plugins):
    result = benchmark(notify_connection_changed, mocker, plugin_manager_with_plugins)
    assert result is not None


def release_resources(plugin_manager):
    plugin_manager.release_resources()


def test_release_resources_with_no_plugins(benchmark, plugin_manager_with_no_plugins):
    benchmark(release_resources, plugin_manager_with_no_plugins)


def test_release_resources_with_plugins(benchmark, plugin_manager_with_plugins):
    benchmark(release_resources, plugin_manager_with_plugins)
