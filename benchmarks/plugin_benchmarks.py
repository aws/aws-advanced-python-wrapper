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

import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginServiceManagerContainer)
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils.properties import Properties

host_info = HostInfo(host="host", port=1234)


@pytest.fixture
def props_with_execute_time_plugin():
    return Properties({"plugins": "execute_time"})


@pytest.fixture
def props_with_aurora_connection_tracker_plugin():
    return Properties({"plugins": "aurora_connection_tracker"})


@pytest.fixture
def props_with_execute_time_and_aurora_connection_tracker_plugin():
    return Properties({"plugins": "execute_time,aurora_connection_tracker"})


@pytest.fixture
def props_with_read_write_splitting_plugin():
    return Properties({"plugins": "read_write_splitting"})


@pytest.fixture
def props_with_aurora_connection_tracker_and_read_write_splitting_plugin():
    return Properties({"plugins": "aurora_connection_tracker,read_write_splitting"})


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
def plugin_manager_with_execute_time_plugin(plugin_service_manager_container_mock, props_with_execute_time_plugin):
    manager: PluginManager = PluginManager(plugin_service_manager_container_mock, props_with_execute_time_plugin)
    return manager


@pytest.fixture
def plugin_manager_with_aurora_connection_tracker_plugin(
        plugin_service_manager_container_mock, props_with_aurora_connection_tracker_plugin):

    manager: PluginManager = PluginManager(
        plugin_service_manager_container_mock, props_with_aurora_connection_tracker_plugin)
    return manager


@pytest.fixture
def plugin_manager_with_execute_time_and_aurora_connection_tracker_plugin(
        plugin_service_manager_container_mock, props_with_execute_time_and_aurora_connection_tracker_plugin):

    manager: PluginManager = PluginManager(
        plugin_service_manager_container_mock, props_with_execute_time_and_aurora_connection_tracker_plugin)
    return manager


@pytest.fixture
def plugin_manager_with_read_write_splitting_plugin(
        plugin_service_manager_container_mock, props_with_read_write_splitting_plugin):

    manager: PluginManager = PluginManager(
        plugin_service_manager_container_mock, props_with_read_write_splitting_plugin)
    return manager


@pytest.fixture
def plugin_manager_with_aurora_connection_tracker_and_read_write_splitting_plugin(
        plugin_service_manager_container_mock, props_with_aurora_connection_tracker_and_read_write_splitting_plugin):

    manager: PluginManager = PluginManager(
        plugin_service_manager_container_mock, props_with_aurora_connection_tracker_and_read_write_splitting_plugin)
    return manager


def init_and_release(mocker, plugin_service_mock, plugin_manager):
    wrapper = AwsWrapperConnection(mocker.MagicMock(), plugin_service_mock, plugin_service_mock, plugin_manager)
    wrapper.release_resources()
    return wrapper


def test_init_and_release_with_execution_time_plugin(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_execute_time_plugin):

    result = benchmark(init_and_release, mocker, plugin_service_mock, plugin_manager_with_execute_time_plugin)
    assert result is not None


def test_init_and_release_with_aurora_connection_tracker_plugin(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_aurora_connection_tracker_plugin):

    result = benchmark(init_and_release, mocker, plugin_service_mock, plugin_manager_with_aurora_connection_tracker_plugin)
    assert result is not None


def test_init_and_release_with_execute_time_and_aurora_connection_tracker_plugin(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_execute_time_and_aurora_connection_tracker_plugin):

    result = benchmark(
        init_and_release, mocker, plugin_service_mock, plugin_manager_with_execute_time_and_aurora_connection_tracker_plugin)
    assert result is not None


def test_init_and_release_with_read_write_splitting_plugin(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_read_write_splitting_plugin):

    result = benchmark(init_and_release, mocker, plugin_service_mock, plugin_manager_with_read_write_splitting_plugin)
    assert result is not None


def test_init_and_release_with_aurora_connection_tracker_and_read_write_splitting_plugin(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_aurora_connection_tracker_and_read_write_splitting_plugin):

    result = benchmark(
        init_and_release,
        mocker,
        plugin_service_mock,
        plugin_manager_with_aurora_connection_tracker_and_read_write_splitting_plugin)
    assert result is not None


def init_and_release_internal_connection_pools(mocker, plugin_service_mock, plugin_manager):
    provider = SqlAlchemyPooledConnectionProvider()
    ConnectionProviderManager.set_connection_provider(provider)

    wrapper = AwsWrapperConnection(mocker.MagicMock(), plugin_service_mock, plugin_service_mock, plugin_manager)
    wrapper.release_resources()

    ConnectionProviderManager.release_resources()
    ConnectionProviderManager.reset_provider()

    return wrapper


def test_init_and_release_with_read_write_splitting_plugin_internal_connection_pools(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_read_write_splitting_plugin):

    result = benchmark(
        init_and_release_internal_connection_pools,
        mocker,
        plugin_service_mock,
        plugin_manager_with_read_write_splitting_plugin)
    assert result is not None


def test_init_and_release_with_aurora_connection_tracker_and_read_write_splitting_plugin_internal_connection_pools(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_aurora_connection_tracker_and_read_write_splitting_plugin):

    result = benchmark(
        init_and_release_internal_connection_pools,
        mocker,
        plugin_service_mock,
        plugin_manager_with_aurora_connection_tracker_and_read_write_splitting_plugin)
    assert result is not None


def create_cursor_baseline(mocker, plugin_service, plugin_manager):
    wrapper = AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)
    cursor = wrapper.cursor()
    return cursor


def test_create_cursor_baseline(benchmark, mocker, plugin_service_mock, plugin_manager_with_execute_time_plugin):
    result = benchmark(create_cursor_baseline, mocker, plugin_service_mock, plugin_manager_with_execute_time_plugin)
    assert result is not None


def execute_query(mocker, plugin_service, plugin_manager):
    wrapper = AwsWrapperConnection(mocker.MagicMock(), plugin_service, plugin_service, plugin_manager)
    cursor = wrapper.cursor()
    results = cursor.execute("some sql")
    return results


def test_execute_query_with_execute_time_plugin(
        benchmark, mocker, plugin_service_mock, plugin_manager_with_execute_time_plugin):

    result = benchmark(execute_query, mocker, plugin_service_mock, plugin_manager_with_execute_time_plugin)
    assert result is not None
