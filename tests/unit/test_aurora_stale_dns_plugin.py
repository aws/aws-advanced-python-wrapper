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

from unittest.mock import MagicMock

import pytest

from aws_wrapper.aurora_stale_dns import AuroraStaleDnsHelper
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.utils.properties import Properties


@pytest.fixture
def plugin_service_mock(mocker):
    service_mock = mocker.MagicMock()
    service_mock.network_bound_methods = {"*"}
    return service_mock


@pytest.fixture
def conn_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def reader_failover_handler_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def host_list_provider_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def connect_func_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def default_properties():
    return Properties()


@pytest.fixture
def initial_conn_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def cluster_host():
    return HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com")


def test_get_verified_connection__is_writer_cluster_dns_false(plugin_service_mock, default_properties, cluster_host, initial_conn_mock,
                                                              connect_func_mock):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    connect_func_mock.return_value = initial_conn_mock

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock


def test_get_verified_connection__cluster_inet_address_none(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = HostInfo("invalid_instance")
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock


def test_get_verified_connection__is_read_only_true(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = HostInfo("")
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    plugin_service_mock.refresh_host_list.assert_called_once()

    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_rds_cluster_dns_true(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock,
                                                              cluster_host):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = cluster_host
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)
    helper._rds_helper.is_rds_cluster_dns = MagicMock(return_value=True)

    helper.writer_host_info = None
    helper.get_writer = MagicMock(return_value=HostInfo("some_writer"))

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    assert return_conn == initial_conn_mock


def test_get_verified_connection__writer_host_address_none(plugin_service_mock, default_properties, initial_conn_mock,
                                                           connect_func_mock, cluster_host):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = cluster_host
    connect_func_mock.return_value = initial_conn_mock
    helper.writer_host_address = None
    helper.writer_host_info = HostInfo("writer_host")
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock


def test_get_verified_connection__writer_host_info_not_none(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock,
                                                            cluster_host):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = cluster_host
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)
    helper.writer_host_info = host_info

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_host_address_equals_cluster_inet_address(plugin_service_mock, default_properties, initial_conn_mock,
                                                                                  connect_func_mock, cluster_host):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = HostInfo('0.0.0.0')
    writer_host_address: HostInfo = HostInfo("writer_host")
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)
    helper.writer_host_info = host_info
    helper.writer_host_address = writer_host_address

    return_conn = helper.get_verified_connection(False, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    assert return_conn != initial_conn_mock
    plugin_service_mock.connect.assert_called_once()


def test_get_verified_connection__writer_host_address_not_equals_cluster_inet_address(plugin_service_mock, default_properties, initial_conn_mock,
                                                                                      connect_func_mock, cluster_host):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = cluster_host
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)
    helper.writer_host_info = host_info
    helper.writer_host_address = None

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__initial_connection_writer_host_address_equals_cluster_inet_address(plugin_service_mock, default_properties,
                                                                                                     initial_conn_mock, connect_func_mock):
    helper = AuroraStaleDnsHelper(plugin_service_mock)
    host_info: HostInfo = HostInfo("")
    writer_host_address: HostInfo = HostInfo("0.0.0.0")
    connect_func_mock.return_value = initial_conn_mock
    helper._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)
    helper.writer_host_info = host_info
    helper.writer_host_address = writer_host_address

    return_conn = helper.get_verified_connection(True, host_list_provider_mock, host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    plugin_service_mock.refresh_host_list.assert_called_once()

    plugin_service_mock.connect.assert_called_once()
    initial_conn_mock.close.assert_called_once()
    assert return_conn != initial_conn_mock
