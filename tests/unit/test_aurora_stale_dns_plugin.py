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
import socket

from unittest.mock import MagicMock

import pytest

from aws_wrapper.aurora_stale_dns_plugin import AuroraStaleDnsHelper
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
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
    return HostInfo("ABC.cluster-XYZ.us-west-2.rds.amazonaws.com")


def test_get_verified_connection__is_writer_cluster_dns_false(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    read_only_host = HostInfo("ABC.cluster-ro-XYZ.us-west-2.rds.amazonaws.com")
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, read_only_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock


def test_get_verified_connection__cluster_inet_address_none(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock,
                                                            cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    connect_func_mock.return_value = initial_conn_mock
    target._rds_helper.is_writer_cluster_dns = MagicMock(return_value=True)
    socket.gethostbyname = MagicMock(return_value=None)

    return_conn = target.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock


def test_get_verified_connection__is_reader_no_writer_hostinfo(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    reader_host_info: HostInfo = HostInfo("ABC.cluster-XYZ.us-west-2.rds.amazonaws.com", 123, HostAvailability.AVAILABLE, HostRole.READER)
    plugin_service_mock.get_host_role.return_value = HostRole.READER
    connect_func_mock.return_value = initial_conn_mock
    socket.gethostbyname = MagicMock(return_value='2.2.2.2')

    return_conn = target.get_verified_connection(True, host_list_provider_mock, reader_host_info, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    plugin_service_mock.force_refresh_host_list.assert_called_once()
    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_rds_cluster_dns_true(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock,
                                                              cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    socket.gethostbyname = MagicMock(return_value='5.5.5.5')
    connect_func_mock.return_value = initial_conn_mock
    target.get_writer = MagicMock(return_value=HostInfo("ABCD.cluster-XYZ.us-west-2.rds.amazonaws.com"))

    return_conn = target.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock


def test_get_verified_connection__writer_host_address_none(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock,
                                                           cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    target.writer_host_info = cluster_host
    socket.gethostbyname = MagicMock(side_effect=['5.5.5.5', None])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock


def test_get_verified_connection__writer_host_info_none(plugin_service_mock, default_properties, initial_conn_mock, connect_func_mock,
                                                        cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    target.writer_host_info = None
    connect_func_mock.return_value = initial_conn_mock
    socket.gethostbyname = MagicMock(return_value='5.5.5.5')

    return_conn = target.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_host_address_equals_cluster_inet_address(plugin_service_mock, default_properties, initial_conn_mock,
                                                                                  connect_func_mock, cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    target.writer_host_info = cluster_host
    socket.gethostbyname = MagicMock(return_value='5.5.5.5')
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_host_address_not_equals_cluster_inet_address(plugin_service_mock, default_properties, initial_conn_mock,
                                                                                      connect_func_mock, cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    target.writer_host_info = HostInfo('instance-2.cluster-XYZ.us-west-2.rds.amazonaws.com')
    socket.gethostbyname = MagicMock(side_effect=['5.5.5.5', '8.8.8.8'])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(False, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    assert return_conn != initial_conn_mock
    plugin_service_mock.connect.assert_called_once()


def test_get_verified_connection__initial_connection_writer_host_address_not_equals_cluster_inet_address(plugin_service_mock, default_properties,
                                                                                                         initial_conn_mock, connect_func_mock,
                                                                                                         cluster_host):
    target = AuroraStaleDnsHelper(plugin_service_mock)
    target.writer_host_info = HostInfo('ABCD.cluster-XYZ.us-west-2.rds.amazonaws.com')
    socket.gethostbyname = MagicMock(side_effect=['5.5.5.5', '8.8.8.8'])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, cluster_host, default_properties, connect_func_mock)

    plugin_service_mock.connect.assert_called_once()
    initial_conn_mock.close.assert_called_once()
    assert return_conn != initial_conn_mock
