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

import pytest

from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.stale_dns_plugin import StaleDnsHelper
from aws_wrapper.utils.properties import Properties


@pytest.fixture
def plugin_service_mock(mocker):
    service_mock = mocker.MagicMock()
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
def writer_cluster():
    return HostInfo("my-cluster.cluster-XYZ.us-west-2.rds.amazonaws.com", 1234, HostAvailability.AVAILABLE, HostRole.WRITER)


@pytest.fixture
def writer_instance():
    return HostInfo("writer-host.XYZ.us-west-2.rds.amazonaws.com", 1234, HostAvailability.AVAILABLE, HostRole.WRITER)


@pytest.fixture
def reader_cluster():
    return HostInfo("my-cluster.cluster-XYZ.us-west-2.rds.amazonaws.com", 1234, HostAvailability.AVAILABLE, HostRole.READER)


@pytest.fixture
def reader_a():
    return HostInfo("reader-a-host.XYZ.us-west-2.rds.amazonaws.com", 1234, HostAvailability.AVAILABLE, HostRole.READER)


@pytest.fixture
def reader_b():
    return HostInfo("reader-b-host.XYZ.us-west-2.rds.amazonaws.com", 1234, HostAvailability.AVAILABLE, HostRole.READER)


@pytest.fixture
def cluster_host_list(writer_cluster, reader_a, reader_b):
    return [writer_cluster, reader_a, reader_b]


@pytest.fixture
def reader_host_list(reader_a, reader_b):
    return [reader_a, reader_b]


@pytest.fixture
def instance_host_list(writer_instance, reader_a, reader_b):
    return [writer_instance, reader_a, reader_b]


def test_get_verified_connection__is_writer_cluster_dns_false(plugin_service_mock, host_list_provider_mock, default_properties, initial_conn_mock,
                                                              connect_func_mock, reader_a):
    connect_func_mock.return_value = initial_conn_mock
    invalid_host = HostInfo("invalid_host")

    target = StaleDnsHelper(plugin_service_mock)
    return_conn = target.get_verified_connection(False, host_list_provider_mock, invalid_host, default_properties, connect_func_mock)

    connect_func_mock.assert_called()

    assert return_conn == initial_conn_mock


def test_get_verified_connection__cluster_inet_address_none(mocker, plugin_service_mock, host_list_provider_mock, default_properties,
                                                            initial_conn_mock, connect_func_mock):
    target = StaleDnsHelper(plugin_service_mock)
    writer_cluster_invalid_cluster_inet_address = HostInfo("my-cluster.cluster-invalid.us-west-2.rds.amazonaws.com", 1234, HostAvailability.AVAILABLE,
                                                           HostRole.WRITER)

    socket.gethostbyname = mocker.MagicMock(return_value=None)
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster_invalid_cluster_inet_address, default_properties,
                                                 connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock


def test_get_verified_connection__no_writer_hostinfo(mocker, plugin_service_mock, host_list_provider_mock, default_properties,
                                                     initial_conn_mock, connect_func_mock, reader_host_list, writer_cluster):
    target = StaleDnsHelper(plugin_service_mock)
    plugin_service_mock.hosts = reader_host_list
    plugin_service_mock.get_host_role.return_value = HostRole.READER
    connect_func_mock.return_value = initial_conn_mock
    socket.gethostbyname = mocker.MagicMock(return_value='2.2.2.2')

    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    plugin_service_mock.force_refresh_host_list.assert_called_once()
    assert return_conn == initial_conn_mock

    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_rds_cluster_dns_true(mocker, plugin_service_mock, host_list_provider_mock, default_properties,
                                                              initial_conn_mock, connect_func_mock, writer_cluster, cluster_host_list):

    connect_func_mock.return_value = initial_conn_mock
    socket.gethostbyname = mocker.MagicMock(return_value='5.5.5.5')
    plugin_service_mock.hosts = cluster_host_list

    target = StaleDnsHelper(plugin_service_mock)
    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    plugin_service_mock.refresh_host_list.assert_called_once()
    assert return_conn == initial_conn_mock


def test_get_verified_connection__writer_host_address_none(mocker, plugin_service_mock, host_list_provider_mock, default_properties,
                                                           initial_conn_mock, connect_func_mock, writer_cluster, instance_host_list):
    target = StaleDnsHelper(plugin_service_mock)
    plugin_service_mock.hosts = instance_host_list
    socket.gethostbyname = mocker.MagicMock(side_effect=['5.5.5.5', None])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock


def test_get_verified_connection__writer_host_info_none(mocker, plugin_service_mock, host_list_provider_mock, default_properties, initial_conn_mock,
                                                        connect_func_mock, writer_cluster, reader_host_list):
    target = StaleDnsHelper(plugin_service_mock)
    plugin_service_mock.hosts = reader_host_list
    socket.gethostbyname = mocker.MagicMock(side_effect=['5.5.5.5', None])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    connect_func_mock.assert_called()
    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_host_address_equals_cluster_inet_address(mocker, plugin_service_mock, host_list_provider_mock,
                                                                                  default_properties, initial_conn_mock, connect_func_mock,
                                                                                  writer_cluster, instance_host_list):
    target = StaleDnsHelper(plugin_service_mock)
    plugin_service_mock.hosts = instance_host_list
    socket.gethostbyname = mocker.MagicMock(side_effect=['5.5.5.5', '5.5.5.5'])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    assert return_conn == initial_conn_mock
    plugin_service_mock.connect.assert_not_called()


def test_get_verified_connection__writer_host_address_not_equals_cluster_inet_address(mocker, plugin_service_mock, host_list_provider_mock,
                                                                                      default_properties, initial_conn_mock, connect_func_mock,
                                                                                      writer_cluster, cluster_host_list):
    target = StaleDnsHelper(plugin_service_mock)
    target._writer_host_info = writer_cluster
    plugin_service_mock.hosts = cluster_host_list
    socket.gethostbyname = mocker.MagicMock(side_effect=['5.5.5.5', '8.8.8.8'])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(False, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    plugin_service_mock.connect.assert_called_once()
    initial_conn_mock.close.assert_called_once()
    assert return_conn != initial_conn_mock
    assert target._writer_host_info != host_list_provider_mock.initial_connection_host_info


def test_get_verified_connection__initial_connection_writer_host_address_not_equals_cluster_inet_address(mocker, plugin_service_mock,
                                                                                                         host_list_provider_mock,
                                                                                                         default_properties, initial_conn_mock,
                                                                                                         connect_func_mock,
                                                                                                         writer_cluster, cluster_host_list):
    target = StaleDnsHelper(plugin_service_mock)
    target._writer_host_info = writer_cluster
    plugin_service_mock.hosts = cluster_host_list
    socket.gethostbyname = mocker.MagicMock(side_effect=['5.5.5.5', '8.8.8.8'])
    connect_func_mock.return_value = initial_conn_mock

    return_conn = target.get_verified_connection(True, host_list_provider_mock, writer_cluster, default_properties, connect_func_mock)

    plugin_service_mock.connect.assert_called_once()
    initial_conn_mock.close.assert_called_once()
    assert return_conn != initial_conn_mock
    assert target._writer_host_info == host_list_provider_mock.initial_connection_host_info
