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
from datetime import datetime

import pytest

from aws_advanced_python_wrapper.host_list_provider import MultiAzRdsHostListProvider, RdsHostListProvider
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


@pytest.fixture
def default_refresh_rate_ns():
    return 5_000_000_000_00000000  # 5 seconds


@pytest.fixture
def hosts():
    return HostInfo("host1"), HostInfo("host2")


@pytest.fixture
def initial_host():
    return HostInfo("initial-host")


@pytest.fixture
def provider(mocker, initial_host):
    return create_provider(mocker, initial_host)


def create_provider(mocker, initial_host):
    return MultiAzRdsHostListProvider(
        mocker.MagicMock(),
        Properties({"host": initial_host.host}),
        "topology_query",
        "host_id_query",
        "is_reader_query",
        "writer_host_query")


def test_refresh__cached_topology(mocker, provider, hosts, default_refresh_rate_ns, mock_conn):
    RdsHostListProvider._topology_cache.put(provider._cluster_id, hosts, default_refresh_rate_ns)
    query_for_topology_spy = mocker.spy(provider, "_query_for_topology")

    result = provider.refresh(mock_conn)
    assert result == hosts
    query_for_topology_spy.assert_not_called()


def test_force_refresh__returns_updated_topology(mocker, provider, hosts, default_refresh_rate_ns, mock_conn):
    RdsHostListProvider._topology_cache.put(provider._cluster_id, hosts, default_refresh_rate_ns)
    mock_query_for_topology = mocker.patch.object(provider, "_query_for_topology")
    new_hosts = (HostInfo("new-host"),)
    mock_query_for_topology.return_value = new_hosts

    result = provider.force_refresh(mock_conn)
    assert result == new_hosts
    mock_query_for_topology.assert_called_once()


def test_force_refresh__query_returns_empty_topology(mocker, provider, hosts, default_refresh_rate_ns, mock_conn):
    RdsHostListProvider._topology_cache.put(provider._cluster_id, hosts, default_refresh_rate_ns)
    mock_query_for_topology = mocker.patch.object(provider, "_query_for_topology")
    mock_query_for_topology.return_value = ()

    result = provider.force_refresh(mock_conn)
    assert result == hosts
    mock_query_for_topology.assert_called_once()


def test_force_refresh__returns_initial_host_list(
        mocker, provider, hosts, initial_host, default_refresh_rate_ns, mock_conn):
    mock_query_for_topology = mocker.patch.object(provider, "_query_for_topology")
    mock_query_for_topology.return_value = ()

    result = provider.force_refresh(mock_conn)
    assert result == (initial_host,)
    mock_query_for_topology.assert_called_once()


def test_topology_cache__separate_clusters(mocker, provider, hosts, default_refresh_rate_ns, mock_conn):
    providerA = create_provider(mocker, HostInfo("initial-host-A"))
    topologyA = HostInfo("host-A-1"), HostInfo("host-A-2")
    mock_query_A = mocker.patch.object(providerA, "_query_for_topology")
    mock_query_A.return_value = topologyA

    assert len(RdsHostListProvider._topology_cache) == 0
    resultA = providerA.refresh(mock_conn)
    assert resultA == topologyA

    providerB = create_provider(mocker, HostInfo("initial-host-B"))
    assert RdsHostListProvider._topology_cache.get(providerB._cluster_id) is None

    topologyB = HostInfo("host-B-1"), HostInfo("host-B-2")
    mock_query_B = mocker.patch.object(providerB, "_query_for_topology")
    mock_query_B.return_value = topologyB

    resultB = providerB.refresh(mock_conn)
    assert resultB == topologyB
    assert len(RdsHostListProvider._topology_cache) == 2
