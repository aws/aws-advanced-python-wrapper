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

from concurrent.futures import TimeoutError
from datetime import datetime, timedelta

import psycopg  # type: ignore
import pytest  # type: ignore

from aws_advanced_python_wrapper.database_dialect import AuroraPgDialect
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.host_list_provider import (
    AuroraTopologyUtils, RdsHostListProvider)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import ProgrammingError
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.storage.storage_service import (
    StorageService, Topology)


@pytest.fixture(autouse=True)
def clear_caches():
    StorageService.clear_all()
    RdsHostListProvider._is_primary_cluster_id_cache.clear()
    RdsHostListProvider._cluster_ids_to_update.clear()


def mock_topology_query(mock_conn, mock_cursor, records):
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor  # Mocks out `with conn.cursor() as cursor:`
    mock_cursor.__iter__.return_value = records  # Mocks out `for record in cursor:`


@pytest.fixture(autouse=True)
def mock_default_behavior(mock_provider_service, mock_conn, mock_cursor):
    mock_provider_service.current_connection = mock_conn
    mock_topology_query(mock_conn, mock_cursor, [("new-host-id", True)])


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_provider_service(mocker):
    service_mock = mocker.MagicMock()
    # Use a real AuroraPgDialect to pass isinstance checks in Python 3.12+
    service_mock.database_dialect = AuroraPgDialect()
    return service_mock


@pytest.fixture
def props():
    return Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})


@pytest.fixture
def initial_hosts():
    return (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com"),)


@pytest.fixture
def cache_hosts():
    return HostInfo("host1"), HostInfo("host2")


@pytest.fixture
def queried_hosts():
    return (HostInfo("new-host-id.xyz.us-east-2.rds.amazonaws.com"),)


@pytest.fixture
def refresh_ns():
    return 5_000_000_000  # 5 seconds


def test_get_topology_caches_topology(mocker, mock_provider_service, mock_conn, props, cache_hosts, refresh_ns):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    spy = mocker.spy(topology_utils, "_query_for_topology")

    result = provider.refresh(mock_conn)

    assert cache_hosts == result
    spy.assert_not_called()


def test_get_topology_force_update(
        mocker, mock_provider_service, mock_conn, cache_hosts, queried_hosts, props, refresh_ns):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    spy = mocker.spy(topology_utils, "_query_for_topology")

    result = provider.force_refresh(mock_conn)

    assert queried_hosts == result
    spy.assert_called_once()


def test_get_topology_timeout(mocker, mock_cursor, mock_provider_service, initial_hosts, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    spy = mocker.spy(topology_utils, "_query_for_topology")

    mock_cursor.execute.side_effect = TimeoutError()
    with pytest.raises(QueryTimeoutError):
        provider.force_refresh()

    spy.assert_called_once()


def test_get_topology_invalid_topology(
        mocker, mock_provider_service, mock_conn, mock_cursor, props, cache_hosts, refresh_ns):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    spy = mocker.spy(topology_utils, "_query_for_topology")
    mock_topology_query(mock_conn, mock_cursor, [("reader", False)])  # Invalid topology: no writer instance

    result = provider.force_refresh()

    assert cache_hosts == result
    spy.assert_called_once()


def test_get_topology_invalid_query(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    mock_cursor.execute.side_effect = ProgrammingError()
    spy = mocker.spy(topology_utils, "_query_for_topology")

    with pytest.raises(AwsWrapperError):
        provider.force_refresh(mock_conn)
    spy.assert_called_once()


def test_get_topology_multiple_writers(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    spy = mocker.spy(topology_utils, "_query_for_topology")
    now = datetime.now()
    records = [("old_writer", True, None, None, now), ("new_writer", True, None, None, now + timedelta(seconds=10))]
    mock_topology_query(mock_conn, mock_cursor, records)

    result = provider.refresh()

    assert 1 == len(result)
    assert result[0].host == "new_writer.xyz.us-east-2.rds.amazonaws.com"
    spy.assert_called_once()


def test_get_topology_no_connection(mocker, mock_provider_service, initial_hosts, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    spy = mocker.spy(topology_utils, "_query_for_topology")
    mock_provider_service.database_dialect = None
    mock_provider_service.current_connection = None

    result = provider.refresh()

    assert initial_hosts == result
    spy.assert_not_called()


def test_no_cluster_id_suggestion_for_separate_clusters(mock_provider_service, mock_conn, mock_cursor):
    props_a = Properties({"host": "instance-A-1.domain.com"})
    topology_utils_a = AuroraTopologyUtils(AuroraPgDialect(), props_a)
    provider_a = RdsHostListProvider(mock_provider_service, props_a, topology_utils_a)
    mock_topology_query(mock_conn, mock_cursor, [("instance-A-1.domain.com", True)])
    expected_hosts_a = (HostInfo("instance-A-1.domain.com", role=HostRole.WRITER),)

    actual_hosts_a = provider_a.refresh()
    assert expected_hosts_a == actual_hosts_a

    props_b = Properties({"host": "instance-B-1.domain.com"})
    topology_utils_b = AuroraTopologyUtils(AuroraPgDialect(), props_b)
    provider_b = RdsHostListProvider(mock_provider_service, props_b, topology_utils_b)
    mock_topology_query(mock_conn, mock_cursor, [("instance-B-1.domain.com", True)])
    expected_hosts_b = (HostInfo("instance-B-1.domain.com", role=HostRole.WRITER),)

    actual_hosts_b = provider_b.refresh()
    assert expected_hosts_b == actual_hosts_b
    assert 2 == len(StorageService.get_all(Topology))


def test_cluster_id_suggestion_for_new_provider_with_cluster_url(mocker, mock_provider_service, mock_conn, mock_cursor):
    props = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils1 = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider1 = RdsHostListProvider(mock_provider_service, props, topology_utils1)
    mock_topology_query(mock_conn, mock_cursor, [("instance-1", True)])
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)

    actual_hosts = provider1.refresh()
    assert expected_hosts == actual_hosts
    assert provider1._is_primary_cluster_id

    topology_utils2 = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider2 = RdsHostListProvider(mock_provider_service, props, topology_utils2)
    spy = mocker.spy(provider2._topology_utils, "_query_for_topology")
    provider2._initialize()

    assert provider1._cluster_id == provider2._cluster_id
    assert provider2._is_primary_cluster_id

    actual_hosts = provider2.refresh()
    assert expected_hosts == actual_hosts
    assert 1 == len(StorageService.get_all(Topology))
    spy.assert_not_called()


def test_cluster_id_suggestion_for_new_provider_with_instance_url(
        mocker, mock_provider_service, mock_conn, mock_cursor):
    props1 = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils1 = AuroraTopologyUtils(AuroraPgDialect(), props1)
    provider1 = RdsHostListProvider(mock_provider_service, props1, topology_utils1)
    mock_topology_query(mock_conn, mock_cursor, [("instance-1", True)])
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)

    actual_hosts = provider1.refresh()
    assert expected_hosts == actual_hosts
    assert provider1._is_primary_cluster_id

    props2 = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})
    topology_utils2 = AuroraTopologyUtils(AuroraPgDialect(), props2)
    provider2 = RdsHostListProvider(mock_provider_service, props2, topology_utils2)
    spy = mocker.spy(provider2._topology_utils, "_query_for_topology")
    provider2._initialize()

    assert provider1._cluster_id == provider2._cluster_id
    assert provider2._is_primary_cluster_id

    actual_hosts = provider2.refresh()
    assert expected_hosts == actual_hosts
    assert 1 == len(StorageService.get_all(Topology))
    spy.assert_not_called()


def test_cluster_id_suggestion_for_existing_provider(mocker, mock_provider_service, mock_conn, mock_cursor):
    props1 = Properties({"host": "instance-2.xyz.us-east-2.rds.amazonaws.com"})
    topology_utils1 = AuroraTopologyUtils(AuroraPgDialect(), props1)
    provider1 = RdsHostListProvider(mock_provider_service, props1, topology_utils1)
    records = [("instance-1", False),
               ("instance-2", True),
               ("instance-3", False)]
    mock_topology_query(mock_conn, mock_cursor, records)
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.READER),
                      HostInfo("instance-2.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),
                      HostInfo("instance-3.xyz.us-east-2.rds.amazonaws.com", role=HostRole.READER))

    actual_hosts = provider1.refresh()
    assert list(expected_hosts).sort(key=lambda h: h.host) == list(actual_hosts).sort(key=lambda h: h.host)
    assert not provider1._is_primary_cluster_id

    props2 = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils2 = AuroraTopologyUtils(AuroraPgDialect(), props2)
    provider2 = RdsHostListProvider(mock_provider_service, props2, topology_utils2)
    provider2._initialize()

    assert provider2._cluster_id != provider1._cluster_id
    assert provider2._is_primary_cluster_id
    assert not provider1._is_primary_cluster_id
    assert 1 == len(StorageService.get_all(Topology))

    provider2.refresh()
    assert "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com" == \
           RdsHostListProvider._cluster_ids_to_update.get(provider1._cluster_id)

    spy = mocker.spy(provider1._topology_utils, "_query_for_topology")
    actual_hosts = provider1.refresh()
    assert 2 == len(StorageService.get_all(Topology))
    assert list(expected_hosts).sort(key=lambda h: h.host) == list(actual_hosts).sort(key=lambda h: h.host)
    assert provider2._cluster_id == provider1._cluster_id
    assert provider2._is_primary_cluster_id
    assert provider1._is_primary_cluster_id
    spy.assert_not_called()


def test_identify_connection_errors(mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = None
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)

    with pytest.raises(AwsWrapperError):
        provider.identify_connection(mock_conn)

    mock_cursor.execute.side_effect = TimeoutError()
    with pytest.raises(QueryTimeoutError):
        provider.identify_connection(mock_conn)


def test_identify_connection_no_match_in_topology(mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = ("non-matching-host",)
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)

    assert provider.identify_connection(mock_conn) is None


def test_identify_connection_empty_topology(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    mock_cursor.fetchone.return_value = ("instance-1",)

    provider.refresh = mocker.MagicMock(return_value=[])
    assert provider.identify_connection(mock_conn) is None


def test_identify_connection_host_in_topology(mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    mock_cursor.fetchone.return_value = ("instance-1",)
    mock_topology_query(mock_conn, mock_cursor, [("instance-1", True)])

    host_info = provider.identify_connection(mock_conn)
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id


def test_host_pattern_setting(mock_provider_service, props):
    props = Properties({"host": "127:0:0:1",
                        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: "?.custom-domain.com"})

    provider = RdsHostListProvider(mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))
    assert "?.custom-domain.com" == provider._topology_utils.instance_template.host

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "invalid_host_pattern"
        provider = RdsHostListProvider(mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "?.proxy-xyz.us-east-2.rds.amazonaws.com"
        provider = RdsHostListProvider(mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = \
            "?.cluster-custom-xyz.us-east-2.rds.amazonaws.com"
        provider = RdsHostListProvider(mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))


def test_get_host_role(mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = (True,)
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)

    assert HostRole.READER == provider.get_host_role(mock_conn)

    mock_cursor.fetchone.return_value = None
    with pytest.raises(AwsWrapperError):
        provider.get_host_role(mock_conn)

    mock_cursor.execute.side_effect = TimeoutError()
    with pytest.raises(QueryTimeoutError):
        provider.get_host_role(mock_conn)


def test_cluster_id_setting(mock_provider_service):
    props = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com",
                        WrapperProperties.CLUSTER_ID.name: "my-cluster-id"})
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    provider._initialize()
    assert provider._cluster_id == "my-cluster-id"


def test_initialize_rds_proxy(mock_provider_service):
    props = Properties({"host": "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    provider._initialize()
    assert provider._cluster_id == "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com/"


def test_get_topology_returns_last_writer(mocker, mock_provider_service, mock_conn, mock_cursor):
    mock_provider_service.current_connection = mock_conn
    mock_topology_query(mock_conn, mock_cursor, [
        ("expected_writer_host", True, 0, 0, None),
        ("unexpected_writer_host_0", True, 0, 0, None),
        ("unexpected_writer_host_no_last_update_time_0", True, 0, 0, datetime.strptime("1000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")),
        ("unexpected_writer_host_no_last_update_time_1", True, 0, 0, datetime.strptime("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")),
        ("expected_writer_host", True, 0, 0, datetime.strptime("3000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))])

    props = Properties({"host": "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)
    spy = mocker.spy(topology_utils, "_query_for_topology")
    provider._initialize()

    result = provider._get_topology(mock_conn, True)
    assert result.hosts[0].host == "expected_writer_host.xyz.us-east-2.rds.amazonaws.com"
    spy.assert_called_once()


def test_force_monitoring_refresh(mock_provider_service, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, props, topology_utils)

    with pytest.raises(AwsWrapperError):
        provider.force_monitoring_refresh(True, 5)
