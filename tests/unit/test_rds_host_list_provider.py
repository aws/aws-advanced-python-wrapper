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
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    provider._initialize()
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    mock_force_refresh = mocker.patch.object(provider, '_force_refresh_monitor')

    result = provider.refresh(mock_conn)

    assert cache_hosts == result
    mock_force_refresh.assert_not_called()


def test_get_topology_force_update(
        mocker, mock_provider_service, mock_conn, cache_hosts, queried_hosts, props, refresh_ns):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    mocker.patch.object(provider, '_force_refresh_monitor', return_value=queried_hosts)

    result = provider.force_refresh(mock_conn)

    assert queried_hosts == result


def test_get_topology_timeout(mocker, mock_cursor, mock_provider_service, initial_hosts, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mocker.patch.object(provider, '_force_refresh_monitor', return_value=None)

    result = provider.force_refresh()

    assert initial_hosts == result


def test_get_topology_invalid_topology(
        mocker, mock_provider_service, mock_conn, mock_cursor, props, cache_hosts, refresh_ns):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    provider._initialize()
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    mocker.patch.object(provider, '_force_refresh_monitor', return_value=())

    result = provider.force_refresh()

    assert cache_hosts == result


def test_get_topology_invalid_query(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mocker.patch.object(provider, '_force_refresh_monitor', side_effect=ProgrammingError())

    with pytest.raises(ProgrammingError):
        provider.force_refresh(mock_conn)


def test_get_topology_multiple_writers(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    expected_hosts = (HostInfo("new_writer.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)
    mocker.patch.object(provider, '_force_refresh_monitor', return_value=expected_hosts)

    result = provider.refresh()

    assert 1 == len(result)
    assert result[0].host == "new_writer.xyz.us-east-2.rds.amazonaws.com"


def test_get_topology_no_connection(mocker, mock_provider_service, initial_hosts, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mock_monitor = mocker.MagicMock()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)
    mock_provider_service.database_dialect = None
    mock_provider_service.current_connection = None

    result = provider.refresh()

    assert initial_hosts == result
    mock_monitor.force_refresh_with_connection.assert_not_called()


def test_identify_connection_errors(mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = None
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)

    with pytest.raises(AwsWrapperError):
        provider.identify_connection(mock_conn)

    mock_cursor.execute.side_effect = TimeoutError()
    with pytest.raises(QueryTimeoutError):
        provider.identify_connection(mock_conn)


def test_identify_connection_no_match_in_topology(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = ("non-matching-host",)
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh_with_connection.return_value = ()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    assert provider.identify_connection(mock_conn) is None


def test_identify_connection_empty_topology(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mock_cursor.fetchone.return_value = ("instance-1",)

    provider.refresh = mocker.MagicMock(return_value=[])
    provider.force_refresh = mocker.MagicMock(return_value=[])
    assert provider.identify_connection(mock_conn) is None


def test_identify_connection_host_in_topology(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mock_cursor.fetchone.return_value = ("instance-1",)
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", host_id="instance-1", role=HostRole.WRITER),)
    mocker.patch.object(provider, '_force_refresh_monitor', return_value=expected_hosts)

    host_info = provider.identify_connection(mock_conn)
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id


def test_host_pattern_setting(mock_provider_service, props):
    props = Properties({"host": "127:0:0:1",
                        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: "?.custom-domain.com"})

    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))
    assert "?.custom-domain.com" == provider._topology_utils.instance_template.host

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "invalid_host_pattern"
        provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "?.proxy-xyz.us-east-2.rds.amazonaws.com"
        provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = \
            "?.cluster-custom-xyz.us-east-2.rds.amazonaws.com"
        provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, AuroraTopologyUtils(AuroraPgDialect(), props))


def test_cluster_id_setting(mock_provider_service):
    props = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com",
                        WrapperProperties.CLUSTER_ID.name: "my-cluster-id"})
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    provider._initialize()
    assert provider._cluster_id == "my-cluster-id"


def test_initialize_rds_proxy(mock_provider_service):
    props = Properties({"host": "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    provider._initialize()
    assert provider._cluster_id == "1"


def test_get_topology_returns_last_writer(mocker, mock_provider_service, mock_conn, mock_cursor):
    mock_provider_service.current_connection = mock_conn
    expected_hosts = (HostInfo("expected_writer_host.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)

    props = Properties({"host": "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com"})
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)
    mocker.patch.object(provider, '_force_refresh_monitor', return_value=expected_hosts)
    provider._initialize()

    result = provider._get_topology(mock_conn, True)
    assert result.hosts[0].host == "expected_writer_host.xyz.us-east-2.rds.amazonaws.com"


def test_force_monitoring_refresh(mocker, mock_provider_service, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)

    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh.return_value = None
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    # force_monitoring_refresh returns empty tuple when monitor cannot refresh topology
    result = provider.force_monitoring_refresh(True, 5)
    assert result == ()


def test_force_monitoring_refresh_with_topology(mocker, mock_provider_service, props):
    topology_utils = AuroraTopologyUtils(AuroraPgDialect(), props)
    provider = RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)

    expected_topology = (HostInfo("host1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh.return_value = expected_topology
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    result = provider.force_monitoring_refresh(True, 5)
    assert result == expected_topology
    mock_monitor.force_refresh.assert_called_once_with(True, 5)
