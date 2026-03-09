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

from aws_advanced_python_wrapper.database_dialect import (
    AuroraPgDialect, MultiAzClusterPgDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.host_list_provider import (
    MultiAzTopologyUtils, RdsHostListProvider)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import ProgrammingError
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.storage.storage_service import (
    StorageService, Topology)


@pytest.fixture(autouse=True)
def clear_caches():
    StorageService.clear_all()


def mock_topology_query(mock_conn, mock_cursor, records, writer_id=None):
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor  # Mocks out `with conn.cursor() as cursor:`
    mock_cursor.__iter__.return_value = records  # Mocks out `for record in cursor:`
    mock_cursor.fetchone.return_value = (writer_id,) if writer_id is not None else (records[0][0],)


@pytest.fixture(autouse=True)
def mock_default_behavior(mock_provider_service, mock_conn, mock_cursor):
    mock_provider_service.current_connection = mock_conn
    mock_topology_query(mock_conn, mock_cursor, [("new-host", "new-host.xyz.us-east-2.rds.amazonaws.com", 5432)])


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
    return Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com", "port": 5432})


@pytest.fixture
def initial_hosts():
    return (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432),)


@pytest.fixture
def cache_hosts():
    return HostInfo("host1.xyz.us-east-2.rds.amazonaws.com", 5432), HostInfo("host2.xyz.us-east-2.rds.amazonaws.com", 5432)


@pytest.fixture
def queried_hosts():
    return (HostInfo("new-host.xyz.us-east-2.rds.amazonaws.com", 5432),)


@pytest.fixture
def refresh_ns():
    return 5_000_000_000  # 5 seconds


def create_provider(mock_provider_service, props):
    dialect = MultiAzClusterPgDialect()
    topology_utils = MultiAzTopologyUtils(dialect, props, "writer_host_query", 0)
    return RdsHostListProvider(mock_provider_service, mock_provider_service, props, topology_utils)


def test_get_topology_caches_topology(mocker, mock_provider_service, mock_conn, props, cache_hosts, refresh_ns):
    provider = create_provider(mock_provider_service, props)
    provider._initialize()
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    mock_monitor = mocker.MagicMock()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    result = provider.refresh(mock_conn)

    assert cache_hosts == result
    mock_monitor.force_refresh_with_connection.assert_not_called()


def test_get_topology_force_update(
        mocker, mock_provider_service, mock_conn, cache_hosts, queried_hosts, props, refresh_ns):
    provider = create_provider(mock_provider_service, props)
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh_with_connection.return_value = queried_hosts
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    result = provider.force_refresh(mock_conn)

    assert queried_hosts == result
    mock_monitor.force_refresh_with_connection.assert_called_once()


def test_get_topology_timeout(mocker, mock_cursor, mock_provider_service, initial_hosts, props):
    provider = create_provider(mock_provider_service, props)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh_with_connection.side_effect = TimeoutError()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    with pytest.raises(QueryTimeoutError):
        provider.force_refresh()

    mock_monitor.force_refresh_with_connection.assert_called_once()


def test_get_topology_invalid_topology(
        mocker, mock_provider_service, mock_conn, mock_cursor, props, cache_hosts, refresh_ns):
    provider = create_provider(mock_provider_service, props)
    provider._initialize()
    StorageService.set(provider._cluster_id, cache_hosts, Topology)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh_with_connection.return_value = ()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    result = provider.force_refresh()

    assert cache_hosts == result
    mock_monitor.force_refresh_with_connection.assert_called_once()


def test_get_topology_invalid_query(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    provider = create_provider(mock_provider_service, props)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh_with_connection.side_effect = ProgrammingError()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    with pytest.raises(ProgrammingError):
        provider.force_refresh(mock_conn)
    mock_monitor.force_refresh_with_connection.assert_called_once()


def test_get_topology_no_connection(mocker, mock_provider_service, initial_hosts, props):
    provider = create_provider(mock_provider_service, props)
    mock_monitor = mocker.MagicMock()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)
    mock_provider_service.database_dialect = None
    mock_provider_service.current_connection = None

    result = provider.refresh()

    assert initial_hosts == result
    mock_monitor.force_refresh_with_connection.assert_not_called()


def test_identify_connection_errors(mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = None
    provider = create_provider(mock_provider_service, props)

    with pytest.raises(AwsWrapperError):
        provider.identify_connection(mock_conn)

    mock_cursor.execute.side_effect = TimeoutError()
    with pytest.raises(QueryTimeoutError):
        provider.identify_connection(mock_conn)


def test_identify_connection_no_match_in_topology(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = ("non-matching-host",)
    provider = create_provider(mock_provider_service, props)
    mock_monitor = mocker.MagicMock()
    mock_monitor.force_refresh_with_connection.return_value = ()
    mocker.patch.object(provider, '_get_or_create_monitor', return_value=mock_monitor)

    assert provider.identify_connection(mock_conn) is None


def test_identify_connection_empty_topology(mocker, mock_provider_service, mock_conn, mock_cursor, props):
    provider = create_provider(mock_provider_service, props)
    mock_cursor.fetchone.return_value = ("instance-1",)

    provider.refresh = mocker.MagicMock(return_value=[])
    assert provider.identify_connection(mock_conn) is None


def test_identify_connection_host_in_topology(mock_provider_service, mock_conn, mock_cursor, props):
    provider = create_provider(mock_provider_service, props)
    mock_cursor.fetchone.return_value = ("instance-1",)
    mock_topology_query(mock_conn, mock_cursor, [("instance-1", "instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)])

    host_info = provider.identify_connection(mock_conn)
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id


def test_host_pattern_setting(mock_provider_service, props):
    props = Properties({"host": "127:0:0:1", "port": 5432,
                        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: "?.custom-domain.com"})

    provider = create_provider(mock_provider_service, props)
    provider._initialize()
    assert "?.custom-domain.com" == provider._topology_utils.instance_template.host

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "invalid_host_pattern"
        provider = create_provider(mock_provider_service, props)
        provider._initialize()

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "?.proxy-xyz.us-east-2.rds.amazonaws.com"
        provider = create_provider(mock_provider_service, props)
        provider._initialize()

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = \
            "?.cluster-custom-xyz.us-east-2.rds.amazonaws.com"
        provider = create_provider(mock_provider_service, props)
        provider._initialize()


def test_get_host_role(mock_provider_service, mock_conn, mock_cursor, props):
    mock_cursor.fetchone.return_value = (True,)
    provider = create_provider(mock_provider_service, props)

    assert HostRole.READER == provider.get_host_role(mock_conn)

    mock_cursor.fetchone.return_value = None
    with pytest.raises(AwsWrapperError):
        provider.get_host_role(mock_conn)

    mock_cursor.execute.side_effect = TimeoutError()
    with pytest.raises(QueryTimeoutError):
        provider.get_host_role(mock_conn)


def test_cluster_id_setting(mock_provider_service):
    props = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com", "port": 5432,
                        WrapperProperties.CLUSTER_ID.name: "my-cluster-id"})
    provider = create_provider(mock_provider_service, props)
    provider._initialize()
    assert provider._cluster_id == "my-cluster-id"


def test_initialize__rds_proxy(mock_provider_service):
    props = Properties({"host": "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com", "port": 5432})
    provider = create_provider(mock_provider_service, props)
    provider._initialize()
    assert provider._cluster_id == "1"


def test_query_for_topology__empty_writer_query_results(
        mock_provider_service, props, mock_conn, mock_cursor, queried_hosts):
    provider = create_provider(mock_provider_service, props)
    records = [("new-host", "new-host.xyz.us-east-2.rds.amazonaws.com", 5432)]

    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor  # Mocks out `with conn.cursor() as cursor:`
    mock_cursor.__iter__.return_value = records  # Mocks out `for record in cursor:`
    mock_cursor.fetchone.side_effect = [None, ("new-host",)]

    result = provider._topology_utils._query_for_topology(mock_conn)
    assert result == queried_hosts
