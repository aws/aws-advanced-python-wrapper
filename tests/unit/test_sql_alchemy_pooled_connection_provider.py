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

import psycopg
import pytest

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import (
    PoolKey, SqlAlchemyPooledConnectionProvider)
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCache


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_pool(mocker, mock_conn):
    pool = mocker.MagicMock()
    pool.checkedout.return_value = 1
    pool.connect.return_value = mock_conn
    return pool


@pytest.fixture
def mock_pool_1_connections(mocker):
    pool = mocker.MagicMock()
    pool.checkedout.return_value = 1
    return pool


@pytest.fixture
def provider(mocker, mock_pool):
    provider = SqlAlchemyPooledConnectionProvider()
    create_pool_patch = mocker.patch.object(provider, "_create_sql_alchemy_pool")
    create_pool_patch.return_value = mock_pool
    return provider


@pytest.fixture
def host_info():
    return HostInfo("url")


@pytest.fixture(autouse=True)
def clear_cache():
    SqlAlchemyPooledConnectionProvider._database_pools.clear()


def test_connect__default_mapping__default_pool_configuration(provider, host_info, mocker, mock_conn, mock_pool):
    expected_urls = {host_info.url}
    expected_keys = {PoolKey(host_info.url, "user1")}
    props = Properties({WrapperProperties.USER.name: "user1", WrapperProperties.PASSWORD.name: "password"})

    conn = provider.connect(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock(), host_info, props)
    assert conn is mock_conn
    assert 1 == provider.num_pools
    assert expected_urls == provider.pool_urls
    assert expected_keys == provider.keys()


def test_connect__custom_configuration_and_mapping(host_info, mocker, mock_conn, mock_pool):
    expected_keys = {PoolKey(host_info.url, f"{host_info.url}+some_unique_key")}
    props = Properties({WrapperProperties.USER.name: "user1", WrapperProperties.PASSWORD.name: "password"})
    attempt_creator_override_func = mocker.MagicMock()

    provider = SqlAlchemyPooledConnectionProvider(
        pool_configurator=lambda _, __: {"creator": attempt_creator_override_func, "pool_size": 10},
        pool_mapping=lambda host, _: f"{host.url}+some_unique_key")
    mock_driver_dialect = mocker.MagicMock()
    mock_driver_dialect.prepare_connect_info.return_value = props
    mock_pool_connection_func = mocker.MagicMock()
    get_connection_func_patch = mocker.patch.object(provider, "_get_connection_func")
    get_connection_func_patch.return_value = mock_pool_connection_func
    mock_pool_initializer_func = mocker.patch.object(provider, "_create_sql_alchemy_pool")
    mock_pool_initializer_func.return_value = mock_pool

    mock_target_driver_connect_func = mocker.MagicMock()
    conn = provider.connect(mock_target_driver_connect_func, mock_driver_dialect, mocker.MagicMock(), host_info, props)
    assert conn is mock_conn
    assert 1 == provider.num_pools
    assert expected_keys == provider.keys()
    get_connection_func_patch.assert_called_with(mock_target_driver_connect_func, mock_driver_dialect, props, props)
    mock_pool_initializer_func.assert_called_with(creator=mock_pool_connection_func, pool_size=10)


def test_accepts_host_info(provider):
    instance_url = "instance-1.XYZ.us-east-2.rds.amazonaws.com"
    instance_host_info = HostInfo(instance_url)
    cluster_url = "my-cluster.cluster-XYZ.us-east-2.rds.amazonaws.com"
    cluster_host_info = HostInfo(cluster_url)
    props = Properties({WrapperProperties.USER.name: "user1", WrapperProperties.PASSWORD.name: "password"})

    assert provider.accepts_host_info(instance_host_info, props) is True
    assert provider.accepts_host_info(cluster_host_info, props) is False


def test_least_connections_strategy(provider, mock_pool):
    writer = HostInfo("writer.XYZ.us-east-2.rds.amazonaws.com")
    reader_1 = HostInfo("reader-1.XYZ.us-east-2.rds.amazonaws.com", role=HostRole.READER)
    reader_2 = HostInfo("reader-2.XYZ.us-east-2.rds.amazonaws.com", role=HostRole.READER)
    hosts = [writer, reader_1, reader_2]
    props = Properties({WrapperProperties.USER.name: "user1", WrapperProperties.PASSWORD.name: "password"})

    # Create cache with 1 pool to reader_url_1_connection and 2 pools to reader_url_2_connections.
    # Each pool holds 1 connection.
    test_database_pools = SlidingExpirationCache()
    test_database_pools.compute_if_absent(
        PoolKey(reader_1.url, "user1"), lambda _: mock_pool, 10 * 60_000_000_000)
    test_database_pools.compute_if_absent(
        PoolKey(reader_2.url, "user1"), lambda _: mock_pool, 10 * 60_000_000_000)
    test_database_pools.compute_if_absent(
        PoolKey(reader_2.url, "user2"), lambda _: mock_pool, 10 * 60_000_000_000)

    result = provider.get_host_info_by_strategy(hosts, HostRole.READER, "least_connections", props)
    assert reader_1 == result


def test_least_connections_strategy__no_hosts_matching_role(provider):
    props = Properties()
    with pytest.raises(AwsWrapperError):
        provider.get_host_info_by_strategy([HostInfo("writer")], HostRole.READER, "least_connections", props)


def test_release_resources(provider, mocker):
    pool1 = mocker.MagicMock()
    pool2 = mocker.MagicMock()
    test_database_pools = SlidingExpirationCache()
    test_database_pools.compute_if_absent(PoolKey("url1", "user1"), lambda _: pool1, 60_000_000_000)
    test_database_pools.compute_if_absent(PoolKey("url1", "user2"), lambda _: pool2, 60_000_000_000)
    SqlAlchemyPooledConnectionProvider._database_pools = test_database_pools

    provider.release_resources()
    pool1.dispose.assert_called_once()
    pool2.dispose.assert_called_once()
    assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)
