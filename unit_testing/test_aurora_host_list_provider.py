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

import types
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.host_list_provider import AuroraHostListProvider
from aws_wrapper.hostinfo import HostInfo, HostRole
from aws_wrapper.pep249 import Error, ProgrammingError
from aws_wrapper.utils.dialect import Dialect
from aws_wrapper.utils.properties import Properties, WrapperProperties


@pytest.fixture(autouse=True)
def clear_caches():
    AuroraHostListProvider._topology_cache.clear()
    AuroraHostListProvider._primary_cluster_id_cache.clear()
    AuroraHostListProvider._suggested_primary_cluster_id_cache.clear()


@pytest.fixture
def defaults():
    defaults = types.SimpleNamespace()
    defaults.a = 1
    defaults.mock_conn = MagicMock()
    defaults.mock_cursor = MagicMock()
    defaults.mock_provider_service = MagicMock()
    defaults.props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})
    defaults.initial_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com"),)
    defaults.cache_hosts = (HostInfo("host1"), HostInfo("host2"))
    defaults.queried_hosts = (HostInfo("new-host-id.xyz.us-east-2.rds.amazonaws.com"),)
    defaults.refresh_ns = 5_000_000_000  # 5 seconds

    mock_topology_query(defaults, [("new-host-id", True)])
    defaults.mock_provider_service.current_connection = defaults.mock_conn

    return defaults


def test_get_topology_caches_topology(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    AuroraHostListProvider._topology_cache.put(provider._cluster_id, defaults.cache_hosts, defaults.refresh_ns)
    spy = mocker.spy(provider, "_query_for_topology")

    result = provider.refresh(defaults.mock_conn)

    assert defaults.cache_hosts == result
    spy.assert_not_called()


def test_get_topology_force_update(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    AuroraHostListProvider._topology_cache.put(provider._cluster_id, defaults.cache_hosts, defaults.refresh_ns)
    spy = mocker.spy(provider, "_query_for_topology")

    result = provider.force_refresh(defaults.mock_conn)

    assert defaults.queried_hosts == result
    spy.assert_called_once()


def test_get_topology_invalid_dialect(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    spy = mocker.spy(provider, "_query_for_topology")
    defaults.mock_provider_service.dialect = None

    result = provider.refresh()

    assert defaults.initial_hosts == result
    spy.assert_called_once()


def test_get_topology_invalid_topology(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    AuroraHostListProvider._topology_cache.put(provider._cluster_id, defaults.cache_hosts, defaults.refresh_ns)
    spy = mocker.spy(provider, "_query_for_topology")
    mock_topology_query(defaults, [("reader", False)])  # Invalid topology: no writer instance

    result = provider.force_refresh()

    assert defaults.cache_hosts == result
    spy.assert_called_once()


def test_get_topology_invalid_query(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    defaults.mock_cursor.execute.side_effect = ProgrammingError()
    spy = mocker.spy(provider, "_query_for_topology")

    with pytest.raises(AwsWrapperError):
        provider.force_refresh(defaults.mock_conn)
    spy.assert_called_once()


def test_get_topology_multiple_writers(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    spy = mocker.spy(provider, "_query_for_topology")
    now = datetime.now()
    records = [("old_writer", True, None, None, now), ("new_writer", True, None, None, now + timedelta(seconds=10))]
    mock_topology_query(defaults, records)

    result = provider.refresh()

    assert 1 == len(result)
    assert "new_writer.xyz.us-east-2.rds.amazonaws.com" == result[0].host
    spy.assert_called_once()


def test_get_topology_no_connection(mocker, defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    spy = mocker.spy(provider, "_query_for_topology")
    defaults.mock_provider_service.dialect = None
    defaults.mock_provider_service.current_connection = None

    result = provider.refresh()

    assert defaults.initial_hosts == result
    spy.assert_not_called()


def test_create_host_with_provider_uninitialized(defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)

    assert provider._cluster_instance_template is None
    with pytest.raises(AwsWrapperError):
        provider._create_host(('instance-1',))

    provider._cluster_instance_template = HostInfo("?.xyz.us-east-2.rds.amazonaws.com")
    assert provider._initial_host_info is None
    with pytest.raises(AwsWrapperError):
        provider._create_host(('instance-1',))


def test_no_cluster_id_suggestion_for_separate_clusters(defaults):
    props_a = Properties({"host": "instance-A-1.domain.com"})
    provider_a = AuroraHostListProvider(defaults.mock_provider_service, props_a)
    mock_topology_query(defaults, [("instance-A-1.domain.com", True)])
    expected_hosts_a = (HostInfo("instance-A-1.domain.com", role=HostRole.WRITER),)

    actual_hosts_a = provider_a.refresh()
    assert expected_hosts_a == actual_hosts_a

    props_b = Properties({"host": "instance-B-1.domain.com"})
    provider_b = AuroraHostListProvider(defaults.mock_provider_service, props_b)
    mock_topology_query(defaults, [("instance-B-1.domain.com", True)])
    expected_hosts_b = (HostInfo("instance-B-1.domain.com", role=HostRole.WRITER),)

    actual_hosts_b = provider_b.refresh()
    assert expected_hosts_b == actual_hosts_b
    assert 2 == len(AuroraHostListProvider._topology_cache)


def test_cluster_id_suggestion_for_new_provider_with_cluster_url(mocker, defaults):
    props = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com"})
    provider1 = AuroraHostListProvider(defaults.mock_provider_service, props)
    mock_topology_query(defaults, [("instance-1", True)])
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)

    actual_hosts = provider1.refresh()
    assert expected_hosts == actual_hosts
    assert provider1._is_primary_cluster_id

    provider2 = AuroraHostListProvider(defaults.mock_provider_service, props)
    spy = mocker.spy(provider2, "_query_for_topology")
    provider2._initialize()

    assert provider1._cluster_id == provider2._cluster_id
    assert provider2._is_primary_cluster_id

    actual_hosts = provider2.refresh()
    assert expected_hosts == actual_hosts
    assert 1 == len(AuroraHostListProvider._topology_cache)
    spy.assert_not_called()


def test_cluster_id_suggestion_for_new_provider_with_instance_url(mocker, defaults):
    props1 = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com"})
    provider1 = AuroraHostListProvider(defaults.mock_provider_service, props1)
    mock_topology_query(defaults, [("instance-1", True)])
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),)

    actual_hosts = provider1.refresh()
    assert expected_hosts == actual_hosts
    assert provider1._is_primary_cluster_id

    props2 = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})
    provider2 = AuroraHostListProvider(defaults.mock_provider_service, props2)
    spy = mocker.spy(provider2, "_query_for_topology")
    provider2._initialize()

    assert provider1._cluster_id == provider2._cluster_id
    assert provider2._is_primary_cluster_id

    actual_hosts = provider2.refresh()
    assert expected_hosts == actual_hosts
    assert 1 == len(AuroraHostListProvider._topology_cache)
    spy.assert_not_called()


def test_cluster_id_suggestion_for_existing_provider(mocker, defaults):
    props1 = Properties({"host": "instance-2.xyz.us-east-2.rds.amazonaws.com"})
    provider1 = AuroraHostListProvider(defaults.mock_provider_service, props1)
    records = [("instance-1", False),
               ("instance-2", True),
               ("instance-3", False)]
    mock_topology_query(defaults, records)
    expected_hosts = (HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", role=HostRole.READER),
                      HostInfo("instance-2.xyz.us-east-2.rds.amazonaws.com", role=HostRole.WRITER),
                      HostInfo("instance-3.xyz.us-east-2.rds.amazonaws.com", role=HostRole.READER))

    actual_hosts = provider1.refresh()
    assert list(expected_hosts).sort(key=lambda h: h.host) == list(actual_hosts).sort(key=lambda h: h.host)
    assert not provider1._is_primary_cluster_id

    props2 = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com"})
    provider2 = AuroraHostListProvider(defaults.mock_provider_service, props2)
    provider2._initialize()

    assert provider2._cluster_id != provider1._cluster_id
    assert provider2._is_primary_cluster_id
    assert not provider1._is_primary_cluster_id
    assert 1 == len(AuroraHostListProvider._topology_cache)

    provider2.refresh()
    assert "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com" == \
           AuroraHostListProvider._suggested_primary_cluster_id_cache.get(provider1._cluster_id)

    spy = mocker.spy(provider1, "_query_for_topology")
    actual_hosts = provider1.refresh()
    assert 2 == len(AuroraHostListProvider._topology_cache)
    assert list(expected_hosts).sort(key=lambda h: h.host) == list(actual_hosts).sort(key=lambda h: h.host)
    assert provider2._cluster_id == provider1._cluster_id
    assert provider2._is_primary_cluster_id
    assert provider1._is_primary_cluster_id
    spy.assert_not_called()


def test_identify_connection_errors(defaults):
    defaults.mock_cursor.fetchone.return_value = None
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)

    with pytest.raises(AwsWrapperError):
        provider.identify_connection(defaults.mock_conn)

    defaults.mock_cursor.execute.side_effect = Error()
    with pytest.raises(AwsWrapperError):
        provider.identify_connection(defaults.mock_conn)


def test_identify_connection_no_match_in_topology(defaults):
    defaults.mock_cursor.fetchone.return_value = ("non-matching-host",)
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)

    assert provider.identify_connection(defaults.mock_conn) is None


def test_identify_connection_empty_topology(defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    defaults.mock_cursor.fetchone.return_value = ("instance-1",)

    with patch.object(provider, 'refresh') as mock_refresh:
        mock_refresh.return_value = []
        assert provider.identify_connection(defaults.mock_conn) is None


def test_identify_connection_host_in_topology(defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    defaults.mock_cursor.fetchone.return_value = ("instance-1",)
    mock_topology_query(defaults, [("instance-1", True)])

    host_info = provider.identify_connection(defaults.mock_conn)
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id


def test_host_pattern_setting(defaults):
    props = Properties({"host": "127:0:0:1",
                        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: "?.custom-domain.com"})

    provider = AuroraHostListProvider(defaults.mock_provider_service, props)
    provider._initialize()
    assert "?.custom-domain.com" == provider._cluster_instance_template.host

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "invalid_host_pattern"
        provider = AuroraHostListProvider(defaults.mock_provider_service, props)
        provider._initialize()

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = "?.proxy-xyz.us-east-2.rds.amazonaws.com"
        provider = AuroraHostListProvider(defaults.mock_provider_service, props)
        provider._initialize()

    with pytest.raises(AwsWrapperError):
        props[WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name] = \
            "?.cluster-custom-xyz.us-east-2.rds.amazonaws.com"
        provider = AuroraHostListProvider(defaults.mock_provider_service, props)
        provider._initialize()


def test_get_topology_aware_dialect_invalid_dialect(defaults):
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)
    mock_dialect = MagicMock(spec=Dialect)
    mock_dialect.__class__ = Dialect
    defaults.mock_provider_service.dialect = mock_dialect

    with pytest.raises(AwsWrapperError):
        provider._get_topology_aware_dialect("AuroraHostListProvider.InvalidDialectForGetHostRole")


def test_get_host_role(defaults):
    defaults.mock_cursor.fetchone.return_value = (True,)
    provider = AuroraHostListProvider(defaults.mock_provider_service, defaults.props)

    assert HostRole.READER == provider.get_host_role(defaults.mock_conn)

    defaults.mock_cursor.fetchone.return_value = None
    with pytest.raises(AwsWrapperError):
        provider.get_host_role(defaults.mock_conn)

    defaults.mock_cursor.execute.side_effect = Error()
    with pytest.raises(AwsWrapperError):
        provider.get_host_role(defaults.mock_conn)


def test_cluster_id_setting(defaults):
    props = Properties({"host": "my-cluster.cluster-xyz.us-east-2.rds.amazonaws.com",
                        WrapperProperties.CLUSTER_ID.name: "my-cluster-id"})
    provider = AuroraHostListProvider(defaults.mock_provider_service, props)
    provider._initialize()
    assert provider._cluster_id == "my-cluster-id"


def test_initialize_rds_proxy(defaults):
    props = Properties({"host": "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com"})
    provider = AuroraHostListProvider(defaults.mock_provider_service, props)
    provider._initialize()
    assert provider._cluster_id == "my-cluster.proxy-xyz.us-east-2.rds.amazonaws.com"


def mock_topology_query(defaults, records):
    defaults.mock_conn.cursor.return_value = defaults.mock_cursor
    defaults.mock_cursor.__enter__.return_value = defaults.mock_cursor  # Mocks out `with conn.cursor() as cursor:`
    defaults.mock_cursor.__iter__.return_value = records  # Mocks out `for record in cursor:`
