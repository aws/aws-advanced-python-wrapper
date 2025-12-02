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

from typing import List

import psycopg
import pytest

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverSuccessError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import Error
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    ReadWriteSplittingPlugin
from aws_advanced_python_wrapper.simple_read_write_splitting_plugin import \
    SimpleReadWriteSplittingPlugin
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils.notifications import \
    OldConnectionSuggestedAction
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.unit.utils.unit_test_utils import AnyInstanceOf

# Common test data
WRITE_ENDPOINT = "writer.cluster-xyz.us-east-1.rds.amazonaws.com"
READ_ENDPOINT = "reader.cluster-xyz.us-east-1.rds.amazonaws.com"
TEST_PORT = 5432

writer_host = HostInfo(host="instance0", role=HostRole.WRITER)
reader_host1 = HostInfo(host="instance1", role=HostRole.READER)
reader_host2 = HostInfo(host="instance2", role=HostRole.READER)
reader_host3 = HostInfo(host="instance3", role=HostRole.READER)

default_hosts: List[HostInfo] = [writer_host, reader_host1, reader_host2, reader_host3]
single_reader_topology: List[HostInfo] = [writer_host, reader_host1]

# Simple plugin specific hosts
simple_writer_host = HostInfo(host=WRITE_ENDPOINT, port=TEST_PORT, role=HostRole.WRITER)
simple_reader_host = HostInfo(host=READ_ENDPOINT, port=TEST_PORT, role=HostRole.READER)
any_host = AnyInstanceOf(HostInfo)


# Plugin configurations
@pytest.fixture(
    params=[
        ("read_write_splitting", ReadWriteSplittingPlugin),
        ("srw", SimpleReadWriteSplittingPlugin),
    ]
)
def plugin_config(request):
    return request.param


@pytest.fixture
def plugin_props(plugin_config):
    plugin_name, _ = plugin_config
    props = Properties()
    if plugin_name == "srw":
        props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
        props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
        props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "600"
        props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = "10"
    return props


@pytest.fixture
def host_list_provider_service_mock(mocker, plugin_config):
    plugin_name, _ = plugin_config
    mock = mocker.MagicMock()
    if plugin_name == "srw":
        mock.initial_connection_host_info = simple_writer_host
    return mock


@pytest.fixture
def reader_conn_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def writer_conn_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def closed_writer_conn_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def new_writer_conn_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def connect_func_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def driver_dialect_mock(mocker, writer_conn_mock, closed_writer_conn_mock):
    def is_closed_side_effect(conn):
        return conn == closed_writer_conn_mock

    driver_dialect_mock = mocker.MagicMock()
    driver_dialect_mock.is_closed.side_effect = is_closed_side_effect
    driver_dialect_mock.get_connection_from_obj.return_value = writer_conn_mock
    driver_dialect_mock.unwrap_connection.return_value = writer_conn_mock
    driver_dialect_mock.can_execute_query.return_value = True
    return driver_dialect_mock


@pytest.fixture
def plugin_service_mock(mocker, driver_dialect_mock, writer_conn_mock):
    plugin_service_mock = mocker.MagicMock()
    plugin_service_mock.driver_dialect = driver_dialect_mock
    plugin_service_mock.hosts = default_hosts
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host

    plugin_service_mock.is_in_transaction = False
    plugin_service_mock.get_host_role.return_value = HostRole.WRITER
    return plugin_service_mock


@pytest.fixture
def plugin_instance(
    plugin_config, plugin_service_mock, plugin_props, host_list_provider_service_mock
):
    _, plugin_class = plugin_config
    plugin = plugin_class(plugin_service_mock, plugin_props)
    plugin._connection_handler._host_list_provider_service = (
        host_list_provider_service_mock
    )

    return plugin


# Common tests for both plugins
def test_set_read_only_true(
    plugin_instance, plugin_service_mock, plugin_config, reader_conn_mock
):
    plugin_name, _ = plugin_config
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.connect.return_value = reader_conn_mock

    if plugin_name == "read_write_splitting":
        plugin_service_mock.current_host_info = writer_host
        plugin_service_mock.get_host_info_by_strategy.return_value = reader_host1
        plugin_service_mock.hosts = single_reader_topology
        plugin_instance._reader_connection = None

        plugin_instance._switch_connection_if_required(True)
        plugin_service_mock.set_current_connection.assert_called_once_with(
            reader_conn_mock, reader_host1
        )
        assert plugin_instance._reader_connection == reader_conn_mock
    else:
        plugin_service_mock.current_host_info = simple_writer_host
        plugin_service_mock.get_host_role.side_effect = lambda conn: (
            HostRole.READER if conn == reader_conn_mock else HostRole.WRITER
        )

        plugin_instance._switch_connection_if_required(True)
        plugin_service_mock.set_current_connection.assert_called_with(
            reader_conn_mock, any_host
        )


def test_set_read_only_false(
    plugin_instance,
    plugin_service_mock,
    plugin_config,
    reader_conn_mock,
    writer_conn_mock,
):
    plugin_name, _ = plugin_config
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.connect.return_value = writer_conn_mock

    if plugin_name == "read_write_splitting":
        plugin_service_mock.current_host_info = reader_host1
        plugin_service_mock.hosts = single_reader_topology
        plugin_instance._writer_host_info = writer_host

        plugin_instance._switch_connection_if_required(False)
        plugin_service_mock.set_current_connection.assert_called_once_with(
            writer_conn_mock, writer_host
        )
        assert plugin_instance._writer_connection == writer_conn_mock
    else:
        plugin_service_mock.current_host_info = simple_reader_host
        plugin_service_mock.get_host_role.side_effect = lambda conn: (
            HostRole.READER if conn == reader_conn_mock else HostRole.WRITER
        )

        plugin_instance._switch_connection_if_required(False)
        plugin_service_mock.set_current_connection.assert_called_with(
            writer_conn_mock, any_host
        )


def test_set_read_only_true_already_on_reader(
    plugin_instance, plugin_service_mock, plugin_config, reader_conn_mock
):
    plugin_name, _ = plugin_config
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_instance._reader_connection = reader_conn_mock

    if plugin_name == "read_write_splitting":
        plugin_service_mock.current_host_info = reader_host1
    else:
        plugin_service_mock.current_host_info = simple_reader_host

    plugin_instance._switch_connection_if_required(True)
    plugin_service_mock.set_current_connection.assert_not_called()


def test_set_read_only_false_already_on_writer(
    plugin_instance, plugin_service_mock, plugin_config, writer_conn_mock
):
    plugin_name, _ = plugin_config
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_instance._writer_connection = writer_conn_mock

    if plugin_name == "read_write_splitting":
        plugin_service_mock.current_host_info = writer_host
        plugin_instance._writer_host_info = writer_host
    else:
        plugin_service_mock.current_host_info = simple_writer_host

    plugin_instance._switch_connection_if_required(False)
    plugin_service_mock.set_current_connection.assert_not_called()


def test_set_read_only_false_in_transaction(
    plugin_instance, plugin_service_mock, reader_conn_mock
):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.is_in_transaction = True
    plugin_service_mock.current_host_info = simple_reader_host

    with pytest.raises(ReadWriteSplittingError):
        plugin_instance._switch_connection_if_required(False)


def test_set_read_only_true_in_transaction_already_on_reader(
    plugin_instance, plugin_service_mock, reader_conn_mock
):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.is_in_transaction = True
    plugin_service_mock.current_host_info = simple_reader_host
    plugin_instance._writer_connection = None
    plugin_instance._reader_connection = reader_conn_mock

    plugin_instance._switch_connection_if_required(True)
    plugin_service_mock.set_current_connection.assert_not_called()
    assert plugin_instance._reader_connection == reader_conn_mock
    assert plugin_instance._writer_connection is None


def test_set_read_only_on_closed_connection(
    plugin_instance, plugin_service_mock, closed_writer_conn_mock
):
    plugin_service_mock.current_connection = closed_writer_conn_mock
    plugin_instance._writer_connection = closed_writer_conn_mock
    plugin_instance._reader_connection = None

    with pytest.raises(ReadWriteSplittingError):
        plugin_instance._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()
    assert plugin_instance._reader_connection is None


def test_notify_connection_change(
    plugin_instance, plugin_service_mock, writer_conn_mock
):
    plugin_instance._in_read_write_split = False
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = simple_writer_host

    suggestion = plugin_instance.notify_connection_changed(set())
    assert suggestion == OldConnectionSuggestedAction.NO_OPINION
    assert plugin_instance._writer_connection == writer_conn_mock

    plugin_instance._writer_connection = None
    plugin_instance._in_read_write_split = True
    suggestion = plugin_instance.notify_connection_changed(set())
    assert suggestion == OldConnectionSuggestedAction.PRESERVE
    assert plugin_instance._writer_connection == writer_conn_mock


def test_connect_non_initial_connection(
    plugin_instance, plugin_config, connect_func_mock, writer_conn_mock, mocker
):
    plugin_name, _ = plugin_config
    connect_func_mock.return_value = writer_conn_mock

    if plugin_name == "read_write_splitting":
        plugin_instance._writer_connection = writer_conn_mock
        plugin_instance._writer_host_info = writer_host
        plugin_instance._reader_connection = None

        conn = plugin_instance.connect(
            mocker.MagicMock(),
            mocker.MagicMock(),
            writer_host,
            Properties(),
            False,
            connect_func_mock,
        )

        assert conn == writer_conn_mock
        connect_func_mock.assert_called()
    else:
        result = plugin_instance.connect(
            None, None, simple_writer_host, Properties(), False, connect_func_mock
        )

        assert result == writer_conn_mock
        connect_func_mock.assert_called_once()


def test_close_pooled_reader_connection_after_set_read_only(
    plugin_props,
    plugin_service_mock,
    plugin_config,
    mocker,
    reader_conn_mock,
    writer_conn_mock,
):
    plugin_name, plugin_class = plugin_config

    def connect_side_effect(host: HostInfo, props, plugin):
        if (
            host in [reader_host1, reader_host2, reader_host3]
            or host.host == READ_ENDPOINT
        ):
            return reader_conn_mock
        elif host == writer_host or host.host == WRITE_ENDPOINT:
            return writer_conn_mock
        return None

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_service_mock.current_host_info = mocker.MagicMock(
        side_effect=[writer_host, writer_host, reader_host1]
    )

    if plugin_name == "read_write_splitting":
        plugin_service_mock.get_host_info_by_strategy.return_value = reader_host1
    else:
        plugin_service_mock.get_host_role.side_effect = lambda conn: (
            HostRole.READER if conn == reader_conn_mock else HostRole.WRITER
        )

    provider = SqlAlchemyPooledConnectionProvider(
        lambda _, __: {"pool_size": 3}, None, 180000000000, 600000000000  # 3 minutes
    )  # 10 minutes

    conn_provider_manager_mock = mocker.MagicMock()
    conn_provider_manager_mock.get_connection_provider.return_value = provider
    plugin_service_mock.get_connection_provider_manager.return_value = (
        conn_provider_manager_mock
    )

    plugin = plugin_class(plugin_service_mock, plugin_props)

    spy = mocker.spy(plugin, "_close_connection_if_idle")

    plugin._switch_connection_if_required(True)
    plugin._switch_connection_if_required(False)

    spy.assert_called_once_with(reader_conn_mock)
    assert spy.call_count == 1


def test_close_pooled_writer_connection_after_set_read_only(
    plugin_service_mock,
    plugin_config,
    plugin_props,
    mocker,
    reader_conn_mock,
    writer_conn_mock,
):
    plugin_name, plugin_class = plugin_config

    def connect_side_effect(host: HostInfo, props, plugin):
        if (
            host in [reader_host1, reader_host2, reader_host3]
            or host.host == READ_ENDPOINT
        ):
            return reader_conn_mock
        elif host == writer_host or host.host == WRITE_ENDPOINT:
            return writer_conn_mock
        return None

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_service_mock.current_host_info = mocker.MagicMock(
        side_effect=[writer_host, writer_host, reader_host1, reader_host1, writer_host]
    )

    if plugin_name == "read_write_splitting":
        plugin_service_mock.get_host_info_by_strategy.return_value = reader_host1
    else:
        plugin_service_mock.get_host_role.side_effect = lambda conn: (
            HostRole.READER if conn == reader_conn_mock else HostRole.WRITER
        )

    provider = SqlAlchemyPooledConnectionProvider(
        lambda _, __: {"pool_size": 3}, None, 180000000000, 600000000000  # 3 minutes
    )  # 10 minutes

    conn_provider_manager_mock = mocker.MagicMock()
    conn_provider_manager_mock.get_connection_provider.return_value = provider
    plugin_service_mock.get_connection_provider_manager.return_value = (
        conn_provider_manager_mock
    )

    plugin = plugin_class(plugin_service_mock, plugin_props)

    spy = mocker.spy(plugin, "_close_connection_if_idle")

    plugin._switch_connection_if_required(True)
    plugin._switch_connection_if_required(False)
    plugin._switch_connection_if_required(True)

    spy.assert_called_with(writer_conn_mock)
    assert spy.call_count == 2


def test_set_read_only_false_writer_connection_fails(
    plugin_instance, plugin_service_mock, reader_conn_mock
):
    def connect_side_effect(host_info: HostInfo, props: Properties, plugin):
        if (
            host_info == writer_host or host_info.host == WRITE_ENDPOINT
        ) and props == Properties():
            raise Error("Connection Error")

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host1
    plugin_service_mock.hosts = single_reader_topology
    plugin_instance._writer_connection = None
    plugin_instance._reader_connection = reader_conn_mock

    with pytest.raises(Error):
        plugin_instance._switch_connection_if_required(False)

    plugin_service_mock.set_current_connection.assert_not_called()


def test_set_read_only_true_reader_connection_failed(
    plugin_instance, plugin_service_mock, writer_conn_mock
):
    def connect_side_effect(host_info: HostInfo, props: Properties, plugin):
        if (
            host_info == reader_host1
            or host_info == reader_host2
            or host_info == reader_host3
        ) and props == Properties():
            raise Error("Connection Error")

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_instance._writer_connection = writer_conn_mock
    plugin_instance._writer_host_info = writer_host
    plugin_instance._reader_connection = None

    plugin_instance._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()
    assert plugin_instance._reader_connection is None


def test_execute_failover_to_new_writer(
    plugin_instance, plugin_service_mock, writer_conn_mock, new_writer_conn_mock
):
    def execute_func():
        raise FailoverSuccessError

    plugin_service_mock.current_connection = new_writer_conn_mock
    plugin_instance._writer_connection = writer_conn_mock
    plugin_instance._writer_host_info = writer_host
    plugin_instance._reader_connection = None

    with pytest.raises(Error):
        plugin_instance.execute(None, "Statement.execute_query", execute_func)

    writer_conn_mock.close.assert_called_once()


def test_connect_incorrect_host_role(
    plugin_instance,
    plugin_service_mock,
    plugin_config,
    mocker,
    connect_func_mock,
    reader_conn_mock,
    host_list_provider_service_mock,
):
    plugin_name, _ = plugin_config
    reader_host_incorrect_role = HostInfo(host="instance-4", role=HostRole.WRITER)

    def get_host_role_side_effect(conn):
        if conn == reader_conn_mock:
            return HostRole.READER
        return HostRole.WRITER

    plugin_service_mock.get_host_role.side_effect = get_host_role_side_effect
    plugin_service_mock.initial_connection_host_info = reader_host_incorrect_role
    host_list_provider_service_mock.is_static_host_list_provider.return_value = False

    connect_func_mock.return_value = reader_conn_mock
    conn = plugin_instance.connect(
        mocker.MagicMock(),
        mocker.MagicMock(),
        writer_host,
        Properties(),
        True,
        connect_func_mock,
    )

    assert conn == reader_conn_mock
    connect_func_mock.assert_called()

    updated_host = host_list_provider_service_mock.initial_connection_host_info
    if plugin_name == "read_write_splitting":
        assert updated_host.host == reader_host_incorrect_role.host
        assert updated_host.role != reader_host_incorrect_role.role
        assert updated_host.role == HostRole.READER
    else:
        assert updated_host == writer_host


# Tests for the Read/Write Splitting Plugin
def test_set_read_only_true_one_host(
    plugin_service_mock, plugin_config, plugin_instance
):
    plugin_name, _ = plugin_config
    if plugin_name != "read_write_splitting":
        pytest.skip("Test only applies to Read/Write Splitting Plugin")

    plugin_service_mock.hosts = [writer_host]

    plugin_instance._writer_connection = writer_conn_mock
    plugin_instance._writer_host_info = writer_host

    plugin_instance._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()
    assert plugin_instance._writer_connection == writer_conn_mock
    assert plugin_instance._reader_connection is None


def test_connect_error_updating_host(
    plugin_service_mock,
    plugin_config,
    plugin_instance,
    host_list_provider_service_mock,
    connect_func_mock,
    mocker,
):
    plugin_name, _ = plugin_config
    if plugin_name != "read_write_splitting":
        pytest.skip("Test only applies to Read/Write Splitting Plugin")

    def get_host_role_side_effect(conn):
        if conn == reader_conn_mock:
            return None
        return HostRole.WRITER

    plugin_service_mock.get_host_role.side_effect = get_host_role_side_effect
    host_list_provider_service_mock.is_static_host_list_provider.return_value = False

    connect_func_mock.return_value = reader_conn_mock

    with pytest.raises(Error):
        plugin_instance.connect(
            mocker.MagicMock(),
            mocker.MagicMock(),
            writer_host,
            Properties(),
            True,
            connect_func_mock,
        )

    host_list_provider_service_mock.initial_connection_host_info.assert_not_called()


# Tests for the Simple Read/Write Splitting Plugin
def test_constructor_missing_write_endpoint(plugin_service_mock, plugin_config):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    props = Properties()
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    # Missing write endpoint

    with pytest.raises(AwsWrapperError):
        SimpleReadWriteSplittingPlugin(plugin_service_mock, props)


def test_constructor_missing_read_endpoint(plugin_service_mock, plugin_config):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    # Missing read endpoint

    with pytest.raises(AwsWrapperError):
        SimpleReadWriteSplittingPlugin(plugin_service_mock, props)


def test_constructor_invalid_initial_connection_type(
    plugin_service_mock, plugin_config
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    props[WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.name] = (
        "other"  # "writer", "reader" are the only valid options
    )

    with pytest.raises(ValueError):
        SimpleReadWriteSplittingPlugin(plugin_service_mock, props)


def test_connect_verification_disabled(
    plugin_service_mock, plugin_config, connect_func_mock, writer_conn_mock
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    props[WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.name] = False

    connect_func_mock.return_value = writer_conn_mock

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)

    result = plugin.connect(
        None, None, simple_writer_host, props, True, connect_func_mock
    )

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()


def test_connect_writer_cluster_endpoint(
    plugin_config,
    plugin_instance,
    plugin_service_mock,
    plugin_props,
    connect_func_mock,
    writer_conn_mock,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    writer_cluster_host = HostInfo(
        host="test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
        port=TEST_PORT,
        role=HostRole.WRITER,
    )

    connect_func_mock.return_value = writer_conn_mock
    plugin_service_mock.get_host_role.return_value = HostRole.WRITER

    result = plugin_instance.connect(
        None, None, writer_cluster_host, plugin_props, True, connect_func_mock
    )

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()
    assert plugin_service_mock.get_host_role.call_count == 1


def test_connect_reader_cluster_endpoint(
    plugin_config,
    plugin_instance,
    plugin_props,
    plugin_service_mock,
    connect_func_mock,
    reader_conn_mock,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    reader_cluster_host = HostInfo(
        host="test-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com",
        port=TEST_PORT,
        role=HostRole.READER,
    )

    connect_func_mock.return_value = reader_conn_mock
    plugin_service_mock.get_host_role.return_value = HostRole.READER

    result = plugin_instance.connect(
        None, None, reader_cluster_host, plugin_props, True, connect_func_mock
    )

    assert result == reader_conn_mock
    connect_func_mock.assert_called_once()
    assert plugin_service_mock.get_host_role.call_count == 1


def test_connect_verification_fails_fallback(
    plugin_config,
    plugin_service_mock,
    connect_func_mock,
    writer_conn_mock,
    host_list_provider_service_mock,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    writer_cluster_host = HostInfo(
        host="test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
        port=TEST_PORT,
        role=HostRole.WRITER,
    )

    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "5"  # Short timeout
    props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = (
        "6"  # Interval > timeout ensures only one call before fallsback
    )

    connect_func_mock.return_value = writer_conn_mock
    plugin_service_mock.get_host_role.return_value = HostRole.READER  # Wrong role

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler.host_list_provider_service = (
        host_list_provider_service_mock
    )

    result = plugin.connect(
        None, None, writer_cluster_host, props, True, connect_func_mock
    )

    assert result == writer_conn_mock
    assert connect_func_mock.call_count == 2
    assert plugin_service_mock.get_host_role.call_count == 1


def test_connect_non_rds_cluster_endpoint(
    plugin_config,
    plugin_instance,
    plugin_service_mock,
    plugin_props,
    connect_func_mock,
    writer_conn_mock,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    custom_host = HostInfo(
        host="custom-db.example.com", port=TEST_PORT, role=HostRole.WRITER
    )

    connect_func_mock.return_value = writer_conn_mock

    result = plugin_instance.connect(
        None, None, custom_host, plugin_props, True, connect_func_mock
    )

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()
    assert plugin_service_mock.get_host_role.call_count == 0


def test_connect_non_rds_cluster_endpoint_with_verification(
    plugin_config,
    plugin_service_mock,
    plugin_props,
    connect_func_mock,
    writer_conn_mock,
    mocker,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    custom_host = HostInfo(
        host="custom-db.example.com", port=TEST_PORT, role=HostRole.WRITER
    )
    connect_func_mock.return_value = writer_conn_mock

    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    props[WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.name] = (
        "writer"  # Forces verification to a writer
    )

    connect_func_mock.return_value = writer_conn_mock
    plugin_service_mock.get_host_role = mocker.MagicMock(
        side_effect=[HostRole.READER, HostRole.WRITER]
    )

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler.host_list_provider_service = (
        host_list_provider_service_mock
    )

    result = plugin.connect(
        None, None, custom_host, plugin_props, True, connect_func_mock
    )

    assert result == writer_conn_mock
    assert connect_func_mock.call_count == 2
    assert plugin_service_mock.get_host_role.call_count == 2


def test_wrong_role_connection_writer_endpoint_to_reader(
    plugin_service_mock, reader_conn_mock, plugin_config, plugin_instance
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = simple_reader_host
    plugin_service_mock.connect.return_value = reader_conn_mock
    plugin_service_mock.get_host_role.return_value = (
        HostRole.READER
    )  # Wrong role for writer

    with pytest.raises(ReadWriteSplittingError):
        plugin_instance._switch_connection_if_required(False)


def test_get_verified_connection_wrong_role_retry_reader(
    plugin_config,
    plugin_instance,
    plugin_service_mock,
    reader_conn_mock,
    writer_conn_mock,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = simple_writer_host

    # First call returns wrong role, second call returns correct role
    plugin_service_mock.connect.side_effect = [writer_conn_mock, reader_conn_mock]
    plugin_service_mock.get_host_role.side_effect = lambda conn: (
        HostRole.READER if conn == reader_conn_mock else HostRole.WRITER
    )

    plugin_instance._switch_connection_if_required(True)

    assert plugin_service_mock.connect.call_count == 2
    writer_conn_mock.close.assert_called_once()


def test_get_verified_connection_sql_exception_retry(
    plugin_config,
    plugin_instance,
    plugin_service_mock,
    reader_conn_mock,
    writer_conn_mock,
):
    plugin_name, _ = plugin_config
    if plugin_name != "srw":
        pytest.skip("Test only applies to Simple Read/Write Splitting Plugin")

    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = simple_writer_host

    # First call raises exception, second call succeeds
    plugin_service_mock.connect.side_effect = [
        Error("Connection failed"),
        reader_conn_mock,
    ]
    plugin_service_mock.get_host_role.return_value = HostRole.READER

    plugin_instance._switch_connection_if_required(True)

    assert plugin_service_mock.connect.call_count == 2
    assert plugin_instance._reader_connection == reader_conn_mock
