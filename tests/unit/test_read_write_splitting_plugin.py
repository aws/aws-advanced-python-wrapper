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

from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import Error
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    ReadWriteSplittingPlugin
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils.notifications import \
    OldConnectionSuggestedAction
from aws_advanced_python_wrapper.utils.properties import Properties

default_props = Properties()
writer_host = HostInfo(host="instance0", role=HostRole.WRITER)
reader_host1 = HostInfo(host="instance1", role=HostRole.READER)
reader_host2 = HostInfo(host="instance2", role=HostRole.READER)
reader_host3 = HostInfo(host="instance3", role=HostRole.READER)

default_hosts: List[HostInfo] = [writer_host, reader_host1, reader_host2, reader_host3]
single_reader_topology: List[HostInfo] = [writer_host, reader_host1]


@pytest.fixture
def host_list_provider_service_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def changes_mock(mocker):
    return mocker.MagicMock()


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
def driver_dialect_mock(mocker, writer_conn_mock):
    def is_closed_side_effect(conn):
        return conn == closed_writer_conn_mock

    driver_dialect_mock = mocker.MagicMock()
    driver_dialect_mock.is_closed.side_effect = is_closed_side_effect
    driver_dialect_mock.get_connection_from_obj.return_value = writer_conn_mock
    driver_dialect_mock.unwrap_connection.return_value = writer_conn_mock

    return driver_dialect_mock


@pytest.fixture
def plugin_service_mock(mocker, driver_dialect_mock):
    plugin_service_mock = mocker.MagicMock()
    plugin_service_mock.driver_dialect = driver_dialect_mock
    plugin_service_mock._all_hosts = default_hosts
    plugin_service_mock.is_in_transaction = False
    return plugin_service_mock


def test_set_read_only_true(plugin_service_mock):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.get_host_info_by_strategy.return_value = reader_host1
    plugin_service_mock._all_hosts = single_reader_topology
    plugin_service_mock.connect.return_value = reader_conn_mock

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._reader_connection = None

    plugin._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_called_once()
    plugin_service_mock.set_current_connection.assert_called_once_with(reader_conn_mock, reader_host1)

    assert plugin._reader_connection == reader_conn_mock


def test_set_read_only_false(plugin_service_mock):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host1
    plugin_service_mock._all_hosts = single_reader_topology

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock

    plugin._switch_connection_if_required(False)

    plugin_service_mock.set_current_connection.assert_called_once()
    plugin_service_mock.set_current_connection.assert_called_once_with(writer_conn_mock, writer_host)

    assert plugin._writer_connection == writer_conn_mock


def test_set_read_only_true_already_on_reader(plugin_service_mock):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host1

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = None
    plugin._reader_connection = reader_conn_mock

    plugin._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()

    assert plugin._reader_connection == reader_conn_mock
    assert plugin._writer_connection is None


def test_set_read_only_false_already_on_writer(plugin_service_mock):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock
    plugin._reader_connection = None

    plugin._switch_connection_if_required(False)

    plugin_service_mock.set_current_connection.assert_not_called()

    assert plugin._writer_connection == writer_conn_mock
    assert plugin._reader_connection is None


def test_set_read_only_false_in_transaction(plugin_service_mock):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host1
    plugin_service_mock.is_in_transaction = True

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = None
    plugin._reader_connection = reader_conn_mock

    plugin._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()

    assert plugin._reader_connection == reader_conn_mock
    assert plugin._writer_connection is None


def test_set_read_only_true_one_host(plugin_service_mock):
    plugin_service_mock._all_hosts = [writer_host]

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock

    plugin._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()

    assert plugin._writer_connection == writer_conn_mock
    assert plugin._reader_connection is None


def test_set_read_only_false_writer_connection_fails(plugin_service_mock):
    def connect_side_effect(host_info: HostInfo, props: Properties):
        if host_info == writer_host and props == default_props:
            raise Error("Connection Error")

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host1
    plugin_service_mock._all_hosts = single_reader_topology

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = None
    plugin._reader_connection = reader_conn_mock

    with pytest.raises(Error):
        plugin._switch_connection_if_required(False)

    plugin_service_mock.set_current_connection.assert_not_called()


def test_set_read_only_true_reader_connection_failed(plugin_service_mock):
    def connect_side_effect(host_info: HostInfo, props: Properties):
        if ((host_info == reader_host1 or host_info == reader_host2 or host_info == reader_host3)
                and props == default_props):
            raise Error("Connection Error")

    plugin_service_mock.connect.side_effect = connect_side_effect

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock
    plugin._reader_connection = None

    plugin._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()

    assert plugin._reader_connection is None


def test_set_read_only_on_closed_connection(plugin_service_mock):
    plugin_service_mock.current_connection = closed_writer_conn_mock

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = closed_writer_conn_mock
    plugin._reader_connection = None

    with pytest.raises(Error):
        plugin._switch_connection_if_required(True)

    plugin_service_mock.set_current_connection.assert_not_called()

    assert plugin._reader_connection is None


def test_execute_failover_to_new_writer(plugin_service_mock, writer_conn_mock):
    def execute_func():
        raise FailoverSuccessError

    plugin_service_mock.current_connection = new_writer_conn_mock

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock
    plugin._reader_connection = None

    with pytest.raises(Error):
        plugin.execute(None, "Statement.execute_query", execute_func)

    writer_conn_mock.close.assert_called_once()


def test_notify_connection_change(plugin_service_mock):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock

    suggestion = plugin.notify_connection_changed(changes_mock)

    assert suggestion == OldConnectionSuggestedAction.NO_OPINION
    assert plugin._writer_connection == writer_conn_mock


def test_connect_non_initial_connection(
        mocker, plugin_service_mock, connect_func_mock, host_list_provider_service_mock):
    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock
    plugin._reader_connection = None

    connect_func_mock.return_value = writer_conn_mock
    conn = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), writer_host, default_props, False, connect_func_mock)

    assert conn == writer_conn_mock

    connect_func_mock.assert_called()
    host_list_provider_service_mock.initial_connection_host_info.assert_not_called()


def test_connect_incorrect_host_role(mocker, plugin_service_mock, connect_func_mock, host_list_provider_service_mock):
    reader_host_incorrect_role = HostInfo(host="instance-4", role=HostRole.WRITER)

    def get_host_role_side_effect(conn):
        if conn == reader_conn_mock:
            return HostRole.READER
        return HostRole.WRITER

    plugin_service_mock.get_host_role.side_effect = get_host_role_side_effect
    plugin_service_mock.initial_connection_host_info = reader_host_incorrect_role
    host_list_provider_service_mock.is_static_host_list_provider.return_value = False

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock

    connect_func_mock.return_value = reader_conn_mock
    conn = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), writer_host, default_props, True, connect_func_mock)

    assert conn == reader_conn_mock
    connect_func_mock.assert_called()

    updated_host = host_list_provider_service_mock.initial_connection_host_info
    assert updated_host.host == reader_host_incorrect_role.host
    assert updated_host.role != reader_host_incorrect_role.role
    assert updated_host.role == HostRole.READER


def test_connect_error_updating_host(mocker, plugin_service_mock, connect_func_mock, host_list_provider_service_mock):
    def get_host_role_side_effect(conn):
        if conn == reader_conn_mock:
            return None
        return HostRole.WRITER

    plugin_service_mock.get_host_role.side_effect = get_host_role_side_effect
    host_list_provider_service_mock.is_static_host_list_provider.return_value = False

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._host_list_provider_service = host_list_provider_service_mock

    connect_func_mock.return_value = reader_conn_mock

    with pytest.raises(Error):
        plugin.connect(
            mocker.MagicMock(), mocker.MagicMock(), writer_host, default_props, True, connect_func_mock)

    host_list_provider_service_mock.initial_connection_host_info.assert_not_called()


def test_close_pooled_reader_connection_after_set_read_only(mocker, plugin_service_mock):
    def connect_side_effect(host, props):
        if host in [reader_host1, reader_host2, reader_host3]:
            return reader_conn_mock
        elif host == writer_host:
            return writer_conn_mock
        return None

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_service_mock.current_host_info = mocker.MagicMock(side_effect=[writer_host, writer_host, reader_host1])
    plugin_service_mock.get_host_info_by_strategy.return_value = reader_host1

    provider = SqlAlchemyPooledConnectionProvider(
        lambda _, __: {"pool_size": 3},
        None,
        180000000000,  # 3 minutes
        600000000000)  # 10 minutes

    conn_provider_manager_mock = mocker.MagicMock()
    conn_provider_manager_mock.get_connection_provider.return_value = provider
    plugin_service_mock.get_connection_provider_manager.return_value = conn_provider_manager_mock

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)

    spy = mocker.spy(plugin, "_close_connection_if_idle")

    plugin._switch_connection_if_required(True)
    plugin._switch_connection_if_required(False)

    spy.assert_called_once_with(reader_conn_mock)
    assert spy.call_count == 1


def test_close_pooled_writer_connection_after_set_read_only(mocker, plugin_service_mock):
    def connect_side_effect(host, props):
        if host in [reader_host1, reader_host2, reader_host3]:
            return reader_conn_mock
        elif host == writer_host:
            return writer_conn_mock
        return None

    plugin_service_mock.connect.side_effect = connect_side_effect
    plugin_service_mock.current_host_info = (
        mocker.MagicMock(side_effect=[writer_host, writer_host, reader_host1, reader_host1, writer_host]))
    plugin_service_mock.get_host_info_by_strategy.return_value = reader_host1

    provider = SqlAlchemyPooledConnectionProvider(
        lambda _, __: {"pool_size": 3},
        None,
        180000000000,  # 3 minutes
        600000000000)  # 10 minutes

    conn_provider_manager_mock = mocker.MagicMock()
    conn_provider_manager_mock.get_connection_provider.return_value = provider
    plugin_service_mock.get_connection_provider_manager.return_value = conn_provider_manager_mock

    plugin = ReadWriteSplittingPlugin(plugin_service_mock, default_props)

    spy = mocker.spy(plugin, "_close_connection_if_idle")

    plugin._switch_connection_if_required(True)
    plugin._switch_connection_if_required(False)
    plugin._switch_connection_if_required(True)

    spy.assert_called_with(writer_conn_mock)
    assert spy.call_count == 2
