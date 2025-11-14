#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#  express or implied. See the License for the specific language governing
#  permissions and limitations under the License.

import psycopg # type: ignore
import pytest # type: ignore

from aws_advanced_python_wrapper.errors import AwsWrapperError, ReadWriteSplittingError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import Error
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    SimpleReadWriteSplittingPlugin
from aws_advanced_python_wrapper.utils.notifications import \
    OldConnectionSuggestedAction
from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperties
from tests.unit.utils.unit_test_utils import AnyInstanceOf

WRITE_ENDPOINT = "writer.cluster-xyz.us-east-1.rds.amazonaws.com"
READ_ENDPOINT = "reader.cluster-xyz.us-east-1.rds.amazonaws.com"
TEST_PORT = 5432

writer_host = HostInfo(host=WRITE_ENDPOINT, port=TEST_PORT, role=HostRole.WRITER)
reader_host = HostInfo(host=READ_ENDPOINT, port=TEST_PORT, role=HostRole.READER)
any_host = AnyInstanceOf(HostInfo)

@pytest.fixture
def default_props():
    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    return props

@pytest.fixture
def host_list_provider_service_mock(mocker):
    mock = mocker.MagicMock()
    mock.initial_connection_host_info = writer_host
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
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host
    plugin_service_mock.is_in_transaction = False
    plugin_service_mock.get_host_role.return_value = HostRole.WRITER
    return plugin_service_mock

def test_constructor_missing_write_endpoint(plugin_service_mock):
    props = Properties()
    props[WrapperProperties.SRW_READ_ENDPOINT.name] = READ_ENDPOINT
    # Missing write endpoint

    with pytest.raises(AwsWrapperError):
        SimpleReadWriteSplittingPlugin(plugin_service_mock, props)

def test_constructor_missing_read_endpoint(plugin_service_mock):
    props = Properties()
    props[WrapperProperties.SRW_WRITE_ENDPOINT.name] = WRITE_ENDPOINT
    # Missing read endpoint

    with pytest.raises(AwsWrapperError):
        SimpleReadWriteSplittingPlugin(plugin_service_mock, props)

def test_set_read_only_true_false(plugin_service_mock, reader_conn_mock, writer_conn_mock, default_props, host_list_provider_service_mock):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host
    plugin_service_mock.connect.return_value = reader_conn_mock
    plugin_service_mock.get_host_role.side_effect = lambda conn: HostRole.READER if conn == reader_conn_mock else HostRole.WRITER

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock()

    # Switch to reader
    plugin._switch_connection_if_required(True)
    plugin_service_mock.set_current_connection.assert_called_with(reader_conn_mock, any_host)

    # Switch back to writer
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host
    plugin_service_mock.connect.return_value = writer_conn_mock
    plugin._switch_connection_if_required(False)
    plugin_service_mock.set_current_connection.assert_called_with(writer_conn_mock, any_host)

def test_set_read_only_true_already_on_reader(plugin_service_mock, reader_conn_mock, default_props):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock
    plugin._reader_connection = reader_conn_mock

    plugin._switch_connection_if_required(True)

    # Should not call set_current_connection since already on reader
    plugin_service_mock.set_current_connection.assert_not_called()

def test_set_read_only_false_already_on_writer(plugin_service_mock, writer_conn_mock, default_props):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock
    plugin._writer_connection = writer_conn_mock

    plugin._switch_connection_if_required(False)

    # Should not call set_current_connection since already on writer
    plugin_service_mock.set_current_connection.assert_not_called()

def test_set_read_only_false_in_transaction(plugin_service_mock, reader_conn_mock, default_props):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host
    plugin_service_mock.is_in_transaction = True

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock

    with pytest.raises(ReadWriteSplittingError):
        plugin._switch_connection_if_required(False)

def test_set_read_only_closed_connection(plugin_service_mock, closed_writer_conn_mock, default_props):
    plugin_service_mock.current_connection = closed_writer_conn_mock

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock

    with pytest.raises(ReadWriteSplittingError):
        plugin._switch_connection_if_required(True)

def test_notify_connection_changed_in_read_write_split(plugin_service_mock, default_props):
    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._in_read_write_split = True

    result = plugin.notify_connection_changed(set())
    assert result == OldConnectionSuggestedAction.PRESERVE

def test_notify_connection_changed_not_in_read_write_split(plugin_service_mock, default_props):
    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._in_read_write_split = False

    result = plugin.notify_connection_changed(set())
    assert result == OldConnectionSuggestedAction.NO_OPINION

def test_connect_non_initial_connection(plugin_service_mock, connect_func_mock, writer_conn_mock, default_props):
    connect_func_mock.return_value = writer_conn_mock

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)

    result = plugin.connect(None, None, writer_host, default_props, False, connect_func_mock)

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()

def test_connect_verification_disabled(plugin_service_mock, connect_func_mock, writer_conn_mock, default_props):
    props = default_props
    props[WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.name] = False

    connect_func_mock.return_value = writer_conn_mock

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)

    result = plugin.connect(None, None, writer_host, props, True, connect_func_mock)

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()

def test_wrong_role_connection_writer_endpoint_to_reader(plugin_service_mock, reader_conn_mock, default_props):
    plugin_service_mock.current_connection = reader_conn_mock
    plugin_service_mock.current_host_info = reader_host
    plugin_service_mock.connect.return_value = reader_conn_mock
    plugin_service_mock.get_host_role.return_value = HostRole.READER  # Wrong role for writer

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock

    with pytest.raises(ReadWriteSplittingError):
        plugin._switch_connection_if_required(False)

def test_get_verified_connection_wrong_role_retry_reader(plugin_service_mock, reader_conn_mock, writer_conn_mock, default_props, host_list_provider_service_mock):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host
    
    # First call returns wrong role, second call returns correct role
    plugin_service_mock.connect.side_effect = [writer_conn_mock, reader_conn_mock]
    plugin_service_mock.get_host_role.side_effect = lambda conn: HostRole.READER if conn == reader_conn_mock else HostRole.WRITER

    props = default_props # SRW_VERIFY_NEW_CONNECTIONS default is True
    props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "5000"
    props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = "100"

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock()

    plugin._switch_connection_if_required(True)

    assert plugin_service_mock.connect.call_count == 2
    writer_conn_mock.close.assert_called_once()

def test_get_verified_connection_sql_exception_retry(plugin_service_mock, reader_conn_mock, writer_conn_mock, default_props, host_list_provider_service_mock):
    plugin_service_mock.current_connection = writer_conn_mock
    plugin_service_mock.current_host_info = writer_host
    
    # First call raises exception, second call succeeds
    plugin_service_mock.connect.side_effect = [Error("Connection failed"), reader_conn_mock]
    plugin_service_mock.get_host_role.return_value = HostRole.READER

    props = default_props # SRW_VERIFY_NEW_CONNECTIONS default is True
    props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "5000"
    props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = "100"

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock()

    plugin._switch_connection_if_required(True)

    assert plugin_service_mock.connect.call_count == 2

def test_connect_writer_cluster_endpoint(plugin_service_mock, connect_func_mock, writer_conn_mock, default_props):
    writer_cluster_host = HostInfo(host="test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com", port=TEST_PORT, role=HostRole.WRITER)
    
    props = default_props # SRW_VERIFY_NEW_CONNECTIONS default is True
    props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "5000"
    props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = "100"

    connect_func_mock.return_value = writer_conn_mock
    plugin_service_mock.connect.return_value = writer_conn_mock
    plugin_service_mock.get_host_role.return_value = HostRole.WRITER

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock

    result = plugin.connect(None, None, writer_cluster_host, props, True, connect_func_mock)

    assert result == writer_conn_mock
    plugin_service_mock.connect.assert_called_once()

def test_connect_reader_cluster_endpoint(plugin_service_mock, connect_func_mock, reader_conn_mock, default_props):
    reader_cluster_host = HostInfo(host="test-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com", port=TEST_PORT, role=HostRole.READER)
    
    props = default_props # SRW_VERIFY_NEW_CONNECTIONS default is True
    props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "5000"
    props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = "100"

    connect_func_mock.return_value = reader_conn_mock
    plugin_service_mock.connect.return_value = reader_conn_mock
    plugin_service_mock.get_host_role.return_value = HostRole.READER

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock

    result = plugin.connect(None, None, reader_cluster_host, props, True, connect_func_mock)

    assert result == reader_conn_mock
    plugin_service_mock.connect.assert_called_once()

def test_connect_verification_fails_fallback(plugin_service_mock, connect_func_mock, writer_conn_mock, reader_conn_mock, default_props):
    writer_cluster_host = HostInfo(host="test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com", port=TEST_PORT, role=HostRole.WRITER)
    
    props = default_props # SRW_VERIFY_NEW_CONNECTIONS default is True
    props[WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name] = "100"  # Short timeout
    props[WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name] = "50"

    connect_func_mock.return_value = writer_conn_mock
    plugin_service_mock.connect.return_value = reader_conn_mock  # Wrong role
    plugin_service_mock.get_host_role.return_value = HostRole.READER

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, props)
    plugin._connection_handler._host_list_provider_service = host_list_provider_service_mock

    result = plugin.connect(None, None, writer_cluster_host, props, True, connect_func_mock)

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()

def test_connect_non_rds_cluster_endpoint(plugin_service_mock, connect_func_mock, writer_conn_mock, default_props):
    custom_host = HostInfo(host="custom-db.example.com", port=TEST_PORT, role=HostRole.WRITER)

    connect_func_mock.return_value = writer_conn_mock

    plugin = SimpleReadWriteSplittingPlugin(plugin_service_mock, default_props)

    result = plugin.connect(None, None, custom_host, default_props, True, connect_func_mock)

    assert result == writer_conn_mock
    connect_func_mock.assert_called_once()
