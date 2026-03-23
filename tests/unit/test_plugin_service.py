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
#  limitations under the License.s

from concurrent.futures import TimeoutError
from unittest.mock import MagicMock

import pytest  # type: ignore

from aws_advanced_python_wrapper.database_dialect import (
    AuroraPgDialect, MultiAzClusterPgDialect, UnknownDatabaseDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl
from aws_advanced_python_wrapper.utils.properties import Properties


def test_get_host_role_unknown_dialect(mocker):
    mock_conn = MagicMock()
    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = UnknownDatabaseDialect()

    with pytest.raises(UnsupportedOperationError):
        plugin_service.get_host_role(mock_conn)


def test_identify_connection_unknown_dialect(mocker):
    mock_conn = MagicMock()
    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = UnknownDatabaseDialect()
    plugin_service._current_connection = mock_conn

    with pytest.raises(UnsupportedOperationError):
        plugin_service.identify_connection(mock_conn)


def test_get_host_role_reader(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (True,)
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert HostRole.READER == plugin_service.get_host_role(mock_conn)


def test_get_host_role_writer(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (False,)
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert HostRole.WRITER == plugin_service.get_host_role(mock_conn)


def test_get_host_role_error(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = ValueError()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(AwsWrapperError):
        plugin_service.get_host_role(mock_conn)


def test_get_host_role_timeout(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = TimeoutError()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(QueryTimeoutError):
        plugin_service.get_host_role(mock_conn)


def test_identify_connection_error_no_result(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(AwsWrapperError):
        plugin_service.identify_connection(mock_conn)


def test_identify_connection_timeout(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = TimeoutError()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(QueryTimeoutError):
        plugin_service.identify_connection(mock_conn)


def test_identify_connection_no_match_in_topology(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("host-value", "non-matching-host")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = ()
    mock_host_list_provider.force_refresh.return_value = ()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert plugin_service.identify_connection(mock_conn) is None


def test_identify_connection_empty_topology(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("host-value", "instance-1")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = []

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert plugin_service.identify_connection(mock_conn) is None


def test_identify_connection_host_in_topology_aurora(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("instance-1", "instance-1")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    expected_host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, HostRole.WRITER, host_id="instance-1")

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = (expected_host,)

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    host_info = plugin_service.identify_connection(mock_conn)
    assert host_info is not None
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id


def test_identify_connection_host_in_topology_multiaz(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Multi-AZ returns different values: (instanceId, instanceName)
    mock_cursor.fetchone.return_value = ("db-WQFQKBTL2LQUPIEFIFBGENS4ZQ", "instance-1")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    expected_host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, HostRole.WRITER, host_id="instance-1")

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = (expected_host,)

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com", "port": 5432}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = MultiAzClusterPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    host_info = plugin_service.identify_connection(mock_conn)
    assert host_info is not None
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id
