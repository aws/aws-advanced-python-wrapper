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

from __future__ import annotations

from typing import Dict, Set
from unittest import mock
from unittest.mock import MagicMock

import psycopg
import pytest

from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.failover_v2_plugin import (
    FailoverV2Plugin, ReaderFailoverResult)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import Error
from aws_advanced_python_wrapper.utils.failover_mode import FailoverMode
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType


@pytest.fixture
def plugin_service_mock():
    mock = MagicMock()
    mock.network_bound_methods = {"*"}
    mock.current_host_info = HostInfo("writer.com", 5432, HostRole.WRITER)
    mock.current_connection = MagicMock(spec=psycopg.Connection)
    mock.driver_dialect.network_bound_methods = {"Connection.execute", "Connection.commit"}
    mock.driver_dialect.is_closed.return_value = False
    mock.is_network_exception.return_value = True
    mock.is_in_transaction = False
    mock.hosts = [
        HostInfo("writer.com", 5432, HostRole.WRITER),
        HostInfo("reader1.com", 5432, HostRole.READER),
        HostInfo("reader2.com", 5432, HostRole.READER)
    ]
    mock.all_hosts = mock.hosts
    mock.get_telemetry_factory.return_value.open_telemetry_context.return_value = None
    return mock


@pytest.fixture
def connection_mock():
    return MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_sql_method(mocker):
    return mocker.MagicMock()


@pytest.fixture
def properties():
    props = Properties()
    WrapperProperties.FAILOVER_TIMEOUT_SEC.set(props, "60")
    WrapperProperties.TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.set(props, "false")
    WrapperProperties.ENABLE_CONNECT_FAILOVER.set(props, "true")
    return props


@pytest.fixture
def failover_v2_plugin(plugin_service_mock, properties):
    return FailoverV2Plugin(plugin_service_mock, properties)


class TestFailoverV2Plugin:
    def test_execute_direct_close_method(self, failover_v2_plugin, connection_mock, mock_sql_method):
        failover_v2_plugin.execute(connection_mock, "Connection.close", mock_sql_method)
        assert failover_v2_plugin._closed_explicitly
        mock_sql_method.assert_called_once()

    def test_execute_with_exception_triggers_failover(self, failover_v2_plugin, connection_mock):
        network_exception = Exception("Network error")
        failover_v2_plugin._plugin_service.is_network_exception.return_value = True
        failover_v2_plugin._deal_with_original_exception = MagicMock(side_effect=AwsWrapperError("test"))

        def failing_func():
            raise network_exception

        with pytest.raises(AwsWrapperError):
            failover_v2_plugin.execute(connection_mock, "Connection.execute", failing_func)

        failover_v2_plugin._deal_with_original_exception.assert_called_once_with(network_exception)

    def test_init_host_provider(self, failover_v2_plugin, mock_sql_method, properties):
        host_list_provider_service = MagicMock()

        failover_v2_plugin.init_host_provider(properties, host_list_provider_service, mock_sql_method)

        assert failover_v2_plugin._host_list_provider_service == host_list_provider_service
        mock_sql_method.assert_called_once()

    def test_notify_host_list_changed(self, failover_v2_plugin):
        changes: Dict[str, Set[HostEvent]] = {"host1": {HostEvent.HOST_DELETED}}
        failover_v2_plugin._stale_dns_helper.notify_host_list_changed = MagicMock()

        failover_v2_plugin.notify_host_list_changed(changes)

        failover_v2_plugin._stale_dns_helper.notify_host_list_changed.assert_called_once_with(changes)

    def test_is_failover_enabled(self, failover_v2_plugin):
        failover_v2_plugin._rds_url_type = RdsUrlType.RDS_WRITER_CLUSTER
        failover_v2_plugin._plugin_service.all_hosts = [HostInfo("host1", 5432)]
        assert failover_v2_plugin._is_failover_enabled()

        failover_v2_plugin._plugin_service.all_hosts = []
        assert not failover_v2_plugin._is_failover_enabled()

        failover_v2_plugin._rds_url_type = RdsUrlType.RDS_PROXY
        assert not failover_v2_plugin._is_failover_enabled()

    def test_invalid_invocation_on_closed_connection_not_explicit(self, failover_v2_plugin):
        failover_v2_plugin._closed_explicitly = False
        failover_v2_plugin._pick_new_connection = MagicMock(side_effect=FailoverSuccessError())

        with pytest.raises(FailoverSuccessError):
            failover_v2_plugin._invalid_invocation_on_closed_connection()

        assert not failover_v2_plugin._is_closed

    def test_invalid_invocation_on_closed_connection_explicit(self, failover_v2_plugin):
        failover_v2_plugin._closed_explicitly = True

        with pytest.raises(AwsWrapperError, match="No operations allowed after connection closed"):
            failover_v2_plugin._invalid_invocation_on_closed_connection()

    def test_deal_with_original_exception_network_error(self, failover_v2_plugin):
        network_exception = Exception("Network error")
        failover_v2_plugin._plugin_service.is_network_exception.return_value = True
        failover_v2_plugin._invalidate_current_connection = MagicMock()
        failover_v2_plugin._pick_new_connection = MagicMock()

        with pytest.raises(AwsWrapperError):
            failover_v2_plugin._deal_with_original_exception(network_exception)

        failover_v2_plugin._invalidate_current_connection.assert_called_once()
        failover_v2_plugin._pick_new_connection.assert_called_once()

    def test_deal_with_original_exception_non_network_error(self, failover_v2_plugin):
        non_network_exception = ValueError("Not a network error")
        failover_v2_plugin._plugin_service.is_network_exception.return_value = False
        failover_v2_plugin._invalidate_current_connection = MagicMock()

        with pytest.raises(AwsWrapperError):
            failover_v2_plugin._deal_with_original_exception(non_network_exception)

        failover_v2_plugin._invalidate_current_connection.assert_not_called()

    def test_failover_writer_mode(self, failover_v2_plugin):
        failover_v2_plugin._failover_mode = FailoverMode.STRICT_WRITER
        failover_v2_plugin._failover_writer = MagicMock()

        failover_v2_plugin._failover()

        failover_v2_plugin._failover_writer.assert_called_once()

    def test_failover_reader_mode(self, failover_v2_plugin):
        failover_v2_plugin._failover_mode = FailoverMode.READER_OR_WRITER
        failover_v2_plugin._failover_reader = MagicMock()

        failover_v2_plugin._failover()

        failover_v2_plugin._failover_reader.assert_called_once()

    def test_failover_reader_success(self, failover_v2_plugin):
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        reader_result = ReaderFailoverResult(MagicMock(), HostInfo("reader.com", 5432, HostRole.READER))
        failover_v2_plugin._get_reader_failover_connection = MagicMock(return_value=reader_result)
        failover_v2_plugin._throw_failover_success_exception = MagicMock(side_effect=FailoverSuccessError())

        with pytest.raises(FailoverSuccessError):
            failover_v2_plugin._failover_reader()

        failover_v2_plugin._plugin_service.set_current_connection.assert_called_once_with(
            reader_result.connection, reader_result.host_info)

    def test_failover_reader_refresh_failed(self, failover_v2_plugin):
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = False

        with pytest.raises(FailoverFailedError):
            failover_v2_plugin._failover_reader()

    def test_get_reader_failover_connection_timeout(self, failover_v2_plugin):
        failover_v2_plugin._failover_timeout_sec = 0.001
        failover_v2_plugin._plugin_service.hosts = []

        with pytest.raises(TimeoutError, match="Failover reader timeout"):
            failover_v2_plugin._get_reader_failover_connection()

    def test_throw_failover_success_exception_in_transaction(self, failover_v2_plugin):
        failover_v2_plugin._is_in_transaction = True

        with pytest.raises(TransactionResolutionUnknownError):
            failover_v2_plugin._throw_failover_success_exception()

        failover_v2_plugin._plugin_service.update_in_transaction.assert_called_once_with(False)

    def test_failover_writer_success(self, failover_v2_plugin):
        writer_host = HostInfo("new-writer.com", 5432, HostRole.WRITER)
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        failover_v2_plugin._plugin_service.all_hosts = [writer_host]
        failover_v2_plugin._plugin_service.hosts = [writer_host]
        failover_v2_plugin._plugin_service.connect.return_value = MagicMock()
        failover_v2_plugin._plugin_service.get_host_role.return_value = HostRole.WRITER
        failover_v2_plugin._throw_failover_success_exception = MagicMock(side_effect=FailoverSuccessError())

        with pytest.raises(FailoverSuccessError):
            failover_v2_plugin._failover_writer()

        failover_v2_plugin._plugin_service.set_current_connection.assert_called_once()

    def test_failover_writer_no_writer_found(self, failover_v2_plugin):
        reader_host = HostInfo("reader.com", 5432, HostRole.READER)
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        failover_v2_plugin._plugin_service.all_hosts = [reader_host]

        with pytest.raises(FailoverFailedError):
            failover_v2_plugin._failover_writer()

    def test_failover_writer_refresh_failed(self, failover_v2_plugin):
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = False

        with pytest.raises(FailoverFailedError):
            failover_v2_plugin._failover_writer()

    def test_failover_writer_connection_failed(self, failover_v2_plugin):
        writer_host = HostInfo("new-writer.com", 5432, HostRole.WRITER)
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        failover_v2_plugin._plugin_service.all_hosts = [writer_host]
        failover_v2_plugin._plugin_service.hosts = [writer_host]
        failover_v2_plugin._plugin_service.connect.side_effect = Exception("Connection failed")

        with pytest.raises(FailoverFailedError):
            failover_v2_plugin._failover_writer()

    def test_failover_writer_connected_to_reader(self, failover_v2_plugin):
        writer_host = HostInfo("new-writer.com", 5432, HostRole.WRITER)
        failover_v2_plugin._plugin_service.force_monitoring_refresh_host_list.return_value = True
        failover_v2_plugin._plugin_service.all_hosts = [writer_host]
        failover_v2_plugin._plugin_service.hosts = [writer_host]
        failover_v2_plugin._plugin_service.connect.return_value = MagicMock()
        failover_v2_plugin._plugin_service.get_host_role.return_value = HostRole.READER

        with pytest.raises(FailoverFailedError):
            failover_v2_plugin._failover_writer()

    def test_invalidate_current_connection_with_transaction(self, failover_v2_plugin, connection_mock):
        failover_v2_plugin._plugin_service.current_connection = connection_mock
        failover_v2_plugin._is_in_transaction = True
        failover_v2_plugin._plugin_service.driver_dialect.is_closed.return_value = True

        with mock.patch.object(failover_v2_plugin._plugin_service.driver_dialect, "execute") as close_mock:
            failover_v2_plugin._invalidate_current_connection()
            connection_mock.rollback.assert_called_once()
            close_mock.assert_called_once()

            connection_mock.close.side_effect = Error("test error")

            try:
                failover_v2_plugin._invalidate_current_connection()
            except Error:
                pytest.fail("_invalidate_current_connection() raised unexpected error")

            close_mock.assert_called()
            assert close_mock.call_count == 2

    def test_pick_new_connection_proceeds_with_failover(self, failover_v2_plugin):
        failover_v2_plugin._is_closed = False
        failover_v2_plugin._closed_explicitly = False
        failover_v2_plugin._failover = MagicMock()

        failover_v2_plugin._pick_new_connection()

        failover_v2_plugin._failover.assert_called_once()

    def test_should_exception_trigger_connection_switch_network_exception(self, failover_v2_plugin):
        failover_v2_plugin._rds_url_type = RdsUrlType.RDS_WRITER_CLUSTER
        failover_v2_plugin._plugin_service.all_hosts = [HostInfo("host", 5432)]
        failover_v2_plugin._plugin_service.is_network_exception.return_value = True

        assert failover_v2_plugin._should_exception_trigger_connection_switch(Exception("network error"))

    def test_should_exception_trigger_connection_switch_read_only_strict_writer(self, failover_v2_plugin):
        failover_v2_plugin._rds_url_type = RdsUrlType.RDS_WRITER_CLUSTER
        failover_v2_plugin._plugin_service.all_hosts = [HostInfo("host", 5432)]
        failover_v2_plugin._failover_mode = FailoverMode.STRICT_WRITER
        failover_v2_plugin._plugin_service.is_network_exception.return_value = False
        failover_v2_plugin._plugin_service.is_read_only_connection_exception.return_value = True

        assert failover_v2_plugin._should_exception_trigger_connection_switch(Exception("read only"))

    def test_should_exception_trigger_connection_switch_failover_disabled(self, failover_v2_plugin):
        failover_v2_plugin._rds_url_type = RdsUrlType.RDS_PROXY

        assert not failover_v2_plugin._should_exception_trigger_connection_switch(Exception("any error"))
