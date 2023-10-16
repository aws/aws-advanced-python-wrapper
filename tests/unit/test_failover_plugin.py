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

from typing import TYPE_CHECKING, Tuple

import psycopg
import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.writer_failover_handler import WriterFailoverHandler

from typing import Dict, FrozenSet, Set
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

from aws_advanced_python_wrapper.errors import (
    FailoverSuccessError, TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.failover_plugin import (FailoverMode,
                                                         FailoverPlugin)
from aws_advanced_python_wrapper.failover_result import (ReaderFailoverResult,
                                                         WriterFailoverResult)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.host_list_provider import \
    HostListProviderService
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249 import Error
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def plugin_service_mock(mocker):
    service_mock = mocker.MagicMock()
    service_mock.network_bound_methods = {"*"}
    return service_mock


@pytest.fixture
def conn_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def driver_dialect_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def reader_failover_handler_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def writer_failover_handler_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def host_list_provider_service_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def host_list_provider_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def init_host_provider_func_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def host_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_sql_method(mocker):
    return mocker.MagicMock()


def test_init_host_provider_failover_disabled(plugin_service_mock, host_list_provider_service_mock,
                                              init_host_provider_func_mock):
    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "False")
    plugin = FailoverPlugin(plugin_service_mock, properties)

    with mock.patch.object(HostListProviderService, "is_static_host_list_provider") as method_mock:
        plugin.init_host_provider(properties, host_list_provider_service_mock, init_host_provider_func_mock)
        method_mock.assert_not_called()


def test_init_host_provider_with_static_host_provider(plugin_service_mock, host_list_provider_service_mock,
                                                      init_host_provider_func_mock, host_list_provider_mock):
    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)

    plugin.init_host_provider(properties, host_list_provider_service_mock, init_host_provider_func_mock)
    plugin._host_list_provider_service.host_list_provider = host_list_provider_mock

    with mock.patch.object(HostListProviderService, "is_static_host_list_provider") as method_mock:
        plugin.init_host_provider(properties, host_list_provider_service_mock, init_host_provider_func_mock)
        plugin._host_list_provider_service.host_list_provider = host_list_provider_mock
        method_mock.assert_not_called()

    assert host_list_provider_service_mock.host_list_provider == host_list_provider_mock


def test_notify_host_list_changed_with_failover_disabled(plugin_service_mock, host_mock):
    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "False")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    changes: Dict[str, Set[HostEvent]] = {}

    get_current_host_mock = PropertyMock(return_value=host_mock)
    type(plugin_service_mock).current_host_info = get_current_host_mock
    get_aliases_mock = PropertyMock()
    type(host_mock).aliases = get_aliases_mock

    plugin.notify_host_list_changed(changes)

    get_current_host_mock.assert_not_called()
    get_aliases_mock.assert_not_called()

    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    plugin.notify_host_list_changed(changes)

    get_current_host_mock.assert_called()
    get_aliases_mock.assert_not_called()


def test_notify_host_list_changed_with_valid_connection_not_in_topology(plugin_service_mock,
                                                                        host_list_provider_service_mock,
                                                                        init_host_provider_func_mock,
                                                                        host_mock):
    host_mock.url = "cluster-url/"
    aliases: FrozenSet[str] = frozenset("instance")
    host_mock.aliases = aliases

    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)

    get_current_host_mock = PropertyMock(return_value=host_mock)
    type(plugin_service_mock).current_host_info = get_current_host_mock
    get_aliases_mock = PropertyMock()
    type(host_mock).aliases = get_aliases_mock

    changes: Dict[str, Set[HostEvent]] = \
        {"cluster-host/": {HostEvent.HOST_DELETED},
         "instance/": {HostEvent.HOST_ADDED}}
    plugin.notify_host_list_changed(changes)

    get_current_host_mock.assert_called()
    get_aliases_mock.assert_not_called()


def test_update_topology(
        plugin_service_mock,
        host_list_provider_service_mock,
        init_host_provider_func_mock,
        driver_dialect_mock):
    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "False")
    plugin = FailoverPlugin(plugin_service_mock, properties)

    with mock.patch.object(plugin_service_mock, "force_refresh_host_list") as force_refresh_mock:
        with mock.patch.object(plugin_service_mock, "refresh_host_list") as refresh_mock:
            plugin._update_topology(False)
            refresh_mock.assert_not_called()
        force_refresh_mock.assert_not_called()

    driver_dialect_mock.is_closed.return_value = True
    type(plugin_service_mock).driver_dialect = PropertyMock(return_value=driver_dialect_mock)

    with mock.patch.object(plugin_service_mock, "force_refresh_host_list") as force_refresh_mock:
        with mock.patch.object(plugin_service_mock, "refresh_host_list") as refresh_mock:
            WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
            plugin = FailoverPlugin(plugin_service_mock, properties)
            plugin._update_topology(False)
            refresh_mock.assert_not_called()
        force_refresh_mock.assert_not_called()

    type(plugin_service_mock).hosts = PropertyMock(return_value=[HostInfo("host")])
    driver_dialect_mock.is_closed.return_value = False

    with mock.patch.object(plugin_service_mock, "force_refresh_host_list") as force_refresh_mock:
        with mock.patch.object(plugin_service_mock, "refresh_host_list") as refresh_mock:
            plugin._update_topology(True)
            refresh_mock.assert_not_called()
        force_refresh_mock.assert_called()

    with mock.patch.object(plugin_service_mock, "force_refresh_host_list") as force_refresh_mock:
        with mock.patch.object(plugin_service_mock, "refresh_host_list") as refresh_mock:
            plugin._update_topology(False)
            refresh_mock.assert_called()
        force_refresh_mock.assert_not_called()


def test_failover_reader(plugin_service_mock, host_list_provider_service_mock, init_host_provider_func_mock):
    type(plugin_service_mock).is_in_transaction = PropertyMock(return_value=False)
    plugin = FailoverPlugin(plugin_service_mock, Properties())
    plugin._failover_mode = FailoverMode.READER_OR_WRITER

    with mock.patch.object(FailoverPlugin, "_failover_reader") as failover_reader_mock:
        host_mock: HostInfo = MagicMock()
        with pytest.raises(FailoverSuccessError):
            plugin._failover(host_mock)
        failover_reader_mock.assert_called_once_with(host_mock)


def test_failover_writer(plugin_service_mock, host_list_provider_service_mock, init_host_provider_func_mock):
    type(plugin_service_mock).is_in_transaction = PropertyMock(return_value=True)
    plugin = FailoverPlugin(plugin_service_mock, Properties())
    plugin._failover_mode = FailoverMode.STRICT_WRITER

    with mock.patch.object(FailoverPlugin, "_failover_writer") as failover_writer_mock:
        with pytest.raises(TransactionResolutionUnknownError):
            plugin._failover(MagicMock())
        failover_writer_mock.assert_called_once_with()


def test_failover_reader_with_valid_failed_host(plugin_service_mock, host_list_provider_service_mock,
                                                init_host_provider_func_mock, conn_mock, reader_failover_handler_mock):
    host: HostInfo = HostInfo("host")
    host._availability = HostAvailability.AVAILABLE
    host._aliases = ["alias1", "alias2"]
    hosts: Tuple[HostInfo, ...] = (host, )
    type(plugin_service_mock).hosts = PropertyMock(return_value=hosts)

    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    plugin.init_host_provider(properties, MagicMock(), MagicMock())
    plugin._reader_failover_handler = reader_failover_handler_mock

    with mock.patch.object(plugin_service_mock, "set_current_connection") as set_current_connection_mock:
        with mock.patch.object(reader_failover_handler_mock, "failover") as failover_mock:
            reader_failover_handler_mock.failover.return_value = ReaderFailoverResult(conn_mock, True, host, None)
            plugin._failover_reader(host)
            failover_mock.assert_called_with(hosts, host)
        set_current_connection_mock.assert_called_with(conn_mock, host)


def test_failover_reader_with_no_failed_host(plugin_service_mock, host_list_provider_service_mock,
                                             init_host_provider_func_mock, reader_failover_handler_mock):
    host: HostInfo = HostInfo("host")
    host._availability = HostAvailability.AVAILABLE
    host._aliases = ["alias1", "alias2"]
    hosts: Tuple[HostInfo, ...] = (host, )
    type(plugin_service_mock).hosts = PropertyMock(return_value=hosts)

    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    plugin.init_host_provider(properties, MagicMock(), MagicMock())
    plugin._reader_failover_handler = reader_failover_handler_mock

    with mock.patch.object(reader_failover_handler_mock, "failover") as failover_reader_mock:
        reader_failover_handler_mock.failover.return_value = \
            ReaderFailoverResult(None, False, host, Error("test error"))

        with pytest.raises(Error):
            plugin._failover_reader(None)
        failover_reader_mock.assert_called_with(hosts, None)


def test_failover_writer_failed_failover_raises_error(plugin_service_mock, host_list_provider_service_mock,
                                                      init_host_provider_func_mock):
    writer_failover_handler_mock: WriterFailoverHandler = MagicMock()
    host: HostInfo = HostInfo("host")
    host._aliases = ["alias1", "alias2"]
    hosts: Tuple[HostInfo, ...] = (host, )
    type(plugin_service_mock).hosts = PropertyMock(return_value=hosts)

    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    plugin.init_host_provider(properties, MagicMock(), MagicMock())
    plugin._writer_failover_handler = writer_failover_handler_mock

    with mock.patch.object(writer_failover_handler_mock, "failover") as failover_writer_mock:
        writer_failover_handler_mock.failover.return_value = \
            WriterFailoverResult(None, False, False, None, "", Error("test error"))
        with pytest.raises(Error):
            plugin._failover_writer()
        failover_writer_mock.assert_called_with(hosts)


def test_failover_writer_failed_failover_with_no_result(plugin_service_mock, host_list_provider_service_mock,
                                                        init_host_provider_func_mock, writer_failover_handler_mock):
    host: HostInfo = HostInfo("host")
    host._aliases = ["alias1", "alias2"]
    hosts: Tuple[HostInfo, ...] = (host, )
    type(plugin_service_mock).hosts = PropertyMock(return_value=hosts)

    writer_result_mock: WriterFailoverResult = MagicMock()
    get_connection_mock = PropertyMock()
    get_topology_mock = PropertyMock()
    type(writer_result_mock).new_connection = get_connection_mock
    type(writer_result_mock).topology = get_topology_mock
    type(writer_result_mock).exception = PropertyMock(return_value=None)
    type(writer_result_mock).is_connected = PropertyMock(return_value=False)

    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    plugin.init_host_provider(properties, MagicMock(), MagicMock())
    plugin._writer_failover_handler = writer_failover_handler_mock

    with mock.patch.object(writer_failover_handler_mock, "failover") as failover_writer_mock:
        writer_failover_handler_mock.failover.return_value = writer_result_mock
        with pytest.raises(Error):
            plugin._failover_writer()
        failover_writer_mock.assert_called_with(hosts)

    get_connection_mock.assert_not_called()
    get_topology_mock.assert_not_called()


def test_failover_writer_success(plugin_service_mock, host_list_provider_service_mock, init_host_provider_func_mock,
                                 writer_failover_handler_mock):
    host: HostInfo = HostInfo("host")
    host._aliases = ["alias1", "alias2"]
    hosts: Tuple[HostInfo, ...] = (host, )
    type(plugin_service_mock).hosts = PropertyMock(return_value=hosts)

    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)
    plugin.init_host_provider(properties, MagicMock(), MagicMock())
    plugin._writer_failover_handler = writer_failover_handler_mock

    with mock.patch.object(writer_failover_handler_mock, "failover") as failover_writer_mock:
        writer_failover_handler_mock.failover.return_value = \
            WriterFailoverResult(None, False, False, None, "", None)
        with pytest.raises(Error):
            plugin._failover_writer()
        failover_writer_mock.assert_called_with(hosts)


def test_invalid_current_connection_with_no_connection(plugin_service_mock, host_list_provider_service_mock,
                                                       init_host_provider_func_mock):
    type(plugin_service_mock).current_connection = PropertyMock(return_value=None)

    is_in_transaction_mock = PropertyMock()
    type(plugin_service_mock).is_in_transaction = is_in_transaction_mock

    plugin = FailoverPlugin(plugin_service_mock, Properties())

    plugin._invalidate_current_connection()
    is_in_transaction_mock.assert_not_called()


def test_invalid_current_connection_in_transaction(plugin_service_mock, host_list_provider_service_mock,
                                                   init_host_provider_func_mock, conn_mock):
    type(plugin_service_mock).is_in_transaction = PropertyMock(return_value=True)
    type(plugin_service_mock).current_connection = PropertyMock(return_value=conn_mock)

    plugin = FailoverPlugin(plugin_service_mock, Properties())

    with mock.patch.object(conn_mock, "rollback") as rollback_mock:
        plugin._invalidate_current_connection()
        rollback_mock.assert_called()

    conn_mock.rollback.side_effect = Error("test error")

    try:
        plugin._invalidate_current_connection()
    except Error:
        pytest.fail("_invalidate_current_connection() raised unexpected error")


def test_invalidate_current_connection_not_in_transaction(plugin_service_mock, host_list_provider_service_mock,
                                                          init_host_provider_func_mock, conn_mock):
    is_in_transaction_mock = PropertyMock(return_value=False)
    type(plugin_service_mock).is_in_transaction = is_in_transaction_mock
    type(plugin_service_mock).current_connection = PropertyMock(return_value=conn_mock)

    plugin = FailoverPlugin(plugin_service_mock, Properties())
    plugin._invalidate_current_connection()
    is_in_transaction_mock.assert_called()


def test_invalidate_current_connection_with_open_connection(plugin_service_mock, host_list_provider_service_mock,
                                                            init_host_provider_func_mock, conn_mock,
                                                            driver_dialect_mock):
    is_in_transaction_mock = PropertyMock(return_value=False)

    type(plugin_service_mock).is_in_transaction = is_in_transaction_mock
    type(plugin_service_mock).current_connection = PropertyMock(return_value=conn_mock)
    type(plugin_service_mock).driver_dialect = PropertyMock(return_value=driver_dialect_mock)

    plugin = FailoverPlugin(plugin_service_mock, Properties())

    with mock.patch.object(conn_mock, "close") as close_mock:
        with mock.patch.object(driver_dialect_mock, "is_closed") as is_closed_mock:
            driver_dialect_mock.is_closed.return_value = False

            plugin._invalidate_current_connection()
            close_mock.assert_called_once()

            conn_mock.close.side_effect = Error("test error")

            try:
                plugin._invalidate_current_connection()
            except Error:
                pytest.fail("_invalidate_current_connection() raised unexpected error")

            is_closed_mock.assert_called()
            assert is_closed_mock.call_count == 2
        close_mock.assert_called()
        assert close_mock.call_count == 2


def test_execute(plugin_service_mock, host_list_provider_service_mock, init_host_provider_func_mock, mock_sql_method):
    properties = Properties()
    WrapperProperties.ENABLE_FAILOVER.set(properties, "False")
    plugin = FailoverPlugin(plugin_service_mock, properties)

    plugin.execute(None, "Connection.executeQuery", mock_sql_method)

    WrapperProperties.ENABLE_FAILOVER.set(properties, "True")
    plugin = FailoverPlugin(plugin_service_mock, properties)

    plugin.execute(None, "close", mock_sql_method)

    assert mock_sql_method.call_count == 2
