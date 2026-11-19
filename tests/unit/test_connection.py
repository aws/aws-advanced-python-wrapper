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

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from mysql.connector import MySQLConnection

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.errors import QueryTimeoutError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginServiceImpl, PluginServiceManagerContainer)
from aws_advanced_python_wrapper.sqlalchemy_driver_dialect import \
    SqlAlchemyDriverDialect
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.wrapper import (AwsWrapperConnection,
                                                 AwsWrapperCursor)


def test_connection_basic():
    conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty wrapper_driver_dialect=psycopg"
    connection_mock: Connection = MagicMock(spec=psycopg.Connection)
    container_mock = MagicMock()
    connection_mock.connect.return_value = MagicMock(spec=psycopg.Connection)

    with patch.object(PluginServiceManagerContainer, "__new__", container_mock), \
            patch.object(PluginServiceImpl, "refresh_host_list"), \
            patch.object(PluginServiceImpl, "current_connection", None), \
            patch.object(PluginServiceImpl, "initial_connection_host_info", HostInfo("localhost")):
        AwsWrapperConnection.connect(connection_mock.connect, conninfo)

        connection_mock.connect.assert_called_with(host="localhost", dbname="postgres", user="postgres",
                                                   password="qwerty")


def _wrapper_with(plugin_service, plugin_manager, conn):
    """Drive AwsWrapperConnection.__init__ past its setup to the host-adoption
    step, which is what decides the connection's current host."""
    plugin_manager.connect.return_value = conn
    plugin_service.database_dialect.get_host_list_provider_supplier.return_value = \
        lambda _service, _props: MagicMock()
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    AwsWrapperConnection.__init__(
        wrapper, MagicMock(), MagicMock(), plugin_service, plugin_manager)
    return wrapper


def test_the_wrapper_adopts_the_host_a_plugin_routed_the_connect_to():
    """The initial connection strategy plugin substitutes an instance for a
    cluster endpoint. Binding the connection to the endpoint instead would
    leave every later host comparison naming the wrong host."""
    routed = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    conn = MagicMock(spec=psycopg.Connection)
    plugin_service = MagicMock()
    plugin_service.current_connection = None
    plugin_service.initial_connection_host_info = HostInfo("mydb.cluster-xyz.us-east-2.rds.amazonaws.com", 5432)
    plugin_service.take_routed_host_info.return_value = routed

    _wrapper_with(plugin_service, MagicMock(), conn)

    plugin_service.set_current_connection.assert_called_once_with(conn, routed)


def test_the_wrapper_falls_back_to_the_initial_host_when_nothing_routed():
    initial = HostInfo("mydb.cluster-xyz.us-east-2.rds.amazonaws.com", 5432)
    conn = MagicMock(spec=psycopg.Connection)
    plugin_service = MagicMock()
    plugin_service.current_connection = None
    plugin_service.initial_connection_host_info = initial
    plugin_service.take_routed_host_info.return_value = None

    _wrapper_with(plugin_service, MagicMock(), conn)

    plugin_service.set_current_connection.assert_called_once_with(conn, initial)


def test_connection_getattr_delegates_to_underlying_connection():
    # Driver-specific extension methods absent from the wrapper's PEP-249 surface
    # (e.g. psycopg's add_notice_handler, called by SQLAlchemy) must reach the
    # live underlying connection via __getattr__.
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    underlying = MagicMock()
    plugin_service = MagicMock()
    plugin_service.current_connection = underlying
    wrapper._plugin_service = plugin_service

    assert wrapper.add_notice_handler is underlying.add_notice_handler


def test_connection_getattr_delegates_driver_attrs_including_underscore():
    # Public AND single-underscore driver attrs delegate to the live underlying
    # connection (SQLAlchemy's psycopg adapter reaches for underscore members like
    # _close). Only dunders stay on the wrapper, and the recursion-critical
    # _plugin_service name is guarded so a miss before it is set raises cleanly.
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    underlying = MagicMock()
    plugin_service = MagicMock()
    plugin_service.current_connection = underlying
    wrapper._plugin_service = plugin_service

    # single-underscore driver attr delegates (regression: was wrongly blocked)
    assert wrapper._close is underlying._close
    # dunder stays on the wrapper, not delegated
    with pytest.raises(AttributeError):
        _ = wrapper.__totally_made_up_dunder__
    # the recursion-critical internal name is guarded when unset
    fresh = AwsWrapperConnection.__new__(AwsWrapperConnection)
    with pytest.raises(AttributeError):
        _ = fresh._plugin_service


def test_cursor_getattr_delegates_to_target_cursor():
    wrapper = AwsWrapperCursor.__new__(AwsWrapperCursor)
    target_cursor = MagicMock()
    wrapper._target_cursor = target_cursor

    assert wrapper.statusmessage is target_cursor.statusmessage


def test_cursor_getattr_delegates_driver_attrs_including_underscore():
    wrapper = AwsWrapperCursor.__new__(AwsWrapperCursor)
    target_cursor = MagicMock()
    wrapper._target_cursor = target_cursor

    # _close must delegate: SQLAlchemy's psycopg cursor adapter calls it.
    assert wrapper._close is target_cursor._close
    with pytest.raises(AttributeError):
        _ = wrapper.__totally_made_up_dunder__
    fresh = AwsWrapperCursor.__new__(AwsWrapperCursor)
    with pytest.raises(AttributeError):
        _ = fresh._target_cursor


def _make_sync_conn(target=None):
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    plugin_service = MagicMock()
    plugin_service.current_connection = target if target is not None else MagicMock()
    wrapper._plugin_service = plugin_service
    wrapper._plugin_manager = MagicMock()
    return wrapper, plugin_service.current_connection


def test_connection_execute_routes_through_cursor_not_raw_driver():
    # psycopg exposes conn.execute(); the wrapper must NOT forward it raw via
    # __getattr__ (that would bypass the plugin chain). It opens a wrapper cursor.
    wrapper, target = _make_sync_conn()
    fake_cursor = MagicMock()
    with patch.object(AwsWrapperConnection, "cursor", return_value=fake_cursor) as cursor_mock:
        result = wrapper.execute("SELECT 1", None)
    cursor_mock.assert_called_once()
    fake_cursor.execute.assert_called_once()
    assert result is fake_cursor
    target.execute.assert_not_called()


def test_connection_set_read_only_routes_through_plugin_manager():
    # Must go through the plugin-aware read_only setter (plugin chain +
    # session-state), not the raw driver set_read_only that __getattr__ would
    # forward -- otherwise the read-only state is lost across failover.
    wrapper, target = _make_sync_conn()
    wrapper.set_read_only(True)
    assert wrapper._plugin_manager.execute.called
    target.set_read_only.assert_not_called()


def test_connection_set_autocommit_routes_through_plugin_manager():
    wrapper, target = _make_sync_conn()
    wrapper.set_autocommit(True)
    assert wrapper._plugin_manager.execute.called
    target.set_autocommit.assert_not_called()


def test_connection_invalidate_prefers_target_invalidate():
    wrapper, target = _make_sync_conn()
    wrapper.release_resources = MagicMock()
    wrapper.invalidate()
    target.invalidate.assert_called_once()
    wrapper.release_resources.assert_called_once()


def test_connection_invalidate_falls_back_to_close():
    # A raw driver connection has no invalidate() -> fall back to close().
    wrapper, _ = _make_sync_conn(MagicMock(spec=psycopg.Connection))
    wrapper.close = MagicMock()
    wrapper.invalidate()
    wrapper.close.assert_called_once()


def test_connection_closed_forwards_target_and_defaults_false():
    wrapper, target = _make_sync_conn()
    target.closed = True
    assert wrapper.closed is True
    # a target lacking `closed` normalizes to False instead of AttributeError
    wrapper2, _ = _make_sync_conn(MagicMock(spec=[]))
    assert wrapper2.closed is False


def test_connection_prepare_threshold_and_prepared_max_forward():
    wrapper, target = _make_sync_conn()
    target.prepare_threshold = 7
    target.prepared_max = 11
    assert wrapper.prepare_threshold == 7
    assert wrapper.prepared_max == 11
    wrapper.prepare_threshold = 99
    wrapper.prepared_max = 100
    assert target.prepare_threshold == 99
    assert target.prepared_max == 100


def _mysql_wrapper_with_default_plugin_only(conn, pooled=False):
    props = Properties({"host": "localhost", "plugins": "", "socket_timeout": "0.1"})
    container = PluginServiceManagerContainer()
    driver_dialect = MySQLDriverDialect(props)
    if pooled:
        driver_dialect = SqlAlchemyDriverDialect(driver_dialect, props)
    plugin_service = PluginServiceImpl(container, props, MagicMock(), MagicMock(), driver_dialect)
    container.plugin_service = plugin_service
    # With telemetry off, methods no plugin subscribes to skip the plugin pipeline, as in production.
    telemetry_factory = MagicMock()
    telemetry_factory.in_use.return_value = False
    container.plugin_manager = PluginManager(container, props, telemetry_factory)
    plugin_service._current_connection = conn

    wrapper = object.__new__(AwsWrapperConnection)
    wrapper._plugin_service = plugin_service
    wrapper._plugin_manager = container.plugin_manager
    return wrapper


@pytest.mark.parametrize("pooled", [False, True], ids=["raw", "pooled"])
@pytest.mark.parametrize(
    "close", [AwsWrapperConnection.close, lambda wrapper: wrapper.__exit__(None, None, None)], ids=["close", "exit"])
def test_close_after_uninterruptible_timeout_defers_driver_close(uninterruptible_call, close, pooled):
    wrapper = _mysql_wrapper_with_default_plugin_only(uninterruptible_call.connection(pooled), pooled)
    with pytest.raises(QueryTimeoutError):
        with wrapper.cursor() as cursor:
            cursor.execute("SELECT SLEEP(3)")

    close(wrapper)

    assert uninterruptible_call.calls_while_running == []
    assert uninterruptible_call.close_threads == []

    uninterruptible_call.finish()
    assert uninterruptible_call.closed.wait(5) is True
    assert uninterruptible_call.close_threads == [uninterruptible_call.worker_thread]


def test_cursor_after_uninterruptible_timeout_raises(uninterruptible_call):
    wrapper = _mysql_wrapper_with_default_plugin_only(uninterruptible_call.conn)
    with pytest.raises(QueryTimeoutError):
        wrapper.cursor().execute("SELECT SLEEP(3)")

    with pytest.raises(QueryTimeoutError):
        wrapper.cursor().execute("SELECT 1")

    assert uninterruptible_call.calls_while_running == []


@pytest.mark.parametrize("pooled", [False, True], ids=["raw", "pooled"])
@pytest.mark.parametrize("name", ["ping", "is_connected"])
def test_driver_attribute_after_uninterruptible_timeout_raises(uninterruptible_call, name, pooled):
    wrapper = _mysql_wrapper_with_default_plugin_only(uninterruptible_call.connection(pooled), pooled)
    with pytest.raises(QueryTimeoutError):
        wrapper.cursor().execute("SELECT SLEEP(3)")

    with pytest.raises(QueryTimeoutError):
        getattr(wrapper, name)()

    assert uninterruptible_call.calls_while_running == []


def test_close_without_timeout_closes_driver_connection():
    conn = MagicMock(spec=MySQLConnection)
    wrapper = _mysql_wrapper_with_default_plugin_only(conn)

    wrapper.close()

    conn.close.assert_called_with()
