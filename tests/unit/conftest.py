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

import threading
import weakref
from typing import List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from sqlalchemy import PoolProxiedConnection

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncOpenedConnectionTracker
from aws_advanced_python_wrapper.aio.minor_plugins import AsyncDeveloperPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.database_dialect import (
    DatabaseDialectManager, DialectUtils)
from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.errors import QueryTimeoutError
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl
from aws_advanced_python_wrapper.sqlalchemy_driver_dialect import \
    SqlAlchemyDriverDialect
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.properties import Properties


def pytest_runtest_setup(item):
    services_container.get_storage_service().clear_all()
    # Monitors are keyed partly by event loop, and each test gets a new
    # loop whose identity can be recycled, so a monitor from the previous
    # test would otherwise be handed to this one.
    services_container.get_monitor_service().stop_all()
    PluginServiceImpl._host_availability_expiring_cache.clear()
    AsyncPluginServiceImpl._host_availability_expiring_cache.clear()
    DatabaseDialectManager._known_endpoint_dialects.clear()
    AsyncOpenedConnectionTracker._tracked.clear()
    AsyncDeveloperPlugin.clear()

    ConnectionProviderManager.reset_provider()
    DatabaseDialectManager.reset_custom_dialect()
    DriverDialectManager.reset_custom_dialect()
    ExceptionManager.reset_custom_handler()


class UninterruptibleCall:
    """A mysql C-extension query that ``abort_connection`` can't interrupt: ``run`` blocks until ``finish``.

    Every call another thread makes on the connection is recorded with the calling thread and whether
    ``run`` was still inside the driver at that moment. ``pool_proxy`` is the same connection as
    SqlAlchemyPooledConnectionProvider hands it to the plugin service.
    """

    def __init__(self, conn, cursor, pool_proxy):
        self.conn = conn
        self.cursor = cursor
        self.pool_proxy = pool_proxy
        self.calls: List[Tuple[str, int, bool]] = []
        self.closed = threading.Event()
        self.worker_thread: Optional[int] = None
        self._running = threading.Event()
        self._finish = threading.Event()

        # No raw socket, so MySQLDriverDialect.abort_connection does nothing, as on the C extension.
        conn._socket = None
        conn.unread_result = False
        conn.in_transaction = True
        conn.close.side_effect = self._recorder("close")
        conn.rollback.side_effect = self._recorder("rollback")
        conn.is_connected.side_effect = self._recorder("is_connected", True)
        conn.ping.side_effect = self._recorder("ping")
        conn.cursor.side_effect = self._recorder("cursor", cursor)
        conn.get_self.return_value = conn
        # mysql-connector cursors hold a weakref.proxy of their connection.
        cursor._connection = weakref.proxy(conn)
        cursor.execute.side_effect = lambda *args, **kwargs: self.run()
        cursor.close.side_effect = self._recorder("cursor.close")
        # A pool proxy forwards the driver connection's methods.
        pool_proxy.driver_connection = conn
        pool_proxy.cursor = MagicMock(side_effect=self._recorder("pool_proxy.cursor", cursor))
        pool_proxy.rollback = MagicMock(side_effect=self._recorder("pool_proxy.rollback"))
        pool_proxy.close = MagicMock(side_effect=self._recorder("pool_proxy.close"))
        pool_proxy.invalidate = MagicMock(side_effect=self._recorder("pool_proxy.invalidate"))
        pool_proxy.ping = conn.ping
        pool_proxy.is_connected = conn.is_connected

    @property
    def calls_while_running(self) -> List[str]:
        return [name for name, _, running in self.calls if running]

    @property
    def close_threads(self) -> List[int]:
        return [thread for name, thread, _ in self.calls if name == "close"]

    def run(self) -> None:
        self.worker_thread = threading.get_ident()
        self._running.set()
        try:
            self._finish.wait(5)
        finally:
            self._running.clear()

    def connection(self, pooled: bool):
        return self.pool_proxy if pooled else self.conn

    @staticmethod
    def driver_dialect(pooled: bool, props: Optional[Properties] = None):
        props = Properties() if props is None else props
        driver_dialect = MySQLDriverDialect(props)
        return SqlAlchemyDriverDialect(driver_dialect, props) if pooled else driver_dialect

    def time_out_on(self, driver_dialect) -> None:
        with pytest.raises(QueryTimeoutError):
            driver_dialect.execute(DbApiMethod.CURSOR_EXECUTE.method_name, self.run, exec_timeout=0.1, conn=self.conn)
        assert self._running.wait(5) is True

    def time_out_host_role_query(self, driver_dialect, conn) -> None:
        thread_pool = services_container.get_thread_pool("UninterruptibleCallExecutor")
        with pytest.raises(QueryTimeoutError):
            DialectUtils.get_host_role(conn, driver_dialect, "SELECT SLEEP(3)", thread_pool, 0.1)
        assert self._running.wait(5) is True

    def finish(self) -> None:
        self._finish.set()

    def _recorder(self, name: str, result=None):
        def record(*args, **kwargs):
            self.calls.append((name, threading.get_ident(), self._running.is_set()))
            if name == "close":
                self.closed.set()
            return result
        return record


@pytest.fixture
def uninterruptible_call(mocker):
    call = UninterruptibleCall(mocker.MagicMock(spec=MySQLConnection), mocker.MagicMock(spec=MySQLCursor),
                               mocker.MagicMock(spec=PoolProxiedConnection))
    yield call
    call.finish()
