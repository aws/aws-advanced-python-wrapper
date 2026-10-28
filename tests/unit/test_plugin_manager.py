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

import contextvars
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection

from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import psycopg
import pytest

from aws_advanced_python_wrapper.connect_time_plugin import ConnectTimePlugin
from aws_advanced_python_wrapper.default_plugin import DefaultPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.execute_time_plugin import ExecuteTimePlugin
from aws_advanced_python_wrapper.failover_plugin import FailoverPlugin
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    HostMonitoringPlugin
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.iam_plugin import IamAuthPlugin
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.plugin_service import (PluginCallContext,
                                                        PluginManager,
                                                        PluginServiceImpl)
from aws_advanced_python_wrapper.utils.decorators import timeout
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.wrapper import AwsWrapperCursor


class DbApiMethodTest(Enum):
    """Test-specific enumeration of API methods for testing purposes."""

    TEST_CALL_A = (DbApiMethod.ALL.id + 1, "test_call_a", False)
    TEST_CALL_B = (DbApiMethod.ALL.id + 2, "test_call_b", False)
    TEST_CALL_C = (DbApiMethod.ALL.id + 3, "test_call_c", False)
    TEST_EXECUTE = (DbApiMethod.ALL.id + 4, "test_execute", False)

    def __init__(self, id: int, method_name: str, always_use_pipeline: bool):
        self.id = id
        self.method_name = method_name
        self.always_use_pipeline = always_use_pipeline


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_fetchone_row(mocker):
    return mocker.MagicMock()


@pytest.fixture
def container(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_driver_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def default_conn_provider(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_telemetry_factory(mocker):
    return mocker.MagicMock()


@pytest.fixture(autouse=True)
def setup(mock_conn, container, mock_plugin_service, mock_driver_dialect):
    container.plugin_service.current_connection = mock_conn
    container.plugin_service.driver_dialect.get_connection_from_obj.return_value = mock_conn
    container.plugin_service.driver_dialect.unwrap_connection.return_value = mock_conn


def test_sort_plugins(mocker):
    props = Properties(plugins="iam,host_monitoring,failover")
    plugin_manager = PluginManager(mocker.MagicMock(), props, mocker.MagicMock())

    plugins = plugin_manager.get_plugins()

    assert plugins is not None
    assert 4 == len(plugins)

    assert isinstance(plugins[0], FailoverPlugin)
    assert isinstance(plugins[1], HostMonitoringPlugin)
    assert isinstance(plugins[2], IamAuthPlugin)
    assert isinstance(plugins[3], DefaultPlugin)


def test_preserve_plugin_order(mocker):
    props = Properties(plugins="iam,host_monitoring,failover", auto_sort_wrapper_plugin_order=False)
    plugin_manager = PluginManager(mocker.MagicMock(), props, mocker.MagicMock())

    plugins = plugin_manager.get_plugins()

    assert plugins is not None
    assert 4 == len(plugins)

    assert isinstance(plugins[0], IamAuthPlugin)
    assert isinstance(plugins[1], HostMonitoringPlugin)
    assert isinstance(plugins[2], FailoverPlugin)
    assert isinstance(plugins[3], DefaultPlugin)


def test_sort_plugins_with_stick_to_prior(mocker):
    props = Properties(plugins="iam,execute_time,connect_time,host_monitoring,failover")
    plugin_manager = PluginManager(mocker.MagicMock(), props, mocker.MagicMock())

    plugins = plugin_manager.get_plugins()

    assert plugins is not None
    assert 6 == len(plugins)

    assert isinstance(plugins[0], FailoverPlugin)
    assert isinstance(plugins[1], HostMonitoringPlugin)
    assert isinstance(plugins[2], IamAuthPlugin)
    assert isinstance(plugins[3], ExecuteTimePlugin)
    assert isinstance(plugins[4], ConnectTimePlugin)
    assert isinstance(plugins[5], DefaultPlugin)


def test_unknown_profile(mocker, mock_telemetry_factory):
    props = Properties(profile_name="unknown_profile")
    with pytest.raises(AwsWrapperError):
        PluginManager(mocker.MagicMock(), props, mock_telemetry_factory())


def test_execute_call_method(mocker, mock_conn, container, mock_plugin_service, mock_driver_dialect, mock_telemetry_factory):
    calls = []
    args = [10, "arg2", 3.33]
    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    container.plugin_service.return_value = mock_plugin_service
    manager._container = container
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)

    make_pipeline_func = mocker.patch.object(manager, '_make_pipeline', wraps=manager._make_pipeline)
    result = manager.execute(mock_conn, DbApiMethodTest.TEST_CALL_A, lambda: _target_call(calls), *args)

    make_pipeline_func.assert_called_once_with(DbApiMethodTest.TEST_CALL_A.method_name)
    assert result == "result_value"
    assert len(calls) == 7
    assert calls[0] == "TestPluginOne:before execute"
    assert calls[1] == "TestPluginTwo:before execute"
    assert calls[2] == "TestPluginThree:before execute"
    assert calls[3] == "target_call"
    assert calls[4] == "TestPluginThree:after execute"
    assert calls[5] == "TestPluginTwo:after execute"
    assert calls[6] == "TestPluginOne:after execute"

    calls.clear()
    result = manager.execute(mock_conn, DbApiMethodTest.TEST_CALL_A, lambda: _target_call(calls), *args)

    # The first execute call should cache the pipeline
    make_pipeline_func.assert_called_once_with(DbApiMethodTest.TEST_CALL_A.method_name)
    assert result == "result_value"
    assert len(calls) == 7
    assert calls[0] == "TestPluginOne:before execute"
    assert calls[1] == "TestPluginTwo:before execute"
    assert calls[2] == "TestPluginThree:before execute"
    assert calls[3] == "target_call"
    assert calls[4] == "TestPluginThree:after execute"
    assert calls[5] == "TestPluginTwo:after execute"
    assert calls[6] == "TestPluginOne:after execute"


def _target_call(calls: List[str]):
    calls.append("target_call")
    return "result_value"


def test_execute_call_b(mocker, container, mock_driver_dialect, mock_telemetry_factory, mock_conn):
    calls = []
    args = [10, "arg2", 3.33]
    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._container = container
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)

    result = manager.execute(mock_conn, DbApiMethodTest.TEST_CALL_B, lambda: _target_call(calls), *args)

    assert result == "result_value"
    assert len(calls) == 5
    assert calls[0] == "TestPluginOne:before execute"
    assert calls[1] == "TestPluginTwo:before execute"
    assert calls[2] == "target_call"
    assert calls[3] == "TestPluginTwo:after execute"
    assert calls[4] == "TestPluginOne:after execute"


def test_execute_call_c(mocker, container, mock_driver_dialect, mock_telemetry_factory, mock_conn):
    calls = []
    args = [10, "arg2", 3.33]
    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._container = container
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)

    result = manager.execute(mock_conn, DbApiMethodTest.TEST_CALL_C, lambda: _target_call(calls), *args)

    assert result == "result_value"
    assert len(calls) == 3
    assert calls[0] == "TestPluginOne:before execute"
    assert calls[1] == "target_call"
    assert calls[2] == "TestPluginOne:after execute"


def test_execute_against_old_target(mocker, container, mock_driver_dialect, mock_telemetry_factory, mock_conn):
    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._container = container
    manager._plugins = ""
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)

    # Set current connection to a new connection object
    container.plugin_service.current_connection = mocker.MagicMock(spec=psycopg.Connection)
    with pytest.raises(AwsWrapperError):
        manager.execute(mock_conn, DbApiMethodTest.TEST_EXECUTE, lambda: _target_call([]))


def test_connect(mocker, container, mock_conn, mock_driver_dialect, mock_telemetry_factory):
    calls = []

    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls, mock_conn)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)
    manager._telemetry_factory = mock_telemetry_factory
    manager._container = container

    result = manager.connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)

    assert result == mock_conn
    assert len(calls) == 4
    assert calls[0] == "TestPluginOne:before connect"
    assert calls[1] == "TestPluginThree:before connect"
    assert calls[2] == "TestPluginThree:after connect"
    assert calls[3] == "TestPluginOne:after connect"


def test_connect__skip_plugin(mocker, container, mock_conn, mock_driver_dialect, mock_telemetry_factory):
    calls = []

    plugin1 = TestPluginOne(calls)
    plugins = [plugin1, TestPluginTwo(calls), TestPluginThree(calls, mock_conn)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)
    manager._telemetry_factory = mock_telemetry_factory
    manager._container = container

    result = manager.connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True, plugin1)

    assert result == mock_conn
    assert len(calls) == 2
    assert calls[0] == "TestPluginThree:before connect"
    assert calls[1] == "TestPluginThree:after connect"


def test_force_connect(mocker, container, mock_conn, mock_driver_dialect, mock_telemetry_factory):
    calls = []

    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls, mock_conn)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)
    manager._telemetry_factory = mock_telemetry_factory
    manager._container = container

    make_pipeline_func = mocker.patch.object(manager, '_make_pipeline', wraps=manager._make_pipeline)
    # The first call to force_connect should generate the plugin pipeline and cache it
    result = manager.force_connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)

    make_pipeline_func.assert_called_once_with(DbApiMethod.FORCE_CONNECT.method_name)
    assert result == mock_conn
    assert len(calls) == 4
    assert calls[0] == "TestPluginOne:before forceConnect"
    assert calls[1] == "TestPluginThree:before forceConnect"
    assert calls[2] == "TestPluginThree:after forceConnect"
    assert calls[3] == "TestPluginOne:after forceConnect"

    calls.clear()

    result = manager.force_connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)

    # The second call should have used the cached plugin pipeline, so make_pipeline should not have been called again
    make_pipeline_func.assert_called_once_with(DbApiMethod.FORCE_CONNECT.method_name)
    assert result == mock_conn
    assert len(calls) == 4
    assert calls[0] == "TestPluginOne:before forceConnect"
    assert calls[1] == "TestPluginThree:before forceConnect"
    assert calls[2] == "TestPluginThree:after forceConnect"
    assert calls[3] == "TestPluginOne:after forceConnect"


def test_force_connect__cached(mocker, container, mock_conn, mock_driver_dialect, mock_telemetry_factory):
    calls = []

    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls, mock_conn)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)
    manager._telemetry_factory = mock_telemetry_factory
    manager._container = container

    result = manager.force_connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)

    assert result == mock_conn
    assert len(calls) == 4
    assert calls[0] == "TestPluginOne:before forceConnect"
    assert calls[1] == "TestPluginThree:before forceConnect"
    assert calls[2] == "TestPluginThree:after forceConnect"
    assert calls[3] == "TestPluginOne:after forceConnect"


def test_exception_before_connect(mocker, container, mock_telemetry_factory):
    calls = []
    plugins = \
        [TestPluginOne(calls), TestPluginTwo(calls), TestPluginRaisesError(calls, True), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)
    manager._container = container

    with pytest.raises(AwsWrapperError):
        result = manager.connect(
            mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)
        assert result is None

    assert len(calls) == 2
    assert calls[0] == "TestPluginOne:before connect"
    assert calls[1] == "TestPluginRaisesError:before connect"


def test_exception_after_connect(mocker, container, mock_telemetry_factory):
    calls = []
    plugins = \
        [TestPluginOne(calls), TestPluginTwo(calls), TestPluginRaisesError(calls, False), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)
    manager._container = container

    with pytest.raises(AwsWrapperError):
        result = manager.connect(
            mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)
        assert result is None

    assert len(calls) == 5
    assert calls[0] == "TestPluginOne:before connect"
    assert calls[1] == "TestPluginRaisesError:before connect"
    assert calls[2] == "TestPluginThree:before connect"
    assert calls[3] == "TestPluginThree:after connect"
    assert calls[4] == "TestPluginRaisesError:after connect"


def test_no_plugins(mocker, container):
    props = Properties(plugins="")

    manager = PluginManager(container, props, mocker.MagicMock())

    assert 1 == manager.num_plugins


def test_plugin_setting(mocker, container, default_conn_provider):
    props = Properties(plugins="iam,iam")

    manager = PluginManager(container, props, mocker.MagicMock())

    assert 3 == manager.num_plugins
    assert isinstance(manager._plugins[0], IamAuthPlugin)
    assert isinstance(manager._plugins[1], IamAuthPlugin)
    assert isinstance(manager._plugins[2], DefaultPlugin)


def test_notify_connection_changed(mocker, mock_telemetry_factory):
    calls = []
    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)

    old_connection_suggestion = manager.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})

    assert old_connection_suggestion == OldConnectionSuggestedAction.PRESERVE
    assert len(calls) == 3
    assert calls[0] == "TestPluginOne:notify_connection_changed"
    assert calls[1] == "TestPluginTwo:notify_connection_changed"
    assert calls[2] == "TestPluginThree:notify_connection_changed"

    manager._plugins = [TestPluginOne(calls), TestPluginTwo(calls)]
    old_connection_suggestion = manager.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
    assert old_connection_suggestion == OldConnectionSuggestedAction.DISPOSE

    manager._plugins = [TestPluginOne(calls)]
    old_connection_suggestion = manager.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
    assert old_connection_suggestion == OldConnectionSuggestedAction.NO_OPINION


def test_notify_host_list_changed(mocker, mock_telemetry_factory):
    calls = []
    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = [None] * (DbApiMethodTest.TEST_EXECUTE.id + 1)

    manager.notify_host_list_changed(
        {"host-1": {HostEvent.CONVERTED_TO_READER}, "host-2": {HostEvent.CONVERTED_TO_WRITER}})

    assert len(calls) == 2
    assert calls[0] == "TestPluginOne:notify_host_list_changed"
    assert calls[1] == "TestPluginThree:notify_host_list_changed"


class TestPlugin(Plugin):
    __test__ = False

    def __init__(self, calls: List[str], connection: Optional[Connection] = None):
        self._calls = calls
        self._connection = connection

    @property
    def subscribed_methods(self) -> Set[str]:
        return {"*"}

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        self._calls.append(type(self).__name__ + ":before connect")
        if self._connection is not None:
            result = self._connection
        else:
            result = connect_func()
        self._calls.append(type(self).__name__ + ":after connect")
        return result

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        self._calls.append(type(self).__name__ + ":before forceConnect")
        if self._connection is not None:
            result = self._connection
        else:
            result = connect_func()
        self._calls.append(type(self).__name__ + ":after forceConnect")
        return result

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._calls.append(type(self).__name__ + ":before execute")
        result = execute_func()
        self._calls.append(type(self).__name__ + ":after execute")
        return result

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        self._calls.append(type(self).__name__ + ":notify_host_list_changed")

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._calls.append(type(self).__name__ + ":notify_connection_changed")
        return OldConnectionSuggestedAction.NO_OPINION

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return False

    def get_host_info_by_strategy(self, role: HostRole, strategy: str, host_list: Optional[List[HostInfo]] = None) -> HostInfo:
        return HostInfo(type(self).__name__ + ":host_info")


class TestPluginOne(TestPlugin):
    pass


class TestPluginTwo(TestPlugin):

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethodTest.TEST_CALL_A.method_name, DbApiMethodTest.TEST_CALL_B.method_name, DbApiMethod.NOTIFY_CONNECTION_CHANGED.method_name}

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._calls.append(type(self).__name__ + ":notify_connection_changed")
        return OldConnectionSuggestedAction.DISPOSE


class TestPluginThree(TestPlugin):
    @property
    def subscribed_methods(self) -> Set[str]:
        return {
            DbApiMethodTest.TEST_CALL_A.method_name,
            DbApiMethod.CONNECT.method_name,
            DbApiMethod.FORCE_CONNECT.method_name,
            DbApiMethod.NOTIFY_CONNECTION_CHANGED.method_name,
            DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name,
        }

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._calls.append(type(self).__name__ + ":notify_connection_changed")
        return OldConnectionSuggestedAction.PRESERVE


class TestPluginRaisesError(TestPlugin):

    def __init__(self, calls: List[str], throw_before_call: bool = True):
        super().__init__(calls)
        self._throw_before_call = throw_before_call

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        self._calls.append(type(self).__name__ + ":before connect")
        if self._throw_before_call:
            raise AwsWrapperError()

        if self._connection is None:
            connect_func()
        self._calls.append(type(self).__name__ + ":after connect")
        raise AwsWrapperError()

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._calls.append(type(self).__name__ + ":before execute")
        if self._throw_before_call:
            raise AwsWrapperError()

        execute_func()
        self._calls.append(type(self).__name__ + ":after execute")
        raise AwsWrapperError()

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        self._calls.append(type(self).__name__ + ":notify_host_list_changed")
        raise AwsWrapperError()

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._calls.append(type(self).__name__ + ":notify_connection_changed")
        raise AwsWrapperError()


def test_default_plugin_excluded_from_is_subscribed(mocker, mock_telemetry_factory):
    # DefaultPlugin subscribes to "*", but it must not mark a method as subscribed on its own --
    # otherwise the direct-call bypass in _execute_with_subscribed_plugins is unreachable.
    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = [DefaultPlugin(mocker.MagicMock(), mocker.MagicMock())]
    manager._telemetry_factory = mock_telemetry_factory

    assert not manager._make_pipeline(DbApiMethod.CURSOR_EXECUTE.method_name).is_subscribed
    assert not manager._make_pipeline(DbApiMethod.CONNECT.method_name).is_subscribed

    # A real subscribing plugin still sets the flag, and only for the methods it subscribes to.
    manager._plugins = [TestPluginTwo([]), DefaultPlugin(mocker.MagicMock(), mocker.MagicMock())]
    assert manager._make_pipeline(DbApiMethodTest.TEST_CALL_A.method_name).is_subscribed
    assert not manager._make_pipeline(DbApiMethod.CURSOR_FETCHALL.method_name).is_subscribed


def test_unsubscribed_method_bypasses_pipeline(mocker, container, mock_telemetry_factory):
    # With only DefaultPlugin in the chain, a non-network-bound method skips the pipeline entirely
    # but must still refresh the cached in-transaction state.
    calls = []
    container.plugin_service.is_network_bound_method.side_effect = \
        lambda name: name == DbApiMethod.CURSOR_EXECUTE.method_name
    container.plugin_service.update_in_transaction.side_effect = \
        lambda *args: calls.append("update_in_transaction")
    container.plugin_service.driver_dialect.execute.side_effect = \
        lambda method_name, func, *args, **kwargs: (calls.append("dialect.execute"), func())[1]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = [DefaultPlugin(container.plugin_service, mocker.MagicMock())]
    manager._container = container
    manager._telemetry_factory = mock_telemetry_factory
    manager._telemetry_factory.open_telemetry_context.return_value = None
    manager._telemetry_in_use = False
    manager._function_cache = [None] * (DbApiMethod.ALL.id + 1)

    def _execute(method):
        return manager._execute_with_subscribed_plugins(
            method,
            lambda plugin, next_func: plugin.execute(mocker.MagicMock(), method.method_name, next_func),
            lambda: (calls.append("target"), "result_value")[1])

    # Not network bound -> bypass, no DriverDialect.execute, transaction state still updated.
    assert _execute(DbApiMethod.CURSOR_LASTROWID) == "result_value"
    assert calls == ["target", "update_in_transaction"]

    # Network bound -> stays on the pipeline so the socket timeout guard is preserved.
    calls.clear()
    assert _execute(DbApiMethod.CURSOR_EXECUTE) == "result_value"
    assert calls == ["dialect.execute", "target", "update_in_transaction"]

    # Telemetry on -> back on the pipeline even for the otherwise-bypassable method, so the
    # per-plugin NESTED spans are still emitted.
    calls.clear()
    manager._telemetry_in_use = True
    manager._function_cache = [None] * (DbApiMethod.ALL.id + 1)
    assert _execute(DbApiMethod.CURSOR_LASTROWID) == "result_value"
    assert calls == ["dialect.execute", "target", "update_in_transaction"]


def test_must_use_pipeline(mocker, container, mock_telemetry_factory):
    # must_use_pipeline is the single authority for the bypass decision, so each term matters.
    container.plugin_service.is_network_bound_method.side_effect = \
        lambda name: name == DbApiMethod.CURSOR_EXECUTE.method_name

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = [DefaultPlugin(container.plugin_service, mocker.MagicMock())]
    manager._container = container
    manager._telemetry_factory = mock_telemetry_factory
    manager._telemetry_in_use = False
    manager._function_cache = [None] * (DbApiMethod.ALL.id + 1)

    # Chain not built yet -> nothing to decide on, so the pipeline is required.
    assert manager.must_use_pipeline(DbApiMethod.CURSOR_LASTROWID)

    # Built, unsubscribed, not network bound, telemetry off -> bypass allowed.
    manager._function_cache[DbApiMethod.CURSOR_LASTROWID.id] = \
        manager._make_pipeline(DbApiMethod.CURSOR_LASTROWID.method_name)
    assert not manager.must_use_pipeline(DbApiMethod.CURSOR_LASTROWID)

    # always_use_pipeline and network-bound methods are always required.
    assert manager.must_use_pipeline(DbApiMethod.CONNECT)
    manager._function_cache[DbApiMethod.CURSOR_EXECUTE.id] = \
        manager._make_pipeline(DbApiMethod.CURSOR_EXECUTE.method_name)
    assert manager.must_use_pipeline(DbApiMethod.CURSOR_EXECUTE)

    # Telemetry re-enables the pipeline for the otherwise-bypassable method.
    manager._telemetry_in_use = True
    assert manager.must_use_pipeline(DbApiMethod.CURSOR_LASTROWID)

    # A real subscribed plugin also forces the pipeline.
    subscriber = mocker.MagicMock()
    subscriber.subscribed_methods = {DbApiMethod.CURSOR_LASTROWID.method_name}
    manager._plugins = [subscriber, DefaultPlugin(container.plugin_service, mocker.MagicMock())]
    manager._telemetry_in_use = False
    manager._function_cache[DbApiMethod.CURSOR_LASTROWID.id] = \
        manager._make_pipeline(DbApiMethod.CURSOR_LASTROWID.method_name)
    assert manager.must_use_pipeline(DbApiMethod.CURSOR_LASTROWID)


# A value the call-context tests look for where no argument may appear.
CALL_CONTEXT_MARKER = "call-context-marker-7f3a"


class CallContextPlugin(Plugin):
    """Records the arguments it is entered with. It can replace the call's arguments, run the rest of the chain more
    than once, or run something while it handles the call."""

    def __init__(
            self,
            plugin_service: PluginServiceImpl,
            name: str,
            seen: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]],
            replace_with: Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]] = None,
            runs: int = 1,
            during: Optional[Callable[[], None]] = None):
        self._plugin_service = plugin_service
        self._name = name
        self._seen = seen
        self._replace_with = replace_with
        self._runs = runs
        self._during = during

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.CURSOR_EXECUTE.method_name, DbApiMethod.CURSOR_EXECUTEMANY.method_name}

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._seen.append((self._name, args, kwargs))
        if self._during is not None:
            self._during()
        context = self._plugin_service.current_call_context
        if self._replace_with is not None and context is not None:
            context.args, context.kwargs = self._replace_with
        result = None
        for _ in range(self._runs):
            result = execute_func()
        return result


class CallContextObserverPlugin(Plugin):
    """Records, for each call it handles, the arguments of the call context it reads."""

    def __init__(self, plugin_service: PluginServiceImpl, methods: Set[str], observed: List[Tuple[str, Optional[Tuple[Any, ...]]]]):
        self._plugin_service = plugin_service
        self._methods = methods
        self._observed = observed

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._methods

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        context = self._plugin_service.current_call_context
        self._observed.append((method_name, None if context is None else context.args))
        return execute_func()


def _call_context_service() -> PluginServiceImpl:
    # current_call_context reads only the calling thread's context, so the service needs no set-up.
    return PluginServiceImpl.__new__(PluginServiceImpl)


def _call_context_manager(mocker, container, mock_telemetry_factory, plugins: List[Plugin]) -> PluginManager:
    container.plugin_service.driver_dialect.execute.side_effect = \
        lambda method_name, func, *args, **kwargs: func()
    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._container = container
    manager._plugins = plugins + [DefaultPlugin(container.plugin_service, mocker.MagicMock())]
    manager._telemetry_factory = mock_telemetry_factory
    manager._telemetry_in_use = False
    manager._function_cache = [None] * (DbApiMethod.ALL.id + 1)
    return manager


def test_call_context_replacement_reaches_later_plugins_and_the_driver(mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    seen: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []
    driver_calls = []
    caller_params = ["plain"]
    manager = _call_context_manager(mocker, container, mock_telemetry_factory, [
        CallContextPlugin(service, "before", seen),
        CallContextPlugin(service, "replacer", seen, replace_with=(("q", ("ENC",)), {"prepare": True})),
        CallContextPlugin(service, "after", seen)])

    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE,
                    lambda *a, **kw: driver_calls.append((a, kw)), "q", caller_params)

    assert seen == [("before", ("q", ["plain"]), {}),
                    ("replacer", ("q", ["plain"]), {}),
                    ("after", ("q", ("ENC",)), {"prepare": True})]
    assert seen[0][1][1] is caller_params
    assert driver_calls == [(("q", ("ENC",)), {"prepare": True})]
    assert caller_params == ["plain"]


@pytest.mark.parametrize("method_name", ["execute", "executemany"])
@pytest.mark.parametrize("replacement", [((), {}), (("q", [("ENC",)]), {"prepare": True})])
def test_cursor_driver_method_receives_the_replacement_as_written(
        mocker, container, mock_telemetry_factory, mock_cursor, method_name, replacement):
    # An empty replacement too: the driver then fails on it, instead of receiving the caller's arguments.
    service = _call_context_service()
    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextPlugin(service, "replacer", [], replace_with=replacement)])
    cursor = AwsWrapperCursor(mocker.MagicMock(), container.plugin_service, manager, mock_cursor)

    getattr(cursor, method_name)("q", [("plain",)])

    getattr(mock_cursor, method_name).assert_called_once_with(*replacement[0], **replacement[1])


def test_without_a_replacement_the_driver_gets_the_callers_arguments(mocker, container, mock_telemetry_factory, mock_conn):
    driver_calls = []
    params = ["v"]
    manager = _call_context_manager(mocker, container, mock_telemetry_factory, [])

    # Nothing subscribed and not network bound: the call bypasses the pipeline.
    container.plugin_service.is_network_bound_method.return_value = False
    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE,
                    lambda *a, **kw: driver_calls.append((a, kw)), "q", params, prepare=True)
    assert container.plugin_service.driver_dialect.execute.call_count == 0

    # Network bound: the call goes through DefaultPlugin.
    container.plugin_service.is_network_bound_method.return_value = True
    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE,
                    lambda *a, **kw: driver_calls.append((a, kw)), "q", params, prepare=True)
    assert container.plugin_service.driver_dialect.execute.call_count == 1

    assert driver_calls == [(("q", ["v"]), {"prepare": True})] * 2
    # The driver gets the caller's own objects, not copies of them.
    assert all(call[0][1] is params for call in driver_calls)


def test_call_context_outer_rerun_starts_from_the_arguments_the_outer_plugin_saw(
        mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    seen: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []
    driver_calls = []
    manager = _call_context_manager(mocker, container, mock_telemetry_factory, [
        CallContextPlugin(service, "retrier", seen, runs=2),
        CallContextPlugin(service, "replacer", seen, replace_with=(("q", ("ENC",)), {"prepare": True}))])

    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: driver_calls.append(a), "q", ("plain",))

    assert [entry for entry in seen if entry[0] == "replacer"] == [("replacer", ("q", ("plain",)), {})] * 2
    assert driver_calls == [("q", ("ENC",))] * 2


def test_call_context_outer_retry_after_an_error_starts_from_the_arguments_the_outer_plugin_saw(
        mocker, container, mock_telemetry_factory, mock_conn):
    # A plugin after the replacer fails once and a plugin before it runs the rest again: the replacement made in the
    # failed run is undone, so the replacer starts again from the caller's arguments and replaces them once.
    service = _call_context_service()
    seen: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []
    driver_calls = []
    failures = [ValueError("inner plugin failed")]

    def fail_once() -> None:
        if failures:
            raise failures.pop()

    class RetryPlugin(CallContextPlugin):
        def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
            try:
                return execute_func()
            except ValueError:
                return execute_func()

    manager = _call_context_manager(mocker, container, mock_telemetry_factory, [
        RetryPlugin(service, "retrier", seen),
        CallContextPlugin(service, "replacer", seen, replace_with=(("q", ("ENC",)), {"prepare": True})),
        CallContextPlugin(service, "failer", seen, during=fail_once)])

    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: driver_calls.append(a), "q", ("plain",))

    assert [entry for entry in seen if entry[0] == "replacer"] == [("replacer", ("q", ("plain",)), {})] * 2
    assert driver_calls == [("q", ("ENC",))]


def test_current_call_context_is_none_outside_a_call_and_after_an_error(mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextPlugin(service, "replacer", [], replace_with=(("q", ("ENC",)), {}))])

    def failing_driver(*args, **kwargs):
        raise ValueError("driver failed")

    assert service.current_call_context is None
    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: None, "q", ("plain",))
    assert service.current_call_context is None
    with pytest.raises(ValueError):
        manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, failing_driver, "q", ("plain",))
    assert service.current_call_context is None


def test_only_execute_and_executemany_get_a_call_context(mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    observed: List[Tuple[str, Optional[Tuple[Any, ...]]]] = []
    methods = [DbApiMethod.CURSOR_EXECUTE, DbApiMethod.CURSOR_EXECUTEMANY, DbApiMethod.CURSOR_FETCHONE, DbApiMethod.CURSOR_FETCHMANY]
    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextObserverPlugin(service, {method.method_name for method in methods}, observed)])

    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: None, "q", ("v",))
    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTEMANY, lambda *a, **kw: None, "q", [("v",)])
    manager.execute(mock_conn, DbApiMethod.CURSOR_FETCHONE, lambda: None)
    manager.execute(mock_conn, DbApiMethod.CURSOR_FETCHMANY, lambda: None, 5)

    assert observed == [(DbApiMethod.CURSOR_EXECUTE.method_name, ("q", ("v",))),
                        (DbApiMethod.CURSOR_EXECUTEMANY.method_name, ("q", [("v",)])),
                        (DbApiMethod.CURSOR_FETCHONE.method_name, None),
                        (DbApiMethod.CURSOR_FETCHMANY.method_name, None)]


def test_nested_execute_has_its_own_call_context_and_restores_the_enclosing_one(
        mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    observed: List[Any] = []

    def nested_execute():
        if observed:
            return  # only the enclosing call makes a nested one
        enclosing = service.current_call_context
        observed.append(("enclosing", None if enclosing is None else enclosing.args))
        manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE,
                        lambda *a, **kw: observed.append(("nested", a, service.current_call_context is not enclosing)),
                        "nested q", ("n",))
        observed.append(("restored", service.current_call_context is enclosing))

    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextPlugin(service, "nester", [], during=nested_execute)])
    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: None, "q", ("o",))

    assert observed == [("enclosing", ("q", ("o",))), ("nested", ("nested q", ("n",)), True), ("restored", True)]


def test_other_method_inside_execute_sees_the_enclosing_call_context(mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    observed: List[Tuple[str, Optional[Tuple[Any, ...]]]] = []

    def nested_fetchone():
        manager.execute(mock_conn, DbApiMethod.CURSOR_FETCHONE, lambda: "row")

    manager = _call_context_manager(mocker, container, mock_telemetry_factory, [
        CallContextObserverPlugin(service, {DbApiMethod.CURSOR_FETCHONE.method_name}, observed),
        CallContextPlugin(service, "nester", [], during=nested_fetchone)])
    manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: None, "q", ("o",))

    assert observed == [(DbApiMethod.CURSOR_FETCHONE.method_name, ("q", ("o",)))]


def test_call_contexts_of_concurrent_threads_are_separate(mocker, container, mock_telemetry_factory, mock_conn):
    service = _call_context_service()
    barrier = threading.Barrier(2)
    mismatches: List[Any] = []
    manager = _call_context_manager(mocker, container, mock_telemetry_factory, [CallContextPlugin(service, "plugin", [])])

    def driver(*args, **kwargs):
        # Both threads are inside a call here at the same time.
        barrier.wait(timeout=5)
        context = service.current_call_context
        if context is None or context.args != args:
            mismatches.append(args)

    def run(sql: str):
        for _ in range(20):
            manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, driver, sql, ())

    threads = [threading.Thread(target=run, args=(sql,)) for sql in ("q0", "q1")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not any(thread.is_alive() for thread in threads)
    assert mismatches == []


def test_late_socket_timeout_worker_sends_the_replacement(mocker, container, mock_telemetry_factory, mock_conn):
    # With the dialect's pool busy, the driver call is still queued when the statement times out and every
    # replacement has been undone. When a worker finally runs it, it must still send the replacement.
    service = _call_context_service()
    release = threading.Event()
    sent = []

    def driver(*args, **kwargs):
        sent.append((args, kwargs))

    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextPlugin(service, "replacer", [], replace_with=(("q", ("ENC",)), {}))])
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(release.wait, 10)  # the only worker is held by an earlier operation that is stuck
        container.plugin_service.driver_dialect.execute.side_effect = \
            lambda method_name, func, *args, **kwargs: timeout(pool, 0.05)(func)()

        with pytest.raises(FuturesTimeoutError):
            manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, driver, "q", ("plain",))
        assert sent == []

        release.set()
        # timeout() doesn't cancel a call that is still queued, so the worker runs it now. If timeout() ever cancels it,
        # nothing is sent. Either way the driver never gets the caller's arguments or the cleared ones.

    assert sent in ([], [(("q", ("ENC",)), {})])


def test_socket_timeout_worker_sends_the_replacement(mocker, container, mock_telemetry_factory, mock_conn):
    # With socket_timeout set, the dialect runs the driver call on a worker thread, which doesn't see this thread's
    # call context. The worker must still send the replacement.
    service = _call_context_service()
    sent = []
    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextPlugin(service, "replacer", [], replace_with=(("q", ("ENC",)), {}))])
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(lambda: None).result(5)  # start the worker before the call, so it can't inherit the call's context
        container.plugin_service.driver_dialect.execute.side_effect = \
            lambda method_name, func, *args, **kwargs: timeout(pool, 5)(func)()
        manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, lambda *a, **kw: sent.append((a, kw)), "q", ("plain",))

    assert sent == [(("q", ("ENC",)), {})]


@pytest.mark.parametrize("fails", [False, True], ids=["returns", "raises"])
def test_context_copied_during_a_call_holds_no_argument_after_it(mocker, container, mock_telemetry_factory, mock_conn, caplog, fails):
    caplog.set_level(logging.DEBUG)
    service = _call_context_service()
    copies: List[contextvars.Context] = []
    manager = _call_context_manager(mocker, container, mock_telemetry_factory,
                                    [CallContextPlugin(service, "copier", [], during=lambda: copies.append(contextvars.copy_context()))])

    driver_error = ValueError("driver failed")

    def driver(*args: Any, **kwargs: Any) -> None:
        if fails:
            raise driver_error

    if fails:
        with pytest.raises(ValueError) as excinfo:
            manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, driver, "q", (CALL_CONTEXT_MARKER,), params=CALL_CONTEXT_MARKER)
        # The driver's own error arrives unchanged, with nothing of the call's arguments added to it.
        assert excinfo.value is driver_error
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__context__ is None
    else:
        manager.execute(mock_conn, DbApiMethod.CURSOR_EXECUTE, driver, "q", (CALL_CONTEXT_MARKER,), params=CALL_CONTEXT_MARKER)

    copied = copies[0].run(lambda: service.current_call_context)
    assert copied is not None
    assert copied.args == ()
    assert copied.kwargs == {}
    # The core neither logs nor traces the call's arguments.
    assert [record for record in caplog.records if CALL_CONTEXT_MARKER in record.getMessage()] == []
    assert CALL_CONTEXT_MARKER not in str(mock_telemetry_factory.mock_calls)


def test_call_context_repr_and_str_show_no_argument():
    context = PluginCallContext(("q", (CALL_CONTEXT_MARKER,)), {"params": CALL_CONTEXT_MARKER})

    assert CALL_CONTEXT_MARKER not in repr(context)
    assert CALL_CONTEXT_MARKER not in str(context)
    assert CALL_CONTEXT_MARKER not in format(context)
