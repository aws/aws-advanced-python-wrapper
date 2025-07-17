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

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection

from typing import Any, Callable, Dict, List, Optional, Set

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
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.plugin_service import PluginManager
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import Properties


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


def test_execute_call_a(mocker, mock_conn, container, mock_plugin_service, mock_driver_dialect, mock_telemetry_factory):
    calls = []
    args = [10, "arg2", 3.33]
    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    container.plugin_service.return_value = mock_plugin_service
    manager._container = container
    manager._plugins = plugins
    manager._telemetry_factory = mock_telemetry_factory
    manager._function_cache = {}

    make_pipeline_func = mocker.patch.object(manager, '_make_pipeline', wraps=manager._make_pipeline)
    result = manager.execute(mock_conn, "test_call_a", lambda: _target_call(calls), *args)

    make_pipeline_func.assert_called_once_with("test_call_a", None)
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
    result = manager.execute(mock_conn, "test_call_a", lambda: _target_call(calls), *args)

    # The first execute call should cache the pipeline
    make_pipeline_func.assert_called_once_with("test_call_a", None)
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
    manager._function_cache = {}

    result = manager.execute(mock_conn, "test_call_b", lambda: _target_call(calls), *args)

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
    manager._function_cache = {}

    result = manager.execute(mock_conn, "test_call_c", lambda: _target_call(calls), *args)

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
    manager._function_cache = {}

    # Set current connection to a new connection object
    container.plugin_service.current_connection = mocker.MagicMock(spec=psycopg.Connection)
    with pytest.raises(AwsWrapperError):
        manager.execute(mock_conn, "test_execute", lambda: _target_call([]))


def test_connect(mocker, container, mock_conn, mock_driver_dialect, mock_telemetry_factory):
    calls = []

    plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls, mock_conn)]

    mocker.patch.object(PluginManager, "__init__", lambda w, x, y, z: None)
    manager = PluginManager(mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())
    manager._plugins = plugins
    manager._function_cache = {}
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
    manager._function_cache = {}
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
    manager._function_cache = {}
    manager._telemetry_factory = mock_telemetry_factory
    manager._container = container

    make_pipeline_func = mocker.patch.object(manager, '_make_pipeline', wraps=manager._make_pipeline)
    # The first call to force_connect should generate the plugin pipeline and cache it
    result = manager.force_connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)

    make_pipeline_func.assert_called_once_with("force_connect", None)
    assert result == mock_conn
    assert len(calls) == 4
    assert calls[0] == "TestPluginOne:before forceConnect"
    assert calls[1] == "TestPluginThree:before forceConnect"
    assert calls[2] == "TestPluginThree:after forceConnect"
    assert calls[3] == "TestPluginOne:after forceConnect"

    calls.clear()

    result = manager.force_connect(mocker.MagicMock(), mocker.MagicMock(), HostInfo("localhost"), Properties(), True)

    # The second call should have used the cached plugin pipeline, so make_pipeline should not have been called again
    make_pipeline_func.assert_called_once_with("force_connect", None)
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
    manager._function_cache = {}
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
    manager._function_cache = {}
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
    manager._function_cache = {}
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
    manager._function_cache = {}

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
    manager._function_cache = {}

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
        return {"test_call_a", "test_call_b", "notify_connection_changed"}

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._calls.append(type(self).__name__ + ":notify_connection_changed")
        return OldConnectionSuggestedAction.DISPOSE


class TestPluginThree(TestPlugin):

    @property
    def subscribed_methods(self) -> Set[str]:
        return {"test_call_a", "connect", "force_connect", "notify_connection_changed", "notify_host_list_changed"}

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
