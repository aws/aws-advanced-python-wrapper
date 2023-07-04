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
    from aws_wrapper.connection_provider import ConnectionProvider
    from aws_wrapper.pep249 import Connection

from typing import Any, Callable, Dict, List, Optional, Set
from unittest import TestCase
from unittest.mock import MagicMock, patch

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostInfo, HostRole
from aws_wrapper.plugins import (DefaultPlugin, DummyPlugin, Plugin,
                                 PluginManager, PluginServiceManagerContainer)
from aws_wrapper.utils.notifications import (ConnectionEvent, HostEvent,
                                             OldConnectionSuggestedAction)
from aws_wrapper.utils.properties import Properties


class TestDriverConnectionProvider(TestCase):
    def test_execute_call_a(self):
        calls = []
        args = [10, "arg2", 3.33]
        plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]
        mock_connection = MagicMock()

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
            manager._function_cache = {}

        with patch.object(manager, '_make_pipeline', wraps=manager._make_pipeline) as make_pipeline_func:
            result = manager.execute(mock_connection, "test_call_a", lambda: self._target_call(calls), *args)

            make_pipeline_func.assert_called_once_with("test_call_a")
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
            result = manager.execute(mock_connection, "test_call_a", lambda: self._target_call(calls), *args)

            # The first execute call should cache the pipeline
            make_pipeline_func.assert_called_once_with("test_call_a")
            assert result == "result_value"
            assert len(calls) == 7
            assert calls[0] == "TestPluginOne:before execute"
            assert calls[1] == "TestPluginTwo:before execute"
            assert calls[2] == "TestPluginThree:before execute"
            assert calls[3] == "target_call"
            assert calls[4] == "TestPluginThree:after execute"
            assert calls[5] == "TestPluginTwo:after execute"
            assert calls[6] == "TestPluginOne:after execute"

    def _target_call(self, calls: List[str]):
        calls.append("target_call")
        return "result_value"

    def test_execute_call_b(self):
        calls = []
        args = [10, "arg2", 3.33]
        plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]
        mock_connection = MagicMock()

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
            manager._function_cache = {}

        result = manager.execute(mock_connection, "test_call_b", lambda: self._target_call(calls), *args)

        assert result == "result_value"
        assert len(calls) == 5
        assert calls[0] == "TestPluginOne:before execute"
        assert calls[1] == "TestPluginTwo:before execute"
        assert calls[2] == "target_call"
        assert calls[3] == "TestPluginTwo:after execute"
        assert calls[4] == "TestPluginOne:after execute"

    def test_execute_call_c(self):
        calls = []
        args = [10, "arg2", 3.33]
        plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]
        mock_connection = MagicMock()

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
            manager._function_cache = {}

        result = manager.execute(mock_connection, "test_call_c", lambda: self._target_call(calls), *args)

        assert result == "result_value"
        assert len(calls) == 3
        assert calls[0] == "TestPluginOne:before execute"
        assert calls[1] == "target_call"
        assert calls[2] == "TestPluginOne:after execute"

    def test_connect(self):
        calls = []
        mock_connection = MagicMock()
        plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls, mock_connection)]

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
            manager._function_cache = {}

        result = manager.connect(HostInfo("localhost"), Properties(), True)

        assert result == mock_connection
        assert len(calls) == 4
        assert calls[0] == "TestPluginOne:before connect"
        assert calls[1] == "TestPluginThree:before connect"
        assert calls[2] == "TestPluginThree:after connect"
        assert calls[3] == "TestPluginOne:after connect"

    def test_exception_before_connect(self):
        calls = []
        plugins = \
            [TestPluginOne(calls), TestPluginTwo(calls), TestPluginRaisesError(calls, True), TestPluginThree(calls)]

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
            manager._function_cache = {}

        with self.assertRaises(AwsWrapperError):
            result = manager.connect(HostInfo("localhost"), Properties(), True)
            assert result is None

        assert len(calls) == 2
        assert calls[0] == "TestPluginOne:before connect"
        assert calls[1] == "TestPluginRaisesError:before connect"

    def test_exception_after_connect(self):
        calls = []
        plugins = \
            [TestPluginOne(calls), TestPluginTwo(calls), TestPluginRaisesError(calls, False), TestPluginThree(calls)]

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
            manager._function_cache = {}

        with self.assertRaises(AwsWrapperError):
            result = manager.connect(HostInfo("localhost"), Properties(), True)
            assert result is None

        assert len(calls) == 5
        assert calls[0] == "TestPluginOne:before connect"
        assert calls[1] == "TestPluginRaisesError:before connect"
        assert calls[2] == "TestPluginThree:before connect"
        assert calls[3] == "TestPluginThree:after connect"
        assert calls[4] == "TestPluginRaisesError:after connect"

    def test_no_plugins(self):
        props = Properties(plugins="")
        container: PluginServiceManagerContainer = MagicMock()
        default_conn_provider: ConnectionProvider = MagicMock()

        manager = PluginManager(container, props, default_conn_provider)

        assert 1 == manager.num_plugins

    def test_plugin_setting(self):
        props = Properties(plugins="dummy,dummy")
        container: PluginServiceManagerContainer = MagicMock()
        default_conn_provider: ConnectionProvider = MagicMock()

        manager = PluginManager(container, props, default_conn_provider)

        assert 3 == manager.num_plugins
        assert isinstance(manager._plugins[0], DummyPlugin)
        assert isinstance(manager._plugins[1], DummyPlugin)
        assert isinstance(manager._plugins[2], DefaultPlugin)

    def test_notify_connection_changed(self):
        calls = []
        plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
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

    def test_notify_host_list_changed(self):
        calls = []
        plugins = [TestPluginOne(calls), TestPluginTwo(calls), TestPluginThree(calls)]

        with patch.object(PluginManager, "__init__", lambda w, x, y, z: None):
            manager = PluginManager(None, None, None)
            manager._plugins = plugins
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

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Connection:
        self._calls.append(type(self).__name__ + ":before connect")
        if self._connection is not None:
            result = self._connection
        else:
            result = connect_func()
        self._calls.append(type(self).__name__ + ":after connect")
        return result

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
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

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
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
        return {"test_call_a", "connect", "notify_connection_changed", "notify_host_list_changed"}

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._calls.append(type(self).__name__ + ":notify_connection_changed")
        return OldConnectionSuggestedAction.PRESERVE


class TestPluginRaisesError(TestPlugin):
    def __init__(self, calls: List[str], throw_before_call: bool = True):
        super().__init__(calls)
        self._throw_before_call = throw_before_call

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Connection:
        self._calls.append(type(self).__name__ + ":before connect")
        if self._throw_before_call:
            raise AwsWrapperError()

        if self._connection is None:
            connect_func()
        self._calls.append(type(self).__name__ + ":after connect")
        raise AwsWrapperError()

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: tuple) -> Any:
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
