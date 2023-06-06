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

from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from .pep249 import Connection, Cursor, Error


class HostInfo:
    url: str
    port: int


class Properties(Dict[str, str]):
    ...


class Plugin:
    _num: int = 0

    def __init__(self, num: int):
        self._num = num

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, execute_func: Callable) -> Any:
        # print("Plugin {}: connect before".format(self._num))
        result = execute_func()
        # print("Plugin {}: connect after".format(self._num))
        return result

    def execute(self, execute_func: Callable) -> Any:
        # print("Plugin {}: execute before".format(self._num))
        result = execute_func()
        # print("Plugin {}: execute after".format(self._num))
        return result

    def subscribed(self, method_name: str) -> bool:
        return True


class PluginManager:
    _plugins: List[Plugin] = []
    _execute_func: Optional[Callable] = None
    _connect_func: Optional[Callable] = None

    def __init__(self, props: Properties = Properties(),
                 num_of_plugins: int = 10, function_cache: bool = True):

        self._num_of_plugins = num_of_plugins
        self._function_cache = function_cache

        print("plugins: {}, cache: {}".format(num_of_plugins, function_cache))

        # Init dummy plugins
        for i in range(self._num_of_plugins):
            self._plugins.append(Plugin(i))

    @property
    def num_of_plugins(self) -> int:
        return self._num_of_plugins

    @property
    def execute_func(self) -> Callable:
        if self._function_cache and self._execute_func:
            return self._execute_func

        # TODO: add support Plugin.subscribed("execute")

        code = "x()"
        num_of_plugins = len(self._plugins)
        if num_of_plugins > 0:
            code = "plugins[{}].execute(x)".format(num_of_plugins - 1)

        for i in range(num_of_plugins - 1, 0, -1):
            code = "plugins[{}].execute(lambda : {})".format(i - 1, code)

        code = "lambda x : {}".format(code)
        # print(code)

        self._execute_func = eval(code, {"plugins": self._plugins})

        assert (self._execute_func is not None)
        return self._execute_func

    @property
    def connect_func(self) -> Callable:
        if self._function_cache and self._connect_func:
            return self._connect_func

        # TODO: add support Plugin.subscribed("connect")

        code = "x()"
        num_of_plugins = len(self._plugins)
        if num_of_plugins > 0:
            code = "plugins[{}].connect(hostInfo, props, initial, x)".format(num_of_plugins - 1)

        for i in range(num_of_plugins - 1, 0, -1):
            code = "plugins[{}].connect(hostInfo, props, initial, lambda : {})".format(i - 1, code)

        code = "lambda hostInfo, props, initial, x : {}".format(code)
        # print(code)
        self._connect_func = eval(code, {"plugins": self._plugins})

        assert (self._connect_func is not None)
        return self._connect_func


class AwsWrapperConnection(Connection):
    __module__ = "pawswrapper"

    def __init__(self, plugin_manager: PluginManager, target_conn: Connection):
        self._target_conn = target_conn
        self._plugin_manager = plugin_manager

    @staticmethod
    def connect(
            conninfo: str = "",
            target: Union[None, str, Callable] = None,
            **kwargs: Union[None, int, str]
    ) -> "AwsWrapperConnection":
        # Check if target provided
        if not target:
            raise Error("Target driver is required")

        if type(target) == str:
            target = eval(target)

        if not callable(target):
            raise Error("Target driver should be a target driver's connect() method/function")

        num_of_plugins = kwargs.get("numOfPlugins")
        if num_of_plugins is not None:
            kwargs.pop("numOfPlugins")
        if type(num_of_plugins) != int:
            num_of_plugins = 10

        function_cache = kwargs.get("functionCache")
        if function_cache is None:
            function_cache = True
        else:
            kwargs.pop("functionCache")
            function_cache = bool(function_cache)

        props: Properties = Properties()
        plugin_manager: PluginManager = PluginManager(props=props, num_of_plugins=num_of_plugins,
                                                      function_cache=function_cache)
        host_info: HostInfo = HostInfo()

        # Target driver is a connect function
        if plugin_manager.num_of_plugins == 0:
            conn = target(conninfo, **kwargs)
        else:
            conn = plugin_manager.connect_func(host_info, props, True, lambda: target(conninfo, **kwargs))

        return AwsWrapperConnection(plugin_manager, conn)

    def close(self) -> None:
        if self._target_conn:
            self._target_conn.close()

    def cursor(self, **kwargs) -> "AwsWrapperCursor":
        _cursor = self._target_conn.cursor(**kwargs)
        return AwsWrapperCursor(self, self._plugin_manager, _cursor)

    def commit(self) -> None:
        self._target_conn.commit()

    def rollback(self) -> None:
        self._target_conn.rollback()

    def tpc_begin(self, xid: Any) -> None:
        self._target_conn.tpc_begin(xid)

    def tpc_prepare(self) -> None:
        self._target_conn.tpc_prepare()

    def tpc_commit(self, xid: Any = None) -> None:
        self._target_conn.tpc_commit(xid)

    def tpc_rollback(self, xid: Any = None) -> None:
        self._target_conn.tpc_rollback(xid)

    def tpc_recover(self) -> Any:
        return self._target_conn.tpc_recover()

    def __enter__(self: "AwsWrapperConnection") -> "AwsWrapperConnection":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._target_conn:
            self._target_conn.close()


class AwsWrapperCursor(Cursor):
    __module__ = "pawswrapper"

    _target_cursor: Cursor
    _conn: AwsWrapperConnection
    _plugin_manager: PluginManager

    def __init__(self, conn: AwsWrapperConnection, plugin_manager: PluginManager, target_cursor: Cursor):
        self._conn = conn
        self._target_cursor = target_cursor
        self._plugin_manager = plugin_manager

    # It's not part of PEP249
    @property
    def connection(self) -> AwsWrapperConnection:
        return self._conn

    @property
    def description(self):
        return self._target_cursor.description

    @property
    def rowcount(self) -> int:
        return self._target_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._target_cursor.arraysize

    def close(self) -> None:
        self._target_cursor.close

    def callproc(self, **kwargs):
        return self._target_cursor.callproc(**kwargs)

    def execute(
            self,
            query: str,
            **kwargs
    ) -> "AwsWrapperCursor":
        if self._plugin_manager.num_of_plugins == 0:
            self._target_cursor = self._target_cursor.execute(query, **kwargs)
            return self

        result = self._plugin_manager.execute_func(lambda: self._target_cursor.execute(query, **kwargs))
        return result

    def executemany(
            self,
            query: str,
            **kwargs
    ) -> None:
        self._target_cursor.executemany(query, **kwargs)

    def nextset(self) -> bool:
        return self._target_cursor.nextset()

    def fetchone(self) -> Any:
        return self._target_cursor.fetchone()

    def fetchmany(self, size: int = 0) -> List[Any]:
        return self._target_cursor.fetchmany(size)

    def fetchall(self) -> List[Any]:
        return self._target_cursor.fetchall()

    def __iter__(self) -> Iterator[Any]:
        return self._target_cursor.__iter__()

    def setinputsizes(self, sizes: Any) -> None:
        return self._target_cursor.setinputsizes(sizes)

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        return self._target_cursor.setoutputsize(size, column)

    def __enter__(self: "AwsWrapperCursor") -> "AwsWrapperCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
