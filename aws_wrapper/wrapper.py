"""
pawswrapper - AWS Wrapper Driver for Python

"""

from ast import Num
from typing import Any, Callable, Sequence, cast, Dict, Generator, Generic, Iterator
from typing import List, NamedTuple, Optional, Type, TypeVar, Tuple, Union
from typing import overload
from .pep249 import Connection, Cursor, Error

class HostInfo:
    url: str
    port: int

class Properties(Dict[str, str]):
    ...

class Plugin:

    _num: int = 0;

    def __init__(self, num: int):
        self._num = num

    def connect(self, hostInfo: HostInfo, props: Properties, initial: bool, executeFunc: Callable) -> Any:
        #print("Plugin {}: connect before".format(self._num))
        result = executeFunc();
        #print("Plugin {}: connect after".format(self._num))
        return result

    def execute(self, executeFunc: Callable) -> Any:
        #print("Plugin {}: execute before".format(self._num))
        result = executeFunc();
        #print("Plugin {}: execute after".format(self._num))
        return result

    def subscribed(self, methodName: str) -> bool:
        return True

class PluginManager:

    _plugins: List[Plugin] = []
    _numOfPlugins = 0
    _functionCache: bool = True
    _executeFunc: Callable = None
    _connectFunc: Callable = None

    def __init__(self, props: Properties = Properties(), numOfPlugins: int = 10, functionCache: bool = True):

        self._numOfPlugins = numOfPlugins
        self._functionCache = functionCache

        print("plugins: {}, cache: {}".format(numOfPlugins, functionCache))

        # Init dummy plugins
        for i in range(self._numOfPlugins):
            self._plugins.append(Plugin(i))

    @property
    def numOfPlugins(self) -> int:
        return self._numOfPlugins;

    @property
    def executeFunc(self) -> Callable:
        if self._functionCache and self._executeFunc:
            return self._executeFunc

        # TODO: add support Plugin.subscribed("execute")

        code = "x()"
        numOfPlugins = len(self._plugins)
        if numOfPlugins > 0:
            code = "plugins[{}].execute(x)".format(numOfPlugins - 1)

        for i in range(numOfPlugins - 1, 0, -1):
            code = "plugins[{}].execute(lambda : {})".format(i - 1, code)
        
        code = "lambda x : {}".format(code)
        #print(code)
        self._executeFunc = eval(code, { "plugins": self._plugins })

        return self._executeFunc

    @property
    def connectFunc(self) -> Callable:
        if self._functionCache and self._connectFunc:
            return self._connectFunc

        # TODO: add support Plugin.subscribed("connect")

        code = "x()"
        numOfPlugins = len(self._plugins)
        if numOfPlugins > 0:
            code = "plugins[{}].connect(hostInfo, props, initial, x)".format(numOfPlugins - 1)

        for i in range(numOfPlugins - 1, 0, -1):
            code = "plugins[{}].connect(hostInfo, props, initial, lambda : {})".format(i - 1, code)
        
        code = "lambda hostInfo, props, initial, x : {}".format(code)
        #print(code)
        self._connectFunc = eval(code, { "plugins": self._plugins })

        return self._connectFunc


class AwsWrapperConnection(Connection):

    __module__ = "pawswrapper"
    
    _targetConn: Connection = None
    _pluginManager: PluginManager = None

    @staticmethod
    def connect(
        conninfo: str = "",
        target: Union[str, callable] = None,
        **kwargs: Union[None, int, str]
    ) -> "AwsWrapperConnection":
        # Check if target provided
        if not target:
            raise Error("Target driver is required")

        if type(target) == str:
            target = eval(target);

        if not callable(target):
            raise Error("Target driver should be a target driver's connect() method/function")

        numOfPlugins = kwargs.get("numOfPlugins")
        if numOfPlugins == None:
            numOfPlugins = 10;
        functionCache = kwargs.get("functionCache")
        if functionCache == None:
            functionCache = True

        props: Properties = Properties()
        pluginManager: PluginManager = PluginManager(props=props, numOfPlugins=numOfPlugins, functionCache=functionCache)
        hostInfo: HostInfo = HostInfo()

        kwargs.pop("numOfPlugins")
        kwargs.pop("functionCache")

        # Target driver is a connect function
        if pluginManager.numOfPlugins == 0:
            conn = target(conninfo, **kwargs)
        else:
            conn = pluginManager.connectFunc(hostInfo, props, True, lambda : target(conninfo, **kwargs))

        return AwsWrapperConnection(pluginManager, conn);

    def __init__(self, pluginManager: PluginManager, targetConn: Connection):
        self._targetConn = targetConn
        self._pluginManager = pluginManager

    def close(self) -> None:
        self._targetConn.close();

    def cursor(self, **kwargs) -> "AwsWrapperCursor":
        _cursor = self._targetConn.cursor(**kwargs)
        return AwsWrapperCursor(self, self._pluginManager, _cursor);

    def commit(self) -> None:
        self._targetConn.commit();

    def rollback(self) -> None:
        self._targetConn.rollback();

    def tpc_begin(self, xid: Any) -> None:
        self._targetConn.tpc_begin(xid);

    def tpc_prepare(self) -> None:
        self._targetConn.tpc_prepare();

    def tpc_commit(self, xid: Any = None) -> None:
        self._targetConn.tpc_commit(xid);

    def tpc_rollback(self, xid: Any = None) -> None:
        self._targetConn.tpc_rollback(xid);

    def tpc_recover(self) -> Any:
        return self._targetConn.tpc_recover();

    def __enter__(self: "AwsWrapperConnection") -> "AwsWrapperConnection":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._targetConn:
            self._targetConn.close()



class AwsWrapperCursor(Cursor):

    __module__ = "pawswrapper"

    _targetCursor: Cursor
    _conn: AwsWrapperConnection
    _pluginManager: PluginManager

    def __init__(self, conn: AwsWrapperConnection, pluginManager: PluginManager, targetCursor: Cursor):
        self._conn = conn
        self._targetCursor = targetCursor
        self._pluginManager = pluginManager

    # It's not part of PEP249
    @property
    def connection(self) -> AwsWrapperConnection:
        return self._conn;

    @property
    def description(self):
        return self._targetCursor.description;

    @property
    def rowcount(self) -> int:
        return self._targetCursor.rowcount;

    @property
    def arraysize(self) -> int:
        return self._targetCursor.arraysize;

    def close(self) -> None:
        return self._targetCursor.close;

    def callproc(self, **kwargs):
        return self._targetCursor.callproc(**kwargs);

    def execute(
        self,
        query: str,
        **kwargs
    ) -> "AwsWrapperCursor":
        if self._pluginManager.numOfPlugins == 0:
            return self._targetCursor.execute(query, **kwargs)

        result = self._pluginManager.executeFunc(lambda : self._targetCursor.execute(query, **kwargs))
        return result

    def executemany(
        self,
        query: str,
        **kwargs
    ) -> None:
        self._targetCursor.executemany(query, **kwargs)

    def nextset(self) -> bool:
        return self._targetCursor.nextset()

    def fetchone(self) -> Any:
        return self._targetCursor.fetchone()

    def fetchmany(self, size: int = 0) -> List[Any]:
        return self._targetCursor.fetchmany(size)

    def fetchall(self) -> List[Any]:
        return self._targetCursor.fetchall()

    def __iter__(self) -> Iterator[Any]:
        return self._targetCursor.__iter__()

    def setinputsizes(self, sizes: Any) -> None:
        return self._targetCursor.setinputsizes(sizes)

    def setoutputsize(self, size: Any, column: int = None) -> None:
        return self._targetCursor.setoutputsize(size, column)

    def __enter__(self: "AwsWrapperCursor") -> "AwsWrapperCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

