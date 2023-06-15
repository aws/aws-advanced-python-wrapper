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

from logging import getLogger
from typing import Any, Callable, Iterator, List, Optional, Union

from aws_wrapper.connection_provider import DriverConnectionProvider
from aws_wrapper.hostinfo import NO_PORT, HostInfo
from aws_wrapper.pep249 import Connection, Cursor, Error
from aws_wrapper.plugins import PluginManager, PluginService
from aws_wrapper.utils.properties import Properties, PropertiesUtils

logger = getLogger(__name__)


class AwsWrapperConnection(Connection):
    __module__ = "aws_wrapper"

    def __init__(self, plugin_manager: PluginManager, target_conn: Connection):
        self._target_conn: Connection = target_conn
        self._plugin_manager: PluginManager = plugin_manager

    @staticmethod
    def connect(
            conninfo: str = "",
            target: Union[None, str, Callable] = None,
            **kwargs: Union[None, int, str]
    ) -> "AwsWrapperConnection":

        # Check if target provided
        if not target:
            raise Error("Target driver is required")

        # TODO: fix target str parsing functionality
        if type(target) == str:
            target = eval(target)

        if not callable(target):
            raise Error("Target driver should be a target driver's connect() method/function")
        target_func: Callable = target

        props: Properties = PropertiesUtils.parse_properties(conn_info=conninfo, **kwargs)
        logger.debug(PropertiesUtils.log_properties(props, "Connection Properties: "))

        plugin_service: PluginService = PluginService()
        plugin_manager: PluginManager = PluginManager(props, plugin_service, DriverConnectionProvider(target_func))
        host_info = HostInfo(props["host"], int(props["port"]) if "port" in props else NO_PORT)

        conn = plugin_manager.connect(host_info, props, True)

        return AwsWrapperConnection(plugin_manager, conn)

    def close(self) -> None:
        if self._target_conn:
            self._target_conn.close()

    def cursor(self, **kwargs: Union[None, int, str]) -> "AwsWrapperCursor":
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
    __module__ = "aws_wrapper"

    def __init__(self, conn: AwsWrapperConnection, plugin_manager: PluginManager, target_cursor: Cursor):
        self._conn: AwsWrapperConnection = conn
        self._plugin_manager: PluginManager = plugin_manager
        self._target_cursor: Cursor = target_cursor

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
        self._target_cursor.close()

    def callproc(self, **kwargs: Union[None, int, str]):
        return self._target_cursor.callproc(**kwargs)

    def execute(
            self,
            query: str,
            **kwargs: Union[None, int, str]
    ) -> "AwsWrapperCursor":
        if self._plugin_manager.num_plugins == 0:
            self._target_cursor = self._target_cursor.execute(query, **kwargs)
            return self

        result = self._plugin_manager.execute(self._target_cursor, "Cursor.execute",
                                              lambda: self._target_cursor.execute(query, **kwargs), query, kwargs)
        return result

    def executemany(
            self,
            query: str,
            **kwargs: Union[None, int, str]
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
