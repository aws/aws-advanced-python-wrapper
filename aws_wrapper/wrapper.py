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
from aws_wrapper.errors import AwsWrapperError, FailoverSuccessError
from aws_wrapper.host_list_provider import AuroraHostListProvider
from aws_wrapper.pep249 import Connection, Cursor, Error
from aws_wrapper.plugin import CanReleaseResources
from aws_wrapper.plugin_service import (PluginManager, PluginService,
                                        PluginServiceImpl,
                                        PluginServiceManagerContainer)
from aws_wrapper.target_driver_dialect import TargetDriverDialectManager
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, PropertiesUtils

logger = getLogger(__name__)


class AwsWrapperConnection(Connection, CanReleaseResources):
    __module__ = "aws_wrapper"

    def __init__(self, plugin_service: PluginService, plugin_manager: PluginManager):
        self._plugin_service = plugin_service
        self._plugin_manager: PluginManager = plugin_manager

    @property
    def target_connection(self):
        return self._plugin_service.current_connection

    @property
    def autocommit(self):
        if not hasattr(self.target_connection, "autocommit"):
            raise AwsWrapperError(Messages.get_formatted("Wrapper.NotImplemented", "autocommit"))

        return self._plugin_manager.execute(
            self.target_connection,
            "Connection.autocommit",
            lambda: self.target_connection.autocommit)

    @autocommit.setter
    def autocommit(self, autocommit: bool):
        if not hasattr(self.target_connection, "autocommit"):
            raise AwsWrapperError(Messages.get_formatted("Wrapper.NotImplemented", "autocommit"))

        self._plugin_manager.execute(
            self.target_connection,
            "Connection.autocommit_setter",
            lambda: setattr(self.target_connection, "autocommit", autocommit))

    @staticmethod
    def connect(
            conninfo: str = "",
            target: Union[None, str, Callable] = None,
            **kwargs: Union[None, int, str]
    ) -> "AwsWrapperConnection":
        if not target:
            raise Error(Messages.get("Wrapper.RequiredTargetDriver"))

        # TODO: fix target str parsing functionality
        if isinstance(target, str):
            target = eval(target)

        if not callable(target):
            raise Error(Messages.get("Wrapper.ConnectMethod"))
        target_func: Callable = target

        props: Properties = PropertiesUtils.parse_properties(conn_info=conninfo, **kwargs)
        logger.debug(PropertiesUtils.log_properties(props, "Connection Properties: "))

        target_driver_dialect_manager: TargetDriverDialectManager = TargetDriverDialectManager()
        target_driver_dialect = target_driver_dialect_manager.get_dialect(target_func, props)
        container: PluginServiceManagerContainer = PluginServiceManagerContainer()
        plugin_service = PluginServiceImpl(container, props, target_driver_dialect)
        plugin_manager: PluginManager = PluginManager(
            container,
            props,
            DriverConnectionProvider(target_func, target_driver_dialect))
        plugin_service.host_list_provider = AuroraHostListProvider(plugin_service, props)

        plugin_manager.init_host_provider(props, plugin_service)

        plugin_service.refresh_host_list()

        if plugin_service.current_connection is not None:
            return AwsWrapperConnection(plugin_service, plugin_manager)

        conn = plugin_manager.connect(plugin_service.initial_connection_host_info, props, True)

        if not conn:
            raise AwsWrapperError(Messages.get("ConnectionWrapper.ConnectionNotOpen"))

        plugin_service.set_current_connection(conn, plugin_service.initial_connection_host_info)

        return AwsWrapperConnection(plugin_service, plugin_manager)

    def close(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.close",
                                     lambda: self.target_connection.close())

    def cursor(self, **kwargs: Union[None, int, str]) -> "AwsWrapperCursor":
        _cursor = self.target_connection.cursor(**kwargs)
        return AwsWrapperCursor(self, self._plugin_manager, _cursor)

    def commit(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.commit",
                                     lambda: self.target_connection.commit())

    def rollback(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.rollback",
                                     lambda: self.target_connection.rollback())

    def tpc_begin(self, xid: Any) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_begin",
                                     lambda: self.target_connection.tpc_begin(xid))

    def tpc_prepare(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_prepare",
                                     lambda: self.target_connection.tpc_prepare())

    def tpc_commit(self, xid: Any = None) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_commit",
                                     lambda: self.target_connection.tpc_commit(xid))

    def tpc_rollback(self, xid: Any = None) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_rollback",
                                     lambda: self.target_connection.tpc_rollback(xid))

    def tpc_recover(self) -> Any:
        return self._plugin_manager.execute(self.target_connection, "Connection.tpc_recover",
                                            lambda: self.target_connection.tpc_recover())

    def release_resources(self):
        self._plugin_manager.release_resources()
        if isinstance(self._plugin_service, CanReleaseResources):
            self._plugin_service.release_resources()

    def __del__(self):
        self.release_resources()

    def __enter__(self: "AwsWrapperConnection") -> "AwsWrapperConnection":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.close",
                                     lambda: self.target_connection.close())


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
    def target_cursor(self) -> Cursor:
        return self._target_cursor

    @property
    def description(self):
        return self.target_cursor.description

    @property
    def rowcount(self) -> int:
        return self.target_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self.target_cursor.arraysize

    def close(self) -> None:
        self._plugin_manager.execute(self.target_cursor, "Cursor.close",
                                     lambda: self.target_cursor.close())

    def callproc(self, **kwargs: Union[None, int, str]):
        return self._plugin_manager.execute(self.target_cursor, "Cursor.callproc",
                                            lambda: self.target_cursor.callproc(**kwargs))

    def execute(
            self,
            query: str,
            **kwargs: Union[None, int, str]
    ) -> "AwsWrapperCursor":
        try:
            return self._plugin_manager.execute(self.target_cursor, "Cursor.execute",
                                                lambda: self.target_cursor.execute(query, **kwargs), query, kwargs)
        except FailoverSuccessError as e:
            self._target_cursor = self.connection.target_connection.cursor()
            raise e

    def executemany(
            self,
            query: str,
            **kwargs: Union[None, int, str]
    ) -> None:
        self._plugin_manager.execute(self.target_cursor, "Cursor.executemany",
                                     lambda: self.target_cursor.executemany(query, **kwargs))

    def nextset(self) -> bool:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.nextset",
                                            lambda: self.target_cursor.nextset())

    def fetchone(self) -> Any:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.fetchone",
                                            lambda: self.target_cursor.fetchone())

    def fetchmany(self, size: int = 0) -> List[Any]:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.fetchmany",
                                            lambda: self.target_cursor.fetchmany(size))

    def fetchall(self) -> List[Any]:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.fetchall",
                                            lambda: self.target_cursor.fetchall())

    def __iter__(self) -> Iterator[Any]:
        return self.target_cursor.__iter__()

    def setinputsizes(self, sizes: Any) -> None:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.setinputsizes",
                                            lambda: self.target_cursor.setinputsizes(sizes))

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.setoutputsize",
                                            lambda: self.target_cursor.setoutputsize(size, column))

    def __enter__(self: "AwsWrapperCursor") -> "AwsWrapperCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
