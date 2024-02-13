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

from typing import (TYPE_CHECKING, Any, Callable, Iterator, List, Optional,
                    Union)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService

from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverSuccessError)
from aws_advanced_python_wrapper.pep249 import Connection, Cursor, Error
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginService, PluginServiceImpl,
    PluginServiceManagerContainer)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils)
from aws_advanced_python_wrapper.utils.telemetry.default_telemetry_factory import \
    DefaultTelemetryFactory
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

logger = Logger(__name__)


class AwsWrapperConnection(Connection, CanReleaseResources):
    __module__ = "aws_advanced_python_wrapper"

    def __init__(
            self,
            target_func: Callable,
            host_list_provider_service: HostListProviderService,
            plugin_service: PluginService,
            plugin_manager: PluginManager):
        self._plugin_service = plugin_service
        self._plugin_manager = plugin_manager

        host_list_provider_init = plugin_service.database_dialect.get_host_list_provider_supplier()
        plugin_service.host_list_provider = host_list_provider_init(host_list_provider_service, plugin_service.props)

        plugin_manager.init_host_provider(plugin_service.props, host_list_provider_service)
        plugin_service.refresh_host_list()

        if plugin_service.current_connection is not None:
            return

        if plugin_service.initial_connection_host_info is None:
            raise AwsWrapperError(Messages.get("AwsWrapperConnection.InitialHostInfoNone"))

        conn = plugin_manager.connect(
            target_func,
            plugin_service.driver_dialect,
            plugin_service.initial_connection_host_info,
            plugin_service.props,
            True)

        if not conn:
            raise AwsWrapperError(Messages.get("AwsWrapperConnection.ConnectionNotOpen"))

        plugin_service.set_current_connection(conn, plugin_service.initial_connection_host_info)

    @property
    def target_connection(self):
        return self._plugin_service.current_connection

    @property
    def is_closed(self):
        return self._plugin_service.driver_dialect.is_closed(self.target_connection)

    @property
    def read_only(self) -> bool:
        return self._plugin_manager.execute(
            self.target_connection,
            "Connection.is_read_only",
            lambda: self._is_read_only())

    @read_only.setter
    def read_only(self, val: bool):
        self._plugin_manager.execute(
            self.target_connection,
            "Connection.set_read_only",
            lambda: self._set_read_only(val),
            val)

    def _is_read_only(self) -> bool:
        is_read_only = self._plugin_service.driver_dialect.is_read_only(self.target_connection)
        self._plugin_service.session_state_service.setup_pristine_readonly(is_read_only)
        return is_read_only

    def _set_read_only(self, val: bool):
        self._plugin_service.session_state_service.setup_pristine_readonly(val)
        self._plugin_service.driver_dialect.set_read_only(self.target_connection, val)
        self._plugin_service.session_state_service.set_read_only(val)

    @property
    def autocommit(self):
        return self._plugin_manager.execute(
            self.target_connection,
            "Connection.autocommit",
            lambda: self._plugin_service.driver_dialect.get_autocommit(self.target_connection))

    @autocommit.setter
    def autocommit(self, val: bool):
        self._plugin_manager.execute(
            self.target_connection,
            "Connection.autocommit_setter",
            lambda: self._set_autocommit(val),
            val)

    def _get_autocommit(self) -> bool:
        autocommit = self._plugin_service.driver_dialect.get_autocommit(self.target_connection)
        self._plugin_service.session_state_service.setup_pristine_autocommit(autocommit)
        return autocommit

    def _set_autocommit(self, val: bool):
        self._plugin_service.session_state_service.setup_pristine_autocommit(val)
        self._plugin_service.driver_dialect.set_autocommit(self.target_connection, val)
        self._plugin_service.session_state_service.set_autocommit(val)

    @staticmethod
    def connect(
            target: Union[None, str, Callable] = None,
            conninfo: str = "",
            *args: Any,
            **kwargs: Any) -> AwsWrapperConnection:
        if not target:
            raise Error(Messages.get("Wrapper.RequiredTargetDriver"))

        if not callable(target):
            raise Error(Messages.get("Wrapper.ConnectMethod"))
        target_func: Callable = target

        props: Properties = PropertiesUtils.parse_properties(conn_info=conninfo, **kwargs)
        logger.debug("Wrapper.Properties", PropertiesUtils.log_properties(PropertiesUtils.mask_properties(props)))

        telemetry_factory = DefaultTelemetryFactory(props)
        context = telemetry_factory.open_telemetry_context(__name__, TelemetryTraceLevel.TOP_LEVEL)

        try:
            driver_dialect_manager: DriverDialectManager = DriverDialectManager()
            driver_dialect = driver_dialect_manager.get_dialect(target_func, props)
            container: PluginServiceManagerContainer = PluginServiceManagerContainer()
            plugin_service = PluginServiceImpl(
                container, props, target_func, driver_dialect_manager, driver_dialect)
            plugin_manager: PluginManager = PluginManager(container, props, telemetry_factory)

            return AwsWrapperConnection(target_func, plugin_service, plugin_service, plugin_manager)
        except Exception as ex:
            context.set_exception(ex)
            context.set_success(False)
            raise ex
        finally:
            context.close_context()

    def close(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.close",
                                     lambda: self.target_connection.close())

    def cursor(self, *args: Any, **kwargs: Any) -> AwsWrapperCursor:
        _cursor = self._plugin_manager.execute(self.target_connection, "Connection.cursor",
                                               lambda: self.target_connection.cursor(*args, **kwargs),
                                               *args, **kwargs)
        return AwsWrapperCursor(self, self._plugin_service, self._plugin_manager, _cursor)

    def commit(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.commit",
                                     lambda: self.target_connection.commit())

    def rollback(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.rollback",
                                     lambda: self.target_connection.rollback())

    def tpc_begin(self, xid: Any) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_begin",
                                     lambda: self.target_connection.tpc_begin(xid), xid)

    def tpc_prepare(self) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_prepare",
                                     lambda: self.target_connection.tpc_prepare())

    def tpc_commit(self, xid: Any = None) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_commit",
                                     lambda: self.target_connection.tpc_commit(xid), xid)

    def tpc_rollback(self, xid: Any = None) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.tpc_rollback",
                                     lambda: self.target_connection.tpc_rollback(xid), xid)

    def tpc_recover(self) -> Any:
        return self._plugin_manager.execute(self.target_connection, "Connection.tpc_recover",
                                            lambda: self.target_connection.tpc_recover())

    def release_resources(self):
        self._plugin_manager.release_resources()
        if isinstance(self._plugin_service, CanReleaseResources):
            self._plugin_service.release_resources()

    def __del__(self):
        self.release_resources()

    def __enter__(self: AwsWrapperConnection) -> AwsWrapperConnection:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._plugin_manager.execute(self.target_connection, "Connection.close",
                                     lambda: self.target_connection.close(), exc_type, exc_val, exc_tb)


class AwsWrapperCursor(Cursor):
    __module__ = "aws_advanced_python_wrapper"

    def __init__(
            self,
            conn: AwsWrapperConnection,
            plugin_service: PluginService,
            plugin_manager: PluginManager,
            target_cursor: Cursor):
        self._conn: AwsWrapperConnection = conn
        self._plugin_service: PluginService = plugin_service
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

    def callproc(self, *args: Any, **kwargs: Any):
        return self._plugin_manager.execute(self.target_cursor, "Cursor.callproc",
                                            lambda: self.target_cursor.callproc(**kwargs), *args, **kwargs)

    def execute(
            self,
            *args: Any,
            **kwargs: Any
    ) -> AwsWrapperCursor:
        try:
            return self._plugin_manager.execute(self.target_cursor, "Cursor.execute",
                                                lambda: self.target_cursor.execute(*args, **kwargs), *args, **kwargs)
        except FailoverSuccessError as e:
            self._target_cursor = self.connection.target_connection.cursor()
            raise e

    def executemany(
            self,
            *args: Any,
            **kwargs: Any
    ) -> None:
        self._plugin_manager.execute(self.target_cursor, "Cursor.executemany",
                                     lambda: self.target_cursor.executemany(*args, **kwargs),
                                     *args, **kwargs)

    def nextset(self) -> bool:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.nextset",
                                            lambda: self.target_cursor.nextset())

    def fetchone(self) -> Any:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.fetchone",
                                            lambda: self.target_cursor.fetchone())

    def fetchmany(self, size: int = 0) -> List[Any]:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.fetchmany",
                                            lambda: self.target_cursor.fetchmany(size), size)

    def fetchall(self) -> List[Any]:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.fetchall",
                                            lambda: self.target_cursor.fetchall())

    def __iter__(self) -> Iterator[Any]:
        return self.target_cursor.__iter__()

    def setinputsizes(self, sizes: Any) -> None:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.setinputsizes",
                                            lambda: self.target_cursor.setinputsizes(sizes), sizes)

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        return self._plugin_manager.execute(self.target_cursor, "Cursor.setoutputsize",
                                            lambda: self.target_cursor.setoutputsize(size, column), size, column)

    def __enter__(self: AwsWrapperCursor) -> AwsWrapperCursor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
