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

from typing import TYPE_CHECKING, Any, Callable, ClassVar, Set

if TYPE_CHECKING:
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection, Cursor

from abc import ABC
from concurrent.futures import Executor, ThreadPoolExecutor

from aws_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_wrapper.errors import UnsupportedOperationError
from aws_wrapper.utils.decorators import timeout
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import (Properties, PropertiesUtils,
                                          WrapperProperties)


class DriverDialect(ABC):
    _executor: ClassVar[Executor] = ThreadPoolExecutor()
    _dialect_code: str = DriverDialectCodes.GENERIC
    _network_bound_methods: Set[str] = {"*"}
    _read_only: bool = False
    _autocommit: bool = False
    _driver_name: str = "Generic"

    @property
    def driver_name(self):
        return self._driver_name

    @property
    def dialect_code(self) -> str:
        return self._dialect_code

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._network_bound_methods

    def is_read_only(self, conn: Connection) -> bool:
        return self._read_only

    def set_read_only(self, conn: Connection, read_only: bool):
        self._read_only = read_only

    def get_autocommit(self, conn: Connection) -> bool:
        return self._autocommit

    def set_autocommit(self, conn: Connection, autocommit: bool):
        self._autocommit = autocommit

    def supports_connect_timeout(self) -> bool:
        return False

    def supports_socket_timeout(self) -> bool:
        return False

    def supports_tcp_keepalive(self) -> bool:
        return False

    # Supports aborting connection from another thread
    def supports_abort_connection(self) -> bool:
        return False

    def can_execute_query(self, conn: Connection) -> bool:
        return True

    def set_password(self, props: Properties, pwd: str):
        WrapperProperties.PASSWORD.set(props, pwd)

    def is_dialect(self, connect_func: Callable) -> bool:
        return True

    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        prop_copy: Properties = Properties(props.copy())

        prop_copy["host"] = host_info.host

        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)

        PropertiesUtils.remove_wrapper_props(prop_copy)
        return prop_copy

    def is_closed(self, conn: Connection) -> bool:
        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "is_closed"))

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "DriverDialect.UnsupportedOperationError", self._driver_name, "abort_connection"))

    def is_in_transaction(self, conn: Connection) -> bool:
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "DriverDialect.UnsupportedOperationError", self._driver_name, "is_in_transaction"))

    def execute(self, conn: Connection, cursor: Cursor, query: str, *args: Any, **kwargs: Any) -> Cursor:
        socket_timeout = kwargs.get(WrapperProperties.SOCKET_TIMEOUT_SEC.name)
        if socket_timeout is not None:
            kwargs.pop(WrapperProperties.SOCKET_TIMEOUT_SEC.name)
            execute_with_timeout = timeout(DriverDialect._executor, socket_timeout)(
                lambda: cursor.execute(query, *args, **kwargs))
            return execute_with_timeout()
        else:
            return cursor.execute(query, *args, **kwargs)

    def get_connection_from_obj(self, obj: object) -> Any:
        if hasattr(obj, "connection"):
            return obj.connection

        return None

    def unwrap_connection(self, conn_obj: object) -> Any:
        return conn_obj

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        return
