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

from typing import TYPE_CHECKING, Any

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.utils.properties import Properties

from sqlalchemy import PoolProxiedConnection


class SqlAlchemyDriverDialect(DriverDialect):
    _driver_name: str = "SQLAlchemy"
    TARGET_DRIVER_CODE: str = "sqlalchemy"

    def __init__(self, underlying_driver: DriverDialect, props: Properties):
        super().__init__(props)
        self._underlying_driver = underlying_driver

    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        return self._underlying_driver.prepare_connect_info(host_info, props)

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                return False

        return self._underlying_driver.get_autocommit(conn)

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                raise AwsWrapperError(Messages.get_formatted("SqlAlchemyDriverDialect.SetValueOnNoneConnection", "autocommit"))

        return self._underlying_driver.set_autocommit(conn, autocommit)

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                return True

        return self._underlying_driver.is_closed(conn)

    def abort_connection(self, conn: Connection):
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                return

        return self._underlying_driver.abort_connection(conn)

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                return False

        return self._underlying_driver.is_in_transaction(conn)

    def is_read_only(self, conn: Connection) -> bool:
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                return False

        return self._underlying_driver.is_read_only(conn)

    def set_read_only(self, conn: Connection, read_only: bool):
        if isinstance(conn, PoolProxiedConnection):
            conn = conn.driver_connection
            if conn is None:
                raise AwsWrapperError(
                    Messages.get_formatted("SqlAlchemyDriverDialect.SetValueOnNoneConnection", "read_only"))

        return self._underlying_driver.set_read_only(conn, read_only)

    def get_connection_from_obj(self, obj: object) -> Any:
        if isinstance(obj, PoolProxiedConnection):
            obj = obj.driver_connection
            if obj is None:
                return None

        return self._underlying_driver.get_connection_from_obj(obj)

    def unwrap_connection(self, conn_obj: object) -> Any:
        if isinstance(conn_obj, PoolProxiedConnection):
            return conn_obj.driver_connection

        return conn_obj

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        from_driver_conn = from_conn
        to_driver_conn = to_conn

        # Check if the given connections are pooled connections that need to be unwrapped.
        if isinstance(from_conn, PoolProxiedConnection):
            from_driver_conn = from_conn.driver_connection

        if isinstance(to_conn, PoolProxiedConnection):
            to_driver_conn = to_conn.driver_connection

        if from_driver_conn is None or to_driver_conn is None:
            return

        return self._underlying_driver.transfer_session_state(from_driver_conn, to_driver_conn)
