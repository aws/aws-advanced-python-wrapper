#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#  express or implied. See the License for the specific language governing
#  permissions and limitations under the License.

from __future__ import annotations

from threading import RLock
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class ThreadSafeConnectionHolder:
    """
    Thread-safe connection container that ensures connections are properly
    closed when replaced or cleared. This class prevents race conditions where
    one thread might close a connection while another thread is using it.
    """

    def __init__(self, initial_connection: Optional[Connection] = None):
        self._connection: Optional[Connection] = initial_connection
        self._lock: RLock = RLock()

    def get(self) -> Optional[Connection]:
        with self._lock:
            return self._connection

    def set(self, new_connection: Optional[Connection], close_previous: bool = True) -> None:
        with self._lock:
            old_connection = self._connection
            self._connection = new_connection

            if close_previous and old_connection is not None and old_connection != new_connection:
                self._close_connection(old_connection)

    def get_and_set(self, new_connection: Optional[Connection], close_previous: bool = True) -> Optional[Connection]:
        with self._lock:
            old_connection = self._connection
            self._connection = new_connection

            if close_previous and old_connection is not None and old_connection != new_connection:
                self._close_connection(old_connection)

            return old_connection

    def compare_and_set(
        self,
        expected_connection: Optional[Connection],
        new_connection: Optional[Connection],
        close_previous: bool = True
    ) -> bool:
        with self._lock:
            if self._connection == expected_connection:
                old_connection = self._connection
                self._connection = new_connection

                if close_previous and old_connection is not None and old_connection != new_connection:
                    self._close_connection(old_connection)

                return True
            return False

    def clear(self) -> None:
        self.set(None, close_previous=True)

    def use_connection(self, func, *args, **kwargs):
        """
        Safely use the connection within a locked context.

        This method ensures the connection cannot be closed by another thread
        while the provided function is executing.

        :param func: Function to call with the connection as the first argument.
        :param args: Additional positional arguments to pass to func.
        :param kwargs: Additional keyword arguments to pass to func.
        :return: The result of calling func, or None if no connection is available.

        Example:
            result = holder.use_connection(lambda conn: conn.cursor().execute("SELECT 1"))
        """
        with self._lock:
            if self._connection is None:
                return None
            return func(self._connection, *args, **kwargs)

    def _close_connection(self, connection: Connection) -> None:
        try:
            if connection is not None:
                connection.close()
        except Exception:  # ignore
            pass

    def __repr__(self) -> str:
        with self._lock:
            return f"ThreadSafeConnectionHolder(connection={self._connection})"
