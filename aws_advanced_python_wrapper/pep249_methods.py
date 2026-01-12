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

from enum import Enum
from typing import Dict


class DbApiMethod(Enum):
    """Enumeration of database API methods with tuple-based attributes."""

    # Connection methods - Core PEP 249
    CONNECTION_CLOSE = (0, "Connection.close", False)
    CONNECTION_COMMIT = (1, "Connection.commit", True)
    CONNECTION_ROLLBACK = (2, "Connection.rollback", True)
    CONNECTION_CURSOR = (3, "Connection.cursor", False)

    # Connection methods - Optional PEP 249 extensions
    CONNECTION_TPC_BEGIN = (4, "Connection.tpc_begin", True)
    CONNECTION_TPC_PREPARE = (5, "Connection.tpc_prepare", True)
    CONNECTION_TPC_COMMIT = (6, "Connection.tpc_commit", True)
    CONNECTION_TPC_ROLLBACK = (7, "Connection.tpc_rollback", True)
    CONNECTION_TPC_RECOVER = (8, "Connection.tpc_recover", False)

    # Connection properties
    CONNECTION_AUTOCOMMIT = (9, "Connection.autocommit", False)
    CONNECTION_AUTOCOMMIT_SETTER = (10, "Connection.autocommit_setter", False)
    CONNECTION_IS_READ_ONLY = (11, "Connection.is_read_only", False)
    CONNECTION_SET_READ_ONLY = (12, "Connection.set_read_only", False)
    CONNECTION_IS_CLOSED = (13, "Connection.is_closed", False)

    # Cursor methods - Core PEP 249
    CURSOR_CLOSE = (14, "Cursor.close", False)
    CURSOR_EXECUTE = (15, "Cursor.execute", False)
    CURSOR_EXECUTEMANY = (16, "Cursor.executemany", False)
    CURSOR_FETCHONE = (17, "Cursor.fetchone", False)
    CURSOR_FETCHMANY = (18, "Cursor.fetchmany", False)
    CURSOR_FETCHALL = (19, "Cursor.fetchall", False)
    CURSOR_NEXTSET = (20, "Cursor.nextset", False)
    CURSOR_SETINPUTSIZES = (21, "Cursor.setinputsizes", False)
    CURSOR_SETOUTPUTSIZE = (22, "Cursor.setoutputsize", False)

    # Cursor methods - Optional extensions
    CURSOR_CALLPROC = (23, "Cursor.callproc", False)
    CURSOR_SCROLL = (24, "Cursor.scroll", False)
    CURSOR_COPY_FROM = (25, "Cursor.copy_from", False)
    CURSOR_COPY_TO = (26, "Cursor.copy_to", False)
    CURSOR_COPY_EXPERT = (27, "Cursor.copy_expert", False)

    # Cursor properties and attributes
    CURSOR_CONNECTION = (28, "Cursor.connection", False)
    CURSOR_ROWNUMBER = (29, "Cursor.rownumber", False)
    CURSOR_NEXT = (30, "Cursor.__next__", False)
    CURSOR_LASTROWID = (31, "Cursor.lastrowid", False)

    # AWS Advaced Python Wrapper Methods for
    CONNECT = (32, "connect", True)
    FORCE_CONNECT = (33, "force_connect", True)
    INIT_HOST_PROVIDER = (34, "init_host_provider", True)
    NOTIFY_CONNECTION_CHANGED = (35, "notify_connection_changed", True)
    NOTIFY_HOST_LIST_CHANGED = (36, "notify_host_list_changed", True)
    GET_HOST_INFO_BY_STRATEGY = (37, "get_host_info_by_strategy", True)
    ACCEPTS_STRATEGY = (38, "accepts_strategy", True)

    # Special marker for all methods
    ALL = (39, "*", False)

    def __init__(self, id: int, method_name: str, always_use_pipeline: bool):
        self.id = id
        self.method_name = method_name
        self.always_use_pipeline = always_use_pipeline


# Reverse lookup for method name to enum
_NAME_TO_METHOD: Dict[str, DbApiMethod] = {method.method_name: method for method in DbApiMethod}


def get_method_by_name(method_name: str) -> DbApiMethod:
    """Get DbApiMethod enum by method name string."""
    return _NAME_TO_METHOD.get(method_name, DbApiMethod.ALL)
