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

# mypy: ignore-errors

"""
aws_advanced_python_wrapper -- PEP249 base classes

    Exceptions
    |__Warning
    |__Error
       |__InterfaceError
       |__DatabaseError
          |__DataError
          |__OperationalError
          |__IntegrityError
          |__InternalError
          |__ProgrammingError
          |__NotSupportedError

"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

from typing import Any, Iterator, List, Optional, Type, TypeVar


class Warning(Exception):
    __module__ = "aws_advanced_python_wrapper"


class Error(Exception):
    __module__ = "aws_advanced_python_wrapper"


class InterfaceError(Error):
    __module__ = "aws_advanced_python_wrapper"


class DatabaseError(Error):
    __module__ = "aws_advanced_python_wrapper"


class DataError(DatabaseError):
    __module__ = "aws_advanced_python_wrapper"


class OperationalError(DatabaseError):
    __module__ = "aws_advanced_python_wrapper"


class IntegrityError(DatabaseError):
    __module__ = "aws_advanced_python_wrapper"


class InternalError(DatabaseError):
    __module__ = "aws_advanced_python_wrapper"


class ProgrammingError(DatabaseError):
    __module__ = "aws_advanced_python_wrapper"


class NotSupportedError(DatabaseError):
    __module__ = "aws_advanced_python_wrapper"


class ConnectionTimeout(OperationalError):
    ...


class PipelineAborted(OperationalError):
    ...


class Connection:
    __module__ = "aws_advanced_python_wrapper"

    @staticmethod
    def connect(
            *args,
            **kwargs
    ) -> Connection:
        ...

    def close(self) -> None:
        ...

    def cursor(self, *args, **kwargs) -> Cursor:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def tpc_begin(self, xid: Any) -> None:
        ...

    def tpc_prepare(self) -> None:
        ...

    def tpc_commit(self, xid: Any = None) -> None:
        ...

    def tpc_rollback(self, xid: Any = None) -> None:
        ...

    def tpc_recover(self) -> Any:
        ...


class Cursor:
    __module__ = "aws_advanced_python_wrapper"
    _Self = TypeVar("_Self", bound="Cursor[Any]")

    def __enter__(self: _Self) -> _Self:
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    @property
    def description(self):
        ...

    @property
    def rowcount(self) -> int:
        ...

    @property
    def arraysize(self) -> int:
        ...

    def close(self) -> None:
        ...

    def callproc(self, *args, **kwargs):
        ...

    def execute(
            self,
            *args,
            **kwargs
    ) -> Cursor:
        ...

    def executemany(
            self,
            *args,
            **kwargs
    ) -> None:
        ...

    def nextset(self) -> bool:
        ...

    def fetchone(self) -> Any:
        ...

    def fetchmany(self, size: int = 0) -> List[Any]:
        ...

    def fetchall(self) -> List[Any]:
        ...

    def __iter__(self) -> Iterator[Any]:
        ...

    def setinputsizes(self, sizes: Any) -> None:
        ...

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        ...
