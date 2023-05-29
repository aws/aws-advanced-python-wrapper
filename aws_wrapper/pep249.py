"""
pawswrapper -- PEP249 base classes

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
# mypy: ignore-errors

from typing import Any, Iterator, List, Optional, Sequence


class Warning(Exception):
    __module__ = "pawswrapper"


class Error(Exception):
    __module__ = "pawswrapper"

    sqlstate: str = None

    def __init__(
        self,
        *args: Sequence[Any]
    ):
        super().__init__(*args)


class InterfaceError(Error):
    __module__ = "pawswrapper"


class DatabaseError(Error):
    __module__ = "pawswrapper"


class DataError(DatabaseError):
    __module__ = "pawswrapper"


class OperationalError(DatabaseError):
    __module__ = "pawswrapper"


class IntegrityError(DatabaseError):
    __module__ = "pawswrapper"


class InternalError(DatabaseError):
    __module__ = "pawswrapper"


class ProgrammingError(DatabaseError):
    __module__ = "pawswrapper"


class NotSupportedError(DatabaseError):
    __module__ = "pawswrapper"


class ConnectionTimeout(OperationalError):
    ...


class PipelineAborted(OperationalError):
    ...


class Connection:

    __module__ = "pawswrapper"

    @staticmethod
    def connect(
        conninfo: str = "",
        **kwargs
    ) -> Any:
        ...

    def close(self) -> None:
        ...

    # TODO: check parameters
    def cursor(self, **kwargs) -> "Cursor":
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

    __module__ = "pawswrapper"

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

    def callproc(self, **kwargs):
        ...

    # TODO: check parameters
    def execute(
        self,
        query: str,
        **kwargs
    ) -> "Cursor":
        ...

    # TODO: check parameters
    def executemany(
        self,
        query: str,
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
