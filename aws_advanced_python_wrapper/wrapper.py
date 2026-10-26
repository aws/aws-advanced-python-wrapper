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
                    Type, TypeVar, Union)

from aws_advanced_python_wrapper.plugin_service import (
    PluginManager, PluginServiceImpl, PluginServiceManagerContainer)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.plugin_service import PluginService

from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249 import Connection, Cursor, Error
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils)
from aws_advanced_python_wrapper.utils.telemetry.default_telemetry_factory import \
    DefaultTelemetryFactory
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

logger = Logger(__name__)

UnwrapType = TypeVar('UnwrapType')


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

        host_list_provider_init = plugin_service.database_dialect.get_host_list_provider_supplier(plugin_service)
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

    def __getattr__(self, name: str) -> Any:
        # Delegate unknown attributes to the live underlying driver connection so
        # driver-specific extension methods that aren't on the wrapper's PEP-249
        # surface (e.g. psycopg's add_notice_handler, called by SQLAlchemy) work
        # transparently -- including single-underscore driver attributes, since
        # SQLAlchemy's psycopg adapter reaches for names like _close. __getattr__
        # runs only on a normal-lookup miss, so it never shadows the wrapper's own
        # API. Only dunders are kept on the wrapper (pickle/copy internals), and
        # _plugin_service is guarded by name -- it is the recursion-critical field
        # this method dereferences (via target_connection), so a miss before it is
        # set raises cleanly instead of recursing through this method.
        if name == "_plugin_service" or name.startswith("__"):
            raise AttributeError(name)
        return getattr(self.target_connection, name)

    @property
    def is_closed(self):
        return self._plugin_service.driver_dialect.is_closed(self.target_connection)

    @property
    def read_only(self) -> bool:
        return self._plugin_manager.execute(
            self.target_connection,
            DbApiMethod.CONNECTION_IS_READ_ONLY,
            lambda: self._is_read_only())

    @read_only.setter
    def read_only(self, val: bool):
        self._plugin_manager.execute(
            self.target_connection,
            DbApiMethod.CONNECTION_SET_READ_ONLY,
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
            DbApiMethod.CONNECTION_AUTOCOMMIT,
            lambda: self._plugin_service.driver_dialect.get_autocommit(self.target_connection))

    @autocommit.setter
    def autocommit(self, val: bool):
        self._plugin_manager.execute(
            self.target_connection,
            DbApiMethod.CONNECTION_AUTOCOMMIT_SETTER,
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

            container = PluginServiceManagerContainer()
            plugin_service = PluginServiceImpl(container, props, target_func, driver_dialect_manager, driver_dialect)
            plugin_manager: PluginManager = PluginManager(container, props, telemetry_factory)

            return AwsWrapperConnection(
                target_func,
                plugin_service,
                plugin_service,
                plugin_manager)
        except Exception as ex:
            if context is not None:
                context.set_exception(ex)
                context.set_success(False)
            raise ex
        finally:
            if context is not None:
                context.close_context()

    def close(self) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_CLOSE,
                                     lambda: self.target_connection.close())
        self.release_resources()

    def invalidate(self) -> None:
        """Mark this connection as invalid and evict it from any owning pool.

        For pool-proxied targets (e.g. SQLAlchemy's ``_ConnectionFairy``),
        calls the underlying ``.invalidate()`` which closes the driver
        connection AND removes it from the pool's tracking, so the next
        pool checkout creates a fresh connection. For raw driver
        connections (no ``invalidate`` method), falls back to ``close()``.

        Use ``invalidate()`` when the connection is known to be in a
        broken state (e.g. after a failover-eligible exception); use
        ``close()`` when the connection was used normally and you intend
        the pool to keep the underlying socket for reuse.
        """
        inv = getattr(self.target_connection, "invalidate", None)
        if callable(inv):
            try:
                inv()
                self.release_resources()
                return
            except Exception:  # noqa: BLE001 - fall through to close
                pass
        self.close()

    def cursor(self, *args: Any, **kwargs: Any) -> AwsWrapperCursor:
        _cursor = self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_CURSOR,
                                               lambda: self.target_connection.cursor(*args, **kwargs),
                                               *args, **kwargs)
        return AwsWrapperCursor(self, self._plugin_service, self._plugin_manager, _cursor)

    def commit(self) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_COMMIT,
                                     lambda: self.target_connection.commit())

    def rollback(self) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_ROLLBACK,
                                     lambda: self.target_connection.rollback())

    def tpc_begin(self, xid: Any) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_TPC_BEGIN,
                                     lambda: self.target_connection.tpc_begin(xid), xid)

    def tpc_prepare(self) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_TPC_PREPARE,
                                     lambda: self.target_connection.tpc_prepare())

    def tpc_commit(self, xid: Any = None) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_TPC_COMMIT,
                                     lambda: self.target_connection.tpc_commit(xid), xid)

    def tpc_rollback(self, xid: Any = None) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_TPC_ROLLBACK,
                                     lambda: self.target_connection.tpc_rollback(xid), xid)

    def tpc_recover(self) -> Any:
        return self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_TPC_RECOVER,
                                            lambda: self.target_connection.tpc_recover())

    # ---- psycopg3-parity passthroughs that need explicit handling ------
    #
    # Driver-specific attributes (add_notice_handler, info, broken, cancel,
    # ...) are forwarded automatically by __getattr__. The members below stay
    # explicit because __getattr__ can't express them: they route through the
    # plugin chain, have setters, or implement a SQLAlchemy adapter contract
    # rather than a plain driver forward.

    @property
    def closed(self) -> bool:
        """Whether the underlying driver connection is closed.

        psycopg3 exposes ``closed`` as a sync bool property; SA's psycopg
        dialect reads it during pool-invalidation paths
        (``sqlalchemy/dialects/postgresql/psycopg.py:595``) after a DBAPI
        exception is classified. The wrapper has the plugin-aware sync
        ``is_closed`` property, but SA's dialect probes the unprefixed
        ``closed`` attribute directly. Without this passthrough, SA's
        post-exception cleanup raises AttributeError on our proxy.
        """
        return bool(getattr(self.target_connection, "closed", False))

    @property
    def prepare_threshold(self) -> Any:
        return self.target_connection.prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: Any) -> None:
        self.target_connection.prepare_threshold = value

    @property
    def prepared_max(self) -> Any:
        return self.target_connection.prepared_max

    @prepared_max.setter
    def prepared_max(self, value: Any) -> None:
        self.target_connection.prepared_max = value

    def set_read_only(self, value: Any) -> None:
        # Route through the existing plugin-aware setter so read_only
        # changes keep their intended plugin-chain semantics.
        self.read_only = value

    def set_autocommit(self, value: Any) -> None:
        # Same plugin-chain routing as read_only.
        self.autocommit = value

    def execute(
            self,
            query: Any,
            params: Any = None,
            *,
            prepare: Any = None,
            binary: bool = False) -> Any:
        # psycopg3's Connection.execute() is a shortcut that opens a cursor
        # and runs the query in one call. Route via our cursor so the query
        # goes through the plugin chain (failover, RWS, etc.) instead of
        # bypassing it -- otherwise SQLAlchemy and other psycopg-aware
        # callers can issue SQL that's invisible to plugins.
        cursor = self.cursor()
        cursor.execute(query, params, prepare=prepare, binary=binary)
        return cursor

    def release_resources(self):
        self._plugin_manager.release_resources()
        if isinstance(self._plugin_service, CanReleaseResources):
            self._plugin_service.release_resources()

    # For testing purposes only
    def _unwrap(self, unwrap_class: Type[UnwrapType]) -> Optional[UnwrapType]:
        return self._plugin_manager._unwrap(unwrap_class)

    def __enter__(self: AwsWrapperConnection) -> AwsWrapperConnection:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._plugin_manager.execute(self.target_connection, DbApiMethod.CONNECTION_CLOSE,
                                     lambda: self.target_connection.close(), exc_type, exc_val, exc_tb)
        self.release_resources()


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

    def __getattr__(self, name: str) -> Any:
        # See AwsWrapperConnection.__getattr__. Delegate unknown attributes to the
        # underlying driver cursor (e.g. psycopg's statusmessage, and single-
        # underscore driver methods like _close that SQLAlchemy's adapter calls).
        # Only dunders stay on the wrapper, and _target_cursor is guarded by name
        # so a miss before __init__ sets it raises instead of recursing.
        if name == "_target_cursor" or name.startswith("__"):
            raise AttributeError(name)
        return getattr(self.target_cursor, name)

    @property
    def description(self):
        return self.target_cursor.description

    @property
    def rowcount(self) -> int:
        return self.target_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self.target_cursor.arraysize

    # Optional for PEP249
    @property
    def lastrowid(self) -> int:
        return self.target_cursor.lastrowid  # type: ignore[attr-defined]

    def close(self) -> None:
        self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_CLOSE,
                                     lambda: self.target_cursor.close())

    def callproc(self, *args: Any, **kwargs: Any):
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_CALLPROC,
                                            lambda: self.target_cursor.callproc(**kwargs), *args, **kwargs)

    def execute(
            self,
            *args: Any,
            **kwargs: Any
    ) -> AwsWrapperCursor:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_EXECUTE,
                                            lambda: self.target_cursor.execute(*args, **kwargs), *args, **kwargs)

    def executemany(
            self,
            *args: Any,
            **kwargs: Any
    ) -> None:
        self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_EXECUTEMANY,
                                     lambda: self.target_cursor.executemany(*args, **kwargs),
                                     *args, **kwargs)

    def nextset(self) -> bool:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_NEXTSET,
                                            lambda: self.target_cursor.nextset())

    def fetchone(self) -> Any:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_FETCHONE,
                                            lambda: self.target_cursor.fetchone())

    def fetchmany(self, size: int = 0) -> List[Any]:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_FETCHMANY,
                                            lambda: self.target_cursor.fetchmany(size), size)

    def fetchall(self) -> List[Any]:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_FETCHALL,
                                            lambda: self.target_cursor.fetchall())

    def __iter__(self) -> Iterator[Any]:
        return self.target_cursor.__iter__()

    def setinputsizes(self, sizes: Any) -> None:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_SETINPUTSIZES,
                                            lambda: self.target_cursor.setinputsizes(sizes), sizes)

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        return self._plugin_manager.execute(self.target_cursor, DbApiMethod.CURSOR_SETOUTPUTSIZE,
                                            lambda: self.target_cursor.setoutputsize(size, column), size, column)

    def __enter__(self: AwsWrapperCursor) -> AwsWrapperCursor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
