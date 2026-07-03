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

"""``AsyncDriverDialect`` -- the load-bearing driver-specific interface.

Per F3-B master spec invariant 8a: every async plugin, core service, and
connection class interacts with driver-specific behavior SOLELY through
this ABC. Plugin code MUST NOT ``import psycopg`` / ``aiomysql`` / ``asyncmy``
directly. Adding a new async driver is a matter of implementing this ABC
once, not modifying plugin code.

Concrete subclasses for 3.0.0: ``AsyncPsycopgDriverDialect`` (SP-2).
Future: ``AsyncAiomysqlDriverDialect``, ``AsyncAsyncmyDriverDialect``,
``AsyncMysqlConnectorAsyncDriverDialect``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Set

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo

from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)


class AsyncDriverDialect(ABC):
    """Async counterpart of :class:`DriverDialect`.

    Static capabilities and pure property accessors stay sync; every method
    that touches driver state or the network is ``async``. Per SP-0 decision
    D7, ``execute`` is NOT mirrored here -- per-query timeout lives in
    ``AsyncPluginService`` / ``AsyncAwsWrapperCursor`` via
    :func:`asyncio.wait_for`.
    """

    _dialect_code: str = DriverDialectCodes.GENERIC
    _network_bound_methods: Set[str] = {DbApiMethod.ALL.method_name}
    _driver_name: str = "Generic"

    # ---- Static capabilities / pure helpers (sync) --------------------

    @property
    def driver_name(self) -> str:
        return self._driver_name

    @property
    def dialect_code(self) -> str:
        return self._dialect_code

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._network_bound_methods

    def supports_connect_timeout(self) -> bool:
        return False

    def supports_socket_timeout(self) -> bool:
        return False

    def supports_tcp_keepalive(self) -> bool:
        return False

    def supports_abort_connection(self) -> bool:
        return False

    def is_dialect(self, connect_func: Callable) -> bool:
        """Introspect ``connect_func`` to decide if this dialect matches it."""
        return True

    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        """Copy ``props`` with host/port overridden from ``host_info`` and
        wrapper-internal keys stripped. Pure dict operation; no I/O."""
        prop_copy = Properties(props.copy())
        prop_copy["host"] = host_info.host
        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)
        PropertiesUtils.remove_wrapper_props(prop_copy)
        return prop_copy

    def set_password(self, props: Properties, pwd: str) -> None:
        WrapperProperties.PASSWORD.set(props, pwd)

    def get_connection_from_obj(self, obj: object) -> Any:
        if hasattr(obj, "connection"):
            return obj.connection
        return None

    def unwrap_connection(self, conn_obj: object) -> Any:
        return conn_obj

    # ---- Async abstract methods (subclasses MUST implement) -----------

    @abstractmethod
    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        """Open a new connection to ``host_info`` using the target async driver."""
        raise NotImplementedError

    async def execute_connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            prepared: Properties,
            props: Properties) -> Any:
        """Run an already-prepared driver connect, applying any driver-specific
        timeout bounding.

        Both connect paths funnel through here -- the dialect's own
        :meth:`connect` and :class:`AsyncDriverConnectionProvider`, which
        prepares props itself and must not bypass per-driver connect handling.

        Default: invoke the driver connect directly. Drivers whose connect
        kwarg doesn't bound the FULL connect (e.g. aiomysql's ``connect_timeout``
        bounds only the TCP connect, not the handshake/auth reads) override this
        to wrap the connect in :func:`asyncio.wait_for`. psycopg's
        ``connect_timeout`` already bounds the whole attempt, so it uses this
        default.
        """
        return await target_func(**prepared)

    @abstractmethod
    async def is_closed(self, conn: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def abort_connection(self, conn: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    async def is_in_transaction(self, conn: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def get_autocommit(self, conn: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    async def is_read_only(self, conn: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    async def can_execute_query(self, conn: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def transfer_session_state(self, from_conn: Any, to_conn: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    async def ping(self, conn: Any) -> bool:
        """Issue a lightweight probe to verify the connection is usable.

        Sync ``DriverDialect.ping`` runs ``SELECT 1`` with a 10s timeout via
        a thread pool. Async implementations run the same probe via
        :func:`asyncio.wait_for` and the driver's native async cursor.
        """
        raise NotImplementedError
