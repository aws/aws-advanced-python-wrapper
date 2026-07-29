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

"""Async PostgreSQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Reached via the sync dialect's ``get_async_dialect_cls`` hook, not a distinct
URL: ``create_async_engine("postgresql+aws_wrapper_psycopg://...")`` resolves
to this class (psycopg3 is a single DBAPI that does both sync and async, so
one URL serves both -- mirrors stock ``postgresql+psycopg``). Subclasses SA's
standard ``PGDialectAsync_psycopg`` and swaps the DBAPI to an adapter that
routes ``connect()`` through the async plugin pipeline while preserving SA's
``AsyncAdapt_psycopg_connection`` greenlet-bridge wrapper that the async
engine expects.

Example::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "postgresql+aws_wrapper_psycopg://user:pwd@"
        "database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,host_monitoring_v2"
    )
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql.psycopg import (
    AsyncAdapt_psycopg_connection, AsyncAdaptFallback_psycopg_connection,
    PGDialectAsync_psycopg)
from sqlalchemy.util import asbool
from sqlalchemy.util.concurrency import await_fallback, await_only

from aws_advanced_python_wrapper.pep249 import \
    OperationalError as _PEP249OperationalError
from aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling import \
    _AsyncFailoverSuccessRewrapMixin


class AwsWrapperAsyncPsycopgAdaptDBAPI:
    """DBAPI adapter that bridges our async ``aio.psycopg`` submodule into
    SA's ``PGDialectAsync_psycopg`` flow.

    Mirrors SA's own ``PsycopgAdaptDBAPI`` but wraps the WRAPPER's async
    submodule rather than ``psycopg`` itself. The adapter's ``connect``
    calls ``aio.psycopg.connect(...)`` (which awaits through the wrapper's
    plugin pipeline) and wraps the resulting connection in
    ``AsyncAdapt_psycopg_connection`` so the engine's greenlet bridge can
    expose it via a sync-looking surface.
    """

    def __init__(self) -> None:
        # Import lazily -- avoids a circular import during pyproject
        # entry-point loading in some environments.
        import aws_advanced_python_wrapper.aio.psycopg as aio_psycopg
        self._aio_psycopg = aio_psycopg
        # Copy the PEP 249 surface onto self (apilevel, paramstyle, Error,
        # Date, STRING, etc.) except the connect callable (handled below).
        for name, value in aio_psycopg.__dict__.items():
            if name == "connect":
                continue
            self.__dict__[name] = value

    @property
    def psycopg(self) -> Any:
        """Back-compat attribute: SA's adapter exposes ``.psycopg`` pointing
        at the wrapped module. We point it at the aio submodule."""
        return self._aio_psycopg

    def __getattr__(self, name: str) -> Any:
        """Forward missing attributes to the wrapped aio psycopg module.

        Copies the PEP 562 forwarding trick from ``aio.psycopg`` up one
        layer: SA's ``PGDialectAsync_psycopg.__init__`` probes the DBAPI
        for ``__version__``, ``adapters``, ``pq``, etc. ``aio.psycopg``
        itself forwards those to the real :mod:`psycopg` via its own
        ``__getattr__``; we forward ours to ``aio.psycopg``.
        """
        return getattr(self._aio_psycopg, name)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_psycopg_connection:
        """Open an async connection through the wrapper and hand SA the
        ``AsyncAdapt_psycopg_connection`` it expects."""
        async_fallback = kwargs.pop("async_fallback", False)
        # SA sometimes plumbs an ``async_creator_fn`` down from user code to
        # let the caller provide the raw awaitable. Our wrapper IS the
        # creator, so discard anything that came in under that key.
        kwargs.pop("async_creator_fn", None)

        # aio.psycopg.connect is async; returns an awaitable that resolves
        # to an AsyncAwsWrapperConnection.
        coro = self._aio_psycopg.connect(*args, **kwargs)

        if asbool(async_fallback):
            return AsyncAdaptFallback_psycopg_connection(await_fallback(coro))
        return AsyncAdapt_psycopg_connection(await_only(coro))


class AwsWrapperPGPsycopgAsyncDialect(
        _AsyncFailoverSuccessRewrapMixin, PGDialectAsync_psycopg):
    """Async SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI.

    Wrapper-specific override pattern
    ---------------------------------
    The wrapper interposes on DBAPI-level calls (connect / execute /
    commit etc.) -- everything SA drives through the DBAPI connection
    contract passes through our plugin pipeline.

    SQLAlchemy's psycopg dialect, however, also calls into psycopg
    internals directly, bypassing the DBAPI connection: it passes a
    ``driver_connection`` into ``psycopg.types.TypeInfo.fetch`` and
    similar helpers. Those helpers ``isinstance``-check their argument
    against the real ``psycopg.Connection`` / ``psycopg.AsyncConnection``
    classes -- our proxy is NOT a subclass, so they raise TypeError.

    Wherever SA's dialect reaches the raw driver connection, we override
    the method here to unwrap to the native psycopg connection via
    ``AsyncAwsWrapperConnection.target_connection`` before handing it
    to psycopg. Current overrides: ``_type_info_fetch``.
    """

    # Same driver name as the sync dialect: this class is reached via the
    # sync dialect's ``get_async_dialect_cls`` (not a distinct URL), mirroring
    # stock psycopg where both sync and async report ``driver = "psycopg"``.
    driver = "aws_wrapper_psycopg"
    supports_statement_cache = True

    # See _AsyncFailoverSuccessRewrapMixin / sqlalchemy_dialects/pg.py.
    # ``dialect.dbapi.OperationalError`` resolves to the wrapper's PEP-249
    # ``OperationalError`` via the shim's ``_dbapi.install`` — rewrap
    # target must be that class for SA's classifier to wrap us to
    # ``sqlalchemy.exc.OperationalError``.
    _failover_success_target_cls = _PEP249OperationalError
    is_async = True

    def _driver_error_module(self):
        # psycopg (async) exposes PEP-249 error classes at top level; lets
        # _normalize_driver_error translate a raw psycopg error into the
        # wrapper's PEP-249 type so SA classifies it (see _exception_handling).
        import psycopg
        return psycopg

    def is_disconnect(self, e, connection, cursor):
        # Mirror sync pg.py / mysql.py for explicit symmetry across all
        # 4 dialects:
        #   - FailoverSuccessError → False (wrapper's target_connection is
        #     auto-rebound to the new writer via plugin_service; SA pool
        #     slot is still valid).
        #   - FailoverFailedError → True (no usable connection).
        # Complements _AsyncFailoverSuccessRewrapMixin for the
        # cursor-creation path that runs before do_execute.
        from aws_advanced_python_wrapper.errors import (FailoverError,
                                                        FailoverFailedError)

        # Catch the whole FailoverError family -- including
        # TransactionResolutionUnknownError -- before upstream probes
        # attributes the wrapper errors don't carry. Only FailoverFailedError
        # means no usable connection (-> True, SA invalidates);
        # FailoverSuccessError and TransactionResolutionUnknownError both mean
        # the wrapper reconnected to a new writer (-> False).
        if isinstance(e, FailoverError):
            return isinstance(e, FailoverFailedError)
        return super().is_disconnect(e, connection, cursor)

    def _type_info_fetch(self, connection: Any, name: str) -> Any:
        """Unwrap to native psycopg.AsyncConnection before TypeInfo.fetch.

        SA native (``sqlalchemy/dialects/postgresql/psycopg.py:838``):
            adapted = connection.connection
            return adapted.await_(TypeInfo.fetch(adapted.driver_connection, name))

        ``adapted.driver_connection`` in our setup is the
        :class:`AsyncAwsWrapperConnection` proxy, which
        ``psycopg.TypeInfo.fetch`` rejects with
        ``TypeError: expected Connection or AsyncConnection, got ...``
        because we don't subclass ``psycopg.AsyncConnection``. Reach
        the underlying native via ``target_connection`` (exposed on
        our wrapper at ``aio/wrapper.py:326``). ``TypeInfo.fetch`` only
        reads catalog rows, so bypassing the plugin pipeline here is
        semantically safe -- there's no DB-side state the plugin chain
        would need to intercept.
        """
        from psycopg.types import TypeInfo
        adapted = connection.connection
        wrapper = adapted.driver_connection
        native = getattr(wrapper, "target_connection", wrapper)
        return adapted.await_(TypeInfo.fetch(native, name))

    @classmethod
    def import_dbapi(cls) -> AwsWrapperAsyncPsycopgAdaptDBAPI:
        # Mirror PGDialectAsync_psycopg.import_dbapi's side effect:
        # SA's AsyncAdapt_psycopg_cursor.execute reads
        # self._psycopg_ExecStatus.TUPLES_OK
        # (sqlalchemy/dialects/postgresql/psycopg.py:679). The class-
        # level attribute defaults to None; SA's native import_dbapi
        # sets it during engine init. Our override replaced the parent
        # import_dbapi wholesale and skipped the assignment, so
        # cursor.execute() crashed with
        # "'NoneType' object has no attribute 'TUPLES_OK'" on first
        # use. Explicit mirror of the side effect here (not
        # super().import_dbapi()) -- avoids pulling in the parent's
        # PsycopgAdaptDBAPI construction we don't need.
        from psycopg.pq import ExecStatus
        from sqlalchemy.dialects.postgresql.psycopg import \
            AsyncAdapt_psycopg_cursor

        # SA types this class attribute as ``None`` (its default value);
        # narrow the ignore to the exact code rather than using a bare
        # ``# type: ignore`` so unrelated errors on this line would
        # still surface.
        AsyncAdapt_psycopg_cursor._psycopg_ExecStatus = ExecStatus  # type: ignore[assignment]
        return AwsWrapperAsyncPsycopgAdaptDBAPI()

    @classmethod
    def get_dialect_cls(cls, url):
        return cls

    @classmethod
    def get_async_dialect_cls(cls, url):
        # The grandparent `PGDialect_psycopg` hard-codes this to return
        # `PGDialectAsync_psycopg`, which would cause `create_async_engine`
        # to swap our subclass out for the stock SA class. Override to
        # return ourselves so URL-based dialect selection actually uses
        # the wrapper.
        return cls

    def create_connect_args(self, url):
        # SQLAlchemy's ``create_engine`` / ``create_async_engine`` reserves
        # ``plugins=`` in the URL query for its own engine-plugin loader.
        # Allow users to spell the wrapper's ``plugins`` connection property
        # as ``wrapper_plugins=`` and rename it back before the DBAPI call.
        # See F2's sync counterpart in ``pg.py`` for the rationale.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs

    def do_ping(self, dbapi_connection) -> bool:
        # Support SQLAlchemy ``pool_pre_ping`` for async PG. psycopg3's
        # AsyncConnection has no ping(); run a lightweight ``SELECT 1``.
        # dbapi_connection is SA's AsyncAdapt_psycopg_connection; reach the
        # native AsyncConnection via ``driver_connection`` ->
        # ``wrapper.target_connection`` and await it through the adapter's
        # ``await_`` greenlet bridge. A failure -> return False so SA's pool
        # recycles the connection. Adopts AWS PR #1245 for the async PG
        # dialect (AWS ships sync MySQL only).
        adapted = dbapi_connection
        wrapper = getattr(adapted, "driver_connection", adapted)
        native = getattr(wrapper, "target_connection", wrapper)
        try:
            adapted.await_(native.execute("SELECT 1"))
            return True
        except Exception:
            return False
