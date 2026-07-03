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

"""Async MySQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``mysql.aws_wrapper_aiomysql`` via a pyproject entry-point
(URL ``mysql+aws_wrapper_aiomysql://``). aiomysql is an async-only DBAPI, so
unlike PG this needs a distinct driver name from the sync
``mysql+aws_wrapper_mysqlconnector`` (the two are different DBAPIs and cannot
share one URL). Subclasses SA's standard ``MySQLDialect_aiomysql`` and swaps
the DBAPI to an adapter that routes ``connect()`` through the async plugin
pipeline while preserving SA's ``AsyncAdapt_aiomysql_connection``
greenlet-bridge wrapper that the async engine expects.

Example::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "mysql+aws_wrapper_aiomysql://user:pwd@"
        "database.cluster-xyz.us-east-1.rds.amazonaws.com:3306/db"
        "?wrapper_dialect=aurora-mysql&wrapper_plugins=failover"
    )
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import util
from sqlalchemy.dialects.mysql.aiomysql import (AsyncAdapt_aiomysql_connection,
                                                MySQLDialect_aiomysql)
from sqlalchemy.engine.characteristics import ConnectionCharacteristic
from sqlalchemy.util.concurrency import await_only

from aws_advanced_python_wrapper.pep249 import \
    OperationalError as _PEP249OperationalError
from aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling import \
    _AsyncFailoverSuccessRewrapMixin


def _unwrap_wrapper_conn(dbapi_conn: Any) -> Any:
    """Reach the AsyncAwsWrapperConnection behind SA's adapter.

    The async dialect wraps the wrapper connection in SA's
    ``AsyncAdapt_aiomysql_connection`` (see the DBAPI adapter's ``connect``),
    which nests the real object at ``._connection``. Fall back to the object
    itself for a bare wrapper connection.
    """
    return getattr(dbapi_conn, "_connection", dbapi_conn)


class _MySQLReadOnlyConnectionCharacteristic(ConnectionCharacteristic):
    """A ``mysql_readonly`` execution-option characteristic, mirroring SA's
    ``PGReadOnlyConnectionCharacteristic``.

    SQLAlchemy ships a ``postgresql_readonly`` characteristic but NO MySQL
    equivalent, so ``execution_options(mysql_readonly=True)`` is silently
    ignored on stock MySQL dialects. Registering this lets read/write-splitting
    users route a read-only connection to a reader the same way PG users do
    (test_sqlalchemy_creator_read_write_splitting_async). Routes through the
    dialect's set/get_readonly -> the wrapper connection's read-only control,
    which the RWS plugin intercepts.
    """

    transactional = True

    def reset_characteristic(self, dialect: Any, dbapi_conn: Any) -> None:
        dialect.set_readonly(dbapi_conn, False)

    def set_characteristic(self, dialect: Any, dbapi_conn: Any, value: Any) -> None:
        dialect.set_readonly(dbapi_conn, value)

    def get_characteristic(self, dialect: Any, dbapi_conn: Any) -> Any:
        return dialect.get_readonly(dbapi_conn)


class AwsWrapperAsyncAiomysqlAdaptDBAPI:
    """DBAPI adapter bridging our async aiomysql submodule into SA's MySQL
    async flow.

    Mirrors the pattern of :class:`AwsWrapperAsyncPsycopgAdaptDBAPI` in
    ``pg_async.py``: wraps the wrapper's async submodule rather than
    ``aiomysql`` itself. The adapter's ``connect`` calls the wrapper's
    async connect (which awaits through the plugin pipeline) and wraps
    the resulting connection in ``AsyncAdapt_aiomysql_connection`` so the
    engine's greenlet bridge can expose it via a sync-looking surface.
    """

    def __init__(self) -> None:
        import aws_advanced_python_wrapper.aio.aiomysql as aio_aiomysql
        self._aio_aiomysql = aio_aiomysql
        # Copy the PEP 249 surface onto self except ``connect`` (handled below).
        for name, value in aio_aiomysql.__dict__.items():
            if name == "connect":
                continue
            self.__dict__[name] = value

    @property
    def aiomysql(self) -> Any:
        return self._aio_aiomysql

    def __getattr__(self, name: str) -> Any:
        return getattr(self._aio_aiomysql, name)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_aiomysql_connection:
        # SA may pass `async_creator_fn` for custom pool factories; we
        # are the creator, so discard it.
        kwargs.pop("async_creator_fn", None)
        coro = self._aio_aiomysql.connect(*args, **kwargs)
        # SA's AsyncAdapt_aiomysql_connection takes (dbapi, connection);
        # we are the dbapi for the adapter's purposes. The connection
        # arg is typed as AsyncIODBAPIConnection (structural); our
        # AsyncAwsWrapperConnection proxies unknown attrs to the target
        # driver conn, so duck-typing holds at runtime. Cast for mypy.
        return AsyncAdapt_aiomysql_connection(
            self, await_only(coro)  # type: ignore[arg-type]
        )


class AwsWrapperMySQLAiomysqlAsyncDialect(
        _AsyncFailoverSuccessRewrapMixin, MySQLDialect_aiomysql):
    """Async SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "aws_wrapper_aiomysql"
    supports_statement_cache = True
    is_async = True

    # Register a ``mysql_readonly`` characteristic (SA ships only
    # ``postgresql_readonly``) so RWS users can route a read-only connection to
    # a reader via execution_options, like PG
    # (test_sqlalchemy_creator_read_write_splitting_async).
    connection_characteristics = util.immutabledict({
        **MySQLDialect_aiomysql.connection_characteristics,
        "mysql_readonly": _MySQLReadOnlyConnectionCharacteristic(),
    })

    # See _AsyncFailoverSuccessRewrapMixin / sqlalchemy_dialects/pg.py.
    # ``dialect.dbapi.OperationalError`` resolves to the wrapper's PEP-249
    # ``OperationalError`` via the shim's ``_dbapi.install`` — rewrap
    # target must be that class for SA's classifier to wrap us to
    # ``sqlalchemy.exc.OperationalError``.
    _failover_success_target_cls = _PEP249OperationalError

    def set_readonly(self, dbapi_conn: Any, value: bool) -> None:
        # dbapi_conn is SA's AsyncAdapt_aiomysql_connection; the wrapper
        # connection (whose set_read_only the RWS plugin intercepts) is at
        # ._connection. set_characteristic runs in SA's greenlet, so bridge the
        # async set_read_only via await_only.
        wrapper = _unwrap_wrapper_conn(dbapi_conn)
        sro = getattr(wrapper, "set_read_only", None)
        if sro is not None:
            await_only(sro(bool(value)))

    def get_readonly(self, dbapi_conn: Any) -> bool:
        # Best-effort: report the wrapper's current read-only intent so SA can
        # store it for reset-on-checkin. Defaults to False (writer) when not
        # exposed, which yields the correct reset-to-writer behavior.
        wrapper = _unwrap_wrapper_conn(dbapi_conn)
        return bool(getattr(wrapper, "read_only", False))

    @classmethod
    def import_dbapi(cls) -> Any:  # type: ignore[override]
        # Parent's return type hint is SA's AsyncAdapt_aiomysql_dbapi class
        # specifically; ours is a shim-compatible duck-type. Use Any to
        # avoid variance grief with mypy.
        return AwsWrapperAsyncAiomysqlAdaptDBAPI()

    @classmethod
    def get_dialect_cls(cls, url):
        return cls

    @classmethod
    def get_async_dialect_cls(cls, url):
        # Grandparent MySQLDialect_pymysql.get_async_dialect_cls hard-
        # returns MySQLDialect_aiomysql; override so URL-based dialect
        # selection picks up our subclass instead of the stock SA class.
        return cls

    def create_connect_args(self, url):
        # SA reserves ``plugins=`` in the URL query for its own engine-
        # plugin loader. Allow users to spell the wrapper's ``plugins``
        # connection property as ``wrapper_plugins=`` and rename it back
        # before the DBAPI call. See F2 pg.py / F3 pg_async.py for rationale.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs

    def _detect_charset(self, connection):
        # Mirror sync mysql.py: walk down to the underlying driver
        # connection via the wrapper's ``target_connection`` accessor
        # instead of relying on _AdhocProxiedConnection's __getattr__
        # to land on a connection that exposes ``.charset``. The async
        # wrapper already has a generic __getattr__, so this override
        # is defensive parity rather than a strict fix.
        proxied = connection.connection
        dbapi = getattr(proxied, "dbapi_connection", proxied)
        inner = getattr(dbapi, "target_connection", dbapi)
        # ``inner`` may be a raw aiomysql Connection (exposes ``charset``
        # directly) OR SQLAlchemy's ``AsyncAdapt_aiomysql_connection`` adapter,
        # which has NO ``charset`` -- it nests the real aiomysql connection at
        # ``._connection``. Reach whichever applies (SA's create_async_engine
        # path drives the adapter case: test_sqlalchemy_creator_*_async).
        real = getattr(inner, "_connection", inner)
        return getattr(real, "charset", None) or getattr(inner, "charset", None)

    def _driver_error_module(self):
        # aiomysql raises pymysql's PEP-249 error classes; lets
        # _normalize_driver_error translate a raw pymysql error into the
        # wrapper's PEP-249 type so SA classifies it (see _exception_handling).
        import pymysql
        return pymysql

    def is_disconnect(self, e, connection, cursor):
        # Mirror sync mysql.py. Two goals:
        # 1. Avoid the upstream probe of ``e.errno`` / ``e.args[0]`` (in
        #    tuple form) on FailoverError subclasses — they don't carry
        #    those attributes and upstream would crash before classifying.
        # 2. Distinguish success vs failure of wrapper-driven failover:
        #    - FailoverSuccessError → the wrapper's target_connection is
        #      now bound to the new writer. The SA pool slot is still
        #      valid; return False so SA keeps reusing it (next query
        #      lands on the new writer). Invalidating would force the
        #      creator lambda to re-fire with the original instance host,
        #      which is now demoted to a reader.
        #    - FailoverFailedError → wrapper has no working connection;
        #      return True so SA invalidates and the creator retries.
        # _AsyncFailoverSuccessRewrapMixin handles do_execute path;
        # this handles the cursor-creation path that runs earlier.
        from aws_advanced_python_wrapper.errors import (FailoverError,
                                                        FailoverFailedError)

        # Catch the whole FailoverError family -- including
        # TransactionResolutionUnknownError -- before upstream probes
        # ``e.errno`` / ``e.args[0]`` (the wrapper errors carry neither). Only
        # FailoverFailedError means no usable connection (-> True, SA
        # invalidates); FailoverSuccessError and TransactionResolutionUnknownError
        # both mean the wrapper reconnected to a new writer (-> False).
        if isinstance(e, FailoverError):
            return isinstance(e, FailoverFailedError)
        return super().is_disconnect(e, connection, cursor)

    def do_ping(self, dbapi_connection) -> bool:  # type: ignore[override]
        # The aiomysql base types do_ping as ``Literal[True]`` (return-or-raise);
        # we deliberately return ``bool`` -- SA's ``_do_ping_w_event`` does
        # ``return self.do_ping(...)``, so a ``False`` return correctly recycles
        # the pooled connection (mysqlconnector/psycopg bases already type bool).
        # Support SQLAlchemy ``pool_pre_ping``. aiomysql's ping() is a
        # coroutine and do_ping runs in SA's greenlet, so bridge via
        # await_only. dbapi_connection is SA's AsyncAdapt_aiomysql_connection;
        # ``._connection`` is the AsyncAwsWrapperConnection, whose
        # ``target_connection`` is the aiomysql connection (or SA's adapter
        # around it, nesting the real conn at ``._connection``). A driver error
        # means the connection is dead -> return False so SA's pool recycles
        # it. Adopts AWS PR #1245 for the async MySQL dialect (AWS ships sync
        # MySQL only).
        wrapper = _unwrap_wrapper_conn(dbapi_connection)
        target = getattr(wrapper, "target_connection", wrapper)
        real = getattr(target, "_connection", target)
        try:
            await_only(real.ping(reconnect=False))
            return True
        except Exception:
            return False
