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

"""PostgreSQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``postgresql.aws_wrapper_psycopg`` via a pyproject entry-point
(URL ``postgresql+aws_wrapper_psycopg://``). Subclasses SA's standard
PGDialect_psycopg and only swaps the DBAPI module to
:mod:`aws_advanced_python_wrapper.psycopg`, which routes connect() through
the wrapper's plugin pipeline.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

from aws_advanced_python_wrapper.pep249 import \
    OperationalError as _PEP249OperationalError
from aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling import \
    _FailoverSuccessRewrapMixin


class AwsWrapperPGPsycopgDialect(_FailoverSuccessRewrapMixin, PGDialect_psycopg):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI.

    Wrapper-specific override pattern
    ---------------------------------
    The wrapper interposes on DBAPI-level calls -- SA drives those
    through our plugin pipeline. But SA's psycopg dialect also calls
    into psycopg internals directly (``psycopg.types.TypeInfo.fetch``
    and friends), passing a ``driver_connection``. Those helpers do a
    strict ``isinstance`` check against ``psycopg.Connection``; our
    proxy is not a subclass, so they raise TypeError.

    Wherever SA reaches the raw driver connection, we override the
    method here to unwrap to the native psycopg connection via
    ``AwsWrapperConnection.target_connection`` before handing it to
    psycopg. Current overrides: ``_type_info_fetch``.
    """

    driver = "aws_wrapper_psycopg"
    supports_statement_cache = True

    # See _FailoverSuccessRewrapMixin. SA's classifier checks
    # ``isinstance(exc, dialect.dbapi.OperationalError)``; for our shim
    # ``dialect.dbapi.OperationalError`` resolves to the wrapper's PEP-249
    # ``OperationalError`` (installed via ``_dbapi.install``), NOT psycopg's
    # native one. The target class must therefore be the wrapper's PEP-249
    # class so SA's classifier matches and wraps to
    # ``sqlalchemy.exc.OperationalError``. (psycopg.OperationalError would
    # only work if SA's dbapi attribute pointed at the real psycopg module,
    # which it doesn't here because ``import_dbapi`` returns our shim.)
    _failover_success_target_cls = _PEP249OperationalError

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.psycopg as dbapi
        return dbapi

    def create_connect_args(self, url):
        # SQLAlchemy's `create_engine` intercepts `plugins=` in the URL query
        # to load SA engine plugins, stripping it before the dialect sees it.
        # The wrapper's own `plugins` connection property would therefore be
        # swallowed. Allow users to spell it as `wrapper_plugins=` in the URL
        # and rename it to `plugins=` before handing kwargs to the DBAPI.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs

    def _driver_error_module(self):
        # psycopg exposes PEP-249 error classes at top level; lets
        # _normalize_driver_error translate a raw psycopg error into the
        # wrapper's PEP-249 type so SA classifies it (see _exception_handling).
        import psycopg
        return psycopg

    def is_disconnect(self, e, connection, cursor):
        # Mirror sync mysql.py for explicit, defensive symmetry. Upstream
        # PGDialect_psycopg.is_disconnect happens to return False for our
        # FailoverSuccessError today (it checks ``connection.closed`` /
        # ``broken`` rather than probing errno), which is why PG passes
        # naturally. Make that behavior explicit here so it doesn't drift
        # if upstream changes:
        #   - FailoverSuccessError → False (pool slot's wrapper is now
        #     bound to the new writer via plugin_service.current_connection;
        #     SA should reuse it, not invalidate).
        #   - FailoverFailedError → True (no usable connection; invalidate).
        from aws_advanced_python_wrapper.errors import (FailoverError,
                                                        FailoverFailedError)

        # Catch the whole FailoverError family -- including
        # TransactionResolutionUnknownError -- before the upstream is_disconnect
        # probes attributes the wrapper errors don't carry. Only
        # FailoverFailedError means there's no usable connection (-> True, SA
        # invalidates); FailoverSuccessError and TransactionResolutionUnknownError
        # both mean the wrapper reconnected to a new writer (-> False).
        if isinstance(e, FailoverError):
            return isinstance(e, FailoverFailedError)
        return super().is_disconnect(e, connection, cursor)

    def _type_info_fetch(self, connection: Any, name: str) -> Any:
        """Unwrap to native psycopg.Connection before TypeInfo.fetch.

        SA native (``sqlalchemy/dialects/postgresql/psycopg.py:462``):
            return TypeInfo.fetch(connection.connection.driver_connection, name)

        In SA's sync psycopg dialect, ``connection.connection`` is the
        DBAPI-level connection and ``driver_connection`` is the native
        psycopg.Connection (SA's ``AdaptedConnection`` base class
        implements ``driver_connection`` as a property returning
        ``self._connection``). When our wrapper is the DBAPI connection,
        ``driver_connection`` returns our wrapper proxy.

        ``psycopg.TypeInfo.fetch`` rejects the proxy with
        ``TypeError: expected Connection or AsyncConnection, got ...``
        (psycopg/_typeinfo.py:90). Reach the underlying native via
        ``target_connection`` (exposed on our wrapper at
        ``wrapper.py:84``) and pass it directly.
        """
        from psycopg.types import TypeInfo

        # connection.connection may BE our wrapper (SA hands us the
        # DBAPI conn directly on the sync path) or SA's adapter
        # wrapping us. Support both shapes: prefer ``driver_connection``
        # when present, else fall back to the conn itself.
        dbapi_conn = connection.connection
        candidate = getattr(dbapi_conn, "driver_connection", dbapi_conn)
        native = getattr(candidate, "target_connection", candidate)
        return TypeInfo.fetch(native, name)

    def do_ping(self, dbapi_connection) -> bool:
        # Support SQLAlchemy ``pool_pre_ping``. psycopg3 has no ``.ping()``;
        # SA's _psycopg_common.do_ping runs ``SELECT 1`` via the DBAPI
        # connection's cursor with an autocommit toggle. Run that liveness
        # query against the native psycopg connection (reached via the
        # wrapper's ``target_connection``) so it works regardless of which
        # psycopg surface the wrapper proxies. A failure -> return False so
        # SA's pool recycles the connection. Mirrors AWS PR #1245's
        # pool_pre_ping support, generalized to PostgreSQL.
        target = getattr(dbapi_connection, "target_connection", dbapi_connection)
        try:
            before_autocommit = target.autocommit
            if not before_autocommit:
                target.autocommit = True
            try:
                target.execute("SELECT 1")
            finally:
                if not before_autocommit and not target.closed:
                    target.autocommit = before_autocommit
            return True
        except Exception:
            return False
