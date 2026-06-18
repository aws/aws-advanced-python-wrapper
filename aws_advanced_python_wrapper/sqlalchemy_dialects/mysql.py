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

"""MySQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``mysql.aws_wrapper_mysqlconnector`` via a pyproject
entry-point (URL ``mysql+aws_wrapper_mysqlconnector://``). Subclasses SA's
standard MySQLDialect_mysqlconnector and only swaps the DBAPI module to
:mod:`aws_advanced_python_wrapper.mysql_connector`, which routes connect()
through the wrapper's plugin pipeline.
"""

from __future__ import annotations

from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector

from aws_advanced_python_wrapper.pep249 import \
    OperationalError as _PEP249OperationalError
from aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling import \
    _FailoverSuccessRewrapMixin


class AwsWrapperMySQLConnectorDialect(
        _FailoverSuccessRewrapMixin, MySQLDialect_mysqlconnector):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "aws_wrapper_mysqlconnector"
    supports_statement_cache = True

    # See _FailoverSuccessRewrapMixin / sqlalchemy_dialects/pg.py for the
    # full rationale. The shim's ``dialect.dbapi.OperationalError`` resolves
    # to the wrapper's PEP-249 ``OperationalError`` via ``_dbapi.install``,
    # so the rewrap target must be that class for SA's classifier to wrap
    # us to ``sqlalchemy.exc.OperationalError``.
    _failover_success_target_cls = _PEP249OperationalError

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.mysql_connector as dbapi
        return dbapi

    def create_connect_args(self, url):
        # See aws_advanced_python_wrapper/sqlalchemy_dialects/pg.py for the
        # `wrapper_plugins` → `plugins` translation rationale.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs

    def _extract_error_code(self, exception: BaseException) -> int:
        # Plugins such as failover re-raise the underlying driver error
        # wrapped in an ``AwsWrapperError``, which hides the numeric MySQL
        # error code that SA's ``has_table`` / ``is_disconnect`` logic keys
        # off. In particular ``has_table`` must see 1146 ("table doesn't
        # exist") to return False so ``create_all`` can proceed -- otherwise
        # the wrapped error propagates and ORM table setup fails. Unwrap to
        # the underlying mysql-connector error so the stock classifier can
        # read ``.errno``. Restores the override that main's
        # SqlAlchemyOrmMysqlDialect carried before it was dropped in the merge.
        from aws_advanced_python_wrapper.errors import AwsWrapperError
        if isinstance(exception, AwsWrapperError) and exception.driver_error is not None:
            exception = exception.driver_error
        return super()._extract_error_code(exception)

    def _driver_error_module(self):
        # mysql-connector-python's PEP-249 exception namespace; lets
        # _normalize_driver_error translate a raw mysql.connector error into the
        # wrapper's PEP-249 type so SA classifies it (see _exception_handling).
        import mysql.connector.errors as _err
        return _err

    def _detect_charset(self, connection):
        # SA's MySQLDialect_mysqlconnector._detect_charset does
        # ``return connection.connection.charset``. That ``.charset`` walks
        # _AdhocProxiedConnection.__getattr__ → dbapi_connection, landing
        # on AwsWrapperConnection which (in the sync wrapper) has no
        # generic attribute passthrough. Reach the underlying mysql-
        # connector connection through the wrapper's explicit
        # ``target_connection`` accessor.
        proxied = connection.connection
        dbapi = getattr(proxied, "dbapi_connection", proxied)
        inner = getattr(dbapi, "target_connection", dbapi)
        return inner.charset

    def is_disconnect(self, e, connection, cursor):
        # Two goals:
        # 1. Prevent the upstream MySQLDialect_mysqlconnector.is_disconnect
        #    from probing ``e.errno`` on FailoverError subclasses (they
        #    don't carry the mysql-connector errno attribute, so upstream
        #    would crash with AttributeError before classifying).
        # 2. Return the right value for each kind of failover signal:
        #    - FailoverSuccessError: the wrapper successfully reconnected
        #      to a new writer; ``AwsWrapperConnection.target_connection``
        #      is auto-rebound via ``plugin_service.current_connection``
        #      to the new endpoint. The pool slot IS still valid -- if
        #      SA invalidates it, the creator lambda re-fires with the
        #      original (now-demoted) instance hostname and lands on the
        #      old reader. Returning False keeps the same wrapper, which
        #      is now on the new writer. (PG passes naturally because
        #      psycopg's upstream is_disconnect returns False for this.)
        #    - FailoverFailedError: the wrapper has no working connection;
        #      pool slot really is dead. Return True so SA invalidates.
        # _FailoverSuccessRewrapMixin still handles the do_execute path;
        # this method handles the cursor-creation path which runs before
        # do_execute reaches the mixin.
        from aws_advanced_python_wrapper.errors import (FailoverError,
                                                        FailoverFailedError)

        # Catch the whole FailoverError family (FailoverSuccessError,
        # FailoverFailedError, AND TransactionResolutionUnknownError) before the
        # upstream is_disconnect probes ``e.errno`` -- none of them carry the
        # mysql-connector errno attribute, so falling through raises
        # ``AttributeError: 'TransactionResolutionUnknownError' object has no
        # attribute 'errno'`` (seen on mid-transaction failover, env-3/env-4).
        # Only FailoverFailedError means there is no usable connection -> True
        # (SA invalidates the slot). FailoverSuccessError and
        # TransactionResolutionUnknownError both mean the wrapper reconnected to
        # a new writer and the pool slot's wrapper is valid -> False.
        if isinstance(e, FailoverError):
            return isinstance(e, FailoverFailedError)
        return super().is_disconnect(e, connection, cursor)

    def do_ping(self, dbapi_connection) -> bool:
        # Support SQLAlchemy ``pool_pre_ping``. SA's
        # MySQLDialect_mysqlconnector.do_ping calls
        # ``dbapi_connection.ping(False)``, but AwsWrapperConnection does NOT
        # expose ``.ping()`` -- so without this override pool_pre_ping raises
        # AttributeError. Reach the underlying mysql-connector connection via
        # the wrapper's ``target_connection`` accessor and ping there; a driver
        # error means the connection is dead -> return False so SA's pool
        # recycles it. Adopts AWS PR #1245, generalized to our dialect family.
        target = getattr(dbapi_connection, "target_connection", dbapi_connection)
        try:
            target.ping(reconnect=False)
            return True
        except Exception:
            return False
