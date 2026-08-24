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

"""Shared exception-handling helpers for the wrapper's SA dialects.

How SQLAlchemy classifies the wrapper's errors
----------------------------------------------
``sqlalchemy.exc.DBAPIError.instance`` walks ``orig.__class__.__mro__`` and
matches each base **by class name** against the names exported from
``sqlalchemy.exc``, gated on ``isinstance(orig, dialect.loaded_dbapi.Error)``.
Our ``loaded_dbapi`` is the wrapper itself, so that gate is the wrapper's
PEP-249 ``Error`` — and because ``FailoverError`` derives from the wrapper's
``pep249.OperationalError``, every failover error already carries a base *named*
``OperationalError`` in its MRO. SA therefore maps the whole family to
``sqlalchemy.exc.OperationalError`` on its own. The class does NOT need to be
driver-native, and nothing at the dialect boundary needs to re-raise anything
for ``except sqlalchemy.exc.OperationalError:`` to fire.

The wrapper's own errors are therefore passed through ``do_execute`` UNCHANGED.
Two things depend on that:

* SA puts the original exception in ``DBAPIError.orig``, so a consumer can write
  ``isinstance(err.orig, FailoverSuccessError)`` — the documented idiom — as
  well as ``except sqlalchemy.exc.OperationalError:``.
* Each dialect's ``is_disconnect`` override sees the real ``FailoverError`` and
  decides pool invalidation from it: ``FailoverFailedError`` -> invalidate,
  ``FailoverSuccessError`` / ``TransactionResolutionUnknownError`` -> keep the
  pooled connection, which the wrapper has already rebound to the new writer.

**Do not reintroduce a rewrap here.** An earlier version of this module caught
``FailoverSuccessError`` and re-raised ``pep249.OperationalError``. It gained
nothing — SA produced the same ``sqlalchemy.exc.OperationalError`` either way —
and it cost two things:

1. ``DBAPIError.orig`` became the substitute, silently breaking
   ``isinstance(err.orig, FailoverSuccessError)``.
2. The substitute is not a ``FailoverError``, so ``is_disconnect`` below no
   longer recognised it and fell through to
   ``MySQLDialect_mysqlconnector.is_disconnect``, which probes ``e.errno``. The
   wrapper's PEP-249 errors carry no ``errno``, and SA calls ``is_disconnect``
   at the top of ``Connection._handle_dbapi_exception`` with no enclosing
   ``try:`` — so an ``AttributeError`` escaped and the consumer's
   ``except DBAPIError:`` never ran at all. Confirmed on a real Aurora MySQL
   failover: a successful failover surfaced to the application as
   ``AttributeError: 'OperationalError' object has no attribute 'errno'``.

What this module DOES do
------------------------
Normalize *raw driver-native* DBAPI errors (``mysql.connector.errors.*`` /
``psycopg.*`` / ``pymysql.*``) into the wrapper's PEP-249 equivalents, for plugin
chains that do not already re-wrap them (``iam`` / ``aws_secrets_manager`` / no
plugins). Without that SA cannot classify them at all, so e.g. ``has_table``
never sees MySQL's 1146 and ``create_all`` fails.
"""

from __future__ import annotations


def _normalize_driver_error(e, driver_error_module):
    """Translate a raw driver-native DBAPI error into the wrapper's PEP-249
    equivalent so SQLAlchemy's classifier recognizes it.

    SA wraps an exception into ``sqlalchemy.exc.DBAPIError`` (enabling
    ``has_table`` / ``is_disconnect`` / retry handling) only when
    ``isinstance(e, dialect.dbapi.Error)`` -- and ``dialect.dbapi.Error`` is the
    wrapper's PEP-249 ``Error``. Plugin chains that re-wrap driver errors as
    ``AwsWrapperError`` (e.g. failover) are already recognized, but auth-only
    chains (``iam`` / ``aws_secrets_manager`` / no plugins) let the raw driver
    error (``mysql.connector.errors.*``, ``psycopg.*``, ``pymysql.*``) escape --
    which SA cannot classify, so e.g. ``has_table`` never catches a 1146
    "table doesn't exist" and ``create_all`` fails.

    Returns an equivalent wrapper PEP-249 error (same PEP-249 subtype matched by
    name; ``errno`` / ``sqlstate`` / ``pgcode`` preserved so the dialect's
    ``_extract_error_code`` still reads the numeric code; original chained via
    ``__cause__``), or ``None`` if ``e`` is already a wrapper error or not a
    recognizable driver error (caller should re-raise the original).
    """
    from aws_advanced_python_wrapper import pep249
    if isinstance(e, pep249.Error):
        return None  # already a wrapper PEP-249 error (incl. AwsWrapperError)
    if driver_error_module is None:
        return None
    base = getattr(driver_error_module, "Error", None)
    if base is None or not isinstance(e, base):
        return None  # not a driver-native DBAPI error -- leave it alone
    target_cls = pep249.Error
    # Most-specific PEP-249 subtype first; DatabaseError (the common parent)
    # last so e.g. a ProgrammingError isn't mis-mapped to it.
    for name in ("DataError", "IntegrityError", "InternalError",
                 "NotSupportedError", "OperationalError", "ProgrammingError",
                 "InterfaceError", "DatabaseError"):
        drv_cls = getattr(driver_error_module, name, None)
        if drv_cls is not None and isinstance(e, drv_cls):
            target_cls = getattr(pep249, name, pep249.Error)
            break
    wrapped = target_cls(str(e))
    for attr in ("errno", "sqlstate", "pgcode"):
        val = getattr(e, attr, None)
        if val is not None:
            try:
                setattr(wrapped, attr, val)
            except Exception:  # noqa: BLE001 - best-effort metadata copy
                pass
    return wrapped


class _DriverErrorNormalizeMixin:
    """Normalize raw driver-native DBAPI errors into the wrapper's PEP-249 types.

    The wrapper's own errors — including the whole ``FailoverError`` family — are
    passed through UNTOUCHED, so ``DBAPIError.orig`` stays the exception the
    wrapper raised and each dialect's ``is_disconnect`` can classify it. See the
    module docstring for why re-raising a substitute class here is wrong.
    """

    def _driver_error_module(self):
        """Driver-native DBAPI exception namespace (module exposing PEP-249
        error classes: ``Error``, ``OperationalError``, ``ProgrammingError``,
        ...). Concrete dialects override to enable normalizing raw driver
        errors into wrapper PEP-249 errors (see :func:`_normalize_driver_error`)
        so SA's classifier works on plugin chains that don't re-wrap driver
        errors (iam / secrets / no-plugins). Default ``None`` => no-op.
        """
        return None

    def do_execute(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            super().do_execute(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except Exception as e:
            normalized = _normalize_driver_error(e, self._driver_error_module())
            if normalized is not None:
                raise normalized from e
            raise

    def do_executemany(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            super().do_executemany(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except Exception as e:
            normalized = _normalize_driver_error(e, self._driver_error_module())
            if normalized is not None:
                raise normalized from e
            raise


class _AsyncDriverErrorNormalizeMixin:
    """Async counterpart of :class:`_DriverErrorNormalizeMixin`.

    IMPORTANT: ``do_execute`` / ``do_executemany`` MUST be SYNCHRONOUS even for
    async dialects. SQLAlchemy's execution context calls
    ``dialect.do_execute(...)`` synchronously inside a greenlet; the async work
    is bridged *inside* SA's ``AsyncAdapt_*_cursor.execute`` (a sync method that
    uses ``await_only``). An ``async def do_execute`` here would merely build a
    coroutine that SA never awaits -- the query would never run, leaving the
    cursor with no result, so ``description`` is ``None`` and SA raises
    ``ResourceClosedError`` ("This result object does not return rows") from
    ``dialect.initialize``'s ``SELECT version()`` (the ``sqlalchemy_creator_*``
    integration tests). So this mixin is functionally identical to the sync
    one; it exists as a distinct class only so async dialects can be wired to a
    different ``_driver_error_module``.
    """

    def _driver_error_module(self):
        """See :meth:`_DriverErrorNormalizeMixin._driver_error_module`."""
        return None

    def do_execute(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            super().do_execute(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except Exception as e:
            normalized = _normalize_driver_error(e, self._driver_error_module())
            if normalized is not None:
                raise normalized from e
            raise

    def do_executemany(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            super().do_executemany(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except Exception as e:
            normalized = _normalize_driver_error(e, self._driver_error_module())
            if normalized is not None:
                raise normalized from e
            raise


__all__ = ["_DriverErrorNormalizeMixin", "_AsyncDriverErrorNormalizeMixin"]
