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

SQLAlchemy classifies DBAPI exceptions in ``Connection._handle_dbapi_exception``
by walking ``dialect.loaded_dbapi.<ErrorClass>`` and wrapping into
``sqlalchemy.exc.<MappedClass>``. SA's classifier needs the raised exception
to be an instance of the **driver-native** ``OperationalError`` class
(e.g. ``psycopg.OperationalError``), not the wrapper's PEP-249
``OperationalError``. Wrapper-internal exceptions like
``FailoverSuccessError`` are single-inherit from ``FailoverError`` (the
driver-native multi-inheritance was reverted in commit ``d994d02`` because
it caused Django's ``wrap_database_errors`` to swallow the failover signal
on MySQL), so SA's classifier lets them escape raw and any user-written
``except sqlalchemy.exc.OperationalError:`` retry loop never fires.

The mixin below sidesteps that by intercepting ``FailoverSuccessError`` at
the ``do_execute`` / ``do_executemany`` boundary and re-raising it as the
driver-native ``OperationalError`` class — which SA's classifier DOES
reclassify reliably to ``sqlalchemy.exc.OperationalError``. The original
wrapper exception is preserved via ``__cause__`` so callers that need the
exact wrapper type can ``isinstance(exc.__cause__, FailoverSuccessError)``.

Each concrete dialect declares its target class via
``_failover_success_target_cls``; the mixin wraps the dialect's
``do_execute`` / ``do_executemany`` calls.

Scope: only ``do_execute`` and ``do_executemany`` are wrapped. If
``FailoverSuccessError`` ever surfaces from ``do_commit`` / ``do_rollback``
/ ``do_begin_twophase`` / etc., it will escape raw — extend the mixin then.
"""

from __future__ import annotations

from typing import ClassVar, Optional, Type

from aws_advanced_python_wrapper.errors import FailoverSuccessError


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


class _FailoverSuccessRewrapMixin:
    """Re-raise ``FailoverSuccessError`` as the driver-native OperationalError.

    Concrete dialect subclasses set ``_failover_success_target_cls`` to the
    driver's own ``OperationalError`` class (e.g. ``psycopg.OperationalError``,
    ``mysql.connector.errors.OperationalError``).
    The mixin's ``do_execute`` wraps the parent's call: on
    ``FailoverSuccessError``, it raises the target class with the same message,
    chaining the original via ``__cause__``. SA's classifier reliably maps
    driver-native ``OperationalError`` -> ``sqlalchemy.exc.OperationalError``,
    so user retry loops (``except sqlalchemy.exc.OperationalError:``) fire.

    Other ``FailoverError`` subclasses (``FailoverFailedError``,
    ``TransactionResolutionUnknownError``) are NOT rewrapped: failed failover
    is a hard error the user should see, and transaction-resolution-unknown
    has its own semantics distinct from a generic OperationalError.
    """

    # Subclasses MUST set this to the driver-native OperationalError class.
    _failover_success_target_cls: ClassVar[Optional[Type[BaseException]]] = None

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
        except FailoverSuccessError as e:
            target = self._failover_success_target_cls
            if target is None:
                raise  # mis-configured dialect; surface the raw error
            raise target(str(e)) from e
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
        except FailoverSuccessError as e:
            target = self._failover_success_target_cls
            if target is None:
                raise
            raise target(str(e)) from e
        except Exception as e:
            normalized = _normalize_driver_error(e, self._driver_error_module())
            if normalized is not None:
                raise normalized from e
            raise


__all__ = ["_FailoverSuccessRewrapMixin"]
