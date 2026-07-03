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

"""Tests for notice/notify handler passthroughs on sync + async wrappers.

SQLAlchemy's psycopg dialect registers a notice handler on every
``engine.connect()`` at ``sqlalchemy/dialects/postgresql/psycopg.py:575``.
These tests guard against a regression where the wrapper doesn't expose
``add_notice_handler`` / ``remove_notice_handler`` and breaks SA's
connect flow.

Covers all four driver shapes:
  * sync + psycopg (PG)
  * sync + mysql-connector (MySQL -- these methods must raise
    AttributeError from the underlying driver since the API is
    PostgreSQL-only).
  * async + psycopg.AsyncConnection (PG)
  * async + aiomysql (MySQL -- same AttributeError semantics).

The handler methods are intentionally sync on BOTH sync and async
wrappers, matching psycopg3's signatures (verified against
psycopg 3.3.3 in the repo's dev venv).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

# ---- Sync wrapper fixtures ---------------------------------------------


def _sync_wrapper_with_target(target_conn: MagicMock) -> AwsWrapperConnection:
    """Build an AwsWrapperConnection without running its heavy __init__.

    ``AwsWrapperConnection.__init__`` wires plugin service + host list
    provider and tries to open a real connection. The notice/notify
    passthroughs only read ``self.target_connection`` (which is a
    property returning ``self._plugin_service.current_connection``),
    so we can stub that end of the graph with a minimal mock and skip
    the rest.
    """
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    plugin_service = MagicMock()
    plugin_service.current_connection = target_conn
    wrapper._plugin_service = plugin_service
    wrapper._plugin_manager = MagicMock()
    return wrapper


def _pg_shaped_target() -> MagicMock:
    """A target conn that behaves like psycopg.Connection: has the
    four notice/notify handler methods."""
    target = MagicMock()
    target.add_notice_handler = MagicMock(return_value=None)
    target.remove_notice_handler = MagicMock(return_value=None)
    target.add_notify_handler = MagicMock(return_value=None)
    target.remove_notify_handler = MagicMock(return_value=None)
    return target


def _mysql_shaped_target() -> MagicMock:
    """A target conn that behaves like mysql-connector / aiomysql: no
    notice/notify handler methods at all. ``spec=`` forces AttributeError
    on undefined attributes (MagicMock by default auto-creates them)."""
    class _MySQLLike:
        # Deliberately empty: models the PG-only-attribute gap.
        pass
    return MagicMock(spec=_MySQLLike)


# ---- Sync wrapper: psycopg path (the load-bearing SA case) --------------


def test_sync_wrapper_add_notice_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _sync_wrapper_with_target(target)

    def _callback(diagnostic):
        pass

    wrapper.add_notice_handler(_callback)
    target.add_notice_handler.assert_called_once_with(_callback)


def test_sync_wrapper_remove_notice_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _sync_wrapper_with_target(target)

    def _callback(diagnostic):
        pass

    wrapper.remove_notice_handler(_callback)
    target.remove_notice_handler.assert_called_once_with(_callback)


def test_sync_wrapper_add_notify_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _sync_wrapper_with_target(target)

    def _callback(notify):
        pass

    wrapper.add_notify_handler(_callback)
    target.add_notify_handler.assert_called_once_with(_callback)


def test_sync_wrapper_remove_notify_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _sync_wrapper_with_target(target)

    def _callback(notify):
        pass

    wrapper.remove_notify_handler(_callback)
    target.remove_notify_handler.assert_called_once_with(_callback)


def test_sync_wrapper_notice_passthrough_bypasses_plugin_chain():
    """Notice/notify handlers touch pure local client-side state; they
    must NOT invoke the plugin pipeline (which is reserved for DB-side
    operations the pipeline legitimately cares about)."""
    target = _pg_shaped_target()
    wrapper = _sync_wrapper_with_target(target)

    wrapper.add_notice_handler(lambda d: None)

    # plugin_manager.execute was not called -- no plugin interception.
    wrapper._plugin_manager.execute.assert_not_called()


# ---- Sync wrapper: MySQL path (API not supported by driver) -------------


@pytest.mark.parametrize("method_name", [
    "add_notice_handler",
    "remove_notice_handler",
    "add_notify_handler",
    "remove_notify_handler",
])
def test_sync_wrapper_raises_attribute_error_on_mysql_target(method_name):
    """mysql-connector doesn't implement notice/notify handlers. The
    pass-through forwards the call to the driver, which AttributeErrors
    -- correct behavior since these are PostgreSQL-only features."""
    target = _mysql_shaped_target()
    wrapper = _sync_wrapper_with_target(target)

    with pytest.raises(AttributeError):
        getattr(wrapper, method_name)(lambda d: None)


# ---- Async wrapper fixtures ---------------------------------------------


def _async_wrapper_with_target(target_conn: MagicMock) -> AsyncAwsWrapperConnection:
    """Build an AsyncAwsWrapperConnection bypassing its __init__ wiring.

    The async wrapper's __init__ merely stores three refs; we replicate
    that here without going through the full plugin-service setup.
    """
    wrapper = AsyncAwsWrapperConnection.__new__(AsyncAwsWrapperConnection)
    wrapper._plugin_service = MagicMock()
    wrapper._plugin_manager = MagicMock()
    wrapper._target_conn = target_conn
    return wrapper


# ---- Async wrapper: psycopg.AsyncConnection path ------------------------


def test_async_wrapper_add_notice_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _async_wrapper_with_target(target)

    def _callback(diagnostic):
        pass

    # psycopg3 keeps add_notice_handler synchronous on AsyncConnection
    # too -- no await needed on the wrapper call.
    wrapper.add_notice_handler(_callback)
    target.add_notice_handler.assert_called_once_with(_callback)


def test_async_wrapper_remove_notice_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _async_wrapper_with_target(target)

    def _callback(diagnostic):
        pass

    wrapper.remove_notice_handler(_callback)
    target.remove_notice_handler.assert_called_once_with(_callback)


def test_async_wrapper_add_notify_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _async_wrapper_with_target(target)

    def _callback(notify):
        pass

    wrapper.add_notify_handler(_callback)
    target.add_notify_handler.assert_called_once_with(_callback)


def test_async_wrapper_remove_notify_handler_delegates_to_target():
    target = _pg_shaped_target()
    wrapper = _async_wrapper_with_target(target)

    def _callback(notify):
        pass

    wrapper.remove_notify_handler(_callback)
    target.remove_notify_handler.assert_called_once_with(_callback)


def test_async_wrapper_notice_passthrough_bypasses_plugin_chain():
    target = _pg_shaped_target()
    wrapper = _async_wrapper_with_target(target)

    wrapper.add_notice_handler(lambda d: None)

    wrapper._plugin_manager.execute.assert_not_called()


# ---- Async wrapper: aiomysql path (API not supported by driver) --------


@pytest.mark.parametrize("method_name", [
    "add_notice_handler",
    "remove_notice_handler",
    "add_notify_handler",
    "remove_notify_handler",
])
def test_async_wrapper_raises_attribute_error_on_aiomysql_target(method_name):
    """aiomysql mirrors mysql-connector: no notice/notify handler API.
    The wrapper's passthrough forwards and the AttributeError bubbles
    -- correct, matches psycopg3 parity (PG-only feature)."""
    target = _mysql_shaped_target()
    wrapper = _async_wrapper_with_target(target)

    with pytest.raises(AttributeError):
        getattr(wrapper, method_name)(lambda d: None)
