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

"""Tests for the psycopg3-parity passthroughs on sync + async wrappers.

Companion to test_notice_handler_passthrough.py. Covers the remaining
psycopg3.Connection / AsyncConnection surface that SQLAlchemy and
other downstream libraries may touch:

  Properties (both wrappers, plain getters):
    info, broken, adapters, prepare_threshold (+ setter),
    prepared_max (+ setter), deferrable.

  read_only is NOT a plain passthrough on either wrapper: the sync
  wrapper has a plugin-aware intercepted property, and the async getter
  normalizes psycopg's None tri-state (and aiomysql's _aws_read_only
  intent stash) to a plain bool so callers get `conn.read_only is
  False/True` -- see test_async_wrapper_read_only_normalizes_to_bool.

  Sync methods (both wrappers):
    fileno, cancel, xid, pipeline, notifies, transaction.

  Sync methods on sync wrapper, async on async wrapper:
    cancel_safe, execute, wait.

  Sync setters on sync wrapper, async on async wrapper:
    set_deferrable, set_isolation_level, set_read_only, set_autocommit.

All passthroughs delegate directly to the target connection (bypass
the plugin chain) -- they are local/client-side operations.

The ``set_read_only`` / ``set_autocommit`` passthroughs on the SYNC
wrapper route through the existing plugin-aware property setters so
their semantics stay consistent with the property assignment form.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.wrapper import (AsyncAwsWrapperConnection,
                                                     AsyncAwsWrapperCursor)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.wrapper import (AwsWrapperConnection,
                                                 AwsWrapperCursor)

# ---- Fixtures ------------------------------------------------------------


def _sync_wrapper(target: MagicMock) -> AwsWrapperConnection:
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    plugin_service = MagicMock()
    plugin_service.current_connection = target
    wrapper._plugin_service = plugin_service
    wrapper._plugin_manager = MagicMock()
    return wrapper


def _async_wrapper(target: MagicMock) -> AsyncAwsWrapperConnection:
    wrapper = AsyncAwsWrapperConnection.__new__(AsyncAwsWrapperConnection)
    wrapper._plugin_service = MagicMock()
    wrapper._plugin_manager = MagicMock()
    wrapper._target_conn = target
    return wrapper


# ---- Properties (both wrappers) -----------------------------------------


@pytest.mark.parametrize("prop_name", [
    "info", "broken", "adapters", "prepare_threshold", "prepared_max",
    "deferrable",
])
def test_sync_wrapper_property_reads_target(prop_name: str) -> None:
    sentinel = object()
    target = MagicMock()
    setattr(target, prop_name, sentinel)
    wrapper = _sync_wrapper(target)
    assert getattr(wrapper, prop_name) is sentinel


@pytest.mark.parametrize("prop_name", [
    "info", "broken", "adapters", "prepare_threshold", "prepared_max",
    "deferrable",
])
def test_async_wrapper_property_reads_target(prop_name: str) -> None:
    sentinel = object()
    target = MagicMock()
    setattr(target, prop_name, sentinel)
    wrapper = _async_wrapper(target)
    assert getattr(wrapper, prop_name) is sentinel


def test_async_wrapper_read_only_normalizes_to_bool() -> None:
    # The async read_only getter is NOT a raw passthrough: psycopg exposes
    # read_only as a tri-state (None == unset/server default), so the wrapper
    # normalizes to a plain bool to honor the integration contract
    # `assert conn.read_only is False`. aiomysql has no native flag, so it
    # falls back to the dialect's _aws_read_only intent stash.
    target = MagicMock()
    target.read_only = None
    target._aws_read_only = False
    wrapper = _async_wrapper(target)
    assert wrapper.read_only is False
    target.read_only = True
    assert wrapper.read_only is True
    # aiomysql shape: native attr absent/None, intent stashed on _aws_read_only.
    target.read_only = None
    target._aws_read_only = True
    assert wrapper.read_only is True


@pytest.mark.parametrize("prop_name", ["prepare_threshold", "prepared_max"])
def test_sync_wrapper_property_setter_writes_target(prop_name: str) -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    setattr(wrapper, prop_name, 42)
    assert getattr(target, prop_name) == 42


@pytest.mark.parametrize("prop_name", ["prepare_threshold", "prepared_max"])
def test_async_wrapper_property_setter_writes_target(prop_name: str) -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    setattr(wrapper, prop_name, 42)
    assert getattr(target, prop_name) == 42


# ---- Sync-only methods on both wrappers ---------------------------------


def test_sync_wrapper_fileno_delegates() -> None:
    target = MagicMock()
    target.fileno = MagicMock(return_value=42)
    wrapper = _sync_wrapper(target)
    assert wrapper.fileno() == 42
    target.fileno.assert_called_once_with()


def test_sync_wrapper_cancel_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.cancel()
    target.cancel.assert_called_once_with()


def test_sync_wrapper_xid_delegates() -> None:
    target = MagicMock()
    target.xid = MagicMock(return_value="xid-obj")
    wrapper = _sync_wrapper(target)
    assert wrapper.xid(1, "gtrid", "bqual") == "xid-obj"
    target.xid.assert_called_once_with(1, "gtrid", "bqual")


def test_sync_wrapper_pipeline_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.pipeline()
    target.pipeline.assert_called_once_with()


def test_sync_wrapper_notifies_delegates_with_kwargs() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.notifies(timeout=5.0, stop_after=10)
    target.notifies.assert_called_once_with(timeout=5.0, stop_after=10)


def test_sync_wrapper_transaction_delegates_with_kwargs() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.transaction(savepoint_name="sp1", force_rollback=True)
    target.transaction.assert_called_once_with(
        savepoint_name="sp1", force_rollback=True)


def test_async_wrapper_fileno_delegates() -> None:
    target = MagicMock()
    target.fileno = MagicMock(return_value=42)
    wrapper = _async_wrapper(target)
    assert wrapper.fileno() == 42


def test_async_wrapper_cancel_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.cancel()
    target.cancel.assert_called_once_with()


def test_async_wrapper_xid_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.xid(1, "g", "b")
    target.xid.assert_called_once_with(1, "g", "b")


def test_async_wrapper_pipeline_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.pipeline()
    target.pipeline.assert_called_once_with()


def test_async_wrapper_notifies_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.notifies(timeout=1.0, stop_after=None)
    target.notifies.assert_called_once_with(timeout=1.0, stop_after=None)


def test_async_wrapper_transaction_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.transaction(savepoint_name=None, force_rollback=False)
    target.transaction.assert_called_once_with(
        savepoint_name=None, force_rollback=False)


# ---- Sync-vs-async split methods ----------------------------------------


def test_sync_wrapper_cancel_safe_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.cancel_safe(timeout=15.0)
    target.cancel_safe.assert_called_once_with(timeout=15.0)


def test_async_wrapper_cancel_safe_awaits_target() -> None:
    target = MagicMock()
    target.cancel_safe = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.cancel_safe(timeout=5.0))
    target.cancel_safe.assert_awaited_once_with(timeout=5.0)


def test_sync_wrapper_execute_delegates_with_all_kwargs() -> None:
    # psycopg3 Connection.execute() opens a cursor, runs the query through
    # the plugin chain, and returns the cursor (not the raw execute result) --
    # the wrapper deliberately routes via its cursor so the SQL is visible to
    # plugins. Verify the prepare/binary kwargs are forwarded all the way to
    # the underlying target cursor's execute.
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    # Route plugin_manager.execute straight to the provided callable so the
    # underlying cursor work actually runs.
    wrapper._plugin_manager.execute = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda obj, method, func, *a, **k: func())

    result = wrapper.execute("SELECT 1", ("p",), prepare=True, binary=True)

    assert isinstance(result, AwsWrapperCursor)
    target.cursor.return_value.execute.assert_called_once_with(
        "SELECT 1", ("p",), prepare=True, binary=True)


def test_async_wrapper_execute_routes_query_to_target_cursor() -> None:
    # psycopg3 AsyncConnection.execute() opens a cursor, runs the query
    # through the plugin chain, and returns the cursor. Verify the query
    # reaches the underlying (target) cursor's execute and a wrapper cursor
    # is returned -- not a direct passthrough of target.execute.
    target = MagicMock()
    target_cursor = target.cursor.return_value
    target_cursor.execute = AsyncMock(return_value="raw-result")
    wrapper = _async_wrapper(target)

    # plugin_manager.execute is awaited on the async path; route it straight
    # to the provided coroutine factory so the target cursor actually runs.
    async def _pm_execute(obj, method, func, *a, **k):
        return await func()

    wrapper._plugin_manager.execute = AsyncMock(  # type: ignore[method-assign]
        side_effect=_pm_execute)

    result = asyncio.run(wrapper.execute("SELECT 1"))

    assert isinstance(result, AsyncAwsWrapperCursor)
    target_cursor.execute.assert_awaited_once_with(
        "SELECT 1", prepare=None, binary=False)


def test_sync_wrapper_wait_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.wait("gen", interval=0.5)
    target.wait.assert_called_once_with("gen", interval=0.5)


def test_async_wrapper_wait_awaits_target() -> None:
    target = MagicMock()
    target.wait = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.wait("gen", interval=0.5))
    target.wait.assert_awaited_once_with("gen", interval=0.5)


# ---- Setter parity ------------------------------------------------------


def test_sync_wrapper_set_deferrable_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.set_deferrable(True)
    target.set_deferrable.assert_called_once_with(True)


def test_async_wrapper_set_deferrable_awaits() -> None:
    target = MagicMock()
    target.set_deferrable = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.set_deferrable(True))
    target.set_deferrable.assert_awaited_once_with(True)


def test_sync_wrapper_set_isolation_level_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.set_isolation_level("SERIALIZABLE")
    target.set_isolation_level.assert_called_once_with("SERIALIZABLE")


def test_sync_wrapper_set_read_only_routes_through_property() -> None:
    """sync wrapper's set_read_only uses the existing plugin-aware
    read_only property setter, not a direct target-connection call."""
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    # Stub out the property setter path so we observe plugin_manager.
    wrapper._plugin_manager.execute = MagicMock(return_value=None)  # type: ignore[method-assign]
    wrapper.set_read_only(True)
    # Plugin-manager was called (property setter routes through plugin chain).
    wrapper._plugin_manager.execute.assert_called_once()


def test_sync_wrapper_set_autocommit_routes_through_property() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper._plugin_manager.execute = MagicMock(return_value=None)  # type: ignore[method-assign]
    wrapper.set_autocommit(True)
    wrapper._plugin_manager.execute.assert_called_once()


def test_async_wrapper_set_read_only_routes_through_plugin_pipeline() -> None:
    """The async wrapper's set_read_only routes CONNECTION_SET_READ_ONLY
    through the plugin pipeline -- parity with the sync wrapper -- so the
    read/write-splitting plugin can swap reader/writer connections. A bare
    passthrough to the target would bypass every plugin and RWS would never
    switch."""
    target = MagicMock()
    target.closed = False  # not closed -> passes the is_closed guard
    wrapper = _async_wrapper(target)
    wrapper._plugin_manager.execute = AsyncMock(return_value=None)  # type: ignore[method-assign]
    asyncio.run(wrapper.set_read_only(True))
    wrapper._plugin_manager.execute.assert_awaited_once()
    # Routed under CONNECTION_SET_READ_ONLY with the value as the trailing arg.
    await_call = wrapper._plugin_manager.execute.await_args
    assert await_call is not None
    await_args = await_call.args
    assert await_args[1] == DbApiMethod.CONNECTION_SET_READ_ONLY
    assert await_args[3] is True


# ---- Plugin-chain bypass assertions -------------------------------------


@pytest.mark.parametrize("call", [
    ("info",), ("broken",), ("adapters",), ("fileno",), ("cancel",),
    ("pipeline",), ("notifies",), ("xid", 1, "g", "b"),
])
def test_sync_wrapper_passthroughs_bypass_plugin_chain(call) -> None:
    """Property / method accessors that reflect local client state
    must never call through the plugin pipeline."""
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    name, *args = call
    attr = getattr(wrapper, name)
    if callable(attr):
        attr(*args)
    wrapper._plugin_manager.execute.assert_not_called()  # type: ignore[attr-defined]
