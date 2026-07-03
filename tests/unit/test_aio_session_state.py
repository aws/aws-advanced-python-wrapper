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

"""Tests for AsyncSessionStateServiceImpl (K.2)."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import pytest

from aws_advanced_python_wrapper.aio.session_state import (
    AsyncSessionStateServiceImpl, AsyncSessionStateTransferHandlers,
    SessionState)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


class _FakeDriverDialect:
    """Minimal driver-dialect stub tracking set_* calls."""

    def __init__(self, autocommit: bool = True,
                 read_only: bool = False) -> None:
        self._autocommit = autocommit
        self._read_only = read_only
        self.set_autocommit_calls: list[bool] = []
        self.set_read_only_calls: list[bool] = []

    async def get_autocommit(self, conn: Any) -> bool:
        return self._autocommit

    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        self.set_autocommit_calls.append(autocommit)
        self._autocommit = autocommit

    async def is_read_only(self, conn: Any) -> bool:
        return self._read_only

    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        self.set_read_only_calls.append(read_only)
        self._read_only = read_only


class _FakePluginService:
    def __init__(self, conn: Any, dialect: _FakeDriverDialect) -> None:
        self.current_connection = conn
        self.driver_dialect = dialect


@pytest.fixture(autouse=True)
def _reset_handlers():
    AsyncSessionStateTransferHandlers.clear_transfer_session_state_on_switch_func()
    AsyncSessionStateTransferHandlers.clear_reset_session_state_on_close_func()
    yield
    AsyncSessionStateTransferHandlers.clear_transfer_session_state_on_switch_func()
    AsyncSessionStateTransferHandlers.clear_reset_session_state_on_close_func()


def _make_service(
        autocommit: bool = True,
        read_only: bool = False,
        transfer_enabled: bool = True,
        reset_enabled: bool = False) -> tuple[
        AsyncSessionStateServiceImpl, _FakeDriverDialect, _FakePluginService]:
    conn = object()
    dialect = _FakeDriverDialect(autocommit=autocommit, read_only=read_only)
    svc = _FakePluginService(conn, dialect)
    props = Properties({
        WrapperProperties.TRANSFER_SESSION_STATE_ON_SWITCH.name:
            "true" if transfer_enabled else "false",
        WrapperProperties.RESET_SESSION_STATE_ON_CLOSE.name:
            "true" if reset_enabled else "false",
    })
    return AsyncSessionStateServiceImpl(svc, props), dialect, svc  # type: ignore[arg-type]


# ----- basic state tracking --------------------------------------------


def test_set_autocommit_stored_when_transfer_enabled() -> None:
    state, _, _ = _make_service(transfer_enabled=True)
    state.set_autocommit(False)
    assert state.get_autocommit() is False


def test_set_autocommit_noop_when_transfer_disabled() -> None:
    state, _, _ = _make_service(transfer_enabled=False)
    state.set_autocommit(False)
    assert state.get_autocommit() is None


def test_set_read_only_stored_when_transfer_enabled() -> None:
    state, _, _ = _make_service(transfer_enabled=True)
    state.set_read_only(True)
    assert state.get_readonly() is True


def test_set_read_only_noop_when_transfer_disabled() -> None:
    state, _, _ = _make_service(transfer_enabled=False)
    state.set_read_only(True)
    assert state.get_readonly() is None


# ----- pristine capture -------------------------------------------------


def test_setup_pristine_captures_from_connection() -> None:
    state, _, _ = _make_service(autocommit=False, read_only=True)

    async def _body():
        await state.setup_pristine_autocommit()
        await state.setup_pristine_readonly()

    asyncio.run(_body())
    assert state._session_state.auto_commit.pristine_value is False
    assert state._session_state.readonly.pristine_value is True


def test_setup_pristine_noop_when_already_set() -> None:
    state, _, _ = _make_service(autocommit=True)

    async def _body():
        await state.setup_pristine_autocommit(autocommit=False)
        # Second call should be a no-op.
        await state.setup_pristine_autocommit(autocommit=True)

    asyncio.run(_body())
    assert state._session_state.auto_commit.pristine_value is False


# ----- begin / complete / reset ---------------------------------------


def test_begin_snapshots_state() -> None:
    state, _, _ = _make_service()
    state.set_autocommit(False)
    state.begin()
    # Snapshot captured; subsequent mutations shouldn't affect the copy.
    state.set_autocommit(True)
    assert state._copy_session_state is not None
    assert state._copy_session_state.auto_commit.value is False


def test_begin_twice_without_complete_raises() -> None:
    state, _, _ = _make_service()
    state.begin()
    with pytest.raises(AwsWrapperError):
        state.begin()


def test_complete_clears_snapshot() -> None:
    state, _, _ = _make_service()
    state.begin()
    state.complete()
    assert state._copy_session_state is None


def test_reset_clears_current_state() -> None:
    state, _, _ = _make_service()
    state.set_autocommit(False)
    state.set_read_only(True)
    state.reset()
    assert state.get_autocommit() is None
    assert state.get_readonly() is None


# ----- apply ----------------------------------------------------------


def test_apply_current_session_state_calls_dialect_setters() -> None:
    state, dialect, _ = _make_service(transfer_enabled=True)
    state.set_autocommit(False)
    state.set_read_only(True)
    new_conn = object()

    asyncio.run(state.apply_current_session_state(new_conn))

    assert dialect.set_autocommit_calls == [False]
    assert dialect.set_read_only_calls == [True]


def test_apply_current_session_state_noop_when_disabled() -> None:
    state, dialect, _ = _make_service(transfer_enabled=False)
    new_conn = object()

    asyncio.run(state.apply_current_session_state(new_conn))

    assert dialect.set_autocommit_calls == []
    assert dialect.set_read_only_calls == []


def test_apply_pristine_session_state_restores_pristine() -> None:
    state, dialect, _ = _make_service(
        autocommit=True, read_only=False, transfer_enabled=True)
    # Pre-populate pristine and mutate current -> can_restore_pristine
    # returns True on begin's copy.
    asyncio.run(state.setup_pristine_autocommit(autocommit=True))
    asyncio.run(state.setup_pristine_readonly(readonly=False))
    state.set_autocommit(False)
    state.set_read_only(True)
    state.begin()
    # Between begin and apply, the snapshot is what gets restored.

    new_conn = object()
    asyncio.run(state.apply_pristine_session_state(new_conn))

    assert dialect.set_autocommit_calls == [True]
    assert dialect.set_read_only_calls == [False]


# ----- custom transfer hook -----------------------------------------


def test_custom_transfer_hook_short_circuits_when_returning_true() -> None:
    state, dialect, _ = _make_service()
    state.set_autocommit(False)
    hook_calls = []

    def my_hook(session_state: SessionState, conn: Any) -> bool:
        hook_calls.append(conn)
        return True

    AsyncSessionStateTransferHandlers.set_transfer_session_state_on_switch_func(
        my_hook)

    new_conn = object()
    asyncio.run(state.apply_current_session_state(new_conn))

    assert hook_calls == [new_conn]
    assert dialect.set_autocommit_calls == []


def test_custom_transfer_hook_supports_async() -> None:
    state, dialect, _ = _make_service()
    state.set_autocommit(False)

    async def my_hook(session_state: SessionState, conn: Any) -> bool:
        return True

    AsyncSessionStateTransferHandlers.set_transfer_session_state_on_switch_func(
        my_hook)

    asyncio.run(state.apply_current_session_state(object()))

    assert dialect.set_autocommit_calls == []


def test_custom_transfer_hook_falls_through_when_returning_false() -> None:
    state, dialect, _ = _make_service()
    state.set_autocommit(False)

    def my_hook(session_state: SessionState, conn: Any) -> bool:
        return False

    AsyncSessionStateTransferHandlers.set_transfer_session_state_on_switch_func(
        my_hook)

    asyncio.run(state.apply_current_session_state(object()))

    # Hook returned False, so the default path ran.
    assert dialect.set_autocommit_calls == [False]


def test_apply_pristine_swallows_dialect_errors() -> None:
    """Pristine restore is best-effort; a failing set_autocommit shouldn't raise."""
    state, dialect, _ = _make_service(transfer_enabled=True)

    asyncio.run(state.setup_pristine_autocommit(autocommit=True))
    state.set_autocommit(False)
    state.begin()

    async def boom(conn: Any, val: Optional[bool]) -> None:
        raise RuntimeError("driver busy")

    dialect.set_autocommit = boom  # type: ignore[assignment]
    # Must not raise.
    asyncio.run(state.apply_pristine_session_state(object()))


# ----- PluginService integration ----------------------------------


def test_plugin_service_exposes_session_state_service() -> None:
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl

    class _Noop:
        network_bound_methods: set[str] = set()

        async def transfer_session_state(self, a, b):
            pass

    props = Properties({
        WrapperProperties.TRANSFER_SESSION_STATE_ON_SWITCH.name: "true",
    })
    svc = AsyncPluginServiceImpl(props, _Noop())  # type: ignore[arg-type]
    assert svc.session_state_service is not None
    # set_autocommit goes through, because transfer is enabled.
    svc.session_state_service.set_autocommit(False)
    assert svc.session_state_service.get_autocommit() is False
