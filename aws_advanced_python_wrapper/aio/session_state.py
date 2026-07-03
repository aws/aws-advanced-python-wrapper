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

"""Async session-state service + state container (K.2).

Port of :mod:`aws_advanced_python_wrapper.states.session_state` +
:mod:`aws_advanced_python_wrapper.states.session_state_service`.
Async-flavored because the driver-dialect getters/setters are
awaitable.

:class:`SessionStateField` and :class:`SessionState` are reused
verbatim from the sync module -- they hold values only, no I/O --
so importing them here keeps state-shape parity across sync/async.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Protocol

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.states.session_state import (
    SessionState, SessionStateField)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService

__all__ = [
    "SessionStateField",
    "SessionState",
    "AsyncSessionStateService",
    "AsyncSessionStateServiceImpl",
    "AsyncSessionStateTransferHandlers",
]

logger = Logger(__name__)


class AsyncSessionStateService(Protocol):
    """Async counterpart of :class:`SessionStateService`.

    State getters stay sync (in-memory); anything that touches the
    driver (pristine capture, apply) is async.
    """

    def get_autocommit(self) -> Optional[bool]:
        ...

    def set_autocommit(self, autocommit: bool) -> None:
        ...

    async def setup_pristine_autocommit(
            self, autocommit: Optional[bool] = None) -> None:
        ...

    def get_readonly(self) -> Optional[bool]:
        ...

    def set_read_only(self, readonly: bool) -> None:
        ...

    async def setup_pristine_readonly(
            self, readonly: Optional[bool] = None) -> None:
        ...

    def reset(self) -> None:
        ...

    def begin(self) -> None:
        ...

    def complete(self) -> None:
        ...

    async def apply_current_session_state(self, new_connection: Any) -> None:
        ...

    async def apply_pristine_session_state(self, new_connection: Any) -> None:
        ...


class AsyncSessionStateTransferHandlers:
    """Class-level hooks consumers can install to override transfer.

    Mirrors sync's :class:`SessionStateTransferHandlers`. Hooks are
    awaited if they return awaitables, so users can supply either
    sync or async callables.
    """

    reset_session_state_on_close_callable: ClassVar[Optional[Callable]] = None
    transfer_session_state_on_switch_callable: ClassVar[Optional[Callable]] = None

    @staticmethod
    def set_reset_session_state_on_close_func(func: Callable) -> None:
        AsyncSessionStateTransferHandlers.reset_session_state_on_close_callable = func

    @staticmethod
    def clear_reset_session_state_on_close_func() -> None:
        AsyncSessionStateTransferHandlers.reset_session_state_on_close_callable = None

    @staticmethod
    def get_reset_session_state_on_close_func() -> Optional[Callable]:
        return AsyncSessionStateTransferHandlers.reset_session_state_on_close_callable

    @staticmethod
    def set_transfer_session_state_on_switch_func(func: Callable) -> None:
        AsyncSessionStateTransferHandlers.transfer_session_state_on_switch_callable = func

    @staticmethod
    def clear_transfer_session_state_on_switch_func() -> None:
        AsyncSessionStateTransferHandlers.transfer_session_state_on_switch_callable = None

    @staticmethod
    def get_transfer_session_state_on_switch_func() -> Optional[Callable]:
        return AsyncSessionStateTransferHandlers.transfer_session_state_on_switch_callable


async def _maybe_await(result: Any) -> Any:
    """Await ``result`` if it's awaitable; return it as-is otherwise.

    Lets consumers install either sync or async hooks on
    :class:`AsyncSessionStateTransferHandlers`.
    """
    if hasattr(result, "__await__"):
        return await result
    return result


class AsyncSessionStateServiceImpl(AsyncSessionStateService):
    """Concrete :class:`AsyncSessionStateService`.

    Identical semantics to sync :class:`SessionStateServiceImpl`:
      * ``set_*`` methods no-op when transfer is disabled via
        ``TRANSFER_SESSION_STATE_ON_SWITCH`` property.
      * ``setup_pristine_*`` captures pristine state from the current
        connection the first time it's called.
      * ``begin`` snapshots state before a switch; ``complete`` clears
        the snapshot.
      * ``apply_current_session_state`` applies captured state to a new
        connection; ``apply_pristine_session_state`` restores pristine.
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._session_state: SessionState = SessionState()
        self._copy_session_state: Optional[SessionState] = None
        self._plugin_service = plugin_service
        self._props = props

    def log_current_state(self) -> None:
        logger.debug(f"Current session state: \n{self._session_state}")

    def _transfer_state_enabled_setting(self) -> bool:
        return bool(WrapperProperties.TRANSFER_SESSION_STATE_ON_SWITCH.get_bool(self._props))

    def _reset_state_enabled_setting(self) -> bool:
        return bool(WrapperProperties.RESET_SESSION_STATE_ON_CLOSE.get_bool(self._props))

    # ----- autocommit ---------------------------------------------------

    def get_autocommit(self) -> Optional[bool]:
        return self._session_state.auto_commit.value

    def set_autocommit(self, autocommit: bool) -> None:
        if not self._transfer_state_enabled_setting():
            return
        self._session_state.auto_commit.value = autocommit

    async def setup_pristine_autocommit(
            self, autocommit: Optional[bool] = None) -> None:
        if not self._transfer_state_enabled_setting():
            return
        if self._session_state.auto_commit.pristine_value is not None:
            return

        if autocommit is None and self._plugin_service.current_connection is not None:
            autocommit = await self._plugin_service.driver_dialect.get_autocommit(
                self._plugin_service.current_connection)

        self._session_state.auto_commit.pristine_value = autocommit
        self.log_current_state()

    # ----- read_only ----------------------------------------------------

    def get_readonly(self) -> Optional[bool]:
        return self._session_state.readonly.value

    def set_read_only(self, readonly: bool) -> None:
        if not self._transfer_state_enabled_setting():
            return
        self._session_state.readonly.value = readonly

    async def setup_pristine_readonly(
            self, readonly: Optional[bool] = None) -> None:
        if not self._transfer_state_enabled_setting():
            return
        if self._session_state.readonly.pristine_value is not None:
            return

        if readonly is None and self._plugin_service.current_connection is not None:
            readonly = await self._plugin_service.driver_dialect.is_read_only(
                self._plugin_service.current_connection)

        self._session_state.readonly.pristine_value = readonly
        self.log_current_state()

    # ----- lifecycle ----------------------------------------------------

    def reset(self) -> None:
        self._session_state.auto_commit.reset()
        self._session_state.readonly.reset()

    def begin(self) -> None:
        self.log_current_state()
        if (not self._transfer_state_enabled_setting()
                and not self._reset_state_enabled_setting()):
            return
        if self._copy_session_state is not None:
            raise AwsWrapperError(
                "Previous session state transfer is not completed.")
        self._copy_session_state = self._session_state.copy()

    def complete(self) -> None:
        self._copy_session_state = None

    # ----- apply --------------------------------------------------------

    async def apply_current_session_state(self, new_connection: Any) -> None:
        if not self._transfer_state_enabled_setting():
            return

        func: Optional[Callable] = (
            AsyncSessionStateTransferHandlers.get_transfer_session_state_on_switch_func())
        if func is not None:
            is_handled = await _maybe_await(func(self._session_state, new_connection))
            if is_handled:
                return

        dialect = self._plugin_service.driver_dialect

        if self._session_state.auto_commit.value is not None:
            self._session_state.auto_commit.reset_pristine_value()
            await self.setup_pristine_autocommit()
            await dialect.set_autocommit(
                new_connection, self._session_state.auto_commit.value)

        if self._session_state.readonly.value is not None:
            self._session_state.readonly.reset_pristine_value()
            await self.setup_pristine_readonly()
            await dialect.set_read_only(
                new_connection, self._session_state.readonly.value)

    async def apply_pristine_session_state(self, new_connection: Any) -> None:
        if not self._transfer_state_enabled_setting():
            return

        func: Optional[Callable] = (
            AsyncSessionStateTransferHandlers.get_transfer_session_state_on_switch_func())
        if func is not None:
            is_handled = await _maybe_await(func(self._session_state, new_connection))
            if is_handled:
                return
        if self._copy_session_state is None:
            return

        dialect = self._plugin_service.driver_dialect

        if self._copy_session_state.auto_commit.can_restore_pristine():
            try:
                await dialect.set_autocommit(
                    new_connection,
                    self._copy_session_state.auto_commit.pristine_value,
                )
            except Exception:  # noqa: BLE001 - best-effort restore
                pass

        if self._copy_session_state.readonly.can_restore_pristine():
            try:
                await dialect.set_read_only(
                    new_connection,
                    self._copy_session_state.readonly.pristine_value,
                )
            except Exception:  # noqa: BLE001 - best-effort restore
                pass
