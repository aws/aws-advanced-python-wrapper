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


from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Protocol

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.states.session_state import SessionState
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

logger = Logger(__name__)


class SessionStateService(Protocol):

    def get_autocommit(self) -> Optional[bool]:
        ...

    def set_autocommit(self, autocommit: bool):
        ...

    def setup_pristine_autocommit(self, autocommit: Optional[bool] = None):
        ...

    def get_readonly(self):
        ...

    def set_read_only(self, readonly: bool):
        ...

    def setup_pristine_readonly(self, readonly: Optional[bool] = None):
        ...

    def reset(self):
        ...

    def begin(self):
        """
        Begin session transfer process
        """
        ...

    def complete(self):
        """
        Complete session transfer process. This method should be called despite whether session transfer is successful or not.
        """
        ...

    def apply_current_session_state(self, new_connection: Connection):
        ...

    def apply_pristine_session_state(self, new_connection: Connection):
        ...


class SessionStateTransferHandlers:
    reset_session_state_on_close_callable: Optional[Callable] = None
    transfer_session_state_on_switch_callable: Optional[Callable] = None

    @staticmethod
    def set_reset_session_state_on_close_func(func: Callable):
        SessionStateTransferHandlers.reset_session_state_on_close_callable = func

    @staticmethod
    def clear_reset_session_state_on_close_func():
        SessionStateTransferHandlers.reset_session_state_on_close_callable = None

    @staticmethod
    def get_reset_session_state_on_close_func() -> Optional[Callable]:
        return SessionStateTransferHandlers.reset_session_state_on_close_callable

    @staticmethod
    def get_transfer_session_state_on_switch_func() -> Optional[Callable]:
        return SessionStateTransferHandlers.transfer_session_state_on_switch_callable

    @staticmethod
    def set_transfer_session_state_on_switch_func(func: Callable):
        SessionStateTransferHandlers.transfer_session_state_on_switch_callable = func

    @staticmethod
    def reset_transfer_session_state_on_switch_func():
        SessionStateTransferHandlers.reset_transfer_session_state_on_switch_callable = None


class SessionStateServiceImpl(SessionStateService):
    def __init__(self, plugin_service: PluginService, props: Properties):
        self._session_state: SessionState = SessionState()
        self._copy_session_state: Optional[SessionState] = None
        self._plugin_service: PluginService = plugin_service
        self._props: Properties = props

    def log_current_state(self):
        logger.debug(f"Current session state: \n{self._session_state}")

    def _transfer_state_enabled_setting(self) -> bool:
        return WrapperProperties.TRANSFER_SESSION_STATE_ON_SWITCH.get_bool(self._props)

    def _reset_state_enabled_setting(self) -> bool:
        return WrapperProperties.RESET_SESSION_STATE_ON_CLOSE.get_bool(self._props)

    def get_autocommit(self) -> Optional[bool]:
        return self._session_state.auto_commit.value

    def set_autocommit(self, autocommit: bool):
        if not self._transfer_state_enabled_setting():
            return
        self._session_state.auto_commit.value = autocommit

    def setup_pristine_autocommit(self, autocommit: Optional[bool] = None):
        if not self._transfer_state_enabled_setting():
            return
        if self._session_state.auto_commit.pristine_value is not None:
            return

        if autocommit is None and self._plugin_service.current_connection is not None:
            autocommit = self._plugin_service.driver_dialect.get_autocommit(self._plugin_service.current_connection)

        self._session_state.auto_commit.pristine_value = autocommit
        self.log_current_state()

    def get_readonly(self):
        return self._session_state.readonly.value

    def set_read_only(self, readonly: bool):
        if not self._transfer_state_enabled_setting():
            return
        self._session_state.readonly.value = readonly

    def setup_pristine_readonly(self, readonly: Optional[bool] = None):
        if not self._transfer_state_enabled_setting():
            return
        if self._session_state.readonly.pristine_value is not None:
            return

        if readonly is None and self._plugin_service.current_connection is not None:
            readonly = self._plugin_service.driver_dialect.is_read_only(self._plugin_service.current_connection)

        self._session_state.readonly.pristine_value = readonly
        self.log_current_state()

    def reset(self):
        self._session_state.auto_commit.reset()
        self._session_state.readonly.reset()

    def begin(self):
        self.log_current_state()
        if not self._transfer_state_enabled_setting() and not self._reset_state_enabled_setting():
            return
        if self._copy_session_state is not None:
            raise AwsWrapperError("Previous session state transfer is not completed.")
        self._copy_session_state = self._session_state.copy()

    def complete(self):
        self._copy_session_state = None

    def apply_current_session_state(self, new_connection: Connection):
        if not self._transfer_state_enabled_setting():
            return

        func: Optional[Callable] = SessionStateTransferHandlers.get_transfer_session_state_on_switch_func()
        if func is not None:
            is_handled: bool = func(self._session_state, new_connection)
            if is_handled:
                # Custom function has handled session transfer
                return

        if self._session_state.auto_commit.value is not None:
            self._session_state.auto_commit.reset_pristine_value()
            self.setup_pristine_autocommit()
            self._plugin_service.driver_dialect.set_autocommit(new_connection, self._session_state.auto_commit.value)

        if self._session_state.readonly.value is not None:
            self._session_state.readonly.reset_pristine_value()
            self.setup_pristine_readonly()
            self._plugin_service.driver_dialect.set_read_only(new_connection, self._session_state.readonly.value)

    def apply_pristine_session_state(self, new_connection: Connection):
        if not self._transfer_state_enabled_setting():
            return

        func: Optional[Callable] = SessionStateTransferHandlers.get_transfer_session_state_on_switch_func()
        if func is not None:
            is_handled: bool = func(self._session_state, new_connection)
            if is_handled:
                # Custom function has handled session transfer
                return
        if self._copy_session_state is None:
            return

        if self._copy_session_state.auto_commit.can_restore_pristine():
            try:
                self._plugin_service.driver_dialect.set_autocommit(new_connection, self._copy_session_state.auto_commit.pristine_value)
            except Exception:
                # Ignore any exception.
                pass

        if self._copy_session_state.readonly.can_restore_pristine():
            try:
                self._plugin_service.driver_dialect.set_read_only(new_connection, self._copy_session_state.readonly.pristine_value)
            except Exception:
                # Ignore any exception.
                pass
