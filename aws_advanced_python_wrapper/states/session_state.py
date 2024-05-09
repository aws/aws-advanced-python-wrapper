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

from typing import Generic, Optional, TypeVar

T = TypeVar("T")


class SessionStateField(Generic[T]):
    _value: Optional[T] = None
    _pristine_value: Optional[T] = None

    def copy(self):
        new_field: SessionStateField[T] = SessionStateField()
        if self._value is not None:
            new_field._value = self._value
        if self._pristine_value is not None:
            new_field._pristine_value = self._pristine_value
        return new_field

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value: Optional[T]):
        self._value = new_value

    @property
    def pristine_value(self):
        return self._pristine_value

    @pristine_value.setter
    def pristine_value(self, new_value: Optional[T]):
        self._pristine_value = new_value

    def reset_value(self):
        self._value = None

    def reset_pristine_value(self):
        self.pristine_value = None

    def reset(self):
        self.reset_value()
        self.reset_pristine_value()

    def is_pristine(self) -> bool:
        if self.value is None:
            # the value has never been set up so the session state has pristine value
            return True
        return self._pristine_value is not None and self._pristine_value == self.value

    def can_restore_pristine(self):
        if self._pristine_value is None:
            return False

        if self._value is not None:
            # it's necessary to restore pristine value only if current session value is not the same as pristine value.
            return self.value != self.pristine_value

        # it's inconclusive if the current value is the same as pristine value, so we need to take the safest path.
        return True

    def __str__(self):
        return f"{self.pristine_value if self.pristine_value is not None else '(blank)'} -> {self.value if self.value is not None else '(blank)'}"

    def __repr__(self):
        return f"{self.pristine_value if self.pristine_value is not None else '(blank)'} -> {self.value if self.value is not None else '(blank)'}"


class SessionState:
    def __init__(self):
        self.auto_commit: SessionStateField[bool] = SessionStateField()
        self.readonly: SessionStateField[bool] = SessionStateField()

    def copy(self):
        new_session_state: SessionState = SessionState()
        new_session_state.auto_commit = self.auto_commit.copy()
        new_session_state.readonly = self.readonly.copy()

        return new_session_state
