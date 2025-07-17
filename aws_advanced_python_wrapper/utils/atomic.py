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

from threading import Lock
from typing import Generic, TypeVar

T = TypeVar('T')


class AtomicInt:
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock: Lock = Lock()

    def __str__(self):
        return f"AtomicInt[value={self._value}]"

    def get(self):
        with self._lock:
            return self._value

    def set(self, value: int):
        with self._lock:
            self._value = value

    def get_and_increment(self):
        with self._lock:
            value = self._value
            self._value += 1
            return value

    def increment_and_get(self):
        with self._lock:
            self._value += 1
            return self._value

    def get_and_decrement(self):
        with self._lock:
            value = self._value
            self._value -= 1
            return value

    def decrement_and_get(self):
        with self._lock:
            self._value -= 1
            return self._value

    def compare_and_set(self, expected_value: int, new_value: int) -> bool:
        with self._lock:
            if self._value == expected_value:
                self._value = new_value
                return True
            return False


class AtomicBoolean:
    def __init__(self, initial_value: bool):
        self._value: bool = initial_value
        self._lock: Lock = Lock()

    def get(self) -> bool:
        with self._lock:
            return self._value

    def set(self, value: bool) -> None:
        with self._lock:
            self._value = value


class AtomicReference(Generic[T]):
    def __init__(self, initial_value: T):
        self._value: T = initial_value
        self._lock: Lock = Lock()

    def get(self) -> T:
        with self._lock:
            return self._value

    def set(self, new_value: T) -> None:
        with self._lock:
            self._value = new_value
