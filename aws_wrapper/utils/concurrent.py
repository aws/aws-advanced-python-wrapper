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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import ItemsView

from threading import Lock
from typing import Callable, Generic, KeysView, List, Optional, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class ConcurrentDict(Generic[K, V]):
    def __init__(self):
        self._dict = dict()
        self._lock = Lock()

    def __len__(self):
        return len(self._dict)

    def get(self, key: K, default_value: Optional[V] = None) -> Optional[V]:
        return self._dict.get(key, default_value)

    def clear(self):
        self._dict.clear()

    def compute_if_present(self, key: K, remapping_func: Callable) -> Optional[V]:
        with self._lock:
            existing_value = self._dict.get(key)
            if existing_value is None:
                return None
            new_value = remapping_func(key, existing_value)
            if new_value is not None:
                self._dict[key] = new_value
                return new_value
            else:
                self._dict.pop(key, None)
                return None

    def compute_if_absent(self, key: K, mapping_func: Callable) -> Optional[V]:
        with self._lock:
            value = self._dict.get(key)
            if value is None:
                new_value = mapping_func(key)
                if new_value is not None:
                    self._dict[key] = new_value
                    return new_value
            return value

    def put_if_absent(self, key: K, new_value: V) -> V:
        with self._lock:
            existing_value = self._dict.get(key)
            if existing_value is None:
                self._dict[key] = new_value
                return new_value
            return existing_value

    def remove(self, key: K) -> V:
        with self._lock:
            return self._dict.pop(key, None)

    def remove_if(self, predicate: Callable) -> bool:
        with self._lock:
            original_len = len(self._dict)
            self._dict = {key: value for key, value in self._dict.items() if not predicate(key, value)}
            return len(self._dict) < original_len

    def remove_matching_values(self, removal_values: List[V]) -> bool:
        with self._lock:
            original_len = len(self._dict)
            self._dict = {key: value for key, value in self._dict.items() if value not in removal_values}
            return len(self._dict) < original_len

    def apply_if(self, predicate: Callable, apply: Callable):
        with self._lock:
            for key, value in self._dict.items():
                if predicate(key, value):
                    apply(key, value)

    def keys(self) -> KeysView:
        return self._dict.keys()

    def items(self) -> ItemsView:
        return self._dict.items()
