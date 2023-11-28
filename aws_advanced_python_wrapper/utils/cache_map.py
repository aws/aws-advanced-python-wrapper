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

import threading
import time
from typing import Dict, Generic, Optional, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class CacheMap(Generic[K, V]):
    def __init__(self):
        self._cache: Dict[K, CacheItem[V]] = {}
        self._cleanup_interval_ns: int = 600_000_000_000  # 10 minutes
        self._cleanup_time_ns: int = time.perf_counter_ns() + self._cleanup_interval_ns
        self._lock = threading.RLock()

    def __len__(self):
        return len(self._cache)

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            value = self._cache.get(key)
            if not value:
                return None

            if value.is_expired():
                self._cache.pop(key)
                return None

            return value.item

    def get_with_default(self, key: K, value_if_absent: V, item_expiration_ns: int) -> V:
        with self._lock:
            old_value = self._cache.get(key)
            if not old_value or old_value.is_expired():
                new_value = CacheItem(value_if_absent, time.perf_counter_ns() + item_expiration_ns)
            else:
                new_value = old_value

            if new_value is not None:
                self._cache[key] = new_value
                return new_value.item

            if key in self._cache:
                self._cache.pop(key)

            return None

    def put(self, key: K, item: V, item_expiration_ns: int):
        self._cache[key] = CacheItem(item, time.perf_counter_ns() + item_expiration_ns)
        self._cleanup()

    def remove(self, key: K):
        self._cache.pop(key, None)
        self._cleanup()

    def clear(self):
        self._cache.clear()

    def get_dict(self) -> Dict[K, V]:
        with self._lock:
            return {key: self._cache[key].item for key in self._cache.keys()}

    def _cleanup(self):
        if self._cleanup_time_ns > time.perf_counter_ns():
            return

        self._cleanup_time_ns = time.perf_counter_ns() + self._cleanup_interval_ns
        with self._lock:
            removal_keys = [key for key, cache_item in self._cache.items() if not cache_item or cache_item.is_expired()]
            for key in removal_keys:
                self._cache.pop(key)


class CacheItem(Generic[V]):
    def __init__(self, item: V, expiration_time: int):
        self.item = item
        self._expiration_time = expiration_time

    def __str__(self):
        return f"CacheItem [item={str(self.item)}, expiration_time={self._expiration_time}]"

    def is_expired(self) -> bool:
        return time.perf_counter_ns() > self._expiration_time
