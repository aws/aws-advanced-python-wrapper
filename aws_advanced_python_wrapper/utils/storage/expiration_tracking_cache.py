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

from time import perf_counter_ns
from typing import Callable, FrozenSet, Generic, List, Optional, Tuple, TypeVar

from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict

K = TypeVar('K')
V = TypeVar('V')


class _CacheItem(Generic[V]):
    def __init__(self, item: V, expiration_ns: int) -> None:
        self.item = item
        self.expiration_ns = expiration_ns

    def is_expired(self) -> bool:
        return perf_counter_ns() > self.expiration_ns

    def extend(self, duration_ns: int) -> None:
        self.expiration_ns = perf_counter_ns() + duration_ns


class ExpirationTrackingCache(Generic[K, V]):
    """A cache that tracks expiration but never cleans itself up.
    Removal of expired entries is handled by an external caller."""

    def __init__(self, expiration_timeout_ns: int) -> None:
        self._expiration_timeout_ns = expiration_timeout_ns
        self._cache: ConcurrentDict[K, _CacheItem[V]] = ConcurrentDict()

    def __len__(self) -> int:
        return len(self._cache)

    def get(self, key: K) -> Optional[V]:
        entry = self._cache.get(key)
        if entry is None or entry.is_expired():
            return None
        return entry.item

    def put(self, key: K, value: V) -> Optional[V]:
        old = self._cache.remove(key)
        self._cache.put(key, _CacheItem(value, perf_counter_ns() + self._expiration_timeout_ns))
        return old.item if old is not None else None

    def compute_if_absent(self, key: K, factory: Callable[[K], V]) -> Optional[V]:
        entry = self._cache.compute_if_absent(
            key, lambda k: _CacheItem(factory(k), perf_counter_ns() + self._expiration_timeout_ns))
        if entry is None:
            return None
        entry.extend(self._expiration_timeout_ns)
        return entry.item

    def get_or_create_for_aliases(self, aliases: FrozenSet[K], factory: Callable[[], V]) -> V:
        entry = self._cache.compute_for_keys(
            aliases,
            lambda: _CacheItem(factory(), perf_counter_ns() + self._expiration_timeout_ns),
            on_existing=lambda e: e.extend(self._expiration_timeout_ns))
        return entry.item

    def extend_expiration(self, key: K) -> None:
        entry = self._cache.get(key)
        if entry is not None:
            entry.extend(self._expiration_timeout_ns)

    def remove(self, key: K) -> Optional[V]:
        entry = self._cache.remove(key)
        return entry.item if entry is not None else None

    def remove_expired_if(self, key: K, predicate: Callable[[V], bool]) -> Optional[V]:
        removed = self._cache.remove_key_if(
            key, lambda entry: entry.is_expired() and predicate(entry.item))
        return removed.item if removed is not None else None

    def detach_value(self, value: V) -> bool:
        for key, entry in self._cache.items():
            if entry.item is value:
                self._cache.remove(key)
                return True
        return False

    def items(self) -> List[Tuple[K, V]]:
        return [(k, entry.item) for k, entry in self._cache.items()]

    def clear(self) -> List[V]:
        values: List[V] = []
        self._cache.clear(lambda _k, entry: values.append(entry.item))
        return values
