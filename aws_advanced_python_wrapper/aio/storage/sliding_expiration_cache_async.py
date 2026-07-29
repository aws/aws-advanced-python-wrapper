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

"""Async-disposal-aware sliding-expiration cache.

Mirrors :class:`aws_advanced_python_wrapper.utils.storage.sliding_expiration_cache.SlidingExpirationCache`
in shape and semantics. Differences:

* ``compute_if_absent`` is ``async def`` because the mapping function
  returns an awaitable.
* ``remove`` / ``clear`` / ``cleanup`` are ``async def`` because the
  disposal callback is async (closes pooled async connections).
* ``should_dispose_func`` stays sync because it's a quick predicate.

Cross-loop usage is callers' responsibility — the cache itself uses
``asyncio.Lock`` for in-loop write serialization.
"""

from __future__ import annotations

import asyncio
from time import perf_counter_ns
from typing import Awaitable, Callable, Generic, List, Optional, Tuple, TypeVar

from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.log import Logger

K = TypeVar("K")
V = TypeVar("V")
logger = Logger(__name__)


class CacheItem(Generic[V]):
    def __init__(self, item: V, expiration_time: int):
        self.item = item
        self.expiration_time = expiration_time

    def __str__(self) -> str:
        return f"CacheItem [item={self.item!s}, expiration_time={self.expiration_time}]"

    def update_expiration(self, expiration_interval_ns: int) -> CacheItem[V]:
        self.expiration_time = perf_counter_ns() + expiration_interval_ns
        return self


class AsyncSlidingExpirationCache(Generic[K, V]):
    def __init__(
            self,
            cleanup_interval_ns: int = 10 * 60_000_000_000,  # 10 minutes
            should_dispose_func: Optional[Callable[[V], bool]] = None,
            item_disposal_func: Optional[Callable[[V], Awaitable[None]]] = None,
    ):
        self._cleanup_interval_ns = cleanup_interval_ns
        self._should_dispose_func = should_dispose_func
        self._item_disposal_func = item_disposal_func

        self._cdict: ConcurrentDict[K, CacheItem[V]] = ConcurrentDict()
        self._cleanup_time_ns: AtomicInt = AtomicInt(perf_counter_ns() + cleanup_interval_ns)
        self._write_lock: asyncio.Lock = asyncio.Lock()

    def __len__(self) -> int:
        return len(self._cdict)

    def __contains__(self, key: K) -> bool:
        return key in self._cdict

    def set_cleanup_interval_ns(self, interval_ns: int) -> None:
        self._cleanup_interval_ns = interval_ns

    def keys(self) -> List[K]:
        return self._cdict.keys()

    def items(self) -> List[Tuple[K, CacheItem[V]]]:
        return self._cdict.items()

    def get(self, key: K) -> Optional[V]:
        cache_item = self._cdict.get(key)
        return cache_item.item if cache_item is not None else None

    async def compute_if_absent(
            self,
            key: K,
            mapping_func: Callable[[K], Awaitable[V]],
            item_expiration_ns: int,
    ) -> Optional[V]:
        await self.cleanup()
        async with self._write_lock:
            existing = self._cdict.get(key)
            if existing is not None:
                return existing.update_expiration(item_expiration_ns).item
            new_value: V = await mapping_func(key)
            new_item = CacheItem(new_value, perf_counter_ns() + item_expiration_ns)
            self._cdict.put(key, new_item)
            return new_value

    async def put(self, key: K, value: V, item_expiration_ns: int) -> None:
        await self.cleanup()
        async with self._write_lock:
            old = self._cdict.remove(key)
            if old is not None and self._item_disposal_func is not None:
                await self._item_disposal_func(old.item)
            self._cdict.put(key, CacheItem(value, perf_counter_ns() + item_expiration_ns))

    async def remove(self, key: K) -> None:
        async with self._write_lock:
            cache_item = self._cdict.remove(key)
        if cache_item is not None and self._item_disposal_func is not None:
            await self._item_disposal_func(cache_item.item)
        await self.cleanup()

    async def clear(self) -> None:
        async with self._write_lock:
            items = self._cdict.items()
            self._cdict.clear()
        if self._item_disposal_func is not None:
            for _, cache_item in items:
                try:
                    await self._item_disposal_func(cache_item.item)
                except Exception:  # noqa: BLE001 - best-effort teardown
                    pass

    async def cleanup(self) -> None:
        current_time = perf_counter_ns()
        if self._cleanup_time_ns.get() > current_time:
            return
        self._cleanup_time_ns.set(current_time + self._cleanup_interval_ns)

        to_dispose: List[V] = []
        async with self._write_lock:
            for key in self._cdict.keys():
                cache_item = self._cdict.remove_key_if(key, self._should_cleanup_item)
                if cache_item is not None:
                    to_dispose.append(cache_item.item)
        for item in to_dispose:
            if self._item_disposal_func is not None:
                try:
                    await self._item_disposal_func(item)
                except Exception:  # noqa: BLE001 - best-effort teardown
                    pass

    def _should_cleanup_item(self, cache_item: CacheItem[V]) -> bool:
        if self._should_dispose_func is not None:
            return perf_counter_ns() > cache_item.expiration_time and self._should_dispose_func(cache_item.item)
        return perf_counter_ns() > cache_item.expiration_time
