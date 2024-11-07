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

from concurrent.futures import Executor, ThreadPoolExecutor
from time import perf_counter_ns, sleep
from typing import Callable, Generic, ItemsView, KeysView, Optional, TypeVar

from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.log import Logger

K = TypeVar('K')
V = TypeVar('V')
logger = Logger(__name__)


class SlidingExpirationCache(Generic[K, V]):
    def __init__(
            self,
            cleanup_interval_ns: int = 10 * 60_000_000_000,  # 10 minutes
            should_dispose_func: Optional[Callable] = None,
            item_disposal_func: Optional[Callable] = None):
        self._cleanup_interval_ns = cleanup_interval_ns
        self._should_dispose_func = should_dispose_func
        self._item_disposal_func = item_disposal_func

        self._cdict: ConcurrentDict[K, CacheItem[V]] = ConcurrentDict()
        self._cleanup_time_ns: AtomicInt = AtomicInt(perf_counter_ns() + self._cleanup_interval_ns)

    def __len__(self):
        return len(self._cdict)

    def set_cleanup_interval_ns(self, interval_ns):
        self._cleanup_interval_ns = interval_ns

    def keys(self) -> KeysView:
        return self._cdict.keys()

    def items(self) -> ItemsView:
        return self._cdict.items()

    def compute_if_absent(self, key: K, mapping_func: Callable, item_expiration_ns: int) -> Optional[V]:
        self._cleanup()
        cache_item = self._cdict.compute_if_absent(
            key, lambda k: CacheItem(mapping_func(k), perf_counter_ns() + item_expiration_ns))
        return None if cache_item is None else cache_item.update_expiration(item_expiration_ns).item

    def get(self, key: K) -> Optional[V]:
        self._cleanup()
        cache_item = self._cdict.get(key)
        return cache_item.item if cache_item is not None else None

    def remove(self, key: K):
        self._remove_and_dispose(key)
        self._cleanup()

    def _remove_and_dispose(self, key: K):
        cache_item = self._cdict.remove(key)
        if cache_item is not None and self._item_disposal_func is not None:
            self._item_disposal_func(cache_item.item)

    def _remove_if_expired(self, key: K):
        item = None

        def _remove_if_expired_internal(_, cache_item):
            if self._should_cleanup_item(cache_item):
                nonlocal item
                item = cache_item.item
                return None

            return cache_item

        self._cdict.compute_if_present(key, _remove_if_expired_internal)

        if item is None or self._item_disposal_func is None:
            return

        self._item_disposal_func(item)

    def _should_cleanup_item(self, cache_item: CacheItem) -> bool:
        if self._should_dispose_func is not None:
            return perf_counter_ns() > cache_item.expiration_time and self._should_dispose_func(cache_item.item)
        return perf_counter_ns() > cache_item.expiration_time

    def clear(self):
        for _, cache_item in self._cdict.items():
            if cache_item is not None and self._item_disposal_func is not None:
                self._item_disposal_func(cache_item.item)
        self._cdict.clear()

    def _cleanup(self):
        current_time = perf_counter_ns()
        if self._cleanup_time_ns.get() > current_time:
            return

        self._cleanup_time_ns.set(current_time + self._cleanup_interval_ns)
        keys = [key for key, _ in self._cdict.items()]
        for key in keys:
            self._remove_if_expired(key)


class SlidingExpirationCacheWithCleanupThread(SlidingExpirationCache, Generic[K, V]):
    def __init__(
            self,
            cleanup_interval_ns: int = 10 * 60_000_000_000,  # 10 minutes
            should_dispose_func: Optional[Callable] = None,
            item_disposal_func: Optional[Callable] = None):
        super().__init__(cleanup_interval_ns, should_dispose_func, item_disposal_func)
        self._executor: Executor = ThreadPoolExecutor(thread_name_prefix="SlidingExpirationCacheWithCleanupThreadExecutor")
        self.init_cleanup_thread()

    def init_cleanup_thread(self) -> None:
        self._executor.submit(self._cleanup_thread_internal)

    def _cleanup_thread_internal(self):
        logger.debug("SlidingExpirationCache.CleaningUp")
        current_time = perf_counter_ns()
        sleep(self._cleanup_interval_ns / 1_000_000_000)
        self._cleanup_time_ns.set(current_time + self._cleanup_interval_ns)
        keys = [key for key, _ in self._cdict.items()]
        for key in keys:
            try:
                self._remove_if_expired(key)
            except Exception:
                pass  # ignore

        self._executor.shutdown()

    def _cleanup(self):
        pass  # do nothing, cleanup thread does the job


class CacheItem(Generic[V]):
    def __init__(self, item: V, expiration_time: int):
        self.item = item
        self.expiration_time = expiration_time

    def __str__(self):
        return f"CacheItem [item={str(self.item)}, expiration_time={self.expiration_time}]"

    def update_expiration(self, expiration_interval_ns: int) -> CacheItem:
        self.expiration_time = perf_counter_ns() + expiration_interval_ns
        return self
