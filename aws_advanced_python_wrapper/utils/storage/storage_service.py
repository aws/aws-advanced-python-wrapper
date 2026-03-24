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
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, TypeVar

from aws_advanced_python_wrapper.utils.events import (DataAccessEvent,
                                                      EventPublisher)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.storage.sliding_expiration_cache import \
    SlidingExpirationCache

if TYPE_CHECKING:
    from datetime import timedelta

logger = Logger(__name__)

V = TypeVar('V')


class StorageService:
    """Instance-based typed key-value cache with expiration and event publishing."""

    def __init__(self, event_publisher: EventPublisher) -> None:
        self._event_publisher = event_publisher
        self._caches: Dict[type, SlidingExpirationCache] = {}
        self._lock = threading.Lock()

    def register(
            self,
            item_type: type,
            item_expiration_time: timedelta,
            should_dispose: Optional[Callable] = None,
            on_dispose: Optional[Callable] = None) -> None:
        with self._lock:
            if item_type not in self._caches:
                item_expiration_ns = int(item_expiration_time.total_seconds() * 1_000_000_000)
                self._caches[item_type] = SlidingExpirationCache(
                    cleanup_interval_ns=item_expiration_ns,
                    should_dispose_func=should_dispose,
                    item_disposal_func=on_dispose)

    def get(self, item_type: Type[V], key: Any) -> Optional[V]:
        cache = self._caches.get(item_type)
        if cache is None:
            return None
        value = cache.get(key)
        if value is not None:
            self._event_publisher.publish(DataAccessEvent(data_type=item_type, key=key))
        return value

    def get_all(self, item_type: Type[V]) -> Optional[SlidingExpirationCache]:
        return self._caches.get(item_type)

    def put(self, item_type: Type[V], key: Any, value: V, item_expiration_ns: Optional[int] = None) -> None:
        cache = self._caches.get(item_type)
        if cache is None:
            raise ValueError(f"Type {item_type} is not registered with StorageService")
        if item_expiration_ns is None:
            item_expiration_ns = cache._cleanup_interval_ns
        cache.put(key, value, item_expiration_ns)

    def exists(self, item_type: type, key: Any) -> bool:
        cache = self._caches.get(item_type)
        return cache is not None and cache.get(key) is not None

    def remove(self, item_type: type, key: Any) -> None:
        cache = self._caches.get(item_type)
        if cache is not None:
            cache.remove(key)

    def clear(self, item_type: type) -> None:
        cache = self._caches.get(item_type)
        if cache is not None:
            cache.clear()

    def size(self, item_type: type) -> int:
        cache = self._caches.get(item_type)
        return len(cache) if cache is not None else 0

    def clear_all(self) -> None:
        for cache in self._caches.values():
            cache.clear()

    def release_resources(self) -> None:
        self.clear_all()
        with self._lock:
            self._caches.clear()
