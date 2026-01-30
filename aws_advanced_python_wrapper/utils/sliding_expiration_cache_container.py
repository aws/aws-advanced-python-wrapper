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

import threading
from threading import Event, Thread
from typing import Callable, Dict, Optional

from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCache

logger = Logger(__name__)


class SlidingExpirationCacheContainer:
    """
    A container class for managing multiple named sliding expiration caches.
    Provides static methods for getting, creating, and releasing caches.

    This container manages SlidingExpirationCache instances and provides a single
    cleanup thread that periodically cleans up all managed caches.
    """

    _caches: Dict[str, SlidingExpirationCache] = {}
    _lock: threading.Lock = threading.Lock()
    _cleanup_thread: Optional[Thread] = None
    _cleanup_interval_ns: int = 300_000_000_000  # 5 minutes default
    _is_stopped: Event = Event()

    @classmethod
    def get_or_create_cache(
        cls,
        name: str,
        cleanup_interval_ns: int = 10 * 60_000_000_000,  # 10 minutes
        should_dispose_func: Optional[Callable] = None,
        item_disposal_func: Optional[Callable] = None
    ) -> SlidingExpirationCache:
        """
        Get an existing cache or create a new one if it doesn't exist.

        The cleanup thread is started lazily when the first cache is created.

        Args:
            name: Unique identifier for the cache
            cleanup_interval_ns: Cleanup interval in nanoseconds (only used when creating new cache)
            should_dispose_func: Optional function to determine if item should be disposed
            item_disposal_func: Optional function to dispose items

        Returns:
            SlidingExpirationCache instance
        """
        with cls._lock:
            if name not in cls._caches:
                cls._caches[name] = SlidingExpirationCache(
                    cleanup_interval_ns=cleanup_interval_ns,
                    should_dispose_func=should_dispose_func,
                    item_disposal_func=item_disposal_func
                )

                # Start cleanup thread if not already running
                if cls._cleanup_thread is None or not cls._cleanup_thread.is_alive():
                    cls._is_stopped.clear()
                    cls._cleanup_thread = Thread(
                        target=cls._cleanup_thread_internal,
                        daemon=True,
                        name="SlidingExpirationCacheContainer-Cleanup"
                    )
                    cls._cleanup_thread.start()

            return cls._caches[name]

    @classmethod
    def release_resources(cls) -> None:
        """
        Clear all caches and stop the cleanup thread.
        This will dispose all cached items and release all resources.
        """
        with cls._lock:
            # Stop the cleanup thread
            cls._is_stopped.set()

            # Clear all caches (will dispose items if disposal function is set)
            for name, cache in cls._caches.items():
                try:
                    cache.clear()
                except Exception as e:
                    logger.warning("SlidingExpirationCacheContainer.ErrorReleasingCache", name, e)

            cls._caches.clear()

        # Wait for cleanup thread to stop (outside the lock)
        if cls._cleanup_thread is not None and cls._cleanup_thread.is_alive():
            cls._cleanup_thread.join(timeout=2.0)
            cls._cleanup_thread = None

    @classmethod
    def _cleanup_thread_internal(cls) -> None:
        while not cls._is_stopped.is_set():
            # Wait for the cleanup interval or until stopped
            if cls._is_stopped.wait(timeout=cls._cleanup_interval_ns / 1_000_000_000):
                break

            # Cleanup all caches
            with cls._lock:
                cache_items = list(cls._caches.items())

            for name, cache in cache_items:
                try:
                    cache.cleanup()
                except Exception as e:
                    logger.debug("SlidingExpirationCacheContainer.ErrorDuringCleanup", name, e)
