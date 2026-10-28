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

import weakref
from dataclasses import dataclass
from threading import Event, Lock, Thread, current_thread
from typing import List, Optional

from aws_advanced_python_wrapper.encryption.buffers import BytesLike, wipe
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.storage.expiration_tracking_cache import \
    ExpirationTrackingCache

logger = Logger(__name__)


@dataclass(frozen=True)
class DataKeyCacheStats:
    size: int
    hits: int
    misses: int
    evictions: int

    @property
    def hit_rate(self) -> float:
        """Percentage of lookups served from the cache."""
        total = self.hits + self.misses
        return 100.0 * self.hits / total if total else 0.0


class DataKeyCache:
    """An in-memory cache of plaintext data keys, so reading an encrypted
    column does not need a KMS ``Decrypt`` call per statement.

    The keys are held in an ``ExpirationTrackingCache``, which fixes each
    key's expiry when it is stored; a cache hit does not extend it. This class
    adds a size bound with oldest-first eviction, and zeroes a key's bytes when
    it leaves the cache. Each ``get`` returns a fresh ``bytearray`` copy, so a
    caller wiping its copy cannot corrupt the cached one. An expired entry is
    never returned, even before the cleanup thread removes it.

    Call :meth:`close` when done with the cache. After ``close`` the cache
    stores nothing. A cache dropped without ``close`` still stops its cleanup
    thread and wipes its keys once it is garbage-collected, because the thread
    holds only a weak reference to it.

    Wiping is best effort: CPython may hold other copies of the key bytes.
    """

    MIN_CLEANUP_INTERVAL_SEC = 30.0
    SHUTDOWN_TIMEOUT_SEC = 5.0

    def __init__(self, max_size: int, ttl_sec: float, enabled: bool = True,
                 *, start_cleanup_thread: bool = True):
        self._max_size = max_size
        self._ttl_sec = ttl_sec
        self._enabled = enabled
        self._entries: ExpirationTrackingCache[str, bytearray] = ExpirationTrackingCache(
            int(ttl_sec * 1_000_000_000))
        self._lock = Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._closed = False
        self._stop = Event()
        self._cleanup_thread: Optional[Thread] = None
        # Holds the cleanup thread for the finalizer, which must not refer to
        # self, or the cache would never be collected.
        self._thread_holder: List[Optional[Thread]] = [None]
        # Runs at close() or when the cache is collected, whichever is first.
        self._finalizer = weakref.finalize(
            self, DataKeyCache._release, self._entries, self._lock, self._stop, self._thread_holder)
        if enabled and start_cleanup_thread:
            self._cleanup_thread = Thread(
                target=DataKeyCache._cleanup_loop, args=(weakref.ref(self), self._stop, self.cleanup_interval_sec),
                name="kms-encryption-data-key-cache-cleanup", daemon=True)
            self._thread_holder[0] = self._cleanup_thread
            self._cleanup_thread.start()

    @property
    def cleanup_interval_sec(self) -> float:
        return max(self._ttl_sec / 4, self.MIN_CLEANUP_INTERVAL_SEC)

    def get(self, key: str) -> Optional[bytearray]:
        """Return a copy of the cached data key, or ``None`` on a miss. The
        caller owns the copy and should wipe it once used."""
        if not self._enabled:
            return None
        with self._lock:
            if self._closed:
                return None
            data_key = self._entries.get(key)
            if data_key is None:
                # The store keeps an expired key until it is removed.
                self._discard(key)
                self._misses += 1
                return None
            self._hits += 1
            return bytearray(data_key)

    def put(self, key: str, data_key: BytesLike) -> None:
        """Store a copy of ``data_key``, evicting the oldest entry when full.
        The caller keeps ownership of the buffer it passed in. A closed cache
        stores nothing."""
        if not self._enabled:
            return
        copy = bytearray(data_key)
        with self._lock:
            if self._closed:
                wipe(copy)
                return
            self._discard(key)
            while len(self._entries) >= self._max_size:
                oldest, _ = self._entries.items()[0]
                self._discard(oldest)
                self._evictions += 1
            self._entries.put(key, copy)

    def remove_expired(self) -> int:
        """Drop every expired entry. Returns the number removed."""
        removed = 0
        with self._lock:
            for key, _ in self._entries.items():
                data_key = self._entries.remove_expired_if(key, lambda _: True)
                if data_key is not None:
                    wipe(data_key)
                    removed += 1
        return removed

    def stats(self) -> DataKeyCacheStats:
        with self._lock:
            return DataKeyCacheStats(len(self._entries), self._hits, self._misses, self._evictions)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def close(self) -> None:
        """Stop the cleanup thread and zero every cached key. Later ``put``
        calls store nothing. Idempotent.

        Call it from ordinary code, not from a finalizer or ``__del__``: a
        garbage collection on the cleanup thread can run inside its wait, where
        stopping the thread would deadlock.
        """
        with self._lock:
            self._closed = True
        self._stop.set()
        self._finalizer()
        thread = self._cleanup_thread
        if thread is not None and thread is not current_thread():
            thread.join(self.SHUTDOWN_TIMEOUT_SEC)
        self._cleanup_thread = None

    def _discard(self, key: str) -> None:
        # Callers hold the lock.
        data_key = self._entries.remove(key)
        if data_key is not None:
            wipe(data_key)

    @staticmethod
    def _release(entries: ExpirationTrackingCache[str, bytearray], lock: Lock, stop: Event,
                 thread_holder: List[Optional[Thread]]) -> None:
        # Runs at close() or when an unclosed cache is collected. Wipe first,
        # so the keys are zeroed whatever happens next. A collection can run
        # on the cleanup thread itself, inside Event.wait while it holds the
        # event's lock, where stop.set() would deadlock; that thread exits on
        # its own, finding the cache gone as soon as wait returns.
        with lock:
            for data_key in entries.clear():
                wipe(data_key)
        if thread_holder[0] is not current_thread():
            stop.set()

    @staticmethod
    def _cleanup_loop(cache_ref: weakref.ref[DataKeyCache], stop: Event, interval_sec: float) -> None:
        # Holds the cache only weakly between runs, so an unclosed cache can
        # still be collected. Event.wait returns as soon as close() (or the
        # finalizer) sets the event, so shutdown does not wait out an interval.
        while not stop.wait(interval_sec):
            cache = cache_ref()
            if cache is None:
                return
            try:
                cache.remove_expired()
            except Exception as e:
                logger.debug("DataKeyCache.CleanupFailed", type(e).__name__)
            del cache
