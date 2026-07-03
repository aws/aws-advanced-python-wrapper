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

import asyncio
import time

from aws_advanced_python_wrapper.aio.storage.sliding_expiration_cache_async import (
    AsyncSlidingExpirationCache, CacheItem)


def test_compute_if_absent_creates_when_missing():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        calls = []

        async def factory(key):
            calls.append(key)
            return 42

        value = await cache.compute_if_absent("k", factory, item_expiration_ns=10**12)
        assert value == 42
        assert calls == ["k"]

    asyncio.run(inner())


def test_compute_if_absent_returns_existing_without_recomputing():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        calls = []

        async def factory(key):
            calls.append(key)
            return 99

        first = await cache.compute_if_absent("k", factory, 10**12)
        second = await cache.compute_if_absent("k", factory, 10**12)
        assert first == 99
        assert second == 99
        assert calls == ["k"]  # factory called only once

    asyncio.run(inner())


def test_compute_if_absent_extends_expiration_on_access():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()

        async def factory(key):
            return 1
        await cache.compute_if_absent("k", factory, item_expiration_ns=10**12)
        items = cache.items()
        first_expiration = items[0][1].expiration_time
        # Access again — expiration should advance
        await cache.compute_if_absent("k", factory, item_expiration_ns=2 * 10**12)
        items_after = cache.items()
        assert items_after[0][1].expiration_time > first_expiration

    asyncio.run(inner())


def test_remove_invokes_async_disposal_callback():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            item_disposal_func=disposal,
        )

        async def factory(key):
            return 7

        await cache.compute_if_absent("k", factory, 10**12)
        await cache.remove("k")
        assert disposed == [7]
        assert "k" not in cache

    asyncio.run(inner())


def test_clear_invokes_disposal_for_each_item():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            item_disposal_func=disposal,
        )

        async def factory(key):
            return ord(key)

        await cache.compute_if_absent("a", factory, 10**12)
        await cache.compute_if_absent("b", factory, 10**12)
        await cache.clear()
        assert sorted(disposed) == [ord("a"), ord("b")]
        assert len(cache) == 0

    asyncio.run(inner())


def test_cleanup_disposes_expired_items():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            cleanup_interval_ns=0,  # always cleanup
            item_disposal_func=disposal,
        )

        async def factory(key):
            return 5

        # Use a very short expiration so the entry is expired immediately
        await cache.compute_if_absent("k", factory, item_expiration_ns=1)
        # Sleep enough to surely exceed 1 ns
        time.sleep(0.001)
        # Trigger cleanup by inserting another key (cleanup runs inline)
        await cache.compute_if_absent("k2", factory, item_expiration_ns=10**12)
        assert disposed == [5]
        assert "k" not in cache
        assert "k2" in cache

    asyncio.run(inner())


def test_cleanup_skips_when_should_dispose_returns_false():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        def should_dispose(value):
            return False  # never dispose

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            cleanup_interval_ns=0,
            should_dispose_func=should_dispose,
            item_disposal_func=disposal,
        )

        async def factory(key):
            return 3

        await cache.compute_if_absent("k", factory, item_expiration_ns=1)
        time.sleep(0.001)
        await cache.compute_if_absent("k2", factory, item_expiration_ns=10**12)
        assert disposed == []
        assert "k" in cache  # not disposed because should_dispose returned False

    asyncio.run(inner())


def test_get_returns_existing_value():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()

        async def factory(key):
            return 11
        await cache.compute_if_absent("k", factory, 10**12)
        assert cache.get("k") == 11
        assert cache.get("missing") is None

    asyncio.run(inner())


def test_keys_and_items_match():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()

        async def factory(key):
            return ord(key)
        await cache.compute_if_absent("a", factory, 10**12)
        await cache.compute_if_absent("b", factory, 10**12)
        assert sorted(cache.keys()) == ["a", "b"]
        assert {k for k, _ in cache.items()} == {"a", "b"}

    asyncio.run(inner())


def test_cache_item_dataclass_holds_value_and_expiration():
    item = CacheItem(item="x", expiration_time=12345)
    assert item.item == "x"
    assert item.expiration_time == 12345
