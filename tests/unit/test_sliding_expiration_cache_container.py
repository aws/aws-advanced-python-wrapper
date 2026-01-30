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

import time

import pytest

from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCache
from aws_advanced_python_wrapper.utils.sliding_expiration_cache_container import \
    SlidingExpirationCacheContainer


@pytest.fixture(autouse=True)
def cleanup_caches():
    """Clean up all caches after each test"""
    yield
    SlidingExpirationCacheContainer.release_resources()


def test_get_or_create_cache_creates_new_cache():
    cache = SlidingExpirationCacheContainer.get_or_create_cache("test_cache")
    assert isinstance(cache, SlidingExpirationCache)


def test_get_or_create_cache_returns_existing_cache():
    cache1 = SlidingExpirationCacheContainer.get_or_create_cache("test_cache")
    cache2 = SlidingExpirationCacheContainer.get_or_create_cache("test_cache")
    assert cache1 is cache2


def test_get_or_create_cache_with_custom_cleanup_interval():
    cache = SlidingExpirationCacheContainer.get_or_create_cache(
        "test_cache",
        cleanup_interval_ns=5_000_000_000  # 5 seconds
    )
    assert cache._cleanup_interval_ns == 5_000_000_000


def test_get_or_create_cache_with_disposal_functions():
    disposed_items = []

    def should_dispose(item):
        return item > 10

    def dispose(item):
        disposed_items.append(item)

    cache = SlidingExpirationCacheContainer.get_or_create_cache(
        "test_cache",
        should_dispose_func=should_dispose,
        item_disposal_func=dispose
    )

    assert cache._should_dispose_func is should_dispose
    assert cache._item_disposal_func is dispose


def test_multiple_caches_are_independent():
    cache1 = SlidingExpirationCacheContainer.get_or_create_cache("cache1")
    cache2 = SlidingExpirationCacheContainer.get_or_create_cache("cache2")

    cache1.compute_if_absent("key1", lambda k: "value1", 1_000_000_000)
    cache2.compute_if_absent("key2", lambda k: "value2", 1_000_000_000)

    assert cache1.get("key1") == "value1"
    assert cache1.get("key2") is None
    assert cache2.get("key2") == "value2"
    assert cache2.get("key1") is None


def test_cleanup_thread_starts_on_first_cache():
    # Cleanup thread should start when first cache is created
    SlidingExpirationCacheContainer.get_or_create_cache("test_cache")

    # Check that cleanup thread is running
    assert SlidingExpirationCacheContainer._cleanup_thread is not None
    assert SlidingExpirationCacheContainer._cleanup_thread.is_alive()


def test_release_resources_clears_all_caches():
    cache1 = SlidingExpirationCacheContainer.get_or_create_cache("cache1")
    cache2 = SlidingExpirationCacheContainer.get_or_create_cache("cache2")

    cache1.compute_if_absent("key1", lambda k: "value1", 1_000_000_000)
    cache2.compute_if_absent("key2", lambda k: "value2", 1_000_000_000)

    SlidingExpirationCacheContainer.release_resources()

    # Caches should be cleared
    assert len(SlidingExpirationCacheContainer._caches) == 0


def test_release_resources_stops_cleanup_thread():
    SlidingExpirationCacheContainer.get_or_create_cache("test_cache")

    cleanup_thread = SlidingExpirationCacheContainer._cleanup_thread
    assert cleanup_thread is not None
    assert cleanup_thread.is_alive()

    SlidingExpirationCacheContainer.release_resources()

    # Give thread time to stop
    time.sleep(0.1)

    # Thread should be stopped
    assert not cleanup_thread.is_alive()


def test_release_resources_disposes_items():
    disposed_items = []

    def dispose(item):
        disposed_items.append(item)

    cache = SlidingExpirationCacheContainer.get_or_create_cache(
        "test_cache",
        item_disposal_func=dispose
    )

    cache.compute_if_absent("key1", lambda k: "value1", 1_000_000_000)
    cache.compute_if_absent("key2", lambda k: "value2", 1_000_000_000)

    SlidingExpirationCacheContainer.release_resources()

    # Items should have been disposed
    assert "value1" in disposed_items
    assert "value2" in disposed_items


def test_cleanup_thread_cleans_expired_items():
    # Use very short intervals for testing
    cache = SlidingExpirationCacheContainer.get_or_create_cache(
        "test_cache",
        cleanup_interval_ns=100_000_000  # 0.1 seconds
    )

    # Add item with very short expiration
    cache.compute_if_absent("key1", lambda k: "value1", 50_000_000)  # 0.05 seconds

    assert cache.get("key1") == "value1"

    # Wait for item to expire and cleanup to run
    time.sleep(0.3)

    # Item should be cleaned up
    assert cache.get("key1") is None


def test_same_cache_name_returns_same_instance_across_calls():
    cache1 = SlidingExpirationCacheContainer.get_or_create_cache("shared_cache")
    cache1.compute_if_absent("key1", lambda k: "value1", 1_000_000_000)

    # Get the same cache again
    cache2 = SlidingExpirationCacheContainer.get_or_create_cache("shared_cache")

    # Should be the same instance with the same data
    assert cache1 is cache2
    assert cache2.get("key1") == "value1"


def test_cleanup_thread_handles_multiple_caches():
    cache1 = SlidingExpirationCacheContainer.get_or_create_cache(
        "cache1",
        cleanup_interval_ns=100_000_000  # 0.1 seconds
    )
    cache2 = SlidingExpirationCacheContainer.get_or_create_cache(
        "cache2",
        cleanup_interval_ns=100_000_000  # 0.1 seconds
    )

    # Add items with short expiration
    cache1.compute_if_absent("key1", lambda k: "value1", 50_000_000)
    cache2.compute_if_absent("key2", lambda k: "value2", 50_000_000)

    assert cache1.get("key1") == "value1"
    assert cache2.get("key2") == "value2"

    # Wait for cleanup
    time.sleep(0.3)

    # Both should be cleaned up
    assert cache1.get("key1") is None
    assert cache2.get("key2") is None


def test_release_resources_handles_disposal_errors():
    def failing_dispose(item):
        raise Exception("Disposal failed")

    cache = SlidingExpirationCacheContainer.get_or_create_cache(
        "test_cache",
        item_disposal_func=failing_dispose
    )

    cache.compute_if_absent("key1", lambda k: "value1", 1_000_000_000)

    # Should not raise exception even if disposal fails
    SlidingExpirationCacheContainer.release_resources()

    # Cache should still be cleared
    assert len(SlidingExpirationCacheContainer._caches) == 0


def test_cleanup_thread_respects_is_stopped_event():
    # Clear the stop event first in case it was set by a previous test
    SlidingExpirationCacheContainer._is_stopped.clear()

    SlidingExpirationCacheContainer.get_or_create_cache("test_cache")

    cleanup_thread = SlidingExpirationCacheContainer._cleanup_thread
    assert cleanup_thread is not None
    assert cleanup_thread.is_alive()

    # Set the stop event
    SlidingExpirationCacheContainer._is_stopped.set()

    # Thread should stop quickly (not wait for full cleanup interval)
    cleanup_thread.join(timeout=1.0)
    assert not cleanup_thread.is_alive()
