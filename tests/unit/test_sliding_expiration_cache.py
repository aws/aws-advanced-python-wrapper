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

from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCache


def test_compute_if_absent():
    cache = SlidingExpirationCache(50_000_000)
    result1 = cache.compute_if_absent(1, lambda _: "a", 1)
    original_item_expiration = cache._cdict._dict[1].expiration_time
    result2 = cache.compute_if_absent(1, lambda _: "b", 5)
    updated_item_expiration = cache._cdict._dict[1].expiration_time

    assert updated_item_expiration > original_item_expiration
    assert "a" == result1
    assert "a" == result2
    assert "a" == cache.get(1)

    time.sleep(0.05)
    result3 = cache.compute_if_absent(1, lambda _: "b", 5)
    assert "b" == result3
    assert "b" == cache.get(1)


def test_remove():
    cache = SlidingExpirationCache(50_000_000, lambda item: item.should_dispose, lambda item: item.dispose())
    item_to_remove = DisposableItem(should_dispose=True)
    result = cache.compute_if_absent("item_to_remove", lambda _: item_to_remove, 15_000_000_000)
    assert item_to_remove == result

    item_to_cleanup = DisposableItem(should_dispose=True)
    result = cache.compute_if_absent("item_to_cleanup", lambda _: item_to_cleanup, 1)
    assert item_to_cleanup == result

    non_disposable_item = DisposableItem(should_dispose=False)
    result = cache.compute_if_absent("non_disposable_item", lambda _: non_disposable_item, 1)
    assert non_disposable_item == result

    non_expired_item = DisposableItem(should_dispose=True)
    result = cache.compute_if_absent("non_expired_item", lambda _: non_expired_item, 15_000_000_000)
    assert non_expired_item == result

    time.sleep(0.05)
    cache.remove("item_to_remove")

    assert cache.get("item_to_remove") is None
    assert item_to_remove.disposed is True

    assert cache.get("item_to_cleanup") is None
    assert item_to_cleanup.disposed is True

    assert non_disposable_item == cache.get("non_disposable_item")
    assert non_disposable_item.disposed is False

    assert non_expired_item == cache.get("non_expired_item")
    assert non_expired_item.disposed is False


def test_clear():
    cache = SlidingExpirationCache(50_000_000, lambda item: item.should_dispose, lambda item: item.dispose())
    item1 = DisposableItem(False)
    item2 = DisposableItem(False)
    cache.compute_if_absent(1, lambda _: item1, 15_000_000_000)
    cache.compute_if_absent(2, lambda _: item2, 15_000_000_000)

    assert 2 == len(cache)
    assert item1 == cache.get(1)
    assert item2 == cache.get(2)

    cache.clear()
    assert 0 == len(cache)
    assert cache.get(1) is None
    assert cache.get(2) is None
    assert item1.disposed is True
    assert item2.disposed is True


class DisposableItem:
    def __init__(self, should_dispose):
        self.should_dispose = should_dispose
        self.disposed = False

    def dispose(self):
        self.disposed = True
