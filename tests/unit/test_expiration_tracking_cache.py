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

from aws_advanced_python_wrapper.utils.storage.expiration_tracking_cache import \
    ExpirationTrackingCache

_LONG_TTL = 15_000_000_000  # 15 seconds
_SHORT_TTL = 50_000_000  # 50 ms


@pytest.fixture
def cache():
    return ExpirationTrackingCache(_LONG_TTL)


def test_put_and_get(cache):
    cache.put("k1", "v1")
    assert cache.get("k1") == "v1"


def test_get_missing_key(cache):
    assert cache.get("missing") is None


def test_get_expired_key():
    cache = ExpirationTrackingCache(_SHORT_TTL)
    cache.put("k1", "v1")
    time.sleep(0.07)
    assert cache.get("k1") is None


def test_put_returns_old_value(cache):
    assert cache.put("k1", "old") is None
    assert cache.put("k1", "new") == "old"
    assert cache.get("k1") == "new"


def test_compute_if_absent(cache):
    result1 = cache.compute_if_absent("k1", lambda _: "v1")
    result2 = cache.compute_if_absent("k1", lambda _: "v2")
    assert result1 == "v1"
    assert result2 == "v1"


def test_compute_if_absent_extends_expiration():
    cache = ExpirationTrackingCache(_SHORT_TTL)
    cache.compute_if_absent("k1", lambda _: "v1")
    time.sleep(0.03)
    # Should extend expiration, not expire
    cache.compute_if_absent("k1", lambda _: "v2")
    assert cache.get("k1") == "v1"


def test_get_or_create_for_aliases(cache):
    result = cache.get_or_create_for_aliases(
        frozenset(["a", "b", "c"]), lambda: "val")
    assert result == "val"
    assert cache.get("a") == "val"
    assert cache.get("b") == "val"
    assert cache.get("c") == "val"


def test_get_or_create_for_aliases_reuses_existing(cache):
    cache.put("b", "existing")
    result = cache.get_or_create_for_aliases(
        frozenset(["a", "b", "c"]), lambda: "new")
    assert result == "existing"
    # "a" and "c" should also be set
    assert cache.get("a") == "existing"


def test_extend_expiration():
    cache = ExpirationTrackingCache(_SHORT_TTL)
    cache.put("k1", "v1")
    time.sleep(0.03)
    cache.extend_expiration("k1")
    time.sleep(0.03)
    assert cache.get("k1") == "v1"


def test_remove(cache):
    cache.put("k1", "v1")
    removed = cache.remove("k1")
    assert removed == "v1"
    assert cache.get("k1") is None


def test_remove_missing_key(cache):
    assert cache.remove("missing") is None


def test_remove_expired_if(cache):
    short_cache = ExpirationTrackingCache(_SHORT_TTL)
    short_cache.put("k1", "v1")
    time.sleep(0.07)
    removed = short_cache.remove_expired_if("k1", lambda v: True)
    assert removed == "v1"


def test_remove_expired_if_not_expired(cache):
    cache.put("k1", "v1")
    removed = cache.remove_expired_if("k1", lambda v: True)
    assert removed is None
    assert cache.get("k1") == "v1"


def test_remove_expired_if_predicate_false():
    cache = ExpirationTrackingCache(_SHORT_TTL)
    cache.put("k1", "v1")
    time.sleep(0.07)
    removed = cache.remove_expired_if("k1", lambda v: False)
    assert removed is None


def test_detach_value(cache):
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    assert cache.detach_value("v1") is True
    assert cache.get("k1") is None
    assert cache.get("k2") == "v2"


def test_detach_value_not_found(cache):
    assert cache.detach_value("missing") is False


def test_items(cache):
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    result = dict(cache.items())
    assert result == {"k1": "v1", "k2": "v2"}


def test_clear(cache):
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    values = cache.clear()
    assert sorted(values) == ["v1", "v2"]
    assert len(cache) == 0


def test_len(cache):
    assert len(cache) == 0
    cache.put("k1", "v1")
    assert len(cache) == 1
    cache.put("k2", "v2")
    assert len(cache) == 2
