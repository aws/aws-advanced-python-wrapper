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

from aws_wrapper.utils.cache_dict import CacheDict


def test_get():
    cache = CacheDict()
    cache.put("a", 1, 1_000_000_000)  # 1 sec
    cache.put("b", 2, 1)

    assert 1 == cache.get("a")
    assert cache.get("b") is None
    assert cache.get("c") is None


def test_get_with_default():
    cache = CacheDict()
    cache.put("a", 1, 1_000_000_000)  # 1 sec
    cache.put("b", 2, 1)
    cache.put("c", 2, 1)

    assert 1 == cache.get_with_default("a", 2, 1000)
    assert 3 == cache.get_with_default("b", 3, 1000)


def test_remove():
    cache = CacheDict()
    cache.put("a", 1, 1_000_000_000)

    cache.remove("a")
    cache.remove("b")
    assert cache.get("a") is None


def test_clear():
    cache = CacheDict()
    cache.put("a", 1, 1_000_000_000)  # 1 sec
    cache.put("b", 2, 1_000_000_000)

    cache.clear()
    assert 0 == len(cache)


def test_get_dict():
    cache = CacheDict()
    cache.put("a", 1, 1_000_000_000)  # 1 sec
    cache.put("b", 2, 1)

    expected = {"a": 1, "b": 2}
    assert expected == cache.get_dict()


def test_cleanup():
    cache = CacheDict()
    cache._cleanup_time_ns = time.perf_counter_ns()
    cache.put("b", 2, 1)
    cache.put("a", 1, 1_000_000_000)  # 1 sec

    assert 1 == cache.get("a")
    assert 1 == len(cache)
    assert cache.get("b") is None
