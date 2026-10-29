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

import gc
import time
import weakref
from types import SimpleNamespace

import pytest

from aws_advanced_python_wrapper.encryption import data_key_cache as module
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    # Replace the module's view of `time` only, so no other thread in the
    # process sees a frozen clock.
    c = Clock()
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=c))
    return c


def make(max_size=3, ttl=60.0, enabled=True):
    return DataKeyCache(max_size, ttl, enabled, start_cleanup_thread=False)


def test_get_returns_a_copy_the_caller_can_wipe(clock):
    cache = make()
    cache.put("k", b"\x01" * 32)
    first = cache.get("k")
    assert first == bytearray(b"\x01" * 32)
    first[:] = b"\x00" * 32
    assert cache.get("k") == bytearray(b"\x01" * 32)


def test_put_copies_so_the_caller_keeps_ownership(clock):
    cache = make()
    key = bytearray(b"\x02" * 32)
    cache.put("k", key)
    key[:] = b"\x00" * 32
    assert cache.get("k") == bytearray(b"\x02" * 32)


def test_oldest_entry_is_evicted_and_wiped(clock):
    cache = make(max_size=2)
    cache.put("a", b"a" * 4)
    entry_a = cache._entries["a"].data_key
    cache.put("b", b"b" * 4)
    cache.put("c", b"c" * 4)
    assert cache.get("a") is None
    assert entry_a == bytearray(4)
    assert len(cache) == 2
    assert cache.stats().evictions == 1


def test_replacing_a_key_wipes_the_old_value_and_does_not_evict(clock):
    cache = make(max_size=2)
    cache.put("a", b"1111")
    cache.put("b", b"2222")
    old = cache._entries["a"].data_key
    cache.put("a", b"3333")
    assert old == bytearray(4)
    assert cache.get("a") == bytearray(b"3333")
    assert cache.get("b") == bytearray(b"2222")
    assert cache.stats().evictions == 0


def test_expired_entry_is_never_returned_and_is_wiped(clock):
    cache = make(ttl=10)
    cache.put("k", b"kkkk")
    stored = cache._entries["k"].data_key
    clock.now += 9.9
    assert cache.get("k") is not None
    clock.now += 0.1
    assert cache.get("k") is None
    assert stored == bytearray(4)
    assert len(cache) == 0


def test_remove_expired(clock):
    cache = make(ttl=10)
    cache.put("old", b"o")
    clock.now += 5
    cache.put("new", b"n")
    clock.now += 5
    assert cache.remove_expired() == 1
    assert cache.get("new") is not None


def test_clear_and_close_wipe_everything(clock):
    cache = make()
    cache.put("a", b"aa")
    stored = cache._entries["a"].data_key
    cache.close()
    assert stored == bytearray(2)
    assert len(cache) == 0
    cache.close()  # idempotent


def test_disabled_cache_never_stores(clock):
    cache = make(enabled=False)
    cache.put("k", b"x")
    assert cache.get("k") is None
    assert len(cache) == 0
    assert cache.stats().misses == 0


def test_stats(clock):
    cache = make()
    cache.put("k", b"x")
    cache.get("k")
    cache.get("missing")
    stats = cache.stats()
    assert (stats.size, stats.hits, stats.misses) == (1, 1, 1)
    assert stats.hit_rate == 50.0


def test_repr_does_not_show_key_material(clock):
    cache = make()
    cache.put("k", b"SECRET")
    assert "SECRET" not in repr(cache._entries["k"])


def test_cleanup_interval_has_a_floor():
    assert make(ttl=40).cleanup_interval_sec == DataKeyCache.MIN_CLEANUP_INTERVAL_SEC
    assert make(ttl=400).cleanup_interval_sec == 100.0


def test_cleanup_thread_stops_promptly_on_close():
    cache = DataKeyCache(10, 3600.0)
    try:
        thread = cache._cleanup_thread
        assert thread is not None and thread.daemon and thread.is_alive()
        started = time.monotonic()
        cache.close()
        assert not thread.is_alive()
        assert time.monotonic() - started < 2.0
    finally:
        cache.close()


def test_closed_cache_stores_nothing(clock):
    # A KMS round trip that finishes after close() must not leave a plaintext
    # key in a cache nobody will wipe.
    cache = make()
    cache.close()
    key = bytearray(b"\x05" * 32)
    cache.put("k", key)
    assert cache.get("k") is None
    assert len(cache) == 0
    assert key == bytearray(b"\x05" * 32)  # the caller's buffer is left alone


def test_finalizer_wipes_first_and_never_sets_the_event_from_the_cleanup_thread(clock):
    # A collection on the cleanup thread can run inside Event.wait, which
    # holds the event's lock, so stop.set() there would deadlock. The keys
    # are wiped regardless.
    import threading
    cache = make()
    cache.put("k", b"\x09" * 4)
    stored = cache._entries["k"].data_key
    stop = threading.Event()
    DataKeyCache._release(cache._entries, cache._lock, stop, [threading.current_thread()])
    assert stored == bytearray(4) and len(cache) == 0
    assert not stop.is_set()
    DataKeyCache._release(cache._entries, cache._lock, stop, [None])
    assert stop.is_set()


def test_close_on_the_cleanup_thread_stops_it_without_joining_itself():
    # There the finalizer leaves the event alone, so close() must set it, and
    # it must not join its own thread.
    import threading
    cache = DataKeyCache(10, 3600.0, start_cleanup_thread=False)
    errors = []

    def run():
        try:
            cache.close()
        except BaseException as e:
            errors.append(e)

    t = threading.Thread(target=run, daemon=True)
    cache._thread_holder[0] = t
    cache._cleanup_thread = t
    t.start()
    t.join(5)
    assert not t.is_alive()
    assert errors == []
    assert cache._stop.is_set()


def test_finalizer_wipes_the_keys_even_if_stopping_the_thread_fails(clock):
    cache = make()
    cache.put("k", b"\x09" * 4)
    stored = cache._entries["k"].data_key

    class BrokenStop:
        def set(self):
            raise RuntimeError("cannot stop")

    with pytest.raises(RuntimeError):
        DataKeyCache._release(cache._entries, cache._lock, BrokenStop(), [None])  # type: ignore[arg-type]
    assert stored == bytearray(4)


def test_a_cache_hit_does_not_protect_an_entry_from_eviction(clock):
    # Eviction is oldest-first by insertion, not least recently used.
    cache = make(max_size=2)
    cache.put("a", b"aaaa")
    cache.put("b", b"bbbb")
    assert cache.get("a") == bytearray(b"aaaa")
    cache.put("c", b"cccc")
    assert cache.get("a") is None
    assert cache.get("b") == bytearray(b"bbbb")


def test_put_after_close_wipes_the_copy_it_refuses(clock, monkeypatch):
    wiped = []
    real = module.wipe
    monkeypatch.setattr(module, "wipe", lambda buf: (wiped.append(buf), real(buf)))
    cache = make()
    cache.close()
    cache.put("k", b"\x07" * 4)
    assert len(cache) == 0
    assert wiped == [bytearray(4)]


def test_finalizer_is_given_the_cleanup_thread():
    cache = DataKeyCache(10, 3600.0)
    try:
        assert cache._cleanup_thread is not None
        assert cache._thread_holder == [cache._cleanup_thread]
        assert any(arg is cache._thread_holder for arg in cache._finalizer.peek()[2])  # type: ignore[index]
    finally:
        cache.close()


def test_unclosed_cache_is_wiped_and_its_thread_stops_when_collected():
    cache = DataKeyCache(10, 3600.0)
    cache.put("k", b"\x07" * 32)
    stored = cache._entries["k"].data_key
    thread = cache._cleanup_thread
    ref = weakref.ref(cache)
    del cache
    for _ in range(100):
        gc.collect()
        if ref() is None:
            break
        time.sleep(0.01)
    assert ref() is None
    assert stored == bytearray(32)
    assert thread is not None
    thread.join(5)
    assert not thread.is_alive()
