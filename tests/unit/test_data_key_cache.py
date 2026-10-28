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
from aws_advanced_python_wrapper.utils.storage import expiration_tracking_cache

NS_PER_SEC = 1_000_000_000


class Clock:
    def __init__(self):
        self.now_ns = 1000 * NS_PER_SEC

    def monotonic(self):
        return self.now_ns / NS_PER_SEC

    def perf_counter_ns(self):
        return self.now_ns

    def advance(self, sec, ns=0):
        self.now_ns += round(sec * NS_PER_SEC) + ns


@pytest.fixture
def clock(monkeypatch):
    # Replace the clock that the two cache modules read, not the time module's.
    # Every ExpirationTrackingCache in the process then sees the frozen clock,
    # and code that reads the time module keeps the real one. The time patch
    # also covers a cache that reads time.monotonic, so the TTL tests never
    # fall back to the real clock.
    c = Clock()
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=c.monotonic), raising=False)
    monkeypatch.setattr(expiration_tracking_cache, "perf_counter_ns", c.perf_counter_ns)
    return c


def make(max_size=3, ttl=60.0, enabled=True):
    return DataKeyCache(max_size, ttl, enabled, start_cleanup_thread=False)


def test_get_returns_copy(clock):
    cache = make()
    cache.put("k", b"\x01" * 32)
    first = cache.get("k")
    assert first == bytearray(b"\x01" * 32)
    first[:] = b"\x00" * 32
    assert cache.get("k") == bytearray(b"\x01" * 32)


def test_put_copies_caller_buffer(clock):
    cache = make()
    key = bytearray(b"\x02" * 32)
    cache.put("k", key)
    key[:] = b"\x00" * 32
    assert cache.get("k") == bytearray(b"\x02" * 32)


def test_oldest_entry_is_evicted_and_wiped(clock):
    cache = make(max_size=2)
    cache.put("a", b"a" * 4)
    entry_a = dict(cache._entries.items())["a"]
    cache.put("b", b"b" * 4)
    cache.put("c", b"c" * 4)
    assert cache.get("a") is None
    assert entry_a == bytearray(4)
    assert len(cache) == 2
    assert cache.stats().evictions == 1


def test_put_existing_key_wipes_old_value_without_eviction(clock):
    cache = make(max_size=2)
    cache.put("a", b"1111")
    cache.put("b", b"2222")
    old = dict(cache._entries.items())["a"]
    cache.put("a", b"3333")
    assert old == bytearray(4)
    assert cache.get("a") == bytearray(b"3333")
    assert cache.get("b") == bytearray(b"2222")
    assert cache.stats().evictions == 0


def test_expired_entry_is_never_returned_and_is_wiped(clock):
    cache = make(ttl=10)
    cache.put("k", b"kkkk")
    stored = dict(cache._entries.items())["k"]
    clock.advance(9.9)
    assert cache.get("k") is not None
    clock.advance(0.1, ns=1)
    assert cache.get("k") is None
    assert stored == bytearray(4)
    assert len(cache) == 0


def test_get_at_ttl_returns_key(clock):
    cache = make(ttl=10)
    cache.put("k", b"kkkk")
    clock.advance(10)
    assert cache.get("k") == bytearray(b"kkkk")


def test_remove_expired(clock):
    cache = make(ttl=10)
    cache.put("old", b"o")
    clock.advance(5)
    cache.put("new", b"n")
    clock.advance(5, ns=1)
    assert cache.remove_expired() == 1
    assert cache.get("new") is not None


def test_close_wipes_every_key(clock):
    cache = make()
    cache.put("a", b"aa")
    stored = dict(cache._entries.items())["a"]
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
    assert "SECRET" not in repr(cache)
    assert "SECRET" not in repr(cache._entries)


def test_cleanup_interval_has_floor():
    assert make(ttl=40).cleanup_interval_sec == DataKeyCache.MIN_CLEANUP_INTERVAL_SEC
    assert make(ttl=400).cleanup_interval_sec == 100.0


def test_cleanup_thread_stops_promptly_on_close():
    cache = DataKeyCache(10, 3600.0)
    try:
        thread = cache._cleanup_thread
        assert thread is not None
        assert thread.daemon
        assert thread.is_alive()
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
    # The caller's buffer is left alone.
    assert key == bytearray(b"\x05" * 32)


def test_finalizer_on_cleanup_thread_wipes_without_setting_event(clock):
    # A collection on the cleanup thread can run inside Event.wait, which
    # holds the event's lock, so stop.set() there would deadlock. The keys
    # are wiped regardless.
    import threading
    cache = make()
    cache.put("k", b"\x09" * 4)
    stored = dict(cache._entries.items())["k"]
    stop = threading.Event()
    DataKeyCache._release(cache._entries, cache._lock, stop, [threading.current_thread()])
    assert stored == bytearray(4)
    assert len(cache) == 0
    assert not stop.is_set()
    DataKeyCache._release(cache._entries, cache._lock, stop, [None])
    assert stop.is_set()


def test_close_on_cleanup_thread_does_not_join_itself():
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


def test_finalizer_stop_fails_keys_still_wiped(clock):
    cache = make()
    cache.put("k", b"\x09" * 4)
    stored = dict(cache._entries.items())["k"]

    class BrokenStop:
        def set(self):
            raise RuntimeError("cannot stop")

    with pytest.raises(RuntimeError):
        DataKeyCache._release(cache._entries, cache._lock, BrokenStop(), [None])  # type: ignore[arg-type]
    assert stored == bytearray(4)


def test_eviction_after_hit_removes_oldest(clock):
    # Eviction is oldest-first by insertion, not least recently used.
    cache = make(max_size=2)
    cache.put("a", b"aaaa")
    cache.put("b", b"bbbb")
    assert cache.get("a") == bytearray(b"aaaa")
    cache.put("c", b"cccc")
    assert cache.get("a") is None
    assert cache.get("b") == bytearray(b"bbbb")


def test_put_after_close_wipes_copy(clock, monkeypatch):
    wiped = []
    real = module.wipe
    monkeypatch.setattr(module, "wipe", lambda buf: (wiped.append(buf), real(buf)))
    cache = make()
    cache.close()
    cache.put("k", b"\x07" * 4)
    assert len(cache) == 0
    assert wiped == [bytearray(4)]


def test_cleanup_loop_logs_failure_and_runs_next_pass(mocker, clock):
    cache = make()
    remove_expired = mocker.patch.object(cache, "remove_expired", side_effect=[RuntimeError("boom"), 0])
    logger = mocker.patch.object(module, "logger")
    stop = mocker.MagicMock()
    stop.wait.side_effect = [False, False, True]
    DataKeyCache._cleanup_loop(weakref.ref(cache), stop, 1.0)
    logger.debug.assert_called_once_with("DataKeyCache.CleanupFailed", "RuntimeError")
    assert remove_expired.call_count == 2


def test_cleanup_loop_cache_collected_ends(mocker):
    stop = mocker.MagicMock()
    stop.wait.return_value = False
    DataKeyCache._cleanup_loop(mocker.MagicMock(return_value=None), stop, 1.0)
    assert stop.wait.call_count == 1


def test_finalizer_holds_cleanup_thread():
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
    stored = dict(cache._entries.items())["k"]
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
