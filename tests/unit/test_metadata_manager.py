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
import threading
import weakref
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, List, Optional, Tuple

import pytest

from aws_advanced_python_wrapper.encryption import metadata_manager as module
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.encryption.sql_dialect import MYSQL, PG
from aws_advanced_python_wrapper.errors import MetadataError

ARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"

COLUMNS = ["table_name", "column_name", "encryption_algorithm", "key_id", "created_at", "updated_at",
           "ks_id", "ks_key_id", "ks_name", "ks_master_key_arn", "ks_encrypted_data_key", "ks_hmac_key",
           "ks_key_spec", "ks_created_at", "ks_last_used_at"]


def row(table, column, key_id=1, with_key=True, algorithm="AES-256-GCM"):
    key = (key_id, "uuid", "n", ARN, "ZWRr", b"h" * 32, "AES_256", None, None) if with_key else (None,) * 9
    return (table, column, algorithm, key_id, None, None) + key


class FakeDb:
    def __init__(self, rows=()):
        self.rows: List[Tuple[Any, ...]] = list(rows)
        self.executed: List[Tuple[str, Tuple[Any, ...]]] = []
        self.error: Optional[Exception] = None
        self.opened = 0

    @contextmanager
    def connection(self):
        self.opened += 1
        yield self

    def cursor(self):
        db = self

        class Cursor:
            description = None

            def execute(self, sql, params=()):
                db.executed.append((sql, tuple(params)))
                if db.error is not None:
                    raise db.error
                self.description = [(c.upper(),) for c in COLUMNS]  # upper case, as some MySQL versions report
                rows = db.rows
                if "WHERE em.table_name" in sql:
                    rows = [r for r in rows if r[0] == params[0] and (len(params) < 2 or r[1] == params[1])]
                self._rows = rows

            def fetchall(self):
                return self._rows

            def close(self):
                pass

        return Cursor()


class Clock:
    now = 100.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    # Replace the module's view of `time` only, so no other thread in the
    # process sees a frozen clock.
    c = Clock()
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=c))
    return c


@pytest.fixture
def managers():
    """Collects managers and closes them even when a test fails, so no
    refresh thread outlives its test."""
    created: List[MetadataManager] = []
    yield created
    for mm in created:
        mm.close()


def config(**overrides):
    values = dict(kms_region="us-east-1", allowed_master_key_arns=frozenset({ARN}),
                  metadata_cache_refresh_interval_sec=0.0, metadata_cache_expiration_sec=60.0)
    values.update(overrides)
    return EncryptionConfig(**values)  # type: ignore[arg-type]


def test_joined_query_is_a_left_join_on_the_integer_id():
    db = FakeDb()
    MetadataManager(db, PG, config()).load_metadata()
    sql = db.executed[0][0]
    assert "FROM encrypt.encryption_metadata em LEFT JOIN encrypt.key_storage ks" in sql
    assert "ON em.key_id OPERATOR(pg_catalog.=) ks.id" in sql
    assert "ks.key_id AS ks_key_id" in sql


def test_mysql_uses_plain_equals():
    db = FakeDb()
    MetadataManager(db, MYSQL, config(metadata_schema="app_enc")).load_metadata()
    assert "ON em.key_id = ks.id" in db.executed[0][0]
    assert "app_enc.encryption_metadata" in db.executed[0][0]


def test_start_loads_snapshot_and_lookups_hit_the_cache(clock):
    db = FakeDb([row("users", "ssn"), row("users", "dob", 2), row("orders", "card", 3)])
    mm = MetadataManager(db, PG, config())
    mm.start()
    assert mm.cache_size == 3 and db.opened == 1
    assert mm.is_column_encrypted("users", "ssn")
    assert not mm.is_column_encrypted("users", "name")
    assert not mm.is_column_encrypted(None, "ssn")
    config_ = mm.column_config("users", "dob")
    assert config_ is not None and config_.key_id == 2 and config_.key_metadata.id == 2
    assert config_.key_metadata.hmac_key == b"h" * 32 and config_.is_usable
    assert {c.column_name for c in mm.table_configs("users")} == {"ssn", "dob"}
    assert mm.table_configs("nope") == ()
    assert db.opened == 1


def test_column_with_missing_key_row_still_reads_as_encrypted(clock):
    db = FakeDb([row("users", "ssn", key_id=7, with_key=False)])
    mm = MetadataManager(db, PG, config())
    mm.start()
    config_ = mm.column_config("users", "ssn")
    assert config_ is not None
    assert config_.key_id == 7 and config_.key_metadata is None
    assert not config_.is_usable
    assert mm.is_column_encrypted("users", "ssn")


def _expired_with_background_refresh(clock, managers, db):
    # With a background refresh configured (but not yet run), an expired
    # snapshot is not reloaded by a lookup: the lookup goes to the database.
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0,
                                        metadata_cache_refresh_interval_sec=3600.0))
    managers.append(mm)
    mm.start()
    clock.now += 10.0
    return mm


def test_expired_snapshot_falls_back_to_the_database(clock, managers):
    db = FakeDb([row("users", "ssn")])
    mm = _expired_with_background_refresh(clock, managers, db)
    assert mm.is_column_encrypted("users", "ssn")
    assert db.opened == 2
    sql, params = db.executed[-1]
    assert "WHERE em.table_name OPERATOR(pg_catalog.=) %s AND em.column_name OPERATOR(pg_catalog.=) %s" in sql
    assert params == ("users", "ssn")


def test_expired_snapshot_table_configs_falls_back_to_the_database(clock, managers):
    # A column encrypted after the snapshot was taken must show up once the
    # snapshot expires; serving the stale snapshot would let a write store
    # plaintext.
    db = FakeDb([row("users", "ssn")])
    mm = _expired_with_background_refresh(clock, managers, db)
    db.rows = [row("users", "ssn"), row("users", "dob", 2)]
    assert {c.column_name for c in mm.table_configs("users")} == {"ssn", "dob"}
    assert db.opened == 2
    assert "WHERE em.table_name OPERATOR(pg_catalog.=) %s" in db.executed[-1][0]


def test_expired_snapshot_is_reloaded_when_there_is_no_background_refresh(clock):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0))  # refresh interval 0
    mm.start()
    clock.now += 10.0
    db.rows = [row("users", "ssn"), row("users", "dob", 2)]
    assert {c.column_name for c in mm.table_configs("users")} == {"ssn", "dob"}
    assert mm.is_column_encrypted("users", "dob")
    assert db.opened == 2  # one reload, then served from the new snapshot
    assert "WHERE" not in db.executed[-1][0]


def test_manager_that_was_never_started_always_queries(clock):
    # The admin utility relies on this: it never starts its manager, so every
    # lookup sees the caller's own transaction.
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config())
    assert mm.is_column_encrypted("users", "ssn")
    assert mm.is_column_encrypted("users", "ssn")
    assert db.opened == 2 and not mm.is_cache_loaded


def test_disabled_cache_always_queries(clock):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_enabled=False))
    mm.start()
    assert db.opened == 0 and not mm.is_cache_loaded
    assert mm.is_column_encrypted("users", "ssn")
    assert [c.column_name for c in mm.table_configs("users")] == ["ssn"]
    assert db.opened == 2


def test_refresh_replaces_the_snapshot_atomically(clock):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config())
    mm.start()
    db.rows = [row("users", "dob")]
    assert mm.refresh() == 1
    assert not mm.is_column_encrypted("users", "ssn")
    assert mm.is_column_encrypted("users", "dob")


def test_failed_refresh_keeps_the_previous_snapshot(clock):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config())
    mm.start()
    db.error = RuntimeError("db down")
    with pytest.raises(MetadataError, match="db down"):
        mm.refresh()
    assert mm.is_column_encrypted("users", "ssn")


def test_initial_load_failure_raises_metadata_error():
    db = FakeDb()
    db.error = RuntimeError("no such schema")
    with pytest.raises(MetadataError):
        MetadataManager(db, PG, config()).start()


def test_lookup_failure_carries_table_and_column(clock):
    db = FakeDb()
    db.error = RuntimeError("x")
    mm = MetadataManager(db, PG, config(metadata_cache_enabled=False))
    with pytest.raises(MetadataError) as err:
        mm.column_config("users", "ssn")
    assert (err.value.table_name, err.value.column_name) == ("users", "ssn")


def test_background_refresh_thread_runs_and_stops(managers):
    db = FakeDb([row("users", "ssn")])
    refreshed = threading.Event()
    mm = MetadataManager(db, PG, config(metadata_cache_refresh_interval_sec=0.01))
    managers.append(mm)
    original = mm.refresh

    def counting_refresh():
        n = original()
        if db.opened >= 3:
            refreshed.set()
        return n

    mm.refresh = counting_refresh  # type: ignore[method-assign]
    mm.start()
    thread = mm._refresh_thread
    assert thread is not None and thread.daemon
    assert refreshed.wait(5)
    mm.close()
    assert not thread.is_alive()
    assert not mm.is_cache_loaded
    mm.close()  # idempotent


def test_background_refresh_survives_errors(managers):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_refresh_interval_sec=0.01))
    managers.append(mm)
    mm.start()
    db.error = RuntimeError("blip")
    attempts = len(db.executed)
    for _ in range(500):
        if len(db.executed) > attempts + 2:
            break
        threading.Event().wait(0.01)
    assert len(db.executed) > attempts + 2  # the loop kept trying
    assert mm._refresh_thread is not None and mm._refresh_thread.is_alive()


class BlockingDb(FakeDb):
    """A FakeDb that, while ``block`` is set, holds the next query's
    connection open after the query ran, until released."""

    def __init__(self, rows=()):
        super().__init__(rows)
        self.block = False
        self.entered = threading.Event()
        self.release = threading.Event()

    @contextmanager
    def connection(self):
        with super().connection() as conn:
            yield conn
        if self.block:
            self.entered.set()
            self.release.wait(5)


def test_refresh_finishing_after_close_does_not_repopulate_the_cache(managers):
    db = BlockingDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config())
    managers.append(mm)
    mm.start()
    db.block = True
    worker = threading.Thread(target=mm.refresh, daemon=True)
    worker.start()
    assert db.entered.wait(5)
    mm.close()
    db.release.set()
    worker.join(5)
    assert not worker.is_alive()
    assert not mm.is_cache_loaded
    # ...and a lookup after close reads the database instead of a cache.
    db.block = False
    opened = db.opened
    assert mm.is_column_encrypted("users", "ssn")
    assert db.opened == opened + 1 and not mm.is_cache_loaded


def test_start_after_close_does_nothing():
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_refresh_interval_sec=0.01))
    mm.close()
    mm.start()
    assert db.opened == 0 and mm._refresh_thread is None and not mm.is_cache_loaded


def test_concurrent_refreshes_cannot_publish_older_data():
    # The first refresh has read the old rows but not yet published them when
    # the data changes and a second refresh starts. Refreshes are serialized,
    # so the second one's newer data is published last.
    db = BlockingDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config())
    db.block = True
    first = threading.Thread(target=mm.refresh, daemon=True)
    first.start()
    assert db.entered.wait(5)
    db.block = False
    db.rows = [row("users", "dob")]
    second = threading.Thread(target=mm.refresh, daemon=True)
    second.start()
    second.join(0.2)
    assert second.is_alive()  # waits for the first refresh
    db.release.set()
    first.join(5)
    second.join(5)
    assert not first.is_alive() and not second.is_alive()
    assert mm.is_column_encrypted("users", "dob")
    assert not mm.is_column_encrypted("users", "ssn")


def test_unclosed_manager_stops_its_thread_when_collected():
    # A long interval, so the thread stops because the finalizer wakes it,
    # not because its next wait happened to time out.
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_refresh_interval_sec=60.0))
    mm.start()
    thread = mm._refresh_thread
    ref = weakref.ref(mm)
    del mm
    for _ in range(100):
        gc.collect()
        if ref() is None:
            break
        threading.Event().wait(0.01)
    assert ref() is None
    assert thread is not None
    thread.join(2)
    assert not thread.is_alive()


def test_finalizer_does_not_set_the_event_from_the_refresh_thread_itself():
    # On the refresh thread the collection can run inside Event.wait, which
    # holds the event's lock, so stop.set() there would deadlock.
    stop = threading.Event()
    MetadataManager._finalize(stop, [threading.current_thread()])
    assert not stop.is_set()
    MetadataManager._finalize(stop, [None])
    assert stop.is_set()


def test_close_from_the_refresh_thread_does_not_fail(managers):
    # Only the refresh thread reports; start()'s initial refresh runs on the
    # calling thread and must not satisfy the test.
    errors: List[BaseException] = []
    done = threading.Event()

    class ClosingDb(FakeDb):
        @contextmanager
        def connection(self):
            if threading.current_thread().name == "kms-encryption-metadata-refresh":
                try:
                    mm.close()
                except BaseException as e:
                    errors.append(e)
                finally:
                    done.set()
            with super().connection() as conn:
                yield conn

    mm = MetadataManager(ClosingDb([row("users", "ssn")]), PG, config(metadata_cache_refresh_interval_sec=0.01))
    managers.append(mm)
    mm.start()
    assert done.wait(5)
    assert errors == []
    assert mm._stop.is_set()


def test_finalizer_is_given_the_refresh_thread(managers):
    # The _finalize test passes the holder by hand; this pins the wiring.
    mm = MetadataManager(FakeDb([row("users", "ssn")]), PG, config(metadata_cache_refresh_interval_sec=60.0))
    managers.append(mm)
    mm.start()
    assert mm._refresh_thread is not None
    assert mm._thread_holder == [mm._refresh_thread]


def test_failed_reload_from_table_configs_names_the_table(clock):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0))
    mm.start()
    clock.now += 10.0
    db.error = RuntimeError("db down")
    with pytest.raises(MetadataError) as err:
        mm.table_configs("users")
    assert (err.value.table_name, err.value.column_name) == ("users", None)


def test_snapshot_age_counts_from_the_start_of_the_load(clock, managers):
    # A slow load must not stretch how long its data is trusted.
    class SlowDb(FakeDb):
        @contextmanager
        def connection(self):
            with super().connection() as conn:
                yield conn
            clock.now += 5.0  # the load takes 5 s

    db = SlowDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0,
                                        metadata_cache_refresh_interval_sec=3600.0))
    managers.append(mm)
    mm.start()
    clock.now += 5.0  # 10 s after the data was read, 5 s after the query returned
    opened = db.opened
    mm.column_config("users", "ssn")
    assert db.opened == opened + 1  # expired: read from the database


def test_lookups_that_expire_together_share_one_reload(clock):
    db = BlockingDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0))  # no background refresh
    mm.start()
    clock.now += 10.0
    db.block = True
    first = threading.Thread(target=mm.column_config, args=("users", "ssn"), daemon=True)
    first.start()
    assert db.entered.wait(5)  # the first lookup's reload is inside its read
    db.block = False
    others = [threading.Thread(target=mm.column_config, args=("users", "ssn"), daemon=True) for _ in range(4)]
    for t in others:
        t.start()
    db.release.set()
    for t in [first] + others:
        t.join(5)
    assert not any(t.is_alive() for t in [first] + others)
    assert db.opened == 2  # start() plus a single reload


def test_lookups_queued_when_close_runs_do_not_reload(clock):
    db = BlockingDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0))  # no background refresh
    mm.start()
    clock.now += 10.0
    db.block = True
    first = threading.Thread(target=mm.column_config, args=("users", "ssn"), daemon=True)
    first.start()
    assert db.entered.wait(5)
    db.block = False
    queued = threading.Thread(target=mm.column_config, args=("users", "ssn"), daemon=True)
    queued.start()
    queued.join(0.2)
    assert queued.is_alive()  # waiting for the first reload
    mm.close()
    db.release.set()
    first.join(5)
    queued.join(5)
    assert not first.is_alive() and not queued.is_alive()
    full_loads = [sql for sql, _ in db.executed if "WHERE" not in sql]
    assert len(full_loads) == 2  # start() and the first lookup's reload; the queued one only queries its column


def test_failed_reload_names_the_lookup(clock):
    db = FakeDb([row("users", "ssn")])
    mm = MetadataManager(db, PG, config(metadata_cache_expiration_sec=10.0))
    mm.start()
    clock.now += 10.0
    db.error = RuntimeError("db down")
    with pytest.raises(MetadataError) as err:
        mm.column_config("users", "ssn")
    assert (err.value.table_name, err.value.column_name) == ("users", "ssn")


# ---- name matching ----

def test_postgresql_lookups_are_exact(clock):
    # Names are stored as the catalog has them; "Users" and users differ.
    db = FakeDb([row("users", "ssn"), row("Users", "SSN", 2)])
    mm = MetadataManager(db, PG, config())
    mm.start()
    assert mm.column_config("users", "ssn").key_id == 1
    assert mm.column_config("Users", "SSN").key_id == 2
    assert mm.column_config("USERS", "ssn") is None
    assert [c.key_id for c in mm.table_configs("Users")] == [2]


def test_mysql_lookups_ignore_case(clock):
    # MySQL compares these names case-insensitively, as the uncached query's
    # WHERE does under the metadata table's collation.
    db = FakeDb([row("Users", "SSN")])
    mm = MetadataManager(db, MYSQL, config())
    mm.start()
    found = mm.column_config("users", "ssn")
    assert found is not None and (found.table_name, found.column_name) == ("Users", "SSN")
    assert mm.is_column_encrypted("USERS", "Ssn")
    assert [c.column_name for c in mm.table_configs("users")] == ["SSN"]
    assert [c.column_name for c in mm.table_configs("USERS")] == ["SSN"]
    assert db.opened == 1  # all from the cache


def test_load_metadata_keeps_the_stored_names():
    db = FakeDb([row("Users", "SSN")])
    assert list(MetadataManager(db, MYSQL, config()).load_metadata()) == [("Users", "SSN")]


@pytest.mark.parametrize("raw", [b"users", bytearray(b"users"), memoryview(b"users")])
def test_bytes_names_are_decoded(clock, raw):
    # psycopg on a SQL_ASCII database, or Connector/Python with
    # use_unicode=False, returns text as bytes. Undecoded, the snapshot would
    # be keyed by bytes and no str lookup would find the encrypted column.
    db = FakeDb([row(raw, b"ssn")])
    mm = MetadataManager(db, PG, config())
    mm.start()
    assert mm.is_column_encrypted("users", "ssn")
    assert mm.column_config("users", "ssn").table_name == "users"


@pytest.mark.parametrize("raw", [b"\xff\xfe", 42, None])
def test_names_that_are_not_text_are_refused(clock, raw):
    db = FakeDb([row(raw, "ssn")])
    mm = MetadataManager(db, PG, config())
    with pytest.raises(MetadataError, match="table_name") as info:
        mm.start()
    assert info.value.__context__ is None


def test_names_that_are_not_text_are_refused_on_an_uncached_lookup():
    db = FakeDb([row("users", 7)])
    mm = MetadataManager(db, PG, config(metadata_cache_enabled=False))
    with pytest.raises(MetadataError, match="column_name"):
        mm.column_config("users", 7)  # type: ignore[arg-type]
