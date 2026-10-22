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

import time
import weakref
from threading import Event, Lock, Thread, current_thread
from types import MappingProxyType
from typing import (TYPE_CHECKING, Any, Dict, List, Mapping, NamedTuple,
                    Optional, Sequence, Tuple)

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.models import (
    ColumnEncryptionConfig, KeyMetadata)
from aws_advanced_python_wrapper.encryption.sql_dialect import fetch_rows
from aws_advanced_python_wrapper.errors import MetadataError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
    from aws_advanced_python_wrapper.encryption.connection_source import \
        ConnectionSource
    from aws_advanced_python_wrapper.encryption.sql_dialect import SqlDialect

logger = Logger(__name__)

_KS_PREFIX = "ks_"


class _Snapshot(NamedTuple):
    columns: Mapping[Tuple[str, str], ColumnEncryptionConfig]
    by_table: Mapping[str, Tuple[ColumnEncryptionConfig, ...]]
    loaded_at: float  # time.monotonic()


class MetadataManager:
    """Reads ``encryption_metadata`` (joined to ``key_storage``) and caches
    which columns are encrypted.

    The cache is one immutable snapshot of the whole table, replaced
    atomically on each refresh, so a lookup never sees a half-loaded state. It
    is loaded by :meth:`start`, refreshed on a background thread every
    ``encryption_metadata_cache_refresh_interval_sec``, and answers lookups
    only while younger than ``encryption_metadata_cache_expiration_sec``.
    With the background refresh disabled, the first lookup after expiry
    reloads it. Before :meth:`start`, after :meth:`close`, or with the cache
    disabled, each lookup queries the database.

    Call :meth:`close` when done. The refresh thread holds the manager only
    weakly, so a manager dropped without ``close`` still stops its thread once
    it is garbage-collected.

    The wrapper's shared storage service is not used: its entries are not
    checked for expiry on read, and it caches per key rather than one
    consistent snapshot.
    """

    SHUTDOWN_TIMEOUT_SEC = 5.0

    def __init__(self, connection_source: ConnectionSource, sql_dialect: SqlDialect, config: EncryptionConfig):
        self._connections = connection_source
        self._sql = sql_dialect
        self._config = config
        self._snapshot: Optional[_Snapshot] = None
        self._started = False
        # Guards _started, _snapshot publication and the thread handle against
        # close(). Never held across a database call.
        self._lifecycle_lock = Lock()
        # Serializes refreshes, so an older load cannot overwrite a newer one.
        self._refresh_lock = Lock()
        self._stop = Event()
        self._refresh_thread: Optional[Thread] = None
        # Holds the refresh thread for the finalizer, which must not refer to
        # self, or the manager would never be collected.
        self._thread_holder: List[Optional[Thread]] = [None]
        weakref.finalize(self, MetadataManager._finalize, self._stop, self._thread_holder)

    # ---- lifecycle ----

    def start(self) -> None:
        """Load the cache and start the background refresh, as configured.

        :raises MetadataError: if the initial load fails.
        """
        with self._lifecycle_lock:
            if self._stop.is_set():
                return
            self._started = True
        if self._config.metadata_cache_enabled:
            self.refresh()
        if not self._config.background_refresh_enabled:
            return
        with self._lifecycle_lock:
            if self._refresh_thread is None and not self._stop.is_set():
                self._refresh_thread = Thread(
                    target=MetadataManager._refresh_loop,
                    args=(weakref.ref(self), self._stop, self._config.metadata_cache_refresh_interval_sec),
                    name="kms-encryption-metadata-refresh", daemon=True)
                self._thread_holder[0] = self._refresh_thread
                self._refresh_thread.start()

    def close(self) -> None:
        """Stop the background refresh and drop the cache. A refresh still
        running is discarded when it finishes. Idempotent.

        Call it from ordinary code, not from a finalizer or ``__del__``: a
        garbage collection on the refresh thread can run inside its wait, where
        stopping the thread would deadlock.
        """
        with self._lifecycle_lock:
            self._stop.set()
            thread, self._refresh_thread = self._refresh_thread, None
            self._snapshot = None
        # Joining the current thread raises; close() can run on the refresh
        # thread if a caller releases the manager from inside a refresh.
        if thread is not None and thread is not current_thread():
            thread.join(self.SHUTDOWN_TIMEOUT_SEC)

    def refresh(self) -> int:
        """Reload every column configuration and replace the cache. After
        :meth:`close` the result is not cached.

        :return: the number of encrypted columns.
        :raises MetadataError: if the load fails; the previous snapshot is kept.
        """
        with self._refresh_lock:
            return self._load_snapshot()

    def load_metadata(self) -> Dict[Tuple[str, str], ColumnEncryptionConfig]:
        """Read every column configuration from the database, bypassing the
        cache. Keyed by ``(table_name, column_name)``."""
        return self._load_all()

    # ---- lookups ----

    def is_column_encrypted(self, table_name: Optional[str], column_name: Optional[str]) -> bool:
        if table_name is None or column_name is None:
            return False
        return self.column_config(table_name, column_name) is not None

    def column_config(self, table_name: Optional[str], column_name: Optional[str]) -> Optional[ColumnEncryptionConfig]:
        """The column's configuration, or ``None`` when it is not encrypted.

        A configured column whose ``key_storage`` row is missing is still
        returned, with ``key_metadata=None``, so the caller fails closed.
        """
        if table_name is None or column_name is None:
            return None
        snapshot = self._usable_snapshot(table_name, column_name)
        if snapshot is not None:
            return snapshot.columns.get((table_name, column_name))
        rows = self._query(
            f"{self._joined_sql()} WHERE em.table_name {self._sql.equals} %s AND em.column_name {self._sql.equals} %s",
            (table_name, column_name), table_name=table_name, column_name=column_name)
        return self._to_column_config(rows[0]) if rows else None

    def table_configs(self, table_name: Optional[str]) -> Sequence[ColumnEncryptionConfig]:
        """Every encrypted column of one table; empty when there are none."""
        if table_name is None:
            return ()
        snapshot = self._usable_snapshot(table_name)
        if snapshot is not None:
            return snapshot.by_table.get(table_name, ())
        rows = self._query(f"{self._joined_sql()} WHERE em.table_name {self._sql.equals} %s",
                           (table_name,), table_name=table_name)
        return tuple(self._to_column_config(row) for row in rows)

    @property
    def cache_size(self) -> int:
        snapshot = self._snapshot
        return 0 if snapshot is None else len(snapshot.columns)

    @property
    def is_cache_loaded(self) -> bool:
        return self._snapshot is not None

    # ---- internals ----

    def _usable_snapshot(self, table_name: Optional[str] = None,
                         column_name: Optional[str] = None) -> Optional[_Snapshot]:
        if not self._config.metadata_cache_enabled:
            return None
        snapshot = self._fresh(self._snapshot)
        if snapshot is None and self._started and not self._config.background_refresh_enabled \
                and not self._stop.is_set():
            # Nothing else will reload an expired snapshot, so reload it here
            # rather than querying the database on every later lookup. Lookups
            # that expire together wait for one reload instead of each running
            # their own.
            with self._refresh_lock:
                if not self._stop.is_set() and self._fresh(self._snapshot) is None:
                    self._load_snapshot(table_name=table_name, column_name=column_name)
            snapshot = self._fresh(self._snapshot)
        return snapshot

    def _load_snapshot(self, *, table_name: Optional[str] = None, column_name: Optional[str] = None) -> int:
        # Callers hold _refresh_lock. Stamped before the query, so expiry
        # counts from when the data was read rather than when the query
        # returned.
        loaded_at = time.monotonic()
        columns = self._load_all(table_name=table_name, column_name=column_name)
        by_table: Dict[str, List[ColumnEncryptionConfig]] = {}
        for config in columns.values():
            by_table.setdefault(config.table_name, []).append(config)
        snapshot = _Snapshot(
            MappingProxyType(dict(columns)),
            MappingProxyType({t: tuple(c) for t, c in by_table.items()}),
            loaded_at)
        with self._lifecycle_lock:
            if not self._stop.is_set():
                self._snapshot = snapshot
        logger.debug("MetadataManager.Refreshed", len(columns))
        return len(columns)

    def _load_all(self, *, table_name: Optional[str] = None,
                  column_name: Optional[str] = None) -> Dict[Tuple[str, str], ColumnEncryptionConfig]:
        # table_name / column_name only label a failure: the lookup that
        # triggered a reload.
        rows = self._query(f"{self._joined_sql()} ORDER BY em.table_name, em.column_name", (),
                           table_name=table_name, column_name=column_name)
        configs = (self._to_column_config(row) for row in rows)
        return {(c.table_name, c.column_name): c for c in configs}

    def _fresh(self, snapshot: Optional[_Snapshot]) -> Optional[_Snapshot]:
        if snapshot is None or time.monotonic() - snapshot.loaded_at >= self._config.metadata_cache_expiration_sec:
            return None
        return snapshot

    def _query(self, sql: str, params: Sequence[Any], *, table_name: Optional[str] = None,
               column_name: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            with self._connections.connection() as conn:
                return fetch_rows(conn, sql, params)
        except Exception as e:
            raise MetadataError(Messages.get_formatted("MetadataManager.LoadFailed", e),
                                table_name=table_name, column_name=column_name) from e

    def _joined_sql(self) -> str:
        # A LEFT join, so a column whose key_storage row is missing still reads
        # as encrypted (with no key) and a write to it fails instead of storing
        # plaintext. An inner join would make it look like an ordinary column.
        # em.key_id is the integer FK to ks.id; ks.key_id is the key's UUID.
        schema = self._config.metadata_schema
        p = _KS_PREFIX
        return ("SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, "
                "em.created_at, em.updated_at, "
                f"ks.id AS {p}id, ks.key_id AS {p}key_id, ks.name AS {p}name, "
                f"ks.master_key_arn AS {p}master_key_arn, ks.encrypted_data_key AS {p}encrypted_data_key, "
                f"ks.hmac_key AS {p}hmac_key, ks.key_spec AS {p}key_spec, "
                f"ks.created_at AS {p}created_at, ks.last_used_at AS {p}last_used_at "
                f"FROM {schema}.encryption_metadata em "
                f"LEFT JOIN {schema}.key_storage ks ON em.key_id {self._sql.equals} ks.id")

    @staticmethod
    def _to_column_config(row: Mapping[str, Any]) -> ColumnEncryptionConfig:
        key_metadata = None if row.get(f"{_KS_PREFIX}id") is None else KeyMetadata.from_row(row, _KS_PREFIX)
        return ColumnEncryptionConfig(
            table_name=row["table_name"],
            column_name=row["column_name"],
            algorithm=row.get("encryption_algorithm") or EncryptionAlgorithm.DEFAULT,
            key_id=None if row.get("key_id") is None else int(row["key_id"]),
            key_metadata=key_metadata,
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    @staticmethod
    def _finalize(stop: Event, thread_holder: List[Optional[Thread]]) -> None:
        # Runs when an unclosed manager is collected. The collection can happen
        # on the refresh thread itself, inside Event.wait while it holds the
        # event's lock, where stop.set() would deadlock. That thread exits on
        # its own anyway: it finds the manager gone as soon as wait returns.
        if thread_holder[0] is not current_thread():
            stop.set()

    @staticmethod
    def _refresh_loop(manager_ref: weakref.ref[MetadataManager], stop: Event, interval_sec: float) -> None:
        # Holds the manager only weakly between runs, so an unclosed manager
        # can still be collected.
        while not stop.wait(interval_sec):
            manager = manager_ref()
            if manager is None:
                return
            try:
                manager.refresh()
            except Exception as e:
                logger.warning("MetadataManager.RefreshFailed", e)
            del manager
