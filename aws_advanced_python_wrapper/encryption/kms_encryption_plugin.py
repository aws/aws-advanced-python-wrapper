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

"""The ``kms_encryption`` plugin encrypts configured columns on the client, with data keys protected by AWS KMS.

``execute``/``executemany`` parameters that the SQL analysis maps to encrypted columns are encrypted into a copy, which
the call context (``PluginService.current_call_context``) hands to the plugins after this one and to the driver. A
statement the plugin must change is refused when that context is missing or holds other arguments than the ones the
plugin analyzed. ``bytes`` SQL is analyzed as text in the connection's encoding when that encoding reads each ASCII
byte as itself and the text encodes back to the same bytes, and on MySQL only when every byte is ASCII. On MySQL the
driver also gets the SQL with ``_binary`` directly before each placeholder whose value the plugin encrypts, so the
server reads the payload as binary data. No ``_binary`` is added where a charset introducer, such as ``_binary``, is
already directly before the placeholder, where the value is ``None`` (in any parameter set, for ``executemany``), or on
a prepared cursor.

A statement the plugin can't encrypt safely, such as one that writes a literal value to an encrypted column, is
refused before the driver is called, and so is a value with no exact stored form, such as a UUID, a list or a dict,
bound for an encrypted column. So is a statement with a placeholder inside a quoted literal, quoted identifier or
comment when the analysis can tie it to a table with encrypted columns, and one that binds a ``%(name)s`` for an
encrypted column and for a use that isn't encrypted. A write the analysis can't fully read, and SQL given as an
object the plugin doesn't analyze (``bytes`` included, outside the cases above), runs unchanged; the database-side
trigger must then reject any plaintext in it. The plugin warns once for each such object type. For such a write,
when the analysis finds a table with encrypted columns in it, it warns once for each kind of statement, gap and set
of those tables, until it has logged 256 warnings of these two kinds on the connection. A write it can't tie to any
table, such as ``EXPLAIN ANALYZE INSERT`` or MySQL SQL with a ``/*! ... */`` comment, runs with no warning.

Encrypted result columns are decrypted on ``fetch*`` and on iteration (``Cursor.__next__``), psycopg's chained reads
and pipeline mode included. In pipeline mode a result's column description arrives with its rows, so the plugin reads
it then; rows of a result with encrypted columns that still come without one raise ``MetadataError``. An encrypted
value is stored as a payload of at least 65 bytes that starts with an HMAC-SHA256 tag over the rest; the database-side
trigger checks that length and that tag. NULL is stored as NULL. A stored value that isn't binary, such as text from a
text column, isn't a payload: reading it raises ``IntegrityCheckError``, as a value that fails its HMAC check does.
With ``encryption_return_unverified_data`` on, the driver's value comes back instead.
The plugin writes no audit records; with ``encryption_audit_logging_enabled`` on, it logs one warning when it is
created.

The shared specification, linked from ``docs/using-the-python-wrapper/using-plugins/UsingTheKmsEncryptionPlugin.md``,
defines the metadata tables and the ``encryption_*`` property names and defaults, and links the trigger SQL.
"""

from __future__ import annotations

import threading
from enum import Enum
from functools import partial
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Mapping,
                    Optional, Set, Tuple, Union)
from weakref import WeakKeyDictionary

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.connection_source import \
    PluginConnectionSource
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.encryption.sql_dialect import (MYSQL,
                                                                SqlDialect)
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyMetadataError,
                                                MetadataError)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.encryption.models import (
        ColumnEncryptionConfig, KeyMetadata)
    from aws_advanced_python_wrapper.encryption.sql_analyzer import \
        StatementAnalysis
    from aws_advanced_python_wrapper.encryption.sql_tokenizer import ColumnRef
    from aws_advanced_python_wrapper.plugin_service import (PluginCallContext,
                                                            PluginService)
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)

_EXECUTE = DbApiMethod.CURSOR_EXECUTE.method_name
_EXECUTEMANY = DbApiMethod.CURSOR_EXECUTEMANY.method_name
_FETCHONE = DbApiMethod.CURSOR_FETCHONE.method_name
_NEXT = DbApiMethod.CURSOR_NEXT.method_name
_WRITES = frozenset({_EXECUTE, _EXECUTEMANY})
_SQL_NAMES = ("query", "operation")
_PARAM_NAMES = ("params", "params_seq", "seq_params")
_PREPARED = ("MySQLCursorPrepared", "CMySQLCursorPrepared")  # mysql-connector 9.7.0; the *Dict classes subclass them
_ASCII = bytes(range(0x80))


class _Pending(Enum):
    """The type of ``_NOT_READY``: a one-member enum, so mypy narrows on ``is _NOT_READY``."""

    NOT_READY = 0


# _decoder's answer while the driver has no description for the result, as in psycopg's pipeline mode until a fetch
# receives the result. Nothing is cached then, so the decoder is built from the description that arrives.
_NOT_READY = _Pending.NOT_READY


class _Result:
    """The analysis of a cursor's last statement, and the row decoder built from it."""

    def __init__(self, analysis: StatementAnalysis, tables: List[str]):
        self.analysis = analysis
        self.tables = tables
        self.decode: Any = _Result  # _Result itself: not built yet; None: no encrypted column to decode

    def __repr__(self) -> str:
        return f"_Result({self.tables})"


class KmsEncryptionPlugin(Plugin):
    """Encrypts and decrypts the configured columns of one connection. Its metadata, data-key cache and KMS client
    are built at the first statement the plugin analyzes; ``release_resources`` closes the metadata manager, the cache
    and the KMS client."""

    _SUBSCRIBED_METHODS: Set[str] = {_EXECUTE, _EXECUTEMANY, _FETCHONE, DbApiMethod.CURSOR_FETCHMANY.method_name,
                                     DbApiMethod.CURSOR_FETCHALL.method_name, _NEXT}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props
        self._config = EncryptionConfig.from_props(props)
        # Under the internal pool, the service's dialect turns generic after connect.
        self._driver_dialect: DriverDialect = plugin_service.driver_dialect
        # Imported here so the wrapper imports without the encryption extra; a missing extra fails at connect.
        from aws_advanced_python_wrapper.encryption import (encryption_service,
                                                            sql_analyzer)
        self._crypto = encryption_service.EncryptionService
        self._analyze = sql_analyzer.analyze
        self._hidden_placeholder_gap = sql_analyzer.AnalysisGap.HIDDEN_PLACEHOLDER
        self._lock = threading.Lock()
        self._sql: Optional[SqlDialect] = None
        self._metadata: Optional[MetadataManager] = None
        self._cache: Optional[DataKeyCache] = None
        self._keys: Optional[KeyManager] = None
        self._kms_client: Any = None
        self._closed = False  # set by release_resources(); a closed plugin builds nothing and looks nothing up
        # Threads may use different cursors of one connection, so the three below share a lock of their own: _lock
        # is held across the first statement's metadata load.
        self._state_lock = threading.Lock()
        self._results: WeakKeyDictionary[Any, _Result] = WeakKeyDictionary()  # by driver cursor: none outlives its cursor
        self._old_keys: Dict[int, KeyMetadata] = {}  # key_storage rows of rotated-out keys, by id
        self._warned: Set[Any] = set()
        if self._config.audit_logging_enabled:
            logger.warning("KmsEncryptionPlugin.AuditNotWritten")

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    # Any, not the base's type: target is the driver's cursor, whose attributes the plugin reads.
    def execute(self, target: Any, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        if method_name in _WRITES:
            return self._execute(target, method_name, execute_func, args, kwargs)
        decode = self._decoder(target)  # before the driver call, so a refused row factory reads no row (see _decoder_after_fetch)
        result = execute_func()
        if decode is None or result is None:
            return result
        if decode is _NOT_READY:  # the description arrives with the rows: at this fetch, or at a loop's first row
            if method_name == _NEXT:
                return map(partial(self._decode_late, target), result)  # per row: the loop has received no row yet
            if method_name != _FETCHONE and not result:
                return result  # no rows to place
            decode = self._decoder_after_fetch(target)  # once: this fetch has received the result, and with it the description
            if decode is None:
                return result
        if method_name == _NEXT:
            return map(decode, result)  # once per loop: the driver's iterator still fetches the rows, a page at a time
        return decode(result) if method_name == _FETCHONE else [decode(row) for row in result]

    def release_resources(self) -> None:
        self._closed = True  # first, and without _lock, so a close never waits on a metadata load; a build re-checks it
        if self._metadata is not None:
            self._metadata.close()
        if self._cache is not None:
            self._cache.close()
        with self._state_lock:
            self._old_keys.clear()  # rotated-out key rows: their HMAC keys are bytes, which can't be wiped, only dropped
        if self._kms_client is not None:
            self._kms_client.close()  # its pooled connections to KMS, which otherwise stay open until a garbage collection

    # ---- writes ----

    def _execute(self, target: Any, method_name: str, execute_func: Callable, args: Any, kwargs: Any) -> Any:
        new_args, new_kwargs = self._encrypted_call(target, method_name, args, kwargs)
        if new_args is not args or new_kwargs is not kwargs:
            context = self._plugin_service.current_call_context
            if context is None or not _holds(context, args, kwargs):
                # Nothing would hand the copies to the driver, or this would change another statement's arguments.
                raise EncryptionFailedError(Messages.get("KmsEncryptionPlugin.ArgumentsChanged"))
            context.args, context.kwargs = new_args, new_kwargs  # undone by the plugin manager when this returns
        return execute_func()

    def _encrypted_call(self, target: Any, method_name: str, args: Any, kwargs: Any) -> Any:
        self._remember(target, None)
        sql: Any = args[0] if args else next((kwargs[n] for n in _SQL_NAMES if n in kwargs), None)
        params_at = 1 if len(args) > 1 else next((n for n in _PARAM_NAMES if n in kwargs), None)
        params = None if params_at is None else (args[1] if params_at == 1 else kwargs[params_at])
        many = method_name == _EXECUTEMANY
        if many and params is not None and not isinstance(params, (list, tuple)):
            params = list(params)  # an iterator can be read only once
            args, kwargs = _replace(args, kwargs, params_at, params)
        if many and params is not None and len(params) == 0:
            return args, kwargs  # an empty batch has no value to encrypt, and the driver sends nothing
        text: Any = sql
        codec: Optional[str] = None
        if isinstance(sql, bytes):
            codec = self._codec(target)
            text = _decoded(sql, codec)
            # mysql-connector keeps reporting the character set it negotiated (python_charset) when SQL changes the
            # session's, and every client character set MySQL accepts, except swe7, reads ASCII bytes as ASCII, so only
            # all-ASCII ``bytes`` SQL is analyzed. swe7 has no Python codec, so a connection reporting it raises above; a
            # session switched to swe7 in SQL is a documented limit.
            if not sql.isascii() and SqlDialect.for_driver_dialect(self._driver_dialect) is MYSQL:
                text = None
        elif type(sql).__module__ == "psycopg.sql":
            text = sql.as_string(target)  # a psycopg Composable: analyzed as text, sent as the object
        if not isinstance(text, str):  # other SQL objects, and bytes the checks above reject, run unanalyzed
            if self._new_warning(type(sql)):
                logger.warning("KmsEncryptionPlugin.NotAnalysed", type(sql).__name__)
            return args, kwargs
        metadata = self._components()
        assert self._sql is not None
        analysis = self._analyze(text, self._sql, (params[0] if params else None) if many else params)
        tables = [t.name for t in analysis.tables if metadata.table_configs(t.name)]
        if tables and analysis.gap is self._hidden_placeholder_gap:
            # The driver binds a hidden placeholder too, so the analysis can't tell which value lands in an encrypted column.
            raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.HiddenPlaceholder", ", ".join(tables)))
        targets: Dict[Any, ColumnEncryptionConfig] = {}
        if tables:
            self._remember(target, _Result(analysis, tables))
            only = analysis.tables[0].name if len(analysis.tables) == 1 else None
            for column in analysis.unbound_writes:
                config = metadata.column_config(column.table or only, column.column)
                if config is not None:
                    raise MetadataError(
                        Messages.get_formatted("KmsEncryptionPlugin.UnboundWrite", config.column_identifier),
                        table_name=config.table_name, column_name=config.column_name)
            if analysis.gap is not None and analysis.writes:
                self._warn_once(analysis, tables)  # passed through; the trigger must reject plaintext
            else:
                targets = self._targets(analysis, metadata, tables, only)
        if targets and params:
            params = [self._encrypt(p, targets) for p in params] if many else self._encrypt(params, targets)
            args, kwargs = _replace(args, kwargs, params_at, params)
            if self._sql is MYSQL and not any(c.__name__ in _PREPARED for c in type(target).__mro__):
                # MySQL reads a payload sent as text as characters (Warning 1300); _binary makes it binary data.
                # Every parameter set shares one SQL text, so a key that is None in any set gets no prefix.
                keys = {k for k in targets if all(_bound(p, k) is not None for p in (params if many else [params]))}
                prefixed = _binary_prefix(text, analysis, keys)
                if prefixed != text:
                    at = 0 if args else next(n for n in _SQL_NAMES if n in kwargs)
                    args, kwargs = _replace(args, kwargs, at, prefixed if codec is None else prefixed.encode(codec))
        return args, kwargs

    def _codec(self, target: Any) -> Optional[str]:
        """The codec name of the connection's client encoding, or None when the dialect can't find the connection.
        psycopg raises NotSupportedError for an encoding Python has no codec for. A MySQL name Python has no codec for
        makes ``_decoded`` raise LookupError."""
        conn = self._driver_dialect.get_connection_from_obj(target)
        if conn is None:
            return None
        if SqlDialect.for_driver_dialect(self._driver_dialect) is MYSQL:
            return conn.python_charset  # not conn.charset: utf8mb4 is no Python codec
        return conn.info.encoding

    def _targets(self, analysis: StatementAnalysis, metadata: MetadataManager, tables: List[str],
                 only: Optional[str]) -> Dict[Any, ColumnEncryptionConfig]:
        """The encrypted column of each placeholder key. Raises for a value that may belong to an encrypted column
        the analysis can't tie it to."""
        annotated = {use.ordinal for use in analysis.annotations}
        # The columns the analysis infers before annotations override them, merged as the analysis merges them.
        inferred = {use.ordinal: use.column for use in (*analysis.comparisons, *analysis.assignments)}
        targets: Dict[Any, ColumnEncryptionConfig] = {}
        encrypted: Set[int] = set()  # the ordinals bound for an encrypted column
        for ordinal, column in sorted(analysis.parameter_columns().items()):
            config = metadata.column_config(column.table or only, column.column)
            if config is not None:
                # A %(name)s used for several encrypted columns is encrypted once, with the first one's key.
                targets.setdefault(analysis.placeholders[ordinal].key, config)
                encrypted.add(ordinal)
            else:
                self._refuse_unmapped(analysis, metadata, tables, only, column, ordinal in annotated,
                                      inferred.get(ordinal))
        for placeholder in analysis.placeholders:
            # One mapping entry can't hold both the payload and the plaintext a %(name)s's other use needs.
            config = targets.get(placeholder.key)
            if config is not None and placeholder.ordinal not in encrypted:
                raise MetadataError(
                    Messages.get_formatted("KmsEncryptionPlugin.SharedParameter", placeholder.key, config.column_identifier),
                    table_name=config.table_name, column_name=config.column_name)
        return targets

    def _refuse_unmapped(self, analysis: StatementAnalysis, metadata: MetadataManager, tables: List[str],
                         only: Optional[str], column: ColumnRef, annotated: bool, inferred: Optional[ColumnRef]) -> None:
        """Raise for a value whose column has no encryption metadata but whose value may still be an encrypted
        column's: the analysis infers an encrypted column for an annotated value, or the column has no table and
        the statement reads an encrypted column of that name."""
        assert self._sql is not None
        checked: Optional[ColumnRef] = column
        if annotated:
            found = None if inferred is None else metadata.column_config(inferred.table or only, inferred.column)
            if found is not None:
                raise MetadataError(
                    Messages.get_formatted("KmsEncryptionPlugin.AnnotationNotEncrypted", found.column_identifier),
                    table_name=found.table_name, column_name=found.column_name)
            if column.table is not None and any(
                    self._sql.metadata_key(column.table) == self._sql.metadata_key(t.name) for t in analysis.tables):
                return  # trusted: it names a table the statement uses
            checked = inferred  # any other annotation counts as none
        if checked is None or checked.table is not None:
            return
        found = next((c for c in (metadata.column_config(t, checked.column) for t in tables) if c is not None), None)
        if found is not None:
            raise MetadataError(
                Messages.get_formatted("KmsEncryptionPlugin.UnknownColumnTable", found.column_identifier),
                table_name=found.table_name, column_name=found.column_name)

    def _encrypt(self, params: Any, targets: Dict[Any, ColumnEncryptionConfig]) -> Any:
        mapping = isinstance(params, Mapping)
        out = dict(params) if mapping else list(params)
        for key, config in targets.items():
            if (key in out) if mapping else key < len(out):
                out[key] = self._seal(out[key], config)
        return out if mapping else (tuple(out) if isinstance(params, tuple) else out)

    def _seal(self, value: Any, config: ColumnEncryptionConfig) -> Any:
        if value is None:
            return None
        if TypeMarker.from_object(value) == TypeMarker.GENERIC:  # it would be stored as str(value) and read back as text
            raise EncryptionFailedError(
                Messages.get_formatted("KmsEncryptionPlugin.NoExactForm", type(value).__name__, config.column_identifier),
                data_type="GENERIC")
        key = config.key_metadata
        if key is None or key.hmac_key is None or config.key_id is None or not config.is_usable:
            raise InvalidKeyMetadataError(
                Messages.get_formatted("KmsEncryptionPlugin.UnusableColumn", config.column_identifier))
        assert self._keys is not None
        data_key = self._keys.decrypt_data_key(key.encrypted_data_key, key.master_key_arn)
        hmac_key = bytearray(key.hmac_key)  # this call's copy, wiped below (best effort: the key row keeps its own bytes)
        try:
            return self._crypto.encrypt(value, data_key, hmac_key, config.algorithm, key_id=config.key_id)
        finally:
            self._crypto.wipe(data_key)
            self._crypto.wipe(hmac_key)

    def _warn_once(self, analysis: StatementAnalysis, tables: List[str]) -> None:
        if self._new_warning((analysis.kind, analysis.gap, tuple(tables)), bounded=True):  # bounded: shapes can come from user SQL
            reason = analysis.detail or (analysis.gap.value if analysis.gap is not None else "")  # the gap's fixed text
            logger.warning("KmsEncryptionPlugin.PassedThrough", ", ".join(tables), reason)

    def _new_warning(self, key: Any, bounded: bool = False) -> bool:
        """Whether a warning for ``key`` is still to be logged on this connection; records it. With ``bounded``, only
        while fewer than 256 keys are recorded."""
        with self._state_lock:
            if key in self._warned or (bounded and len(self._warned) >= 256):
                return False
            self._warned.add(key)
            return True

    # ---- reads ----

    def _remember(self, target: Any, state: Optional[_Result]) -> None:
        """Set or clear the state of the driver cursor's last statement."""
        with self._state_lock:
            if state is None:
                self._results.pop(target, None)
            else:
                self._results[target] = state

    def _decoder(self, target: Any) -> Union[Callable[[Any], Any], None, _Pending]:
        """The row decoder of the cursor's last statement, None when it has no encrypted column to decode, or
        ``_NOT_READY`` while the driver has no description for its result."""
        with self._state_lock:
            state = self._results.get(target)
        if state is None or state.decode is None:
            return None  # no result remembered, or one without encrypted columns: nothing to look up
        if self._closed:  # before the driver reads a row; a decoder built before the close would read key rows and call KMS
            raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        if state.decode is not _Result:
            return state.decode
        description = target.description
        if description is None:
            return _NOT_READY
        assert self._metadata is not None
        names = [d[0] for d in description]
        lowered = [str(n).lower() for n in names]  # so a name repeated in another letter case counts as a repeat
        known = state.analysis.result_columns
        only = state.analysis.tables[0].name if len(state.analysis.tables) == 1 else None
        positions: Dict[int, ColumnEncryptionConfig] = {}
        for i, name in enumerate(names):
            ref = known[i] if known is not None and i < len(known) else None
            if ref is not None:
                config = self._metadata.column_config(ref.table or only, ref.column)
            else:  # left to name matching in any letter case: a unique encrypted column of that name, named once in the result
                found = [c for t in state.tables for c in self._metadata.table_configs(t)
                         if c.column_name.lower() == lowered[i]]
                config = found[0] if len(found) == 1 and lowered.count(lowered[i]) == 1 else None
            if config is not None:
                positions[i] = config
        factory = getattr(target, "row_factory", None)  # psycopg only; checked before any row is built, except through _decoder_after_fetch
        if positions and factory is not None:
            from psycopg import rows
            if factory not in (rows.tuple_row, rows.dict_row, rows.namedtuple_row):
                raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.RowFactory", factory))
        # A dict row keeps only the last column of each name, so it is decoded with that column's configuration.
        last = {name: i for i, name in enumerate(names)}
        by_name = {name: positions[i] for name, i in last.items() if i in positions}
        state.decode = (lambda row: self._decode_row(row, positions, by_name)) if positions else None
        return state.decode

    def _decoder_after_fetch(self, target: Any) -> Optional[Callable[[Any], Any]]:
        """The row decoder of a result whose rows have arrived but which had no description when the read began:
        psycopg's pipeline mode fills it in when a fetch, or a loop's first row, receives the result. So an unsupported
        row factory is refused only after the driver has built rows with it, from the stored payloads."""
        decode = self._decoder(target)  # built at the first rows, then cached on the result
        if decode is _NOT_READY:  # rows of a result with encrypted columns, and still no description to place them
            raise MetadataError(Messages.get("KmsEncryptionPlugin.NoResultDescription"))
        return decode

    def _decode_late(self, target: Any, row: Any) -> Any:
        """Decode a row of a loop that began before its result had a description (see _decoder_after_fetch)."""
        decode = self._decoder_after_fetch(target)
        return row if decode is None else decode(row)

    def _decode_row(self, row: Any, positions: Dict[int, ColumnEncryptionConfig],
                    by_name: Dict[Any, ColumnEncryptionConfig]) -> Any:
        if isinstance(row, dict):
            out = dict(row)
            for name, config in by_name.items():
                out[name] = self._open(out[name], config)
            return out
        values = list(row)
        for i, config in positions.items():
            values[i] = self._open(values[i], config)
        if hasattr(row, "_make"):
            return type(row)._make(values)
        return tuple(values) if isinstance(row, tuple) else values

    def _open(self, value: Any, config: ColumnEncryptionConfig) -> Any:
        if value is None:
            return None
        if not isinstance(value, (bytes, bytearray, memoryview)):  # text from a text column, say: not a payload
            return self._unverified(value, config, "KmsEncryptionPlugin.NotAPayload")
        if self._closed:  # a loop that began before the close decodes lazily and never reaches _decoder again
            raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        assert self._keys is not None
        key_id = self._crypto.key_id_from_payload(value)
        if key_id is None or key_id == config.key_id:  # the column's own key row
            key = config.key_metadata
            if key is None or key.hmac_key is None or not config.is_usable:
                raise InvalidKeyMetadataError(
                    Messages.get_formatted("KmsEncryptionPlugin.UnusableColumn", config.column_identifier))
        else:  # a value written before the column's key was rotated, or a value that isn't a payload
            with self._state_lock:
                kept = self._old_keys.get(key_id)
            key = kept or self._keys.key_metadata_by_id(key_id)
            if key is None or not key.is_usable or key.hmac_key is None:
                # Never retried with the column's current key. The message leaves out the id: for a stored value
                # that isn't a payload, it is four bytes of that value.
                return self._unverified(value, config, "KmsEncryptionPlugin.UnknownKey")
            with self._state_lock:
                if self._closed:  # closed during the lookup: the close cleared _old_keys under this lock, so don't refill it
                    raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
                self._old_keys[key_id] = key
        data_key = self._keys.decrypt_data_key(key.encrypted_data_key, key.master_key_arn)
        hmac_key = bytearray(key.hmac_key)
        try:
            return self._crypto.decrypt(value, data_key, hmac_key, config.algorithm)
        except IntegrityCheckError:
            if not self._config.return_unverified_data:
                raise
        finally:
            self._crypto.wipe(data_key)
            self._crypto.wipe(hmac_key)
        return value

    def _unverified(self, value: Any, config: ColumnEncryptionConfig, message_key: str) -> Any:
        if self._config.return_unverified_data:
            return value  # lenient mode: the driver's value, nothing logged
        raise IntegrityCheckError(Messages.get_formatted(message_key, config.column_identifier),
                                  algorithm=config.algorithm)

    # ---- lifecycle ----

    def _components(self) -> MetadataManager:
        if self._closed:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        if self._metadata is None:
            with self._lock:
                if self._closed:  # closed while this thread waited for another thread's build
                    raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
                if self._metadata is None:
                    config = self._config
                    sql = SqlDialect.for_driver_dialect(self._driver_dialect)
                    source = PluginConnectionSource(self._plugin_service)
                    session = AwsCredentialsManager.get_session(self._plugin_service.current_host_info, self._props,
                                                                config.kms_region)
                    client = KeyManager.create_kms_client(session, config)
                    metadata = MetadataManager(source, sql, config)
                    metadata.start()
                    # The cache's constructor starts its cleanup thread, so it comes after every step that can
                    # fail; if that thread can't start, stop the refresh thread start() began.
                    try:
                        cache = DataKeyCache(config.data_key_cache_max_size, config.data_key_cache_expiration_sec,
                                             enabled=config.data_key_cache_enabled)
                    except BaseException:  # noqa: BLE001 - release the started manager, then re-raise the error as is
                        metadata.close()
                        raise
                    self._sql, self._cache, self._kms_client = sql, cache, client
                    self._keys = KeyManager(client, source, sql, config, cache)
                    self._metadata = metadata  # last: the check outside the lock reads it
                    if self._closed:  # closed during this build: release what it started, which that close couldn't see
                        self.release_resources()
                        raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        return self._metadata


def _replace(args: Any, kwargs: Any, at: Any, value: Any) -> Any:
    if isinstance(at, int):
        return (*args[:at], value, *args[at + 1:]), kwargs
    return args, {**kwargs, at: value}


def _holds(context: PluginCallContext, args: Tuple[Any, ...], kwargs: Mapping[str, Any]) -> bool:
    """Whether ``context`` holds, by identity, the arguments the plugin was handed, so it is this call's."""
    return (len(context.args) == len(args) and all(a is b for a, b in zip(context.args, args))
            and context.kwargs.keys() == kwargs.keys() and all(context.kwargs[k] is v for k, v in kwargs.items()))


def _decoded(sql: bytes, codec: Optional[str]) -> Optional[str]:
    """The text the analysis reads in ``sql``, or None when ``codec`` is None, reads an ASCII byte as another character,
    can't decode ``sql``, or doesn't encode the text back to ``sql``."""
    if codec is None:
        return None
    try:
        # A codec that reads an ASCII byte as another character (shift_jis_2004 reads 0x5C and 0x7E as a yen sign and
        # an overline), or text that doesn't encode back to the same bytes, would show the analysis another statement.
        if _ASCII.decode(codec) != _ASCII.decode("ascii"):
            return None
        text = sql.decode(codec)
        return text if text.encode(codec) == sql else None
    except (UnicodeDecodeError, UnicodeEncodeError):  # their .object is the whole statement: keep nothing of it
        return None


def _bound(params: Any, key: Any) -> Any:
    """The value one parameter set binds to ``key``, or None if it binds none."""
    if isinstance(params, Mapping):
        return params.get(key)
    return params[key] if isinstance(key, int) and key < len(params) else None


def _binary_prefix(sql: str, analysis: StatementAnalysis, encrypted_keys: Set[Any]) -> str:
    """Insert ``_binary `` before every placeholder bound for an encrypted column that has no introducer."""
    starts = sorted({p.start for p in analysis.placeholders
                     if p.key in encrypted_keys and p.introducer is None and not p.hidden})
    # Joined once: splicing at each placeholder copies the whole SQL each time, quadratic in a bulk INSERT.
    parts: List[str] = []
    previous = 0
    for start in starts:
        parts += (sql[previous:start], "_binary ")
        previous = start
    parts.append(sql[previous:])
    return "".join(parts)


class KmsEncryptionPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return KmsEncryptionPlugin(plugin_service, props)
