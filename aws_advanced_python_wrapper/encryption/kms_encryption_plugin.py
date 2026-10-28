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

import threading
from contextlib import nullcontext
from enum import Enum
from functools import partial
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Mapping,
                    Optional, Sequence, Set, Tuple, Union)
from weakref import WeakKeyDictionary

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.connection_source import \
    PluginConnectionSource
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (EncryptionConfigError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidAlgorithmError,
                                                InvalidKeyMetadataError,
                                                MetadataError)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import (CanReleaseResources, Plugin,
                                                PluginFactory)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.encryption.models import (
        ColumnEncryptionConfig, KeyMetadata)
    from aws_advanced_python_wrapper.encryption.sql_analyzer import \
        StatementAnalysis
    from aws_advanced_python_wrapper.encryption.sql_tokenizer import ColumnRef
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)

_EXECUTE = DbApiMethod.CURSOR_EXECUTE.method_name
_EXECUTEMANY = DbApiMethod.CURSOR_EXECUTEMANY.method_name
_FETCHONE = DbApiMethod.CURSOR_FETCHONE.method_name
_ITER = DbApiMethod.CURSOR_ITER.method_name
_NEXT = DbApiMethod.CURSOR_NEXT.method_name
_NEXTSET = DbApiMethod.CURSOR_NEXTSET.method_name
_COPY = DbApiMethod.CURSOR_COPY.method_name
_EXECUTES = frozenset({_EXECUTE, _EXECUTEMANY})
_ONE_ROW = frozenset({_FETCHONE, _NEXT})
# Bounds the warnings recorded on one connection: statement shapes can come from user SQL.
_MAX_WARNING_KEYS = 256
_SQL_NAMES = ("query", "operation")
_COPY_SQL_NAMES = ("statement",)
_PARAM_NAMES = ("params", "params_seq", "seq_params")
# With the data-key cache off, one call's data keys by encrypted data key and master key, zeroed when the call ends.
_CallKeys = Dict[Tuple[Optional[str], Optional[str]], bytearray]


class _Pending(Enum):
    """The type of ``_NOT_READY``: a one-member enum, so mypy narrows on ``is _NOT_READY``."""

    NOT_READY = 0


# _decoder's answer while the driver has no description for the result, as in psycopg's pipeline mode until a fetch
# receives the result. Nothing is cached then, so the decoder is built from the description that arrives.
_NOT_READY = _Pending.NOT_READY


class _Result:
    """The analysis of a cursor's last statement, the encrypted columns its tables had when it ran, and the row decoder
    built from those. ``many`` is set for an ``executemany``: ``nextset()`` keeps its result, and the decoder is built
    again from each set's description."""

    def __init__(self, analysis: StatementAnalysis, configs: Dict[str, Sequence[ColumnEncryptionConfig]], many: bool):
        self.analysis = analysis
        # By the metadata key of each table, as the snapshot keys them: a fetch looks nothing up.
        self.configs = configs
        self.many = many
        # _Result itself: not built yet; None: no encrypted column to decode.
        self.decode: Any = _Result
        # A result set after the first of a multi-statement string.
        self.later = False

    def __repr__(self) -> str:
        return f"_Result({list(self.configs)})"


class KmsEncryptionPlugin(Plugin, CanReleaseResources):
    """Encrypts and decrypts the configured columns of one connection. Its metadata, data-key cache and KMS client
    are built at the first statement the plugin analyzes; ``release_resources``, which each close of the connection
    calls, closes the metadata manager, the cache and the KMS client."""

    _SUBSCRIBED_METHODS: Set[str] = {_EXECUTE, _EXECUTEMANY, _FETCHONE, DbApiMethod.CURSOR_FETCHMANY.method_name,
                                     DbApiMethod.CURSOR_FETCHALL.method_name, _ITER, _NEXT, _NEXTSET, _COPY}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props
        self._config = EncryptionConfig.from_props(props)
        # Under the internal pool, the service's dialect turns generic after connect.
        self._driver_dialect: DriverDialect = plugin_service.driver_dialect
        # Before any connection opens: the plugins are built after the driver dialect is chosen.
        if not self._driver_dialect.engine_name:
            raise EncryptionConfigError(
                Messages.get_formatted("KmsEncryptionPlugin.UnsupportedDriver", self._driver_dialect.driver_name))
        # Imported here so the wrapper imports without the encryption extra; a missing extra fails at connect.
        from aws_advanced_python_wrapper.encryption import (encryption_service,
                                                            sql_analyzer)
        self._crypto = encryption_service.EncryptionService
        self._analyze = sql_analyzer.analyze
        self._gaps = sql_analyzer.AnalysisGap
        self._lock = threading.Lock()
        # Built here, not with the metadata: _execute scopes a statement's lookups before the first build runs.
        self._source = PluginConnectionSource(plugin_service)
        self._metadata: Optional[MetadataManager] = None
        self._cache: Optional[DataKeyCache] = None
        self._keys: Optional[KeyManager] = None
        self._kms_client: Any = None
        # Set by release_resources(); a closed plugin builds nothing and looks nothing up.
        self._closed = False
        # Threads may use different cursors of one connection, so the three below share a lock of their own: _lock
        # is held across the first statement's metadata load.
        self._state_lock = threading.Lock()
        # By driver cursor: none outlives its cursor.
        self._results: WeakKeyDictionary[Any, _Result] = WeakKeyDictionary()
        # Key_storage rows of rotated-out keys, by id.
        self._old_keys: Dict[int, KeyMetadata] = {}
        self._warned: Set[Any] = set()
        if self._config.audit_logging_enabled:
            logger.warning("KmsEncryptionPlugin.AuditNotWritten")

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    # Any, not the base's type: target is the driver's cursor, whose attributes the plugin reads.
    def execute(self, target: Any, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        if method_name == _NEXTSET:
            return self._next_set(target, execute_func)

        if method_name in _EXECUTES:
            return self._execute(target, method_name, execute_func, args, kwargs)
        if method_name == _COPY:
            scope = nullcontext() if self._config.metadata_cache_enabled else self._source.statement()
            with scope:
                self._refuse_copy(target, args, kwargs)
            return execute_func()
        # Before the driver call, so a refused row factory reads no row (see _decoder_after_fetch).
        decode = self._decoder(target)
        result = execute_func()
        if decode is None or result is None:
            return result
        # The description arrives with the rows: at this fetch, or at a loop's first row.
        if decode is _NOT_READY:
            if method_name == _ITER:
                # Per row: the loop has received no row yet.
                return map(partial(self._decode_late, target), result)
            if method_name not in _ONE_ROW and not result:
                return result
            # Once: this fetch has received the result, and with it the description.
            decode = self._decoder_after_fetch(target)
            if decode is None:
                return result
        if method_name == _ITER:
            # Once per loop: the driver's iterator still fetches the rows, a page at a time.
            return map(decode, result)
        data_keys = self._call_keys()
        try:
            return decode(result, data_keys) if method_name in _ONE_ROW else [decode(row, data_keys) for row in result]
        finally:
            self._wipe(data_keys)

    def release_resources(self) -> None:
        # First, and without _lock, so a close never waits on a metadata load; a build re-checks it.
        self._closed = True
        with self._state_lock:
            # Rotated-out key rows: their HMAC keys are bytes, which can't be wiped, only dropped.
            self._old_keys.clear()
        # The KMS client's close() frees its pooled connections to KMS, which otherwise stay open until a garbage collection.
        for resource in (self._metadata, self._cache, self._kms_client):
            if resource is not None:
                try:
                    resource.close()
                except Exception as e:  # noqa: BLE001 - one failed release step must not stop the others or raise from close()
                    logger.warning("KmsEncryptionPlugin.ReleaseFailed", type(resource).__name__, type(e).__name__)

    def _execute(self, target: Any, method_name: str, execute_func: Callable, args: Tuple[Any, ...],
                 kwargs: Dict[str, Any]) -> Any:
        # The metadata snapshot answers the lookups; without it, the statement's lookups share one connection, closed
        # before the driver call.
        scope = nullcontext() if self._config.metadata_cache_enabled else self._source.statement()
        with scope:
            new_args, new_kwargs = self._encrypted_call(target, method_name, args, kwargs)
        if new_args is not args or new_kwargs is not kwargs:
            context = self._plugin_service.current_call_context
            if context is None or not context.holds(args, kwargs):
                # Nothing would hand the copies to the driver, or this would change another statement's arguments.
                raise EncryptionFailedError(Messages.get("KmsEncryptionPlugin.ArgumentsChanged"))
            # Undone by the plugin manager when this returns.
            context.args, context.kwargs = new_args, new_kwargs
        return execute_func()

    def _encrypted_call(self, target: Any, method_name: str, args: Tuple[Any, ...],
                        kwargs: Dict[str, Any]) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        self._remember(target, None)
        sql_at = 0 if args else _keyword(kwargs, _SQL_NAMES)
        params_at = 1 if len(args) > 1 else _keyword(kwargs, _PARAM_NAMES)
        sql = _argument(args, kwargs, sql_at)
        params = _argument(args, kwargs, params_at)
        many = method_name == _EXECUTEMANY
        if many and params is not None:
            if not isinstance(params, (list, tuple)):
                params = list(params)
                args, kwargs = _replace(args, kwargs, params_at, params)
            # An empty batch has no value to encrypt, and the driver sends nothing.
            if not params:
                return args, kwargs
        text, codec = self._driver_dialect.analyzable_sql(sql, target)
        # Other SQL objects, and bytes the driver dialect rejects, run unanalyzed.
        if text is None:
            if self._new_warning(type(sql)):
                logger.warning("KmsEncryptionPlugin.NotAnalysed", type(sql).__name__)
            return args, kwargs
        metadata = self._components()
        analysis = self._analyze(text, type(self._driver_dialect), params[0] if many and params else params)
        tables: List[str] = []
        configs: Dict[str, Sequence[ColumnEncryptionConfig]] = {}
        for table in analysis.tables:
            found = metadata.table_configs(table.name)
            if found:
                tables.append(table.name)
                configs[self._driver_dialect.metadata_key(table.name)] = found
        self._refuse_shadowing_cte(analysis, metadata)
        if tables and analysis.gap is self._gaps.HIDDEN_PLACEHOLDER:
            # The driver binds a hidden placeholder too, so the analysis can't tell which value lands in an encrypted column.
            raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.HiddenPlaceholder", ", ".join(tables)))
        if tables and analysis.gap is self._gaps.EXECUTABLE_COMMENT:
            # MySQL runs the comment's body, which can name another table or move a value to another column.
            raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.ExecutableComment", ", ".join(tables)))
        targets: Dict[Any, ColumnEncryptionConfig] = {}
        if tables:
            only = analysis.tables[0].name if len(analysis.tables) == 1 else None
            # First, so a refused statement leaves no result.
            self._refuse_unsafe_comparisons(analysis, metadata, tables, only)
            if (params and analysis.gap is not None and analysis.gap is not self._gaps.DRIVER_ERROR
                    and self._config.fail_on_unmapped_parameter):
                # The values would reach the server as given, a compared value included.
                raise MetadataError(Messages.get_formatted(
                    "KmsEncryptionPlugin.UnmappedParameters", ", ".join(tables), analysis.gap.value))
            self._remember(target, _Result(analysis, configs, many))
            for column in analysis.unbound_writes:
                config = metadata.column_config(column.table or only, column.column)
                if config is not None:
                    raise MetadataError(
                        Messages.get_formatted("KmsEncryptionPlugin.UnboundWrite", config.column_identifier),
                        table_name=config.table_name, column_name=config.column_name)
            if analysis.gap is not None and analysis.writes:
                # Passed through; the trigger must reject plaintext.
                self._warn_once(analysis, tables)
            else:
                targets = self._targets(analysis, metadata, tables, only)
        if targets and params:
            data_keys = self._call_keys()
            try:
                params = ([self._encrypt(p, targets, data_keys) for p in params] if many
                          else self._encrypt(params, targets, data_keys))
            finally:
                self._wipe(data_keys)
            args, kwargs = _replace(args, kwargs, params_at, params)
            prefix = self._driver_dialect.binary_placeholder_prefix(target)
            if prefix is not None:
                # Every parameter set shares one SQL text, so a key that is None in any set gets no prefix.
                keys = {k for k in targets if all(_bound(p, k) is not None for p in (params if many else [params]))}
                prefixed = _binary_prefix(text, analysis, keys, prefix)
                if prefixed != text:
                    args, kwargs = _replace(args, kwargs, sql_at, prefixed if codec is None else prefixed.encode(codec))
        return args, kwargs

    def _refuse_copy(self, target: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
        """Raise for a ``COPY ... FROM`` that names an encrypted column, and for a ``COPY`` that the plugin can't parse
        and that can write a table with encrypted columns: its rows reach the server as the application writes them, and
        no annotation can change that. A ``COPY ... FROM`` without a column list on such a table runs with a warning, and
        the database trigger must reject plaintext. Also raise for a CTE that has the name of such a table, and for a
        ``COPY`` with parameters on such a table while ``encryption_fail_on_unmapped_parameter`` is on, because the
        plugin doesn't encrypt them."""
        self._remember(target, None)
        sql = _argument(args, kwargs, 0 if args else _keyword(kwargs, _COPY_SQL_NAMES))
        text, _ = self._driver_dialect.analyzable_sql(sql, target)
        if text is None:
            if self._new_warning(type(sql)):
                logger.warning("KmsEncryptionPlugin.NotAnalysed", type(sql).__name__)
            return
        metadata = self._components()
        params = _argument(args, kwargs, 1 if len(args) > 1 else _keyword(kwargs, _PARAM_NAMES))
        analysis = self._analyze(text, type(self._driver_dialect), params)
        self._refuse_shadowing_cte(analysis, metadata)
        tables = [table.name for table in analysis.tables if metadata.table_configs(table.name)]
        only = analysis.tables[0].name if len(analysis.tables) == 1 else None
        for column in analysis.unbound_writes:
            config = metadata.column_config(column.table or only, column.column)
            if config is not None:
                raise MetadataError(
                    Messages.get_formatted("KmsEncryptionPlugin.CopyRefused", config.column_identifier),
                    table_name=config.table_name, column_name=config.column_name)
        if tables and analysis.writes and analysis.gap is not None and analysis.gap is not self._gaps.NO_COLUMN_LIST:
            raise MetadataError(Messages.get_formatted(
                "KmsEncryptionPlugin.CopyUnread", ", ".join(tables), analysis.gap.value))
        if tables and params and self._config.fail_on_unmapped_parameter:
            # The plugin never changes a COPY's arguments, so a compared value would reach the server unencrypted.
            raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.CopyParameters", ", ".join(tables)))
        if tables and analysis.writes and analysis.gap is self._gaps.NO_COLUMN_LIST:
            self._warn_once(analysis, tables)

    def _refuse_shadowing_cte(self, analysis: StatementAnalysis, metadata: MetadataManager) -> None:
        # The analysis reads the name as the CTE, but the database can read or write the table of that name.
        for name in analysis.cte_names:
            if metadata.table_configs(name):
                raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.CteShadowsTable", name))

    def _refuse_unsafe_comparisons(self, analysis: StatementAnalysis, metadata: MetadataManager, tables: List[str],
                                   only: Optional[str]) -> None:
        """Raise for a comparison of a value with an encrypted column where a comparison that never matches would still
        select rows: an encrypted value equals no stored value. The column the value is bound to, annotations included,
        and the column it is compared with are both checked, also in a write that would otherwise be passed through.
        Among several tables, a compared column without a table isn't looked up by name when the value's annotation
        names a table the statement uses, as ``_refuse_unmapped`` trusts it."""
        # Built only for a statement with an unsafe comparison.
        columns: Optional[Dict[int, ColumnRef]] = None
        trusted: Set[int] = set()
        for use in analysis.comparisons:
            if use.safe:
                continue
            if columns is None:
                columns = analysis.parameter_columns()
                trusted = {u.ordinal for u in analysis.annotations if self._names_a_table_used(analysis, u.column)}
            for column in (columns[use.ordinal], use.column):
                # Among several tables, by name, as _refuse_unmapped finds it.
                if column.table is None and only is None:
                    if use.ordinal in trusted:
                        # The value's annotation names its column, as _refuse_unmapped trusts it.
                        continue
                    config = _encrypted_column_by_name(metadata, tables, column.column)
                else:
                    config = metadata.column_config(column.table or only, column.column)
                if config is not None:
                    raise MetadataError(
                        Messages.get_formatted("KmsEncryptionPlugin.UnsafeComparison", config.column_identifier),
                        table_name=config.table_name, column_name=config.column_name)

    def _targets(self, analysis: StatementAnalysis, metadata: MetadataManager, tables: List[str],
                 only: Optional[str]) -> Dict[Any, ColumnEncryptionConfig]:
        """The encrypted column of each placeholder key. Raises for a value that may belong to an encrypted column
        the analysis can't tie it to."""
        annotated = {use.ordinal for use in analysis.annotations}
        # The columns the analysis infers before annotations override them, merged as the analysis merges them.
        inferred = {use.ordinal: use.column for use in (*analysis.comparisons, *analysis.assignments)}
        targets: Dict[Any, ColumnEncryptionConfig] = {}
        # The ordinals bound for an encrypted column.
        encrypted: Set[int] = set()
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
        checked: Optional[ColumnRef] = column
        if annotated:
            found = None if inferred is None else metadata.column_config(inferred.table or only, inferred.column)
            if found is not None:
                raise MetadataError(
                    Messages.get_formatted("KmsEncryptionPlugin.AnnotationNotEncrypted", found.column_identifier),
                    table_name=found.table_name, column_name=found.column_name)
            if self._names_a_table_used(analysis, column):
                # Trusted: it names a table the statement uses.
                return
            # Any other annotation counts as none.
            checked = inferred
        if checked is None or checked.table is not None:
            return
        found = _encrypted_column_by_name(metadata, tables, checked.column)
        if found is not None:
            raise MetadataError(
                Messages.get_formatted("KmsEncryptionPlugin.UnknownColumnTable", found.column_identifier),
                table_name=found.table_name, column_name=found.column_name)

    def _names_a_table_used(self, analysis: StatementAnalysis, column: ColumnRef) -> bool:
        """Whether an annotated value's column names a table the statement uses, read or written, which makes the
        annotation trusted."""
        key = self._driver_dialect.metadata_key
        return column.table is not None and any(key(column.table) == key(t.name) for t in analysis.tables)

    def _encrypt(self, params: Any, targets: Dict[Any, ColumnEncryptionConfig], keys: Optional[_CallKeys]) -> Any:
        mapping = isinstance(params, Mapping)
        out = dict(params) if mapping else list(params)
        for key, config in targets.items():
            if (key in out) if mapping else key < len(out):
                out[key] = self._seal(out[key], config, key, keys)
        if isinstance(params, tuple):
            return tuple(out)
        return out

    def _seal(self, value: Any, config: ColumnEncryptionConfig, parameter: Union[int, str],
              keys: Optional[_CallKeys]) -> Any:
        if value is None:
            return None
        # It would be stored as str(value) and read back as text.
        if TypeMarker.from_object(value) == TypeMarker.GENERIC:
            # A position counts from 1 in the message.
            position = parameter + 1 if isinstance(parameter, int) else parameter
            raise EncryptionFailedError(
                Messages.get_formatted("KmsEncryptionPlugin.NoExactForm", position, type(value).__name__,
                                       config.column_identifier),
                data_type="GENERIC")
        key = config.key_metadata
        if key is None or key.hmac_key is None or config.key_id is None or not config.is_usable:
            raise self._unusable(config)
        data_key = self._data_key(key, keys)
        # This call's copy, wiped below (best effort: the key row keeps its own bytes).
        hmac_key = bytearray(key.hmac_key)
        try:
            return self._crypto.encrypt(value, data_key, hmac_key, config.algorithm, key_id=config.key_id)
        finally:
            if keys is None:
                self._crypto.wipe(data_key)
            self._crypto.wipe(hmac_key)

    def _call_keys(self) -> Optional[_CallKeys]:
        """A new holder for one call's data keys when the data-key cache is off, so each key is decrypted once per
        call; None when it is on."""
        return None if self._config.data_key_cache_enabled else {}

    def _data_key(self, key: KeyMetadata, keys: Optional[_CallKeys]) -> bytearray:
        """The key row's data key, from ``keys`` when the call holds it. A key kept in ``keys`` is zeroed with them; any
        other is the caller's to zero."""
        if self._keys is None:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.MetadataNotLoaded"))
        if keys is None:
            return self._keys.decrypt_data_key(key.encrypted_data_key, key.master_key_arn)
        name = (key.encrypted_data_key, key.master_key_arn)
        data_key = keys.get(name)
        if data_key is None:
            data_key = self._keys.decrypt_data_key(key.encrypted_data_key, key.master_key_arn)
            keys[name] = data_key
        return data_key

    def _wipe(self, keys: Optional[_CallKeys]) -> None:
        if keys is not None:
            for data_key in keys.values():
                self._crypto.wipe(data_key)

    def _warn_once(self, analysis: StatementAnalysis, tables: List[str]) -> None:
        # Bounded: shapes can come from user SQL.
        if self._new_warning((analysis.kind, analysis.gap, tuple(tables)), bounded=True):
            # Without a detail, the gap's fixed text.
            reason = analysis.detail or (analysis.gap.value if analysis.gap is not None else "")
            logger.warning("KmsEncryptionPlugin.PassedThrough", ", ".join(tables), reason)

    def _new_warning(self, key: Any, bounded: bool = False) -> bool:
        """Whether a warning for ``key`` is still to be logged on this connection; records it. With ``bounded``, only
        while fewer than ``_MAX_WARNING_KEYS`` keys are recorded."""
        with self._state_lock:
            if key in self._warned or (bounded and len(self._warned) >= _MAX_WARNING_KEYS):
                return False
            self._warned.add(key)
            return True

    def _remember(self, target: Any, state: Optional[_Result]) -> None:
        """Set or clear the state of the driver cursor's last statement."""
        with self._state_lock:
            if state is None:
                self._results.pop(target, None)
            else:
                self._results[target] = state

    def _next_set(self, target: Any, execute_func: Callable) -> Any:
        """Call the driver's ``nextset()``, and once the driver has moved to another result set, build the decoder
        again from the new set's description at the next fetch. After the first set of a multi-statement string, the
        plugin can't tell which statement produced a set, so a fetch raises when a column's name is that of an
        encrypted column of the string's tables. For an ``executemany``, every set comes from the one statement: on
        mysql-connector, the sets of a multi-statement string differ. Returns the driver's result as it is."""
        # First: a nextset() that raises, or returns None at the last set, hasn't moved.
        result = execute_func()
        if result:
            with self._state_lock:
                state = self._results.get(target)
                if state is not None:
                    # Not built: the next fetch builds it from this set's description.
                    state.decode = _Result
                    state.later = not state.many
        return result

    def _decoder(self, target: Any) -> Union[Callable[..., Any], None, _Pending]:
        """The row decoder of the cursor's last statement, None when it has no encrypted column to decode, or
        ``_NOT_READY`` while the driver has no description for its result."""
        with self._state_lock:
            state = self._results.get(target)
        if state is None or state.decode is None:
            return None
        # Before the driver reads a row; a decoder built before the close would read key rows and call KMS.
        if self._closed:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        if state.decode is not _Result:
            return state.decode
        description = target.description
        if description is None:
            return _NOT_READY
        key = self._driver_dialect.metadata_key
        names = [d[0] for d in description]
        # So a name repeated in another letter case counts as a repeat.
        lowered = [str(n).lower() for n in names]
        known = state.analysis.result_columns
        only = state.analysis.tables[0].name if len(state.analysis.tables) == 1 else None
        # A column whose table has none isn't encrypted.
        configs = state.configs
        positions: Dict[int, ColumnEncryptionConfig] = {}
        for i, _ in enumerate(names):
            ref = known[i] if known is not None and i < len(known) and not state.later else None
            if ref is not None:
                table, column = ref.table or only, key(ref.column)
                config = None if table is None else next(
                    (c for c in configs.get(key(table), ()) if key(c.column_name) == column), None)
            else:
                # Left to name matching in any letter case: a unique encrypted column of that name, named once in the result.
                found = [c for table_configs in configs.values() for c in table_configs if c.column_name.lower() == lowered[i]]
                # A later result set may come from any statement of the string.
                if found and (state.later or len(found) > 1 or lowered.count(lowered[i]) > 1):
                    raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.UnplacedColumn", names[i]))
                config = found[0] if found else None
            if config is not None:
                positions[i] = config
        # Checked before any row is built, except through _decoder_after_fetch.
        if positions:
            factory = self._driver_dialect.unsupported_row_factory(target)
            if factory is not None:
                raise MetadataError(Messages.get_formatted("KmsEncryptionPlugin.RowFactory", factory))
        # A dict row keeps only the last column of each name, so it is decoded with that column's configuration.
        last = {name: i for i, name in enumerate(names)}
        by_name = {name: positions[i] for name, i in last.items() if i in positions}
        state.decode = (lambda row, keys=None: self._decode_row(row, positions, by_name, keys)) if positions else None
        return state.decode

    def _decoder_after_fetch(self, target: Any) -> Optional[Callable[..., Any]]:
        """The row decoder of a result whose rows have arrived but which had no description when the read began:
        psycopg's pipeline mode fills it in when a fetch, or a loop's first row, receives the result. So an unsupported
        row factory is refused only after the driver has built rows with it, from the stored payloads."""
        # Built at the first rows, then cached on the result.
        decode = self._decoder(target)
        # Rows of a result with encrypted columns, and still no description to place them.
        if decode is _NOT_READY:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.NoResultDescription"))
        return decode

    def _decode_late(self, target: Any, row: Any) -> Any:
        """Decode a row of a loop that began before its result had a description (see _decoder_after_fetch)."""
        decode = self._decoder_after_fetch(target)
        return row if decode is None else decode(row)

    def _decode_row(self, row: Any, positions: Dict[int, ColumnEncryptionConfig],
                    by_name: Dict[Any, ColumnEncryptionConfig], keys: Optional[_CallKeys]) -> Any:
        if isinstance(row, dict):
            out = dict(row)
            for name, config in by_name.items():
                out[name] = self._open(out[name], config, keys)
            return out
        values = list(row)
        for i, config in positions.items():
            values[i] = self._open(values[i], config, keys)
        if hasattr(row, "_make"):
            return type(row)._make(values)
        return tuple(values) if isinstance(row, tuple) else values

    def _open(self, value: Any, config: ColumnEncryptionConfig, keys: Optional[_CallKeys]) -> Any:
        if value is None:
            return None
        # Text from a text column, say: not a payload.
        if not isinstance(value, (bytes, bytearray, memoryview)):
            return self._unverified(value, config, "KmsEncryptionPlugin.NotAPayload")
        # A loop that began before the close decodes lazily and never reaches _decoder again.
        if self._closed:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        if self._keys is None:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.MetadataNotLoaded"))
        key_id = self._crypto.key_id_from_payload(value)
        # The column's own key row.
        if key_id is None or key_id == config.key_id:
            key = config.key_metadata
            if key is None or key.hmac_key is None or not config.is_usable:
                raise self._unusable(config)
        else:
            # A value written before the column's key was rotated, or a value that isn't a payload.
            # Before the lookup: no earlier key row makes it usable. repr quotes the name and keeps a trailing space or
            # a control character visible.
            if not EncryptionAlgorithm.is_supported(config.algorithm):
                raise InvalidAlgorithmError(
                    Messages.get_formatted("KmsEncryptionPlugin.UnsupportedAlgorithm", config.column_identifier,
                                           repr(config.algorithm), ", ".join(EncryptionAlgorithm.ALL)),
                    algorithm=config.algorithm)
            with self._state_lock:
                kept = self._old_keys.get(key_id)
            key = kept or self._keys.key_metadata_by_id(key_id)
            if key is None or not key.is_usable or key.hmac_key is None:
                # Never retried with the column's current key. The message leaves out the id: for a stored value
                # that isn't a payload, it is four bytes of that value.
                return self._unverified(value, config, "KmsEncryptionPlugin.UnknownKey")
            with self._state_lock:
                # Closed during the lookup: the close cleared _old_keys under this lock, so don't refill it.
                if self._closed:
                    raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
                self._old_keys[key_id] = key
        data_key = self._data_key(key, keys)
        hmac_key = bytearray(key.hmac_key)
        try:
            return self._crypto.decrypt(value, data_key, hmac_key, config.algorithm)
        except IntegrityCheckError:
            if not self._config.return_unverified_data:
                raise
        finally:
            if keys is None:
                self._crypto.wipe(data_key)
            self._crypto.wipe(hmac_key)
        return value

    def _unverified(self, value: Any, config: ColumnEncryptionConfig, message_key: str) -> Any:
        if self._config.return_unverified_data:
            # Lenient mode: the driver's value, nothing logged.
            return value
        raise IntegrityCheckError(Messages.get_formatted(message_key, config.column_identifier),
                                  algorithm=config.algorithm)

    @staticmethod
    def _unusable(config: ColumnEncryptionConfig) -> Union[InvalidAlgorithmError, InvalidKeyMetadataError]:
        """The error for a column that can't be written or read: its algorithm when that is unsupported, else its key row."""
        if not EncryptionAlgorithm.is_supported(config.algorithm):
            # repr quotes the name and keeps a trailing space or a control character visible.
            return InvalidAlgorithmError(
                Messages.get_formatted("KmsEncryptionPlugin.UnsupportedAlgorithm", config.column_identifier,
                                       repr(config.algorithm), ", ".join(EncryptionAlgorithm.ALL)),
                algorithm=config.algorithm)
        return InvalidKeyMetadataError(Messages.get_formatted("KmsEncryptionPlugin.UnusableColumn", config.column_identifier))

    def _components(self) -> MetadataManager:
        if self._closed:
            raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        if self._metadata is None:
            with self._lock:
                # Closed while this thread waited for another thread's build.
                if self._closed:
                    raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
                if self._metadata is None:
                    config = self._config
                    session = AwsCredentialsManager.get_session(self._plugin_service.current_host_info, self._props,
                                                                config.kms_region)
                    client = KeyManager.create_kms_client(session, config)
                    metadata = MetadataManager(self._source, self._driver_dialect, config)
                    metadata.start()
                    # The cache's constructor starts its cleanup thread, so it comes after every step that can
                    # fail; if that thread can't start, stop the refresh thread start() began.
                    try:
                        cache = DataKeyCache(config.data_key_cache_max_size, config.data_key_cache_expiration_sec,
                                             enabled=config.data_key_cache_enabled)
                    except BaseException:  # noqa: BLE001 - release the started manager, then re-raise the error as is
                        metadata.close()
                        raise
                    self._cache, self._kms_client = cache, client
                    self._keys = KeyManager(client, self._source, self._driver_dialect, config, cache)
                    # Last: the check outside the lock reads it.
                    self._metadata = metadata
                    # Closed during this build: release what it started, which that close couldn't see.
                    if self._closed:
                        self.release_resources()
                        raise MetadataError(Messages.get("KmsEncryptionPlugin.Closed"))
        return self._metadata


def _keyword(kwargs: Dict[str, Any], names: Tuple[str, ...]) -> Optional[str]:
    """The first of ``names`` that ``kwargs`` holds, or None."""
    for name in names:
        if name in kwargs:
            return name
    return None


def _argument(args: Tuple[Any, ...], kwargs: Dict[str, Any], at: Union[int, str, None]) -> Any:
    """The argument at position or keyword ``at``, or None when ``at`` is None."""
    if at is None:
        return None
    if isinstance(at, int):
        return args[at]
    return kwargs[at]


def _replace(args: Tuple[Any, ...], kwargs: Dict[str, Any], at: Any, value: Any) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    if isinstance(at, int):
        return (*args[:at], value, *args[at + 1:]), kwargs
    return args, {**kwargs, at: value}


def _bound(params: Any, key: Any) -> Any:
    """The value one parameter set binds to ``key``, or None if it binds none."""
    if isinstance(params, Mapping):
        return params.get(key)
    return params[key] if isinstance(key, int) and key < len(params) else None


def _encrypted_column_by_name(metadata: MetadataManager, tables: List[str], name: str) -> Optional[ColumnEncryptionConfig]:
    """The first encrypted column called ``name`` in any of ``tables``, for a column named without its table."""
    for table in tables:
        config = metadata.column_config(table, name)
        if config is not None:
            return config
    return None


def _binary_prefix(sql: str, analysis: StatementAnalysis, encrypted_keys: Set[Any], prefix: str) -> str:
    """Insert ``prefix`` before every placeholder bound for an encrypted column, so the server reads the payload as
    binary data. No prefix is added where a charset introducer, such as ``_binary``, is already directly before the
    placeholder, or for a key whose value is ``None`` (in any parameter set, for ``executemany``)."""
    starts = sorted({p.start for p in analysis.placeholders
                     if p.key in encrypted_keys and p.introducer is None and not p.hidden})
    # Joined once: splicing at each placeholder copies the whole SQL each time, quadratic in a bulk INSERT.
    parts: List[str] = []
    previous = 0
    for start in starts:
        parts += (sql[previous:start], prefix)
        previous = start
    parts.append(sql[previous:])
    return "".join(parts)


class KmsEncryptionPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return KmsEncryptionPlugin(plugin_service, props)
