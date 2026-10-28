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

import base64
import enum
import gc
import ipaddress
import logging
import os
import re
import threading
import uuid
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import (Any, Callable, Dict, Iterator, List, NamedTuple, Optional,
                    Sequence, Set, Tuple)
from unittest.mock import MagicMock

import boto3
import mysql.connector
import psycopg
import pytest
from botocore.exceptions import ProfileNotFound
from mysql.connector import cursor as mysql_cursor
from psycopg import rows as pg_rows
from psycopg import sql as pg_sql
from psycopg.types.json import Json, Jsonb

from aws_advanced_python_wrapper import errors
from aws_advanced_python_wrapper.aurora_connection_tracker_plugin import \
    AuroraConnectionTrackerPluginFactory
from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.encryption import key_manager as km_module
from aws_advanced_python_wrapper.encryption import \
    kms_encryption_plugin as plugin_module
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.kms_encryption_plugin import \
    KmsEncryptionPluginFactory
from aws_advanced_python_wrapper.encryption.models import (
    ColumnEncryptionConfig, KeyMetadata)
from aws_advanced_python_wrapper.encryption.sql_analyzer import AnalysisGap
from aws_advanced_python_wrapper.errors import (EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyMetadataError,
                                                MetadataError)
from aws_advanced_python_wrapper.failover_plugin import FailoverPluginFactory
from aws_advanced_python_wrapper.failover_v2_plugin import \
    FailoverV2PluginFactory
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.pg_driver_dialect import PgDriverDialect
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.plugin_service import (PluginCallContext,
                                                        PluginManager,
                                                        PluginServiceImpl)
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.wrapper import (AwsWrapperConnection,
                                                 AwsWrapperCursor)

ARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
DATA_KEY = bytes(range(32))
HMAC_KEY = bytes(range(100, 132))
PROPS = {"encryption_kms_region": "us-east-1", "encryption_allowed_master_key_arns": ARN}
DIALECTS = {"pg": DriverDialectCodes.PSYCOPG, "mysql": DriverDialectCodes.MYSQL_CONNECTOR_PYTHON}
KEY_ROW = KeyMetadata(id=7, key_id="k7", master_key_arn=ARN, encrypted_data_key="ZWRr", hmac_key=HMAC_KEY)
OLD_HMAC = bytes(range(140, 172))
SSN = ColumnEncryptionConfig("users", "ssn", key_id=7, key_metadata=KEY_ROW)
DOB = ColumnEncryptionConfig("users", "dob", key_id=8, key_metadata=replace(KEY_ROW, id=8))
ARCHIVE_SSN = ColumnEncryptionConfig("archive", "ssn", key_id=7, key_metadata=KEY_ROW)
INSERT = "INSERT INTO users (ssn) VALUES (%s)"
SELECT = "SELECT ssn FROM users WHERE id = %s"
# Captured before any test patches them.
REAL_GET_SESSION = AwsCredentialsManager.get_session
REAL_CREATE_KMS_CLIENT = KeyManager.create_kms_client


def marker() -> str:
    return f"kiro-plain-{uuid.uuid4()}"


def payload(value: Any, key_id: int = 7) -> bytes:
    sealed = EncryptionService.encrypt(value, DATA_KEY, HMAC_KEY, key_id=key_id)
    assert sealed is not None
    return sealed


def _clean(err: BaseException, *secrets: str) -> None:
    """No secret in the error's text, repr or args, and nothing chained."""
    text = f"{err} {err!r} {err.args!r}"
    for key in (DATA_KEY, HMAC_KEY):
        assert key.hex() not in text and base64.b64encode(key).decode() not in text and repr(key) not in text
    for secret in secrets:
        assert secret not in text
    assert err.__cause__ is None and err.__context__ is None


class FakeMetadata:
    """Answers lookups from fixed configurations, comparing names as the real manager does."""

    def __init__(self, sql: Any, configs: Sequence[ColumnEncryptionConfig], env: Any):
        self._sql = sql
        self._configs = {(sql.metadata_key(c.table_name), sql.metadata_key(c.column_name)): c for c in configs}
        self._env = env
        self.starts = 0
        self.closes = 0

    def start(self) -> None:
        self.starts += 1
        self._env.on_start(self)

    def close(self) -> None:
        self.closes += 1

    def column_config(self, table: Optional[str], column: Optional[str]) -> Optional[ColumnEncryptionConfig]:
        if table is None or column is None:
            return None
        return self._configs.get((self._sql.metadata_key(table), self._sql.metadata_key(column)))

    def table_configs(self, table: Optional[str]) -> List[ColumnEncryptionConfig]:
        key = None if table is None else self._sql.metadata_key(table)
        return [c for (t, _), c in self._configs.items() if t == key]


class FakeDriverCursor:
    """Records what the driver received and returns the rows it is given."""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []
        self.rows: List[Any] = []
        self.description: Optional[List[Tuple[str]]] = None
        self.arraysize = 1
        self.fetches = 0
        self.connection: Any = None  # psycopg's Composable.as_string reads it

    def execute(self, *args: Any, **kwargs: Any) -> FakeDriverCursor:
        self.calls.append(("execute", args, kwargs))
        return self  # as psycopg's does

    def executemany(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("executemany", args, kwargs))

    def fetchone(self) -> Any:
        self.fetches += 1
        return self.rows.pop(0) if self.rows else None

    def fetchmany(self, size: int = 0) -> List[Any]:
        self.fetches += 1
        size = size or self.arraysize  # the wrapper passes 0 by default, which the drivers read as arraysize
        out, self.rows = self.rows[:size], self.rows[size:]
        return out

    def fetchall(self) -> List[Any]:
        self.fetches += 1
        out, self.rows = self.rows, []
        return out

    def __iter__(self) -> Iterator[Any]:
        while self.rows:  # one row at a time, as the drivers do, so a broken loop resumes where it stopped
            yield self.rows.pop(0)


@pytest.fixture
def env(mocker):
    """The plugin's collaborators, patched where the plugin looks them up: no database, no KMS, no thread."""
    env = SimpleNamespace(configs=[SSN, DOB], built=[], on_start=lambda metadata: None)

    def build(source, sql, config):
        env.built.append(FakeMetadata(sql, env.configs, env))
        return env.built[-1]

    env.metadata = mocker.patch.object(plugin_module, "MetadataManager", side_effect=build)
    env.keys = mocker.patch.object(plugin_module, "KeyManager")
    env.keys.return_value.decrypt_data_key.side_effect = lambda edk, arn: bytearray(DATA_KEY)
    env.keys.return_value.key_metadata_by_id.return_value = None
    env.cache = mocker.patch.object(plugin_module, "DataKeyCache")
    mocker.patch.object(plugin_module, "PluginConnectionSource")
    env.session = mocker.patch.object(AwsCredentialsManager, "get_session")
    return env


def _service(mock: Any) -> Any:
    """A mock plugin service whose ``current_call_context`` is the real one: the calling thread's call context."""
    type(mock).current_call_context = PluginServiceImpl.current_call_context  # a mock's type is its own
    return mock


def make_plugin(mocker, dialect: str = DriverDialectCodes.PSYCOPG, **props: Any) -> Tuple[Any, Any]:
    service = _service(mocker.MagicMock())
    service.driver_dialect.dialect_code = dialect
    service.current_host_info = HostInfo("localhost", 5432)
    return KmsEncryptionPluginFactory.get_instance(service, Properties({**PROPS, **props})), service


@pytest.fixture
def plugin(mocker, env):
    return make_plugin(mocker)[0]


def cursor(plugin: Any, raw: Optional[FakeDriverCursor] = None) -> Tuple[AwsWrapperCursor, FakeDriverCursor]:
    """A wrapper cursor over a fake driver cursor, whose calls run through ``plugin``, then DefaultPlugin."""
    driver = raw if raw is not None else FakeDriverCursor()
    return _cursor_over([plugin], driver), driver


def call(cur: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    """Call a method of the wrapper cursor that ``cursor(plugin)`` returned, so it runs through the plugin chain."""
    return getattr(cur, method)(*args, **kwargs)


def plugin_records(caplog: Any) -> List[logging.LogRecord]:
    return [r for r in caplog.records if r.name == plugin_module.__name__]


def own_records(caplog: Any) -> List[logging.LogRecord]:
    """The records the test's own thread logged: a thread an earlier test left running can log while the root logger
    is at DEBUG."""
    return [r for r in caplog.records if r.thread == threading.get_ident()]


def _carrying(caplog: Any, secret: str) -> List[logging.LogRecord]:
    """The records holding the secret anywhere a log reporter could show it: the message, any attribute (args,
    exc_info, stack_info, extras), or an exception along the record's whole __cause__/__context__ chain."""
    found = []
    for record in caplog.records:
        chain: List[BaseException] = []
        todo: List[Optional[BaseException]] = [record.exc_info[1] if record.exc_info else None]
        while todo:
            err = todo.pop()
            if err is not None and not any(err is e for e in chain):
                chain.append(err)
                todo += [err.__cause__, err.__context__]
        texts = [record.getMessage(), repr(vars(record)), *(f"{e!r} {e.args!r} {e}" for e in chain)]
        if any(secret in text for text in texts):
            found.append(record)
    return found


def _undecodable(secret: str) -> bytes:
    return b"INSERT INTO users (ssn) VALUES ('" + secret.encode() + b"\xff')"  # 0xff starts no UTF-8 sequence


def _plant_controls(secret: str) -> None:
    """One record per channel _carrying reads, each holding the secret where only that channel finds it: a wrapper
    message, std-logging args the message leaves out, and a decode error on an exception's context."""
    plugin_module.logger.debug("KmsEncryptionPlugin.NotAnalysed", secret)
    logging.getLogger("aws_advanced_python_wrapper.execute_time_plugin").debug("control", {"sql": secret})
    try:
        try:
            _undecodable(secret).decode("utf-8")
        except UnicodeDecodeError:
            raise LookupError("control")  # the decode error is left on __context__ only
    except LookupError:
        logging.getLogger("aws_advanced_python_wrapper.plugin_service").debug("control", exc_info=True)


def spy_crypto(mocker: Any, plugin: Any) -> List[Tuple[str, Any, Any, int, str]]:
    """Run the plugin's EncryptionService calls, recording for each the key buffers, the data key's length and the
    algorithm it got."""
    seen: List[Tuple[str, Any, Any, int, str]] = []

    def spy(name: str) -> Callable[..., Any]:
        def run(value: Any, data_key: Any, hmac_key: Any, algorithm: str, **kwargs: Any) -> Any:
            seen.append((name, data_key, hmac_key, len(data_key), algorithm))
            return getattr(EncryptionService, name)(value, data_key, hmac_key, algorithm, **kwargs)
        return run

    mocker.patch.object(plugin, "_crypto", SimpleNamespace(
        encrypt=spy("encrypt"), decrypt=spy("decrypt"), wipe=EncryptionService.wipe,
        key_id_from_payload=EncryptionService.key_id_from_payload))
    return seen


def _connection(plugins: str, raw: FakeDriverCursor, dialect: str = DriverDialectCodes.PSYCOPG) -> AwsWrapperConnection:
    """A wrapper connection over a fake driver whose cursors are ``raw``, running the real plugin chain that
    ``plugins`` builds."""
    driver_conn, service = MagicMock(), _service(MagicMock())
    driver_conn.cursor.return_value = raw
    service.current_connection = driver_conn
    service.current_host_info = HostInfo("localhost", 5432)
    service.driver_dialect.dialect_code = dialect
    # The real dialect's network-bound methods, read before the plugins are built: the plugins that subscribe through
    # them (aurora_connection_tracker, failover, host monitoring) then handle the statement, as on a real connection.
    service.network_bound_methods = (MySQLDriverDialect if dialect == DriverDialectCodes.MYSQL_CONNECTOR_PYTHON
                                     else PgDriverDialect)(Properties()).network_bound_methods
    service.driver_dialect.get_connection_from_obj.return_value = driver_conn
    service.driver_dialect.unwrap_connection.return_value = driver_conn
    service.driver_dialect.execute.side_effect = lambda name, func, *args, **kwargs: func()
    manager = PluginManager(MagicMock(plugin_service=service), Properties({**PROPS, "plugins": plugins}), MagicMock())
    conn = object.__new__(AwsWrapperConnection)
    conn._plugin_service, conn._plugin_manager = service, manager
    return conn


def _chain(plugins: str, dialect: str = DriverDialectCodes.PSYCOPG) -> Tuple[AwsWrapperConnection, FakeDriverCursor]:
    raw = FakeDriverCursor()
    return _connection(plugins, raw, dialect), raw


def _cursor_over(plugins: List[Any], raw: FakeDriverCursor) -> AwsWrapperCursor:
    """A wrapper cursor over ``raw`` whose calls run through ``plugins`` in that order, then DefaultPlugin."""
    conn = _connection("", raw)
    conn._plugin_manager._plugins[:0] = plugins  # before the chain is first built
    return conn.cursor()


class _Recorder(Plugin):
    """Records the arguments each ``execute`` and ``executemany`` reaches it with, and runs the rest ``runs`` times."""

    def __init__(self, runs: int = 1) -> None:
        self.seen: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []
        self._runs = runs

    @property
    def subscribed_methods(self) -> Set[str]:
        return {"Cursor.execute", "Cursor.executemany"}

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self.seen.append((method_name, args, kwargs))
        result = None
        for _ in range(self._runs):
            result = execute_func()
        return result


class _RecorderFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: Any, props: Properties) -> Plugin:
        return _Recorder()


class _LateRecorderFactory(_RecorderFactory):
    """The same plugin at another weight, since a weight belongs to the factory type."""


@pytest.fixture
def recorders() -> Iterator[None]:
    """Plugin codes ``recorder_385`` and ``recorder_395``, weighted on either side of ``kms_encryption`` (390)."""
    factories = {"recorder_385": (_RecorderFactory, 385), "recorder_395": (_LateRecorderFactory, 395)}
    for code, (factory, weight) in factories.items():
        PluginManager.register_plugin(code, factory, weight)
    try:
        yield
    finally:
        for code, (factory, _) in factories.items():
            PluginManager.PLUGIN_FACTORIES.pop(code, None)
            PluginManager.PLUGIN_FACTORY_WEIGHTS.pop(factory, None)


class _OpaqueSql:
    """SQL given as an object the plugin doesn't analyze (not str, bytes or a psycopg Composable)."""

    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        return self.text

    __repr__ = __str__


class _Row(NamedTuple):
    id: int
    ssn: Any


# ---- the call context ----

@pytest.mark.parametrize("context", ["none", "foreign"])
@pytest.mark.parametrize("method, sql, params", [
    ("execute", INSERT, lambda m: (m,)),
    ("executemany", INSERT, lambda m: [(m,)]),
    # Nothing to encrypt, but the iterator becomes a list, so the plugin still changes the arguments.
    ("executemany", "INSERT INTO audit (note) VALUES (%s)", lambda m: ((m,) for _ in range(1))),
], ids=["execute", "executemany", "iterator"])
def test_a_change_without_the_statements_own_context_is_refused_before_the_driver(mocker, env, context, method, sql, params):
    plugin, service = make_plugin(mocker)
    secret = marker()
    handed = params(secret)
    # The context of another statement with equal values: what an enclosing statement's context would hold.
    foreign = PluginCallContext((sql, params(secret)), {})
    foreign_args = foreign.args
    if context == "foreign":
        type(service).current_call_context = mocker.PropertyMock(return_value=foreign)
    driver = mocker.MagicMock()
    with pytest.raises(EncryptionFailedError) as info:
        plugin.execute(FakeDriverCursor(), f"Cursor.{method}", driver, sql, handed)
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.ArgumentsChanged")
    assert driver.call_count == 0 and foreign.args is foreign_args and foreign.kwargs == {}
    _clean(info.value, secret)


@pytest.mark.parametrize("form", ["keywords", "shorter", "extra-keyword"])
def test_a_context_holding_other_arguments_is_refused(mocker, env, form):
    # keywords: the call passes only keyword arguments, and the context's params are equal but not the same object.
    # shorter: the context holds the same first argument but not the rest.
    # extra-keyword: the context holds the same positional arguments and one keyword more.
    plugin, service = make_plugin(mocker)
    secret = marker()
    handed = (secret,)
    if form == "keywords":
        args, kwargs = (), {"query": INSERT, "params": handed}
        foreign = PluginCallContext((), {"query": INSERT, "params": (secret,)})
    elif form == "shorter":
        args, kwargs = (INSERT, handed), {}
        foreign = PluginCallContext((INSERT,), {})
    else:
        args, kwargs = (INSERT, handed), {}
        foreign = PluginCallContext((INSERT, handed), {"params": handed})
    type(service).current_call_context = mocker.PropertyMock(return_value=foreign)
    driver = mocker.MagicMock()
    with pytest.raises(EncryptionFailedError) as info:
        plugin.execute(FakeDriverCursor(), "Cursor.execute", driver, *args, **kwargs)
    assert driver.call_count == 0
    _clean(info.value, secret)


def test_the_copies_are_in_the_context_before_the_rest_of_the_chain_runs(mocker, env):
    plugin, service = make_plugin(mocker)
    params = (marker(),)
    context = PluginCallContext((), {"query": INSERT, "params": params})
    type(service).current_call_context = mocker.PropertyMock(return_value=context)
    seen: List[Any] = []
    plugin.execute(FakeDriverCursor(), "Cursor.execute", lambda: seen.append(dict(context.kwargs)), **context.kwargs)
    assert seen[0]["query"] is INSERT and EncryptionService.decrypt(seen[0]["params"][0], DATA_KEY, HMAC_KEY) == params[0]


def test_plugins_after_it_get_the_payloads_and_plugins_before_it_the_callers_values(env, recorders):
    conn, raw = _chain("recorder_395,kms_encryption,recorder_385")
    before, after = [p for p in conn._plugin_manager._plugins if isinstance(p, _Recorder)]
    one, many = (marker(),), [(marker(),), (marker(),)]
    snapshot = list(many)
    cur = conn.cursor()
    cur.execute(INSERT, one)
    cur.executemany(INSERT, many)
    assert before.seen == [("Cursor.execute", (INSERT, one), {}), ("Cursor.executemany", (INSERT, many), {})]
    assert before.seen[0][1][1] is one and before.seen[1][1][1] is many and many == snapshot
    sent = [args for _, args, _ in raw.calls]
    assert [args for _, args, _ in after.seen] == sent and all(a[1] is b[1] for (_, a, _), b in zip(after.seen, sent))
    assert type(sent[0][1]) is tuple and sent[0][1] is not one
    assert [EncryptionService.decrypt(sent[0][1][0], DATA_KEY, HMAC_KEY)] == list(one)
    assert [EncryptionService.decrypt(r[0], DATA_KEY, HMAC_KEY) for r in sent[1][1]] == [r[0] for r in many]


def test_a_plugin_before_it_that_runs_the_rest_twice_sends_one_payload_each_time(plugin):
    raw = FakeDriverCursor()
    retrier = _Recorder(runs=2)
    cur = _cursor_over([retrier, plugin], raw)
    secret = marker()
    cur.execute(INSERT, (secret,))
    sent = [args[1][0] for _, args, _ in raw.calls]
    assert len(sent) == 2 and sent[0] != sent[1] and retrier.seen == [("Cursor.execute", (INSERT, (secret,)), {})]
    assert [EncryptionService.decrypt(value, DATA_KEY, HMAC_KEY) for value in sent] == [secret, secret]


def test_read_state_goes_with_the_driver_cursor(plugin):
    raw = FakeDriverCursor()
    plugin.execute(raw, "Cursor.execute", lambda: None, SELECT, (1,))  # nothing to replace, so no context needed
    assert list(plugin._results.keys()) == [raw]
    del raw
    gc.collect()
    assert len(plugin._results) == 0


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
def test_each_value_decrypts_once_on_fetch(plugin, env, method):
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(payload(secret),), (None,)]
    result = call(cur, method)
    assert (result if method == "fetchone" else result[0]) == (secret,)
    assert env.keys.return_value.decrypt_data_key.call_count == 1


def test_iteration_and_chained_reads_decrypt(plugin):
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(payload(secret),)]
    assert list(cur) == [(secret,)]
    raw.rows = [(payload(secret),)]
    assert cur.execute(SELECT, (1,)).fetchone() == (secret,)


class PipelinedDriverCursor(FakeDriverCursor):
    """As psycopg's cursor in pipeline mode: ``execute`` returns before the result, and ``arriving`` holds the
    description and rows that the first fetch, or the first row of a loop, receives."""

    def __init__(self) -> None:
        super().__init__()
        self.arriving: Optional[Tuple[Optional[List[Tuple[str]]], List[Any]]] = None

    def _receive(self) -> None:
        if self.arriving is not None:
            self.description, self.rows = self.arriving
            self.arriving = None

    def fetchone(self) -> Any:
        self._receive()
        return super().fetchone()

    def fetchmany(self, size: int = 0) -> List[Any]:
        self._receive()
        return super().fetchmany(size)

    def fetchall(self) -> List[Any]:
        self._receive()
        return super().fetchall()

    def __iter__(self) -> Iterator[Any]:
        self._receive()  # at the loop's first next(), not at iter(): psycopg's __next__ is what syncs the pipeline
        yield from super().__iter__()


def _read_rows(cur: Any, read: str, count: int) -> List[Any]:
    if read == "fetchone":
        rows = [call(cur, "fetchone") for _ in range(count)]
        assert call(cur, "fetchone") is None
        return rows
    if read == "fetchmany":
        return call(cur, "fetchmany", count + 1)
    return call(cur, "fetchall") if read == "fetchall" else list(cur)


@pytest.mark.parametrize("read", ["fetchone", "fetchmany", "fetchall", "iterate"])
def test_columns_known_only_at_fetch_are_decrypted(plugin, env, read):
    raw = PipelinedDriverCursor()
    cur, _ = cursor(plugin, raw)
    secret = marker()
    call(cur, "execute", SELECT, (1,))  # leaves the description None, as pipeline mode does
    raw.arriving = ([("ssn",)], [(payload(secret),), (None,)])
    assert _read_rows(cur, read, 2) == [(secret,), (None,)]
    assert env.keys.return_value.decrypt_data_key.call_count == 1


@pytest.mark.parametrize("read", ["fetchone", "fetchmany", "fetchall", "iterate"])
def test_plain_columns_known_only_at_fetch_are_returned_as_given(plugin, env, read):
    raw = PipelinedDriverCursor()
    cur, _ = cursor(plugin, raw)
    rows = [(marker(),), (None,)]
    call(cur, "execute", "SELECT name FROM users WHERE id = %s", (1,))  # a table with encrypted columns, none of them read
    raw.arriving = ([("name",)], list(rows))
    read_back = _read_rows(cur, read, 2)
    assert read_back == rows and all(got is given for got, given in zip(read_back, rows))
    assert env.keys.return_value.decrypt_data_key.call_count == 0


@pytest.mark.parametrize("read", ["fetchone", "fetchmany", "fetchall", "iterate"])
def test_rows_without_a_description_for_an_encrypted_result_are_refused(plugin, read):
    raw = PipelinedDriverCursor()
    cur, _ = cursor(plugin, raw)
    secret = marker()
    call(cur, "execute", SELECT, (1,))
    raw.arriving = (None, [(payload(secret),)])
    with pytest.raises(MetadataError) as info:
        _read_rows(cur, read, 1)
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.NoResultDescription")
    _clean(info.value, secret)


@pytest.mark.parametrize("read", ["fetchone", "fetchmany", "fetchall", "iterate"])
def test_unsupported_row_factory_is_refused_when_the_columns_arrive(plugin, read):
    raw = PipelinedDriverCursor()
    raw.row_factory = pg_rows.class_row(dict)
    cur, _ = cursor(plugin, raw)
    secret = marker()
    call(cur, "execute", SELECT, (1,))
    raw.arriving = ([("ssn",)], [(payload(secret),)])
    with pytest.raises(MetadataError) as info:
        _read_rows(cur, read, 1)
    assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.RowFactory", raw.row_factory)
    _clean(info.value, secret)


@pytest.mark.parametrize("read", ["fetchmany", "fetchall"])
def test_an_empty_result_without_a_description_returns_no_rows(plugin, read):
    raw = PipelinedDriverCursor()
    cur, _ = cursor(plugin, raw)
    call(cur, "execute", SELECT, (1,))
    raw.arriving = (None, [])  # no rows to decrypt, so a missing description is nothing to refuse
    assert _read_rows(cur, read, 0) == []


def test_round_trip_through_the_real_chain(env):
    conn, raw = _chain("kms_encryption")
    secret = marker()
    cur = conn.cursor()
    cur.execute(INSERT, (secret,))
    stored = raw.calls[0][1][1][0]
    assert isinstance(stored, bytes) and len(stored) >= 65
    raw.description = [("ssn",)]
    cur.execute(SELECT, (1,))
    raw.rows = [(stored,)]
    assert cur.fetchall() == [(secret,)]
    raw.rows = [(stored,)]
    assert [row for row in cur] == [(secret,)]


@pytest.mark.parametrize("sql", ["UPDATE users SET dob = %(v)s, ssn = %(v)s WHERE id = %(id)s",
                                 "UPDATE users SET dob = %(v)s WHERE ssn = %(v)s"])
def test_repeated_named_placeholder_uses_the_first_columns_key(plugin, sql):
    cur, raw = cursor(plugin)
    params = {"v": marker(), "id": 1}
    snapshot = dict(params)
    call(cur, "execute", sql, params)
    sent = raw.calls[0][1][1]
    assert params == snapshot and sent is not params and type(sent) is dict
    assert EncryptionService.key_id_from_payload(sent["v"]) == DOB.key_id


@pytest.mark.parametrize("dialect", ["pg", "mysql"])
@pytest.mark.parametrize("method, sql", [("execute", "INSERT INTO users (ssn, name) VALUES (%(v)s, %(v)s)"),
                                         ("execute", "INSERT INTO users (name, ssn) VALUES (%(v)s, %(v)s)"),
                                         ("execute", "UPDATE users SET ssn = %(v)s WHERE name = %(v)s"),
                                         ("execute", "SELECT id FROM users WHERE ssn = %(v)s OR name = %(v)s"),
                                         ("execute", "SELECT ssn FROM users WHERE ssn = %(v)s LIMIT %(v)s"),  # a use with no column at all
                                         ("executemany", "INSERT INTO users (ssn, name) VALUES (%(v)s, %(v)s)")],
                         ids=["insert-encrypted-first", "insert-plain-first", "update", "select", "limit", "insert-many"])
def test_named_placeholder_for_an_encrypted_and_a_plain_use_is_refused(mocker, env, dialect, method, sql):
    # One mapping entry can't hold both the payload for ssn and the plaintext for the other use.
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    sets = [{"v": marker()}, {"v": marker()}]
    cur, raw = cursor(plugin)
    with pytest.raises(MetadataError) as info:
        call(cur, method, sql, sets if method == "executemany" else sets[0])
    assert raw.calls == [] and env.keys.return_value.decrypt_data_key.call_count == 0
    assert (info.value.table_name, info.value.column_name) == ("users", "ssn")
    assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.SharedParameter", "v", "users.ssn")
    _clean(info.value, sets[0]["v"], sets[1]["v"])


@pytest.mark.parametrize("sql_by_name", [False, True], ids=["params", "query-and-params"])
def test_keyword_params_are_encrypted(plugin, sql_by_name):
    cur, raw = cursor(plugin)
    secret = marker()
    params = (secret,)
    if sql_by_name:
        call(cur, "execute", query=INSERT, params=params)
    else:
        call(cur, "execute", INSERT, params=params)
    sent = raw.calls[0][2]["params"]
    assert params == (secret,) and isinstance(sent, tuple) and EncryptionService.decrypt(sent[0], DATA_KEY, HMAC_KEY) == secret


def test_null_write_passes_without_a_key(plugin, env):
    cur, raw = cursor(plugin)
    call(cur, "execute", INSERT, (None,))
    assert raw.calls[0][1][1] == (None,) and env.keys.return_value.decrypt_data_key.call_count == 0


def test_missing_value_is_left_to_the_driver(plugin):
    cur, raw = cursor(plugin)
    call(cur, "execute", "INSERT INTO users (name, ssn) VALUES (%s, %s)", ("kiro-name",))
    assert raw.calls[0][1][1] == ("kiro-name",)  # the driver raises its own error for the missing value


# ---- registration and lifecycle ----

def test_kms_encryption_sorts_between_tracker_and_failover(mocker):
    manager = PluginManager(mocker.MagicMock(), Properties(plugins=""), mocker.MagicMock())
    factories = manager.create_plugin_factories_from_list(
        ["failover_v2", "failover", "kms_encryption", "aurora_connection_tracker"])
    assert [type(f) for f in factories] == [AuroraConnectionTrackerPluginFactory, KmsEncryptionPluginFactory,
                                            FailoverPluginFactory, FailoverV2PluginFactory]


def test_subscribes_to_exactly_the_cursor_execute_and_fetch_methods(plugin):
    assert plugin.subscribed_methods == {m.method_name for m in (
        DbApiMethod.CURSOR_EXECUTE, DbApiMethod.CURSOR_EXECUTEMANY, DbApiMethod.CURSOR_FETCHONE,
        DbApiMethod.CURSOR_FETCHMANY, DbApiMethod.CURSOR_FETCHALL, DbApiMethod.CURSOR_NEXT)}


def test_release_resources_closes_metadata_and_cache(mocker, plugin, env):
    make_plugin(mocker)[0].release_resources()  # before any build: nothing to close
    cur, _ = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    plugin._old_keys[9] = replace(KEY_ROW, id=9)  # as a read of a rotated-out key leaves it
    plugin.release_resources()
    assert env.built[0].closes == 1 and env.cache.return_value.close.call_count == 1 and plugin._old_keys == {}
    assert env.keys.create_kms_client.return_value.close.call_count == 1
    plugin.release_resources()  # close() and __exit__ both release; the components' close() is idempotent
    secret = marker()
    with pytest.raises(MetadataError) as info:
        call(cur, "execute", INSERT, (secret,))
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.Closed")
    _clean(info.value, secret)
    assert env.metadata.call_count == 1 and env.cache.call_count == 1  # nothing is built again


def test_close_during_the_first_build_releases_what_it_built(plugin, env):
    env.on_start = lambda metadata: plugin.release_resources()  # another thread's close, while start() loads
    cur, raw = cursor(plugin)
    secret = marker()
    with pytest.raises(MetadataError) as info:
        call(cur, "execute", INSERT, (secret,))
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.Closed")
    _clean(info.value, secret)
    assert env.built[0].closes == 1 and env.cache.return_value.close.call_count == 1
    assert env.keys.create_kms_client.return_value.close.call_count == 1 and raw.calls == []


def test_no_metadata_lookup_for_a_result_after_close(mocker, plugin, env):
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(payload(secret),)]
    plugin.release_resources()
    lookups = [mocker.spy(env.built[0], "column_config"), mocker.spy(env.built[0], "table_configs")]
    with pytest.raises(MetadataError) as info:
        call(cur, "fetchone")
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.Closed")
    _clean(info.value, secret)
    assert [s.call_count for s in lookups] == [0, 0] and raw.fetches == 0


def _rotated_rows(mocker, env, count: int) -> Tuple[Any, AwsWrapperCursor, FakeDriverCursor, str]:
    """A plugin whose result holds ``count`` rows under a rotated-out key, so decoding one reads that key's row."""
    old_id = 1802201963
    env.keys.return_value.key_metadata_by_id.return_value = replace(
        KEY_ROW, id=old_id, key_id="k-old", encrypted_data_key="b2xk", hmac_key=OLD_HMAC)
    plugin, _ = make_plugin(mocker)
    secret = marker()
    cur, raw = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    raw.description = [("ssn",)]
    raw.rows = [(EncryptionService.encrypt(secret, DATA_KEY, OLD_HMAC, key_id=old_id),) for _ in range(count)]
    return plugin, cur, raw, secret


def _assert_refused_after_close(info: Any, env: Any, plugin: Any, secret: str, counts: Tuple[int, int]) -> None:
    keys = env.keys.return_value
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.Closed")
    _clean(info.value, secret)
    assert (keys.key_metadata_by_id.call_count, keys.decrypt_data_key.call_count) == counts  # no key row, no KMS call
    assert plugin._old_keys == {}  # the close dropped the rotated-out key row, and nothing put it back


@pytest.mark.parametrize("fetch", ["fetchone", "fetchall"])
def test_a_decoder_built_before_close_refuses_after_it(mocker, env, fetch):
    plugin, cur, raw, secret = _rotated_rows(mocker, env, 2)
    assert call(cur, "fetchone") == (secret,)  # builds the result's decoder, which reads the rotated-out key row
    plugin.release_resources()
    keys = env.keys.return_value
    counts, fetches = (keys.key_metadata_by_id.call_count, keys.decrypt_data_key.call_count), raw.fetches
    with pytest.raises(MetadataError) as info:
        call(cur, fetch)
    _assert_refused_after_close(info, env, plugin, secret, counts)
    assert raw.fetches == fetches  # refused before the driver reads a row


def test_a_loop_begun_before_close_refuses_after_it(mocker, env):
    plugin, cur, raw, secret = _rotated_rows(mocker, env, 2)
    rows = iter(cur)
    assert next(rows) == (secret,)
    plugin.release_resources()
    keys = env.keys.return_value
    counts = (keys.key_metadata_by_id.call_count, keys.decrypt_data_key.call_count)
    with pytest.raises(MetadataError) as info:
        next(rows)
    _assert_refused_after_close(info, env, plugin, secret, counts)


def test_a_close_during_a_rotated_key_lookup_puts_nothing_back(mocker, env):
    plugin, cur, raw, secret = _rotated_rows(mocker, env, 1)
    keys = env.keys.return_value
    row = keys.key_metadata_by_id.return_value

    def closed_mid_lookup(key_id):  # another thread closes the connection while this one reads the key row
        plugin.release_resources()
        return row

    keys.key_metadata_by_id.side_effect = closed_mid_lookup
    with pytest.raises(MetadataError) as info:
        call(cur, "fetchone")
    _assert_refused_after_close(info, env, plugin, secret, (1, 0))


def test_a_result_without_encrypted_columns_still_reads_after_close(plugin, env):
    cur, raw = cursor(plugin)
    call(cur, "execute", "SELECT id FROM users WHERE id = %s", (1,))
    raw.description, raw.rows = [("id",)], [(1,), (2,)]
    assert call(cur, "fetchone") == (1,)  # finds no encrypted column, so the result keeps no decoder
    plugin.release_resources()
    assert call(cur, "fetchall") == [(2,)]  # looks nothing up, so the close doesn't refuse it
    assert env.keys.return_value.decrypt_data_key.call_count == 0


def test_close_sets_the_flag_before_reading_the_components(plugin, env):
    outcome: List[Any] = []

    class Probe:  # entered by release_resources() after it reads the components and before it returns
        def __enter__(self) -> None:
            try:
                outcome.append(plugin._components())
            except MetadataError as e:
                outcome.append(e)

        def __exit__(self, *exc: Any) -> None:
            return None

    plugin._state_lock = Probe()
    plugin.release_resources()  # before any build
    assert len(outcome) == 1 and isinstance(outcome[0], MetadataError)
    assert str(outcome[0]) == Messages.get("KmsEncryptionPlugin.Closed") and env.metadata.call_count == 0


def test_a_thread_waiting_for_the_build_sees_a_close(plugin, env):
    built = object()

    class Contended:  # while this thread waited, another one built the components and a close released them
        def __enter__(self) -> None:
            plugin._metadata = built
            plugin._closed = True

        def __exit__(self, *exc: Any) -> None:
            return None

    plugin._lock = Contended()
    with pytest.raises(MetadataError) as info:
        plugin._components()
    assert str(info.value) == Messages.get("KmsEncryptionPlugin.Closed") and env.metadata.call_count == 0


ERROR_BASES = [
    (errors.EncryptionPluginError, (errors.AwsWrapperError,)),
    (errors.EncryptionError, (errors.EncryptionPluginError, errors.OperationalError)),
    (errors.IntegrityCheckError, (errors.IntegrityError, errors.EncryptionError)),
    (errors.DecryptionError, (errors.EncryptionError,)),
    (errors.EncryptionFailedError, (errors.EncryptionError,)),
    (errors.InvalidKeyError, (errors.EncryptionError,)),
    (errors.InvalidAlgorithmError, (errors.EncryptionError,)),
    (errors.TypeConversionError, (errors.EncryptionError,)),
    (errors.EncryptionConfigError, (errors.EncryptionPluginError, errors.InterfaceError)),
    (errors.KeyManagementError, (errors.EncryptionPluginError, errors.OperationalError)),
    (errors.UnauthorizedMasterKeyError, (errors.KeyManagementError,)),
    (errors.InvalidKeyMetadataError, (errors.KeyManagementError,)),
    (errors.MetadataError, (errors.EncryptionPluginError, errors.OperationalError)),
]


@pytest.mark.parametrize("cls, bases", ERROR_BASES, ids=[c.__name__ for c, _ in ERROR_BASES])
def test_encryption_error_classes(cls, bases):
    assert cls.__bases__ == bases and cls.__module__ == "aws_advanced_python_wrapper"


def test_every_plugin_message_key_is_used_and_exists():
    module = Path(plugin_module.__file__)
    used = set(re.findall(r'"(KmsEncryptionPlugin\.\w+)"', module.read_text()))
    resource = module.parents[1] / "resources" / "aws_advanced_python_wrapper_messages.properties"
    defined = set(re.findall(r"^(KmsEncryptionPlugin\.\w+)=", resource.read_text(), re.M))
    assert used == defined == {f"KmsEncryptionPlugin.{k}" for k in (
        "AnnotationNotEncrypted", "ArgumentsChanged", "AuditNotWritten", "Closed", "HiddenPlaceholder", "NoExactForm",
        "NoResultDescription", "NotAnalysed", "NotAPayload", "PassedThrough", "RowFactory", "SharedParameter",
        "UnboundWrite", "UnknownColumnTable", "UnknownKey", "UnusableColumn")}


def test_factory_and_build_install_no_log_filter(mocker, env):
    loggers = [logging.getLogger(n) for n in ("", "botocore", "botocore.parsers", "botocore.endpoint", "botocore.auth")]
    before = [list(lg.filters) for lg in loggers]
    plugin, _ = make_plugin(mocker)
    cur, _ = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    assert [list(lg.filters) for lg in loggers] == before


def test_dialect_captured_at_get_instance(mocker, env):
    plugin, service = make_plugin(mocker)
    service.driver_dialect = mocker.MagicMock(dialect_code="generic")  # as under the internal pool after connect
    cur, raw = cursor(plugin)
    call(cur, "execute", INSERT, (marker(),))
    assert len(raw.calls[0][1][1][0]) >= 65


def test_nothing_read_at_connect_build_once_under_lock(mocker, env):
    plugin, service = make_plugin(mocker)
    assert (env.metadata.call_count, env.cache.call_count, env.session.call_count) == (0, 0, 0)
    held: List[bool] = []
    env.on_start = lambda metadata: held.append(plugin._lock.locked())
    cur, _ = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    call(cur, "execute", SELECT, (1,))
    assert held == [True] and env.metadata.call_count == 1 and env.cache.call_count == 1
    # The KMS client comes from create_kms_client, which installs the botocore filters.
    env.session.assert_called_once_with(service.current_host_info, plugin._props, "us-east-1")
    env.keys.create_kms_client.assert_called_once_with(env.session.return_value, plugin._config)
    assert env.keys.call_args.args[0] is env.keys.create_kms_client.return_value


class _SpyLock:
    """A lock that reports each thread arriving at it, so the test needs no sleeps."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.arrivals = threading.Semaphore(0)
        self.held = False

    def __enter__(self) -> _SpyLock:
        self.arrivals.release()
        self._lock.acquire()
        self.held = True
        return self

    def __exit__(self, *exc: Any) -> None:
        self.held = False
        self._lock.release()


class _Guarded:
    """Forwards to a container, recording each use of it and each use made while ``lock`` is not held."""

    def __init__(self, inner: Any, lock: _SpyLock) -> None:
        self._inner, self._spy = inner, lock
        self.uses: List[str] = []
        self.unlocked: List[str] = []

    def _use(self, what: str) -> None:
        self.uses.append(what)
        if not self._spy.held:
            self.unlocked.append(what)

    def __getattr__(self, name: str) -> Any:
        self._use(name)
        return getattr(self._inner, name)

    def __contains__(self, key: Any) -> bool:
        self._use("in")
        return key in self._inner

    def __len__(self) -> int:
        self._use("len")
        return len(self._inner)

    def __getitem__(self, key: Any) -> Any:
        self._use("getitem")
        return self._inner[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._use("setitem")
        self._inner[key] = value


def test_read_state_changes_only_under_the_state_lock(mocker, env):
    old = replace(KEY_ROW, id=9, key_id="k9", hmac_key=OLD_HMAC)
    env.keys.return_value.key_metadata_by_id.return_value = old
    plugin, _ = make_plugin(mocker, DriverDialectCodes.MYSQL_CONNECTOR_PYTHON)
    spy = _SpyLock()
    plugin._state_lock = spy  # set directly: the plugin is this test's own, and mocker would probe the guards
    guards = {name: _Guarded(getattr(plugin, name), spy) for name in ("_results", "_old_keys", "_warned")}
    for name, guard in guards.items():
        setattr(plugin, name, guard)
    cur, raw = cursor(plugin)
    secret = marker()
    # A gap write (clears the cursor's state, records a warning), then a read of a value under a rotated-out key.
    call(cur, "execute", "UPDATE users u JOIN accounts a ON a.id = u.id SET ssn = %s WHERE a.id = %s", (marker(), 1))
    call(cur, "execute", SELECT, (1,))
    raw.description = [("ssn",)]
    raw.rows = [(EncryptionService.encrypt(secret, DATA_KEY, OLD_HMAC, key_id=old.id),)]
    assert call(cur, "fetchall") == [(secret,)]
    plugin.release_resources()
    assert {name: bool(guard.uses) for name, guard in guards.items()} == dict.fromkeys(guards, True)
    assert {name: guard.unlocked for name, guard in guards.items()} == dict.fromkeys(guards, [])


def test_concurrent_first_statements_build_once(mocker, env):
    plugin, _ = make_plugin(mocker)
    entered, release = threading.Event(), threading.Event()
    env.on_start = lambda metadata: (entered.set(), release.wait(5))
    spy = _SpyLock()
    mocker.patch.object(plugin, "_lock", spy)
    results: Dict[int, Any] = {}
    first = threading.Thread(target=lambda: results.__setitem__(1, plugin._components()))
    first.start()
    assert entered.wait(5) and spy.arrivals.acquire(timeout=5)  # the first thread is inside start(), holding the lock
    second = threading.Thread(target=lambda: results.__setitem__(2, plugin._components()))
    second.start()
    assert spy.arrivals.acquire(timeout=5)  # the second thread waits for the lock
    release.set()
    first.join(5)
    second.join(5)
    assert not first.is_alive() and not second.is_alive()
    assert env.metadata.call_count == 1 and results[1] is results[2]


def test_failed_build_raises_and_is_retried(mocker, env):
    plugin, _ = make_plugin(mocker)
    boom = MetadataError("kiro-unit: metadata unreadable")
    failures = [boom]

    def fail_once(metadata):
        if failures:
            raise failures.pop()

    env.on_start = fail_once
    cur, raw = cursor(plugin)
    with pytest.raises(MetadataError) as info:
        call(cur, "execute", INSERT, (marker(),))
    assert info.value is boom and boom.__cause__ is None and boom.__context__ is None
    assert raw.calls == [] and env.cache.call_count == 0  # the cache, whose constructor starts a thread, comes last
    call(cur, "execute", INSERT, (marker(),))
    assert env.metadata.call_count == 2 and [m.starts for m in env.built] == [1, 1]
    assert plugin._metadata is env.built[1] and len(raw.calls) == 1


def test_failed_cache_start_stops_the_metadata_refresh(mocker, env):
    plugin, _ = make_plugin(mocker)
    boom = RuntimeError("can't start new thread")
    env.cache.side_effect = [boom, mocker.DEFAULT]
    cur, raw = cursor(plugin)
    with pytest.raises(RuntimeError) as info:
        call(cur, "execute", SELECT, (1,))
    assert info.value is boom and env.built[0].closes == 1 and raw.calls == []
    call(cur, "execute", SELECT, (1,))
    assert env.metadata.call_count == 2 and len(raw.calls) == 1


@pytest.fixture
def aws_env(monkeypatch, tmp_path):
    """No ambient AWS profile or credentials, and no credential lookup that leaves the process."""
    config = tmp_path / "config"
    config.write_text("[profile other]\nregion = us-east-1\n")
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                 "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                 "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    AwsCredentialsManager.release_resources()
    yield
    AwsCredentialsManager.release_resources()


@pytest.mark.parametrize("arm", ["profile", "endpoint"])
def test_setup_failure_is_raised_unwrapped(mocker, env, aws_env, arm):
    mocker.patch.object(km_module, "_install_botocore_filters")
    env.keys.create_kms_client.side_effect = REAL_CREATE_KMS_CLIENT
    if arm == "profile":
        env.session.side_effect = REAL_GET_SESSION
        plugin, _ = make_plugin(mocker, aws_profile="kiro-no-such-profile")
        expected: type = ProfileNotFound
    else:
        env.session.return_value = boto3.Session(region_name="us-east-1", aws_access_key_id="kiro-fake",
                                                 aws_secret_access_key="kiro-fake")
        plugin, _ = make_plugin(mocker, encryption_kms_endpoint="not a url")
        expected = ValueError
    cur, raw = cursor(plugin)
    for _ in range(2):  # raised again at the next statement, which builds again
        with pytest.raises(expected) as info:
            call(cur, "execute", INSERT, (marker(),))
        assert type(info.value) is expected
    assert raw.calls == [] and env.metadata.call_count == 0


# ---- executemany ----

@pytest.mark.parametrize("kind", ["list", "generator"])
def test_empty_batch_reaches_the_driver_untouched(plugin, env, caplog, kind):
    cur, raw = cursor(plugin)
    rows: Any = [] if kind == "list" else (r for r in ())
    plugin._results[raw] = "stale"
    with caplog.at_level(logging.DEBUG):
        assert call(cur, "executemany", INSERT, rows) is None
    assert raw.calls == [("executemany", (INSERT, []), {})] and (kind == "generator" or raw.calls[0][1][1] is rows)
    assert raw not in plugin._results and env.metadata.call_count == 0 and own_records(caplog) == []


def test_empty_batch_of_unanalyzed_sql_logs_nothing(plugin, env, caplog):
    cur, raw = cursor(plugin)
    sql, rows = _OpaqueSql(INSERT), []
    with caplog.at_level(logging.DEBUG):
        call(cur, "executemany", sql, rows)
    assert raw.calls == [("executemany", (sql, []), {})] and env.metadata.call_count == 0 and own_records(caplog) == []


@pytest.mark.parametrize("kind", ["list", "generator"])
def test_non_empty_batch_is_encrypted_row_by_row(plugin, kind):
    cur, raw = cursor(plugin)
    rows = [(marker(),), (marker(),)]
    snapshot = list(rows)
    call(cur, "executemany", INSERT, rows if kind == "list" else (r for r in rows))
    sent = raw.calls[0][1][1]
    assert rows == snapshot and [EncryptionService.decrypt(r[0], DATA_KEY, HMAC_KEY) for r in sent] == [r[0] for r in rows]


def test_executemany_with_none_passes_none(plugin):
    cur, raw = cursor(plugin)
    call(cur, "executemany", "INSERT INTO audit (note) VALUES (%s)", None)
    assert raw.calls == [("executemany", ("INSERT INTO audit (note) VALUES (%s)", None), {})]


# ---- refusals and read failures ----

@pytest.mark.parametrize("dialect", ["pg", "mysql"])
@pytest.mark.parametrize("args, kwargs", [((), {}), ((None,), {}), (((),), {}), (([],), {}), (({},), {}),
                                          ((), {"params": {}})],
                         ids=["absent", "none", "tuple", "list", "dict", "keyword-dict"])
def test_unbound_write_names_the_column(mocker, env, dialect, args, kwargs):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    secret = marker()
    sql = f"INSERT INTO users (ssn) VALUES ('{secret}')"
    cur, raw = cursor(plugin)
    with pytest.raises(MetadataError) as info:
        call(cur, "execute", sql, *args, **kwargs)
    assert raw.calls == [] and (info.value.table_name, info.value.column_name) == ("users", "ssn")
    assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.UnboundWrite", "users.ssn")
    _clean(info.value, secret)


@pytest.mark.parametrize("dialect", ["pg", "mysql"])
def test_literal_write_in_a_later_statement_is_refused(mocker, env, dialect):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    secret = marker()
    cur, raw = cursor(plugin)
    with pytest.raises(MetadataError) as info:
        call(cur, "execute", f"SELECT 1; INSERT INTO users (name, ssn) VALUES ('kiro-name', '{secret}')")
    assert raw.calls == [] and str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.UnboundWrite", "users.ssn")
    _clean(info.value, secret)


def test_row_factory_refused_before_any_row(plugin):
    cur, raw = cursor(plugin)
    raw.row_factory = pg_rows.class_row(dict)
    call(cur, "execute", SELECT, (1,))
    secret = marker()
    raw.description, raw.rows = [("ssn",)], [(payload(secret),)]
    with pytest.raises(MetadataError) as info:
        call(cur, "fetchall")
    assert raw.fetches == 0
    _clean(info.value, secret)


@pytest.mark.parametrize("factory, shape", [(pg_rows.tuple_row, lambda i, s: (i, s)),
                                            (pg_rows.dict_row, lambda i, s: {"id": i, "ssn": s}),
                                            (pg_rows.namedtuple_row, _Row)],
                         ids=["tuple_row", "dict_row", "namedtuple_row"])
def test_allowed_row_factories_decrypt_only_the_encrypted_column(plugin, factory, shape):
    cur, raw = cursor(plugin)
    raw.row_factory = factory
    secret = marker()
    call(cur, "execute", "SELECT id, ssn FROM users WHERE id = %s", (1,))
    assert repr(plugin._results[raw]) == "_Result(['users'])"  # the tables only, never the statement or its values
    raw.description, raw.rows = [("id",), ("ssn",)], [shape(1, payload(secret))]
    row = call(cur, "fetchone")
    assert row == shape(1, secret) and type(row) is type(shape(1, secret))
    assert call(cur, "fetchone") is None


@pytest.mark.parametrize("configs", [[SSN], [SSN, ARCHIVE_SSN]], ids=["one-encrypted", "both-encrypted"])
def test_dict_row_with_a_repeated_name_decodes_the_value_it_holds(mocker, env, configs):
    env.configs = configs
    plugin, _ = make_plugin(mocker)
    cur, raw = cursor(plugin)
    archive = marker()
    archive_stored = payload(archive) if ARCHIVE_SSN in configs else archive.encode()
    call(cur, "execute", "SELECT u.ssn, a.ssn FROM users u JOIN archive a ON a.id = u.id WHERE u.id = %s", (1,))
    # The drivers build a dict row from the names in order, so it holds the last ssn column's value.
    raw.description, raw.rows = [("ssn",), ("ssn",)], [{"ssn": archive_stored}]
    assert call(cur, "fetchall") == [{"ssn": archive if ARCHIVE_SSN in configs else archive_stored}]


@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
@pytest.mark.parametrize("path", ["write", "read"])
@pytest.mark.parametrize("row", [None, replace(KEY_ROW, master_key_arn="")], ids=["missing", "unusable"])
def test_unusable_own_key_row(mocker, env, row, path, lenient):
    env.configs = [replace(SSN, key_metadata=row)]  # missing: the LEFT JOIN found no key_storage row
    plugin, _ = make_plugin(mocker, encryption_return_unverified_data=lenient)
    cur, raw = cursor(plugin)
    secret = marker()
    if path == "read":
        call(cur, "execute", SELECT, (1,))
        raw.description, raw.rows = [("ssn",)], [(payload(secret),)]
    with pytest.raises(InvalidKeyMetadataError) as info:
        call(cur, "execute", INSERT, (secret,)) if path == "write" else call(cur, "fetchall")
    assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.UnusableColumn", "users.ssn")
    assert env.keys.return_value.decrypt_data_key.call_count == 0 and len(raw.calls) == (1 if path == "read" else 0)
    _clean(info.value, secret)


@pytest.mark.parametrize("lenient", [False, True, "False", "true"],
                         ids=["strict", "lenient", "strict-url-string", "lenient-url-string"])
@pytest.mark.parametrize("kind", ["payload", "legacy-plaintext"])
def test_foreign_key_id_without_a_row(mocker, env, caplog, kind, lenient):
    # A SQLAlchemy URL passes the setting as a string, and reading "False" as truthy would make strict mode lenient.
    plugin, _ = make_plugin(mocker, encryption_return_unverified_data=lenient)
    lenient = lenient in (True, "true")
    secret = marker()
    stored = payload(secret, key_id=1802201963) if kind == "payload" else secret.encode()
    stored_id = int.from_bytes(stored[32:36], "big")
    assert stored_id != SSN.key_id
    cur, raw = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(stored,)]
    with caplog.at_level(logging.DEBUG):
        if lenient:
            assert call(cur, "fetchall") == [(stored,)]
        else:
            with pytest.raises(IntegrityCheckError) as info:
                call(cur, "fetchall")
            assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.UnknownKey", "users.ssn")
            assert str(stored_id) not in str(info.value)
            _clean(info.value, secret)
    env.keys.return_value.key_metadata_by_id.assert_called_once_with(stored_id)
    assert env.keys.return_value.decrypt_data_key.call_count == 0 and plugin_records(caplog) == []


@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_tampered_value(mocker, env, lenient):
    plugin, _ = make_plugin(mocker, encryption_return_unverified_data=lenient)
    secret = marker()
    tampered = bytearray(payload(secret))
    tampered[-1] ^= 1
    stored = bytes(tampered)
    cur, raw = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(stored,)]
    if lenient:
        assert call(cur, "fetchall") == [(stored,)]
    else:
        with pytest.raises(IntegrityCheckError) as info:
            call(cur, "fetchall")
        _clean(info.value, secret)


def test_rotated_key_row_decrypts_and_is_kept(mocker, env):
    old = replace(KEY_ROW, id=1802201963, key_id="k-old", encrypted_data_key="b2xk", hmac_key=OLD_HMAC)
    env.keys.return_value.key_metadata_by_id.return_value = old
    plugin, _ = make_plugin(mocker)
    secret = marker()
    cur, raw = cursor(plugin)
    for _ in range(2):
        call(cur, "execute", SELECT, (1,))
        raw.description = [("ssn",)]
        raw.rows = [(EncryptionService.encrypt(secret, DATA_KEY, OLD_HMAC, key_id=old.id),)]
        assert call(cur, "fetchall") == [(secret,)]
    env.keys.return_value.key_metadata_by_id.assert_called_once_with(old.id)
    env.keys.return_value.decrypt_data_key.assert_called_with("b2xk", ARN)


# Statement shapes, with users.ssn and users.dob the encrypted columns. Each outcome is the key of the refusal (naming
# users.ssn), or the ordinals encrypted (every other value is sent as written).
NOT_ENCRYPTED = "KmsEncryptionPlugin.AnnotationNotEncrypted"
NO_TABLE = "KmsEncryptionPlugin.UnknownColumnTable"
D = "SELECT s.ssn FROM (SELECT id, ssn FROM users) s JOIN archive a ON a.id = s.id WHERE s.ssn = "
W = "SELECT s.ssn FROM (SELECT id, ssn FROM archive) s JOIN users u ON u.id = s.id WHERE s.ssn = "
CTE = "WITH w AS (SELECT id, ssn FROM users) SELECT w.ssn FROM w JOIN archive a ON a.id = w.id WHERE w.ssn = "
JOIN = "SELECT u.id FROM users u JOIN audit x ON x.id = u.id WHERE "
ALONE = "SELECT s.ssn FROM (SELECT id, ssn FROM users) s WHERE s.ssn = "
IN_DERIVED = "UPDATE archive SET note = %s WHERE id IN (SELECT s.id FROM (SELECT id, ssn FROM users) s WHERE s.ssn = "
SA_SELECT = ("SELECT anon_1.id, anon_1.name, anon_1.ssn, addresses.id AS id_1, addresses.user_id, addresses.email \n"
             "FROM (SELECT users.id AS id, users.name AS name, users.ssn AS ssn \nFROM users \nWHERE users.name = ")
SHAPES: List[Tuple[str, Sequence[str], str, Any]] = [
    ("R1", ["pg"], "INSERT INTO users (name, ssn) VALUES (%s, /*@encrypt:users.name*/ %s)", NOT_ENCRYPTED),
    ("R2", ["mysql"], "INSERT INTO users (name, ssn) VALUES (%s, /*@encrypt:nosuch.col*/ %s)", NOT_ENCRYPTED),
    ("R3", ["pg"], "INSERT INTO users (name, ssn) VALUES (%s, /*@encrypt:users.snn*/ %s)", NOT_ENCRYPTED),
    ("R4", ["mysql"], "UPDATE users SET ssn = /*@encrypt:users.name*/ %s WHERE id = %s", NOT_ENCRYPTED),
    ("R5", ["pg"], "SELECT id FROM users WHERE ssn = /*@encrypt:users.name*/ %s", NOT_ENCRYPTED),
    ("R6", ["mysql"], "UPDATE users SET name = %s WHERE ssn = /*@encrypt:users.name*/ %s", NOT_ENCRYPTED),
    ("C1", ["pg"], "INSERT INTO users (name, ssn) VALUES (/*@encrypt:users.ssn*/ %s, %s)", {0, 1}),
    ("C2", ["pg"], "INSERT INTO users (name, ssn) VALUES (%s, /*@encrypt:users.ssn*/ %s)", {1}),
    ("C4", ["pg"], "UPDATE users SET ssn = upper(/*@encrypt:users.ssn*/ %s) WHERE id = %s",
     "KmsEncryptionPlugin.UnboundWrite"),
    ("G1", ["pg", "mysql"], D + "%s", NO_TABLE),
    ("G2", ["pg", "mysql"], CTE + "%s", NO_TABLE),
    ("G3", ["pg", "mysql"], IN_DERIVED + "%s)", NO_TABLE),
    ("G3a", ["pg", "mysql"], IN_DERIVED + "/*@encrypt:users.ssn*/ %s)", {1}),
    ("G4", ["pg", "mysql"], JOIN + "ssn = %s", NO_TABLE),
    ("G5", ["pg", "mysql"], ALONE + "%s", {0}),
    ("G9", ["pg", "mysql"], "SELECT s.name FROM (SELECT id, name FROM users) s JOIN archive a ON a.id = s.id "
                            "WHERE s.name = %s", set()),
    ("G10", ["pg", "mysql"], W + "%s", NO_TABLE),
    ("G10a", ["pg", "mysql"], W + "/*@encrypt:archive.ssn*/ %s", set()),
    ("G11", ["pg", "mysql"], D + "/*@encrypt:users.ssn*/ %s", {0}),
    ("G12", ["pg", "mysql"], "SELECT u.id FROM users u JOIN archive a ON a.id = u.id WHERE u.ssn = %s", {0}),
    ("G13", ["pg", "mysql"], JOIN + "note = %s", set()),
    ("O2", ["pg"], "SELECT anon_1.id, anon_1.ssn, addresses.email \nFROM (SELECT users.id AS id, users.ssn AS ssn \n"
                   "FROM users) AS anon_1 JOIN addresses ON addresses.user_id = anon_1.id \nWHERE anon_1.ssn = %(ssn_1)s", NO_TABLE),
    ("O2", ["mysql"], "SELECT anon_1.id, anon_1.ssn, addresses.email \nFROM (SELECT users.id AS id, users.ssn AS ssn \n"
                      "FROM users) AS anon_1 INNER JOIN addresses ON addresses.user_id = anon_1.id \nWHERE anon_1.ssn = %s",
     NO_TABLE),
    ("O4", ["pg"], SA_SELECT + "%(name_1)s::VARCHAR) AS anon_1 JOIN addresses ON addresses.user_id = anon_1.id \n"
                               "WHERE anon_1.ssn = %(ssn_1)s", NO_TABLE),
    ("O4", ["mysql"], SA_SELECT + "%s) AS anon_1 INNER JOIN addresses ON addresses.user_id = anon_1.id \n"
                                  "WHERE anon_1.ssn = %s", NO_TABLE),
    ("X1", ["pg", "mysql"], D + "/*@encrypt:ssn*/ %s", NO_TABLE),
    ("X2", ["pg", "mysql"], D + "/*@encrypt:s.ssn*/ %s", NO_TABLE),
    ("X3", ["pg", "mysql"], JOIN + "ssn = /*@encrypt:ssn*/ %s", NO_TABLE),
    ("X4", ["pg", "mysql"], CTE + "/*@encrypt:w.ssn*/ %s", NO_TABLE),
    ("X5", ["pg", "mysql"], ALONE + "/*@encrypt:ssn*/ %s", {0}),
    ("N1", ["pg", "mysql"], ALONE + "/*@encrypt:s.ssn*/ %s", NOT_ENCRYPTED),
    ("N4", ["pg", "mysql"], D + "/*@encrypt:nosuch.ssn*/ %s", NO_TABLE),
    ("N5", ["pg", "mysql"], W + "/*@encrypt:public.archive.ssn*/ %s", set()),
    ("N6", ["pg", "mysql"], W + "/*@encrypt:ARCHIVE.ssn*/ %s", set()),
    ("C3", ["pg", "mysql"], JOIN + "x.id = upper(/*@encrypt:ssn*/ %s)", set()),
    # Literals and expressions ahead of the parameters still leave only ssn's value encrypted.
    ("J1", ["pg", "mysql"], "INSERT INTO users (name, email, ssn) VALUES ('lit', %s, %s)", {1}),
    ("J2", ["pg", "mysql"], "INSERT INTO users (name, ssn, email) VALUES (%s, %s, %s)", {1}),
    ("J3", ["pg", "mysql"], "UPDATE users SET name = 'lit', ssn = %s WHERE id = %s", {0}),
    ("J4", ["pg", "mysql"], "UPDATE users SET updated_at = NOW(), ssn = %s WHERE id = %s", {0}),
    ("J5", ["pg", "mysql"], "SELECT name FROM users WHERE name = 'lit' AND ssn = %s", {0}),
    ("COPY", ["pg"], "COPY users (id, ssn) FROM PROGRAM 'printf \"1,x\\n\"' WITH (FORMAT csv)",
     "KmsEncryptionPlugin.UnboundWrite"),
]


@pytest.mark.parametrize("dialect, sql, outcome", [pytest.param(d, sql, outcome, id=f"{label}-{d}")
                                                   for label, dialects, sql, outcome in SHAPES for d in dialects])
def test_statement_shapes_are_encrypted_sent_or_refused(mocker, env, dialect, sql, outcome):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    names = re.findall(r"%\((\w+)\)s", sql)
    params: Any = {n: marker() for n in names} if names else tuple(marker() for _ in range(sql.count("%s")))
    cur, raw = cursor(plugin)
    if isinstance(outcome, str):
        with pytest.raises(MetadataError) as info:
            call(cur, "execute", sql, params)
        assert raw.calls == [] and (info.value.table_name, info.value.column_name) == ("users", "ssn")
        assert str(info.value) == Messages.get_formatted(outcome, "users.ssn")
        _clean(info.value, *(params.values() if names else params))
        return
    call(cur, "execute", sql, params)
    sent = raw.calls[0][1][1]
    assert outcome or sent is params
    for ordinal, value in enumerate(params):
        assert (len(sent[ordinal]) >= 65) if ordinal in outcome else (sent[ordinal] is value)


@pytest.mark.parametrize("dialect, sql, gap", [
    ("pg", "INSERT INTO users (name, ssn) SELECT name, ssn FROM users WHERE ssn = /*@encrypt:users.name*/ %s",
     AnalysisGap.VALUES_FROM_QUERY),
    ("pg", "INSERT INTO users (name, ssn) SELECT a.name, a.ssn FROM archive a JOIN audit x ON x.id = a.id WHERE ssn = %s",
     AnalysisGap.VALUES_FROM_QUERY),
    ("mysql", "UPDATE users u JOIN accounts a ON a.id = u.id SET ssn = %s WHERE a.id = %s", AnalysisGap.AMBIGUOUS_TABLE),
], ids=["annotated", "table-less", "multi-table-update"])
def test_gap_write_passes_with_one_warning(mocker, env, caplog, dialect, sql, gap):
    # Each statement has a shape a refusal would catch without its gap; the gap rule comes first.
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    cur, raw = cursor(plugin)
    params = tuple(marker() for _ in range(sql.count("%s")))
    with caplog.at_level(logging.DEBUG):
        for _ in range(2):
            call(cur, "execute", sql, params)
    assert [c[1][1] is params for c in raw.calls] == [True, True]
    warnings = plugin_records(caplog)
    assert len(warnings) == 1 and warnings[0].levelno == logging.WARNING
    message = warnings[0].getMessage()
    assert message.startswith("[KmsEncryptionPlugin] A statement writing table(s) users") and f"({gap.value})" in message
    assert not [r for r in caplog.records for p in params if p in r.getMessage()]


# A %s the driver still binds, hidden beside the real placeholders: where it hides, the kind of statement, and the SQL
# (on MySQL with backticks for the double quotes).
HIDDEN = [
    ("literal", "write", "INSERT INTO users (name, ssn) VALUES ('x%s', %s)"),
    ("literal", "select", "SELECT ssn FROM users WHERE name <> 'x%s' AND id = %s"),
    ("identifier", "write", 'UPDATE users AS "u%s" SET ssn = %s WHERE id = %s'),
    ("identifier", "select", 'SELECT ssn AS "x%s" FROM users WHERE id = %s'),
    ("comment", "write", "UPDATE users SET ssn = %s /* %s */ WHERE id = %s"),
    ("comment", "select", "SELECT ssn FROM users WHERE id = %s -- %s"),
]


@pytest.mark.parametrize("dialect", ["pg", "mysql"])
@pytest.mark.parametrize("method, sql, refused",
                         [pytest.param("execute", sql, True, id=f"{where}-{kind}") for where, kind, sql in HIDDEN]
                         + [pytest.param("executemany", "INSERT INTO users (name, ssn) VALUES ('x%s', %s)", True, id="literal-write-many"),
                            pytest.param("execute", "INSERT INTO audit (note) VALUES (%s) /* %s */", False, id="plain-table")])
def test_hidden_placeholder_on_a_table_with_encrypted_columns_is_refused(mocker, env, dialect, method, sql, refused):
    # The driver's count of placeholders decides which value lands in which column, and the analysis can't follow it.
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    sql = sql.replace('"', "`") if dialect == "mysql" else sql
    sets = [tuple(marker() for _ in range(sql.count("%s"))) for _ in range(2)]
    params = sets if method == "executemany" else sets[0]
    cur, raw = cursor(plugin)
    if not refused:  # no encrypted column to protect: sent as given
        call(cur, method, sql, params)
        assert len(raw.calls) == 1 and raw.calls[0][1][0] is sql and raw.calls[0][1][1] is params
        return
    with pytest.raises(MetadataError) as info:
        call(cur, method, sql, params)
    assert raw.calls == [] and str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.HiddenPlaceholder", "users")
    _clean(info.value, *sets[0], *sets[1])


@pytest.mark.parametrize("dialect, kind", [("mysql", "gap"), ("pg", "not-analyzed")])
def test_pass_through_warnings_quote_no_literal(mocker, env, caplog, dialect, kind):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    cur, raw = cursor(plugin)
    secret = marker()
    sql: Any = (f"UPDATE users u JOIN accounts a ON a.id = u.id SET ssn = '{secret}'" if kind == "gap"
                else _OpaqueSql(f"INSERT INTO users (ssn) VALUES ('{secret}')"))
    with caplog.at_level(logging.DEBUG):
        call(cur, "execute", sql)
    assert raw.calls[0][1][0] is sql and len(plugin_records(caplog)) == 1
    assert not [r for r in caplog.records if secret in r.getMessage()]


def test_pass_through_warnings_stop_at_256_shapes(mocker, env, caplog):
    env.configs = [ColumnEncryptionConfig(f"t{i}", "ssn", key_id=7, key_metadata=KEY_ROW) for i in range(300)]
    plugin, _ = make_plugin(mocker, DriverDialectCodes.MYSQL_CONNECTOR_PYTHON)
    cur, raw = cursor(plugin)
    with caplog.at_level(logging.DEBUG):
        for i in range(300):  # an unqualified SET in a multi-table UPDATE is a gap write, passed through with one warning per shape
            call(cur, "execute", f"UPDATE t{i} u JOIN accounts a ON a.id = u.id SET ssn = %s WHERE a.id = %s",
                 (marker(), 1))
    assert len(raw.calls) == 300 and len(plugin_records(caplog)) == 256 and len(plugin._warned) == 256


# ---- values with no exact stored form, and stored values that aren't payloads ----

class _Shade(enum.Enum):
    DARK = "dark"


class _Level(enum.IntEnum):
    HIGH = 3


class _Color(str, enum.Enum):
    RED = "red"


NO_EXACT_FORM: Dict[str, Callable[[str], Any]] = {
    "uuid": lambda m: uuid.uuid4(), "dict": lambda m: {"ssn": m}, "list": lambda m: [m], "tuple": lambda m: (m,),
    "ipv4": lambda m: ipaddress.IPv4Address("192.0.2.1"), "timedelta": lambda m: timedelta(days=1),
    "enum": lambda m: _Shade.DARK, "jsonb": lambda m: Jsonb({"ssn": m}), "json": lambda m: Json([m]),
    "psycopg-binary": lambda m: psycopg.Binary(m.encode())}


@pytest.mark.parametrize("dialect, sql", [
    ("pg", INSERT), ("mysql", INSERT), ("mysql", "SELECT id FROM users WHERE ssn = %s"),
    ("pg", "SELECT id FROM users WHERE ssn = ANY(%s)"), ("pg", "SELECT id FROM users WHERE ssn <> ALL(%s)"),
], ids=["pg-insert", "mysql-insert", "mysql-compare", "pg-any", "pg-all"])
@pytest.mark.parametrize("kind", NO_EXACT_FORM)
def test_value_with_no_exact_stored_form_is_refused(mocker, env, dialect, sql, kind):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    cur, raw = cursor(plugin)
    secret = marker()
    value = NO_EXACT_FORM[kind](secret)
    with pytest.raises(EncryptionFailedError) as info:
        call(cur, "execute", sql, (value,))
    assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.NoExactForm", type(value).__name__, "users.ssn")
    assert info.value.data_type == "GENERIC" and raw.calls == []
    assert env.keys.return_value.decrypt_data_key.call_count == 0
    _clean(info.value, secret)


def test_batch_refused_at_a_late_value_sends_nothing(plugin, env):
    cur, raw = cursor(plugin)
    with pytest.raises(EncryptionFailedError):
        call(cur, "executemany", INSERT, [(marker(),), (marker(),), (uuid.uuid4(),)])
    assert raw.calls == [] and env.keys.return_value.decrypt_data_key.call_count == 2  # once per value sealed before it


def test_lenient_mode_still_refuses_a_value_with_no_exact_stored_form(mocker, env):
    plugin, _ = make_plugin(mocker, encryption_return_unverified_data=True)  # lenient mode covers reads only
    cur, raw = cursor(plugin)
    with pytest.raises(EncryptionFailedError) as info:
        call(cur, "execute", INSERT, (uuid.uuid4(),))
    assert info.value.data_type == "GENERIC" and raw.calls == []
    assert env.keys.return_value.decrypt_data_key.call_count == 0


@pytest.mark.parametrize("value", [_Level.HIGH, Decimal("1.10"), _Color.RED, mysql.connector.Binary(b"kiro-bytes")],
                         ids=["intenum", "decimal", "str-enum", "mysql-binary"])
def test_values_with_an_exact_form_still_encrypt(plugin, value):
    cur, raw = cursor(plugin)
    call(cur, "execute", INSERT, (value,))
    sent = raw.calls[0][1][1][0]
    assert len(sent) >= 65 and EncryptionService.decrypt(sent, DATA_KEY, HMAC_KEY) == value


def test_stored_generic_payload_reads_back_as_text(plugin):
    cur, raw = cursor(plugin)
    value = uuid.uuid4()
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(payload(value),)]
    assert call(cur, "fetchall") == [(str(value),)]


@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
@pytest.mark.parametrize("kind", ["marker", "hex-text"])
@pytest.mark.parametrize("read", ["fetchone", "fetchall", "iteration"])
def test_non_binary_stored_value_is_not_a_payload(mocker, env, caplog, read, kind, lenient):
    plugin, _ = make_plugin(mocker, encryption_return_unverified_data=lenient)
    secret = marker()
    stored = secret if kind == "marker" else "\\x" + payload(secret).hex()  # a text column holding a payload's hex
    cur, raw = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(stored,)]
    fetch = (lambda: list(cur)) if read == "iteration" else (lambda: call(cur, read))
    with caplog.at_level(logging.DEBUG):
        if lenient:
            assert fetch() == ((stored,) if read == "fetchone" else [(stored,)])
        else:
            with pytest.raises(IntegrityCheckError) as info:
                fetch()
            assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.NotAPayload", "users.ssn")
            assert info.value.algorithm == SSN.algorithm
            _clean(info.value, secret, stored)
    assert env.keys.return_value.key_metadata_by_id.call_count == 0 and plugin_records(caplog) == []
    assert env.keys.return_value.decrypt_data_key.call_count == 0


@pytest.mark.parametrize("form", [bytearray, memoryview], ids=["bytearray", "memoryview"])
def test_binary_stored_forms_decrypt(plugin, form):
    # mysql-connector's C-extension prepared cursor returns bytearray for a binary column.
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(form(payload(secret)),)]
    assert call(cur, "fetchall") == [(secret,)]


def test_int_stored_value_is_not_a_payload(plugin, env):
    cur, raw = cursor(plugin)
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(10 ** 6,)]  # bytes(10**6) would be a megabyte of zeros
    with pytest.raises(IntegrityCheckError) as info:
        call(cur, "fetchall")
    assert str(info.value) == Messages.get_formatted("KmsEncryptionPlugin.NotAPayload", "users.ssn")
    assert env.keys.return_value.key_metadata_by_id.call_count == 0


# ---- bytes SQL and MySQL _binary ----

@pytest.mark.parametrize("dialect, conn, codec", [
    ("pg", SimpleNamespace(info=SimpleNamespace(encoding="iso8859-1")), "latin-1"),
    ("mysql", SimpleNamespace(python_charset="utf8", charset="utf8mb4"), "utf-8"),  # utf8mb4 is no Python codec
], ids=["pg-latin1", "mysql-utf8mb4"])
def test_bytes_sql_is_analyzed_in_the_connection_encoding(mocker, env, caplog, dialect, conn, codec):
    plugin, service = make_plugin(mocker, DIALECTS[dialect])
    service.driver_dialect.get_connection_from_obj.return_value = conn
    literal = "caf\xe9" if dialect == "pg" else "cafe"  # latin-1's 0xE9 isn't UTF-8; MySQL analyzes only ASCII bytes SQL
    sql = f"INSERT INTO users (name, ssn) VALUES ('{literal}', %s)".encode(codec)
    secret = marker()
    cur, raw = cursor(plugin)
    with caplog.at_level(logging.DEBUG):
        call(cur, "execute", sql, (secret,))
        call(cur, "execute", SELECT.encode(codec), (1,))
        sent_sql, sent = raw.calls[0][1]
        raw.description, raw.rows = [("ssn",)], [(sent[0],)]
        assert call(cur, "fetchall") == [(secret,)]
    # The driver gets the caller's bytes, with only _binary inserted on MySQL.
    assert sent_sql is sql if dialect == "pg" else sent_sql == sql.replace(b"', %s", b"', _binary %s")
    assert len(sent[0]) >= 65 and plugin_records(caplog) == []


@pytest.mark.parametrize("dialect, conn, sql", [
    ("pg", SimpleNamespace(info=SimpleNamespace(encoding="utf-8")), None),  # None: _undecodable's statement
    ("pg", None, INSERT.encode()),
    ("pg", SimpleNamespace(info=SimpleNamespace(encoding="shift_jis_2004")), b"UPDATE users SET ssn = %s WHERE name ~ '^O'"),
    ("pg", SimpleNamespace(info=SimpleNamespace(encoding="big5")), b"INSERT INTO users (name, ssn) VALUES ('\xa1\xfe', %s)"),
    ("mysql", SimpleNamespace(python_charset="utf8", charset="utf8mb4"), b"UPDATE users SET name = '\xc4\x81\x5c', ssn = %s"),
    ("mysql", SimpleNamespace(python_charset="latin1", charset="latin1"), b"UPDATE users SET name = 'caf\xe9', ssn = %s"),
    ("mysql", SimpleNamespace(python_charset="cp932", charset="cp932"), b"UPDATE users SET name = '\x93\xfa\x96{', ssn = %s"),
], ids=["undecodable", "no-connection", "shift-jis-2004", "not-reversible", "mysql-stale-charset", "mysql-latin1",
        "mysql-cp932"])
def test_bytes_sql_it_cannot_decode_runs_unanalyzed(mocker, env, caplog, dialect, conn, sql):
    # shift_jis_2004 reads 0x7E as an overline, where the server reads "~"; big5 re-encodes 0xA1FE as 0xA241. On MySQL any
    # non-ASCII byte is enough: after SET NAMES sent as SQL the connection still reports the character set it negotiated,
    # and sjis 0xC4 0x815C, read as UTF-8, is U+0101 and a backslash that escapes the closing quote.
    plugin, service = make_plugin(mocker, DIALECTS[dialect])
    service.driver_dialect.get_connection_from_obj.return_value = conn
    secret = marker()
    sql = _undecodable(secret) if sql is None else sql
    params = (secret,) if b"%s" in sql else None
    cur, raw = cursor(plugin)
    with caplog.at_level(logging.DEBUG):
        for _ in range(2):
            call(cur, "execute", sql, params)
        warnings = [(r.levelno, r.getMessage()) for r in plugin_records(caplog)]
        assert warnings == [(logging.WARNING, Messages.get_formatted("KmsEncryptionPlugin.NotAnalysed", "bytes"))]
        assert [c[1][0] is sql and c[1][1] is params for c in raw.calls] == [True, True]
        assert _carrying(caplog, secret) == [] and env.metadata.call_count == 0
        _plant_controls(secret)
    assert len(_carrying(caplog, secret)) == 3


@pytest.mark.parametrize("sql", [INSERT.encode(), "UPDATE users SET name = '\u0436', ssn = %s".encode("koi8_r")],
                         ids=["ascii", "non-ascii"])
def test_bytes_sql_in_a_charset_python_has_no_codec_for_raises(mocker, env, sql):
    # The codec lookup comes before MySQL's ASCII check, so a non-ASCII statement raises too.
    plugin, service = make_plugin(mocker, DriverDialectCodes.MYSQL_CONNECTOR_PYTHON)
    service.driver_dialect.get_connection_from_obj.return_value = SimpleNamespace(python_charset="koi8r", charset="koi8r")
    cur, raw = cursor(plugin)
    with pytest.raises(LookupError) as info:  # as mysql-connector itself raises for str SQL there
        call(cur, "execute", sql, (marker(),))
    assert raw.calls == [] and info.value.__cause__ is None and info.value.__context__ is None


MYSQL_INSERT = "INSERT INTO users (name, ssn, dob) VALUES (%s, %s, %s)"


@pytest.mark.parametrize("form", ["positional", "operation"])
@pytest.mark.parametrize("method", ["execute", "executemany"])
def test_mysql_binary_prefix_goes_before_encrypted_placeholders(mocker, env, method, form):
    plugin, _ = make_plugin(mocker, DriverDialectCodes.MYSQL_CONNECTOR_PYTHON)
    cur, raw = cursor(plugin)
    sets = [("kiro-name", marker(), marker()) for _ in range(3)]
    params = sets if method == "executemany" else sets[0]
    snapshot = list(sets)
    slot = "seq_params" if method == "executemany" else "params"
    if form == "positional":
        call(cur, method, MYSQL_INSERT, params)
    else:
        call(cur, method, operation=MYSQL_INSERT, **{slot: params})
    _, args, kwargs = raw.calls[0]
    sent_sql, sent = args if form == "positional" else (kwargs["operation"], kwargs[slot])
    assert sent_sql == "INSERT INTO users (name, ssn, dob) VALUES (%s, _binary %s, _binary %s)" and sets == snapshot
    for row, sent_row in zip(sets, sent if method == "executemany" else [sent]):
        assert sent_row[0] == "kiro-name"
        assert [EncryptionService.decrypt(v, DATA_KEY, HMAC_KEY) for v in sent_row[1:]] == list(row[1:])


_PreparedCursor = type("MySQLCursorPrepared", (FakeDriverCursor,), {})
_CPreparedCursor = type("CMySQLCursorPrepared", (FakeDriverCursor,), {})


@pytest.mark.parametrize("dialect, cls, sql, null", [
    ("mysql", FakeDriverCursor, INSERT, True),
    ("mysql", FakeDriverCursor, "INSERT INTO users (ssn) VALUES (_binary %s)", False),  # as Django's BinaryField
    ("mysql", _PreparedCursor, INSERT, False),
    ("mysql", type("MySQLCursorPreparedDict", (_PreparedCursor,), {}), INSERT, False),
    ("mysql", _CPreparedCursor, INSERT, False),
    ("mysql", type("CMySQLCursorPreparedDict", (_CPreparedCursor,), {}), INSERT, False),
    ("pg", FakeDriverCursor, INSERT, False),
], ids=["none", "introducer", "MySQLCursorPrepared", "MySQLCursorPreparedDict", "CMySQLCursorPrepared",
        "CMySQLCursorPreparedDict", "pg"])
def test_mysql_binary_prefix_is_left_out(mocker, env, dialect, cls, sql, null):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    cur, raw = cursor(plugin, cls())
    secret = marker()
    call(cur, "execute", sql, (None if null else secret,))
    sent_sql, sent = raw.calls[0][1]
    assert sent_sql is sql and (sent == (None,) if null else EncryptionService.decrypt(sent[0], DATA_KEY, HMAC_KEY) == secret)


@pytest.mark.parametrize("named", [False, True], ids=["positional", "named"])
def test_mysql_binary_prefix_leaves_out_a_key_that_is_none_in_any_set(mocker, env, named):
    plugin, _ = make_plugin(mocker, DriverDialectCodes.MYSQL_CONNECTOR_PYTHON)
    cur, raw = cursor(plugin)
    sql = "INSERT INTO users (ssn, dob) VALUES (%(s)s, %(d)s)" if named else "INSERT INTO users (ssn, dob) VALUES (%s, %s)"
    rows = [(marker(), marker()), (None, marker()), (marker(), marker())]  # every set shares one SQL text
    call(cur, "executemany", sql, [{"s": s, "d": d} for s, d in rows] if named else rows)
    sent_sql, sent = raw.calls[0][1]
    assert sent_sql == sql.replace(", %", ", _binary %") and (sent[1]["s"] if named else sent[1][0]) is None


def test_prepared_cursor_names_match_the_driver_classes():
    prepared = {"MySQLCursorPrepared", "MySQLCursorPreparedDict", "CMySQLCursorPrepared", "CMySQLCursorPreparedDict"}

    def check(module: Any, prefix: str) -> None:
        classes = [getattr(module, n) for n in dir(module) if n.startswith(prefix) and isinstance(getattr(module, n), type)]
        assert len(classes) > 2
        for cls in classes:
            assert any(c.__name__ in plugin_module._PREPARED for c in cls.__mro__) == (cls.__name__ in prepared)

    check(mysql_cursor, "MySQLCursor")
    check(pytest.importorskip("mysql.connector.cursor_cext"), "CMySQLCursor")


# ---- reads ----

@pytest.mark.parametrize("dialect", ["pg", "mysql"])
@pytest.mark.parametrize("sql, names, at", [
    ("SELECT ssn AS s FROM users", ["s"], 0),
    ("SELECT users.id AS users_id, users.ssn AS users_ssn FROM users WHERE users.id = %(pk_1)s", ["users_id", "users_ssn"], 1),
    ("SELECT * FROM users", ["id", "name", "ssn"], 2),  # by name: the result columns aren't known
    ("SELECT * FROM users", ["id", "name", "SSN"], 2),  # a table that declares the column in another case
], ids=["alias", "legacy-query", "star", "star-other-case"])
def test_reads_decrypt_by_position_or_unique_name(mocker, env, dialect, sql, names, at):
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", sql, {"pk_1": 1} if "%(" in sql else None)
    row = [f"kiro-{n}" for n in names]
    expected = tuple(row[:at] + [secret] + row[at + 1:])
    row[at] = payload(secret)
    raw.description, raw.rows = [(n,) for n in names], [tuple(row)]
    assert call(cur, "fetchall") == [expected]


@pytest.mark.parametrize("dialect", ["pg", "mysql"])
def test_star_decrypts_a_column_the_metadata_names_in_another_case(mocker, env, dialect):
    env.configs = [replace(SSN, column_name="SSN")]  # a quoted "SSN" on PostgreSQL, or registered as SSN on MySQL
    plugin, _ = make_plugin(mocker, DIALECTS[dialect])
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", "SELECT * FROM users", None)
    raw.description, raw.rows = [("id",), ("SSN",)], [(1, payload(secret))]
    assert call(cur, "fetchall") == [(1, secret)]


@pytest.mark.parametrize("configs", [[SSN], [SSN, ARCHIVE_SSN]], ids=["one-encrypted", "both-encrypted"])
def test_star_over_a_join_sharing_an_encrypted_name_returns_it_as_stored(mocker, env, configs):
    env.configs = configs
    plugin, _ = make_plugin(mocker)
    cur, raw = cursor(plugin)
    call(cur, "execute", "SELECT * FROM users u JOIN archive a ON a.id = u.id WHERE u.id = %s", (1,))
    users_ssn = payload(marker())
    archive_ssn = payload(marker()) if ARCHIVE_SSN in configs else marker()  # plaintext when archive.ssn isn't encrypted
    raw.description, raw.rows = [("id",), ("ssn",), ("id",), ("ssn",)], [(1, users_ssn, 1, archive_ssn)]
    assert call(cur, "fetchall") == [(1, users_ssn, 1, archive_ssn)]
    assert env.keys.return_value.decrypt_data_key.call_count == 0


@pytest.mark.parametrize("configs", [[SSN], [SSN, ARCHIVE_SSN]], ids=["one-encrypted", "both-encrypted"])
def test_star_over_a_join_sharing_the_name_in_another_case_returns_it_as_stored(mocker, env, configs):
    env.configs = configs
    plugin, _ = make_plugin(mocker)
    cur, raw = cursor(plugin)
    call(cur, "execute", "SELECT * FROM users u JOIN archive a ON a.id = u.id WHERE u.id = %s", (1,))
    users_ssn = payload(marker())
    archive_ssn = payload(marker()) if ARCHIVE_SSN in configs else marker()  # plaintext when archive.ssn isn't encrypted
    # MySQL reports a column's name as its table declares it, and PostgreSQL a quoted name's case
    raw.description, raw.rows = [("id",), ("ssn",), ("id",), ("SSN",)], [(1, users_ssn, 1, archive_ssn)]
    assert call(cur, "fetchall") == [(1, users_ssn, 1, archive_ssn)]
    assert env.keys.return_value.decrypt_data_key.call_count == 0


def test_broken_iteration_resumes_and_decrypts_each_value_once(plugin, env):
    cur, raw = cursor(plugin)
    secrets = [marker() for _ in range(3)]
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(payload(s),) for s in secrets]
    for row in cur:
        assert row == (secrets[0],)
        break
    assert list(cur) == [(s,) for s in secrets[1:]]
    assert env.keys.return_value.decrypt_data_key.call_count == 3


def test_composable_insert_is_encrypted_and_sent_as_the_object(plugin):
    cur, raw = cursor(plugin)
    sql = pg_sql.SQL("INSERT INTO {} (ssn) VALUES (%s)").format(pg_sql.Identifier("users"))
    secret = marker()
    call(cur, "execute", sql, (secret,))
    sent_sql, sent = raw.calls[0][1]
    assert sent_sql is sql and EncryptionService.decrypt(sent[0], DATA_KEY, HMAC_KEY) == secret


# ---- logging, key copies and AES-128-GCM ----

@pytest.mark.parametrize("setting", [True, False, None, "False", "true"],
                         ids=["True", "False", "unset", "url-False", "url-true"])
def test_audit_logging_warns_once_at_creation(mocker, env, caplog, setting):
    props = {} if setting is None else {"encryption_audit_logging_enabled": setting}
    warns = setting in (True, "true")
    expected = [(logging.WARNING, Messages.get("KmsEncryptionPlugin.AuditNotWritten"))] if warns else []
    with caplog.at_level(logging.DEBUG):
        plugin, _ = make_plugin(mocker, **props)
        assert [(r.levelno, r.getMessage()) for r in plugin_records(caplog)] == expected
        cur, _ = cursor(plugin)
        call(cur, "execute", INSERT, (marker(),))
    assert len(plugin_records(caplog)) == len(expected)  # nothing more at the first statement


@pytest.mark.parametrize("setting, enabled", [("False", False), ("true", True)], ids=["url-False", "url-true"])
def test_data_key_cache_enabled_reads_a_string_setting(mocker, env, setting, enabled):
    plugin, _ = make_plugin(mocker, encryption_data_key_cache_enabled=setting)
    cur, _ = cursor(plugin)
    call(cur, "execute", INSERT, (marker(),))
    assert env.cache.call_args.kwargs["enabled"] is enabled


@pytest.mark.parametrize("dialect, chain", [("pg", WrapperProperties.DEFAULT_PLUGINS),
                                            ("mysql", WrapperProperties.MYSQL_CONNECTOR_DEFAULT_PLUGINS)],
                         ids=["pg", "mysql"])
def test_no_plugin_in_the_default_chain_logs_a_bound_value(mocker, env, caplog, dialect, chain):
    # The plugins before kms_encryption get the caller's values, the plugins after it and the driver the payloads.
    conn, raw = _chain(f"{chain},kms_encryption,execute_time", DIALECTS[dialect])
    secret = marker()
    with caplog.at_level(logging.DEBUG):
        conn.cursor().execute(INSERT, (secret,))
        assert len(raw.calls[0][1][1][0]) >= 65 and caplog.records and _carrying(caplog, secret) == []
        _plant_controls(secret)
    assert len(_carrying(caplog, secret)) == 3


@pytest.mark.parametrize("arm, calls", [("write", 2), ("write-raises", 1), ("read", 2), ("read-tampered", 1)],
                         ids=["write", "write-raises", "read", "read-tampered"])
def test_key_copies_are_wiped_after_each_value(mocker, env, arm, calls):
    plugin, _ = make_plugin(mocker)
    cur, raw = cursor(plugin)
    stored = [payload(marker()) for _ in range(calls)]
    if arm == "read-tampered":
        stored[0] = stored[0][:-1] + bytes([stored[0][-1] ^ 1])
    seen = spy_crypto(mocker, plugin)
    if arm == "write":
        call(cur, "executemany", INSERT, [(marker(),), (marker(),)])
    elif arm == "write-raises":
        with pytest.raises(EncryptionFailedError):  # raised inside EncryptionService.encrypt
            call(cur, "execute", INSERT, (2 ** 70,))
    else:
        call(cur, "execute", SELECT, (1,))
        raw.description, raw.rows = [("ssn",)], [(s,) for s in stored]
        if arm == "read":
            call(cur, "fetchall")
        else:
            with pytest.raises(IntegrityCheckError):
                call(cur, "fetchall")
    assert [s[0] for s in seen] == ["encrypt" if arm.startswith("write") else "decrypt"] * calls
    for _, data_key, hmac_key, size, algorithm in seen:
        assert type(data_key) is bytearray and type(hmac_key) is bytearray and (size, algorithm) == (32, SSN.algorithm)
        assert not any(data_key) and not any(hmac_key)
    assert len({id(s[1]) for s in seen}) == calls  # a fresh data-key copy for each value


def test_aes_128_gcm_column_round_trips_with_a_16_byte_key(mocker, env):
    env.configs = [replace(SSN, algorithm="AES-128-GCM", key_metadata=replace(KEY_ROW, key_spec="AES_128"))]
    env.keys.return_value.decrypt_data_key.side_effect = lambda edk, arn: bytearray(DATA_KEY[:16])
    plugin, _ = make_plugin(mocker)
    seen = spy_crypto(mocker, plugin)
    cur, raw = cursor(plugin)
    secret = marker()
    call(cur, "execute", INSERT, (secret,))
    stored = raw.calls[0][1][1][0]
    call(cur, "execute", SELECT, (1,))
    raw.description, raw.rows = [("ssn",)], [(stored,)]
    assert len(stored) >= 65 and call(cur, "fetchall") == [(secret,)]
    assert [(s[0], s[3], s[4]) for s in seen] == [("encrypt", 16, "AES-128-GCM"), ("decrypt", 16, "AES-128-GCM")]
