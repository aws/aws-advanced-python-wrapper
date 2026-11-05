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

"""The KMS Encryption Plugin over a connection's life: pipelines, several cursors and threads, session character
sets, connection limits, the plugin's own connections and the wrapper's release_resources()."""

from __future__ import annotations

import secrets
import threading
from typing import TYPE_CHECKING, Any, Dict, Iterator, List

import mysql.connector
import psycopg
import pytest

import aws_advanced_python_wrapper
from aws_advanced_python_wrapper.encryption.connection_source import \
    PluginConnectionSource
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                EncryptionPluginError)
from .utils.conditions import (disable_on_engines, enable_on_engines,
                               enable_on_features)
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (DbUser, binary_type,
                                          create_column_trigger,
                                          encrypt_column, encrypted_table,
                                          install_trigger_sql, is_pg,
                                          key_management_utility, kms_builds,
                                          plain_connect, raised, read_column,
                                          statement_log_lines, wrapper_connect,
                                          write)
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import KmsSettings, PayloadVerifier

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

_TABLE = "kms_lifecycle_users"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
# A table whose encrypted column has the package's trigger, which refuses plaintext.
_GUARDED = "kms_lifecycle_guarded"
# A MySQL table named with \u00c9, which the swe7 character set writes as @.
_SWE7_TABLE, _SWE7_SPELLING = "kms_lifecycle_\u00c9", "kms_lifecycle_@"


@pytest.fixture(scope="module")
def key_rows(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
             kms_app_user: DbUser) -> Iterator[Dict[str, int]]:
    """The module's table, with ``ssn`` and ``dob`` encrypted under key rows of their own; yields their ids."""
    with encrypted_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}, dob {binary_type()}", ("ssn", "dob"),
                         kms_settings, kms_schema, kms_master_key_arn) as key_ids:
        yield key_ids


@pytest.fixture(scope="module")
def guarded(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[None]:
    with encrypted_table(_GUARDED, f"name VARCHAR(100), ssn {binary_type()}", ("ssn",), kms_settings, kms_schema,
                         kms_master_key_arn, triggered=("ssn",)):
        yield


@pytest.fixture(scope="module")
def swe7_table(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
               kms_app_user: DbUser) -> Iterator[str]:
    """A MySQL table named with \u00c9 whose encrypted column has the package's trigger."""
    with plain_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{_SWE7_TABLE}`")
            cur.execute(f"CREATE TABLE `{_SWE7_TABLE}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), "
                        f"ssn VARBINARY(1024))")
        conn.commit()
        encrypt_column(conn, kms_settings, kms_schema, _SWE7_TABLE, "ssn", kms_master_key_arn)
        install_trigger_sql(conn, kms_schema)
        create_column_trigger(conn, _SWE7_TABLE, "ssn")
    try:
        yield _SWE7_TABLE
    finally:
        with plain_connect() as conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{_SWE7_TABLE}`")
            conn.commit()


@pytest.fixture
def conn(build: str, key_rows: Dict[str, int], kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
         kms_app_user: DbUser) -> Iterator[AwsWrapperConnection]:
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        yield conn


def _opened(verifier: PayloadVerifier, name: str) -> List[Any]:
    """The values of the rows named ``name``, opened without the plugin."""
    return [opened[1] for opened in verifier.opened(_TABLE, "ssn", name) if opened is not None]


@disable_on_engines([DatabaseEngine.MYSQL])
@kms_builds()
def test_psycopg_pipeline_reads_decrypt(build: str, key_rows: Dict[str, int], kms_settings: KmsSettings,
                                        kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                        kms_verifier: PayloadVerifier, kms_statement_log: str) -> None:
    reads: Dict[Any, List[Any]] = {}
    stored: Dict[Any, bool] = {}
    pids: List[int] = []
    for metadata in ("loaded", "loaded in the pipeline"):
        for read in ("fetchone", "fetchmany", "fetchall", "for"):
            name, values = f"it42-{len(reads)}", [f"pt-it42-{len(reads)}-{i}" for i in range(3)]
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                pids.append(conn.target_connection.info.backend_pid)
                if metadata == "loaded":
                    read_column(conn, _SELECT, "it42-nothing")  # the connection's first statement loads the metadata
                with conn.cursor() as cur:
                    with conn.pipeline():
                        for value in values:
                            cur.execute(_INSERT, (name, value))
                        cur.execute(_SELECT, (name,))
                        if read == "fetchone":
                            rows = [cur.fetchone() for _ in values]
                        elif read == "fetchmany":
                            rows = cur.fetchmany(2) + cur.fetchmany(5)
                        elif read == "fetchall":
                            rows = cur.fetchall()
                        else:
                            rows = list(cur)
                conn.commit()
            reads[(metadata, read)] = [row[0] for row in rows]
            stored[(metadata, read)] = _opened(kms_verifier, name) == values
    lookups = [line for line in statement_log_lines(kms_statement_log) if f"{kms_schema}.".encode() in line]
    in_pipeline = [line for line in lookups if any(f"[{pid}]".encode() in line for pid in pids)]
    assert reads == {key: [f"pt-it42-{i}-{j}" for j in range(3)] for i, key in enumerate(reads)}, \
        "in a pipeline every fetch path returns the rows decrypted, with the metadata loaded before or inside it"
    assert stored == {key: True for key in stored}, "the pipeline's writes store payloads of the values written"
    assert len(lookups) > 0 and in_pipeline == [], \
        "the plugin reads its metadata on its own connection, never in the application's pipeline session"


@kms_builds()
def test_interleaved_cursors_and_threads_keep_their_own_values_cursors(build: str, conn: AwsWrapperConnection,
                                                                       key_rows: Dict[str, int],
                                                                       kms_verifier: PayloadVerifier) -> None:
    name = f"it43-{build}"
    cursor_args = {} if is_pg() else {"buffered": True}  # two unread results at once on one MySQL connection
    with conn.cursor(**cursor_args) as first, conn.cursor(**cursor_args) as second:
        first.execute(_INSERT, (name, "pt-it43-ssn-1"))
        second.execute(f"INSERT INTO {_TABLE} (dob, name, ssn) VALUES (%s, %s, %s)",
                       ("pt-it43-dob-2", name, "pt-it43-ssn-2"))
        conn.commit()
        first.execute(_SELECT, (name,))
        second.execute(f"SELECT name, dob, ssn FROM {_TABLE} WHERE name = %s ORDER BY id", (name,))
        first_rows, second_rows = [tuple(r) for r in first.fetchall()], [tuple(r) for r in second.fetchall()]
    assert (first_rows, second_rows) == ([("pt-it43-ssn-1",), ("pt-it43-ssn-2",)],
                                         [(name, None, "pt-it43-ssn-1"), (name, "pt-it43-dob-2", "pt-it43-ssn-2")]), \
        "two cursors of one connection, executed before either fetches, each decode their own columns"
    assert _opened(kms_verifier, name) == ["pt-it43-ssn-1", "pt-it43-ssn-2"], "each cursor's own values were stored"


@enable_on_engines([DatabaseEngine.PG])
@kms_builds()
def test_interleaved_cursors_and_threads_keep_their_own_values_psycopg_threads(
        build: str, key_rows: Dict[str, int], kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
        kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
    # Threads on one psycopg connection, half reading another column list.
    wrong: List[str] = []
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as shared:
        shared.autocommit = True

        def work(n: int) -> None:
            try:
                with shared.cursor() as cur:
                    for i in range(5):
                        value = f"pt-it43-thread-{n}-{i}"
                        cur.execute(_INSERT, (f"it43-thread-{n}", value))
                        if n % 2:
                            cur.execute(f"{_SELECT} DESC LIMIT 1", (f"it43-thread-{n}",))
                            got = cur.fetchone()[0]
                        else:
                            cur.execute(f"SELECT name, ssn FROM {_TABLE} WHERE name = %s ORDER BY id DESC LIMIT 1",
                                        (f"it43-thread-{n}",))
                            got = cur.fetchone()[1]
                        if got != value:
                            wrong.append(f"thread {n} statement {i} read a {type(got).__name__}")
            except Exception as err:  # noqa: BLE001 - reported by class
                wrong.append(f"thread {n} raised {type(err).__name__}")

        threads = [threading.Thread(target=work, args=(n,), name=f"kms-it43-{n}") for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
    assert (wrong, [t.name for t in threads if t.is_alive()]) == ([], []), \
        "threads on one psycopg connection each encrypt their own parameters and decrypt their own rows"
    assert all(_opened(kms_verifier, f"it43-thread-{n}") == [f"pt-it43-thread-{n}-{i}" for i in range(5)]
               for n in range(8)), "each thread's values were stored as payloads"


@kms_builds()
def test_mysql_session_charsets_never_store_plaintext_decoding_off(
        build: str, key_rows: Dict[str, int], kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
        kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
    name = f"it44-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, "pt-it44-seed"))
        conn.commit()
    # Connection options that turn text decoding off, which the plugin's own metadata reads share.
    options: List[Dict[str, Any]] = [{"client_encoding": "SQL_ASCII"}] if is_pg() else \
        [{"use_unicode": False}, {"raw": True}]
    outcomes = {}
    for option in options:
        label = ",".join(f"{key}={value}" for key, value in option.items())
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, **option) as off:
            write = raised(off, lambda: off.cursor().execute(_INSERT, (f"{name}-off", "pt-it44-off")))
            off.commit()
            read = raised(off, lambda: read_column(off, _SELECT, name))
        outcomes[label] = (type(write).__name__, type(read).__name__)
    if is_pg():  # the plugin reads the metadata's algorithm name as bytes (the guide's SQL_ASCII note)
        assert outcomes == {"client_encoding=SQL_ASCII": ("InvalidAlgorithmError", "InvalidAlgorithmError")}, \
            "on a SQL_ASCII connection a write and a read raise InvalidAlgorithmError"
    else:
        assert [label for label, raised in outcomes.items() if "NoneType" in raised] == [], \
            f"with use_unicode=False or raw=True a write and a read raise instead of running: {outcomes}"
    assert kms_verifier.stored(_SELECT, (f"{name}-off",)) == [], "nothing is stored with text decoding off"


@enable_on_engines([DatabaseEngine.MYSQL])
@kms_builds()
def test_mysql_session_charsets_never_store_plaintext_swe7(build: str, swe7_table: str, kms_settings: KmsSettings,
                                                           kms_schema: str, kms_master_key_arn: str,
                                                           kms_app_user: DbUser,
                                                           kms_verifier: PayloadVerifier) -> None:
    name = f"it44-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        with conn.cursor() as cur:  # in the negotiated utf8mb4 session the plugin encrypts for the table
            cur.execute(f"INSERT INTO `{swe7_table}` (name, ssn) VALUES (%s, %s)", (f"{name}-utf8", "pt-it44-utf8"))
        conn.commit()
        # SQL as non-ASCII bytes runs unanalyzed on MySQL, so its plaintext reaches the trigger.
        with conn.cursor() as cur:
            as_bytes = raised(conn, lambda: cur.execute(
                f"INSERT INTO `{swe7_table}` (name, ssn) VALUES (%s, %s)".encode(), (f"{name}-bytes", "pt-it44-bytes")))
        with conn.cursor() as cur:
            cur.execute("SET NAMES swe7")
        # The server reads this name as the table's own; the plugin reads it as a table without encrypted columns.
        with conn.cursor() as cur:
            swe7 = raised(conn, lambda: cur.execute(f"INSERT INTO `{_SWE7_SPELLING}` (name, ssn) VALUES (%s, %s)",
                                                    (f"{name}-swe7", "pt-it44-swe7")))
    select = f"SELECT ssn FROM `{swe7_table}` WHERE name = %s"
    utf8_stored = [kms_verifier.decode(swe7_table, "ssn", payload)[1]
                   for payload in kms_verifier.stored(select, (f"{name}-utf8",))]
    assert utf8_stored == ["pt-it44-utf8"], "in the negotiated session the write to the table named with \u00c9 is encrypted"
    assert (isinstance(as_bytes, mysql.connector.Error), kms_verifier.stored(select, (f"{name}-bytes",))) == (True, []), \
        f"a write in non-ASCII bytes SQL goes unanalyzed and the trigger rejects its plaintext: {as_bytes!r}"
    assert (swe7 is not None, isinstance(swe7, EncryptionPluginError), kms_verifier.stored(select, (f"{name}-swe7",))) \
        == (True, False, []), \
        f"after SET NAMES swe7 the plaintext write fails in the driver or at the trigger and stores nothing: {swe7!r}"


@kms_builds("mysql-pure")
def test_connection_limit_needs_room_for_the_plugins_own_connection(build: str, key_rows: Dict[str, int],
                                                                    kms_settings: KmsSettings, kms_schema: str,
                                                                    kms_master_key_arn: str) -> None:
    database = TestEnvironment.get_current().get_database_info().get_default_db_name()
    outcomes = {}
    for limit in (1, 2):
        user = DbUser(f"kms_limit_{secrets.token_hex(4)}", secrets.token_hex(16))
        with plain_connect(admin=True) as admin, admin.cursor() as cur:
            if is_pg():
                cur.execute("SET log_statement = 'none'")
                cur.execute(f"CREATE ROLE {user.name} LOGIN PASSWORD '{user.password}' CONNECTION LIMIT {limit}")
                for statement in (f"GRANT USAGE ON SCHEMA {kms_schema} TO {user.name}",
                                  f"GRANT SELECT ON ALL TABLES IN SCHEMA {kms_schema} TO {user.name}",
                                  f"GRANT SELECT, INSERT ON {_TABLE} TO {user.name}",
                                  f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {user.name}"):
                    cur.execute(statement)
            else:
                cur.execute("SET SESSION sql_log_off = 1")
                cur.execute(f"CREATE USER %s IDENTIFIED BY %s WITH MAX_USER_CONNECTIONS {limit}",
                            (user.name, user.password))
                cur.execute(f"GRANT SELECT, INSERT ON {database}.{_TABLE} TO %s", (user.name,))
                cur.execute(f"GRANT SELECT ON {kms_schema}.* TO %s", (user.name,))
            admin.commit()
        try:
            with wrapper_connect(build, user, kms_settings, kms_schema, kms_master_key_arn) as limited:
                err = raised(limited, lambda: read_column(limited, _SELECT, "it45-nothing"))
            outcomes[limit] = None if err is None else type(err).__name__
        finally:
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                if is_pg():
                    cur.execute(f"DROP OWNED BY {user.name}")
                    cur.execute(f"DROP ROLE IF EXISTS {user.name}")
                else:
                    cur.execute("DROP USER IF EXISTS %s", (user.name,))
                admin.commit()
    assert outcomes == {1: "MetadataError", 2: None}, \
        "with room for one connection the first statement raises MetadataError, and with room for two it runs"


@kms_builds("mysql-pure")
def test_trigger_rejection_error_class_with_and_without_failover(build: str, guarded: None, kms_settings: KmsSettings,
                                                                 kms_schema: str, kms_master_key_arn: str,
                                                                 kms_app_user: DbUser) -> None:
    # Without a column list the plugin passes the write through with a warning, so the trigger sees the plaintext.
    unlisted = f"INSERT INTO {_GUARDED} VALUES (DEFAULT, %s, %s)"
    row = (f"it46-{build}", "pt-it46")
    driver_errors = (psycopg.Error, mysql.connector.Error)
    chains: Dict[str, Dict[str, Any]] = {
        "kms_encryption": {"plugins": "kms_encryption"},
        "failover_v2,kms_encryption": {"plugins": "failover_v2,kms_encryption", "auto_sort_wrapper_plugin_order": False},
        "failover_v2,kms_encryption preserving the driver's class": {
            "plugins": "failover_v2,kms_encryption", "auto_sort_wrapper_plugin_order": False,
            "preserve_driver_exception_type": True}}
    seen = {}
    for chain, props in chains.items():
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, **props) as conn:
            for call in ("execute", "executemany"):
                with conn.cursor() as cur:
                    err = raised(conn, (lambda: cur.execute(unlisted, row)) if call == "execute"
                                 else (lambda: cur.executemany(unlisted, [row])))
                if isinstance(err, driver_errors):
                    seen[(chain, call)] = "the driver's error"
                elif type(err) is AwsWrapperError and isinstance(err.__cause__, driver_errors):
                    seen[(chain, call)] = "AwsWrapperError from the driver's error"
                else:
                    seen[(chain, call)] = type(err).__name__
    assert seen == {
        ("kms_encryption", "execute"): "the driver's error",
        ("kms_encryption", "executemany"): "the driver's error",
        ("failover_v2,kms_encryption", "execute"): "AwsWrapperError from the driver's error",
        ("failover_v2,kms_encryption", "executemany"): "the driver's error",
        ("failover_v2,kms_encryption preserving the driver's class", "execute"): "the driver's error",
        ("failover_v2,kms_encryption preserving the driver's class", "executemany"): "the driver's error"}, \
        "a trigger rejection reaches the application as the driver's error, except from execute() under failover_v2, " \
        "which wraps it in AwsWrapperError unless preserve_driver_exception_type is on"


@kms_builds()
def test_plugin_lookups_use_their_own_autocommit_connection(build: str, monkeypatch: pytest.MonkeyPatch,
                                                            key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                            kms_schema: str, kms_master_key_arn: str,
                                                            kms_app_user: DbUser) -> None:
    name = f"it47-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, "pt-it47-1"))
        conn.commit()
    with plain_connect() as plain:
        key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(_TABLE, "ssn", kms_master_key_arn)
        plain.commit()
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, "pt-it47-2"))
        conn.commit()
    opened: List[Any] = []
    open_connection = PluginConnectionSource._open

    def counted_open(source: PluginConnectionSource) -> Any:
        opened.append(source)
        return open_connection(source)

    monkeypatch.setattr(PluginConnectionSource, "_open", counted_open)
    session = "SET search_path TO public" if is_pg() else "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, '')"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                         encryption_metadata_cache_enabled=False) as conn:
        with conn.cursor() as cur:
            cur.execute(session)  # a session setting as the first statement, inside the application's transaction
            before = len(opened)
            cur.execute(_SELECT, (name,))
            at_execute = len(opened) - before
            # The first row names the key row the rotation replaced, which the plugin looks up while the rest of
            # the result is still unread on the application's connection.
            first = cur.fetchone()[0]
            rest = [row[0] for row in cur.fetchall()]
            at_fetch = len(opened) - before - at_execute
        names_after = read_column(conn, f"SELECT name FROM {_TABLE} WHERE name = %s", name)
        conn.commit()
    assert [first, *rest] == ["pt-it47-1", "pt-it47-2"], \
        "a lookup during an unread result runs on the plugin's own connection, and the rows read back"
    assert (at_execute, at_fetch, names_after) == (1, 1, [name, name]), \
        "with the metadata cache off, the execute and the fetch each open one plugin connection, and the " \
        "application's transaction goes on after the session setting and the lookups"


@kms_builds("mysql-cext")
def test_release_resources_leaves_open_connections_working(build: str, conn: AwsWrapperConnection,
                                                           key_rows: Dict[str, int]) -> None:
    name = f"it48-{build}"
    with conn.cursor() as cur:
        cur.execute(_INSERT, (name, "pt-it48-ssn"))  # loads the metadata and ssn's data key, not dob's
    conn.commit()
    aws_advanced_python_wrapper.release_resources()
    with conn.cursor() as cur:
        cur.execute(_INSERT, (name, "pt-it48-ssn-after"))
        cur.execute(f"INSERT INTO {_TABLE} (name, dob) VALUES (%s, %s)", (name, "pt-it48-dob"))
    conn.commit()
    assert (read_column(conn, _SELECT, name), read_column(conn, f"SELECT dob FROM {_TABLE} WHERE name = %s ORDER BY id", name)) == \
        (["pt-it48-ssn", "pt-it48-ssn-after", None], [None, None, "pt-it48-dob"]), \
        "after release_resources() an open connection still writes and reads a loaded column and a column whose " \
        "key it hadn't loaded"


@kms_builds()
def test_socket_timeout_hands_the_driver_the_encrypted_copy(build: str, key_rows: Dict[str, int],
                                                            monkeypatch: pytest.MonkeyPatch,
                                                            kms_settings: KmsSettings, kms_schema: str,
                                                            kms_master_key_arn: str, kms_app_user: DbUser,
                                                            kms_verifier: PayloadVerifier) -> None:
    name = f"socket-{build}"
    on_main: List[bool] = []
    # With socket_timeout the wrapper runs the driver call on a worker thread.
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, socket_timeout=5) as conn:
        with conn.target_connection.cursor() as probe:
            driver_cursor = type(probe)
        original = driver_cursor.execute

        def recording(self: Any, *args: Any, **kwargs: Any) -> Any:
            on_main.append(threading.current_thread() is threading.main_thread())
            return original(self, *args, **kwargs)
        monkeypatch.setattr(driver_cursor, "execute", recording)
        write(conn, _INSERT, (name, "pt-socket"))
        got = read_column(conn, _SELECT, name)
    assert False in on_main, "with socket_timeout the driver's execute runs on a worker thread"
    assert _opened(kms_verifier, name) == ["pt-socket"], "the worker thread gets the encrypted copy of the parameters"
    assert got == ["pt-socket"], "the row read on the worker thread is decrypted"
