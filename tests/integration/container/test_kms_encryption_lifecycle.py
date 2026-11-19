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

import secrets
import threading
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

import mysql.connector
import pytest

import aws_advanced_python_wrapper
from aws_advanced_python_wrapper.encryption.sql_analyzer import AnalysisGap
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                EncryptionPluginError,
                                                InvalidAlgorithmError,
                                                MetadataError)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from .utils.conditions import enable_on_engines, enable_on_features
from .utils.connection_utils import ConnectionUtils
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (DB_ERRORS, DbUser,
                                          KmsEncryptionTestBase, binary_type,
                                          class_name, create_column_trigger,
                                          drop_user, encrypt_column,
                                          encrypted_table, error_chain,
                                          install_trigger_sql, is_pg,
                                          key_management_utility, kms_builds,
                                          plain_connect, raised, read_column,
                                          redact, statement_log_lines,
                                          wrapper_connect, write)
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import KMS_THREAD_JOIN_TIMEOUT_SEC

if TYPE_CHECKING:
    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import KmsSettings, PayloadVerifier

_TABLE = "kms_lifecycle_users"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
# A table whose encrypted column has the package's trigger, which refuses plaintext.
_GUARDED = "kms_lifecycle_guarded"
# A MySQL table named with É, which the swe7 character set writes as @.
_SWE7_TABLE, _SWE7_SPELLING = "kms_lifecycle_É", "kms_lifecycle_@"
_FAIL_ON_UNMAPPED = WrapperProperties.ENCRYPTION_FAIL_ON_UNMAPPED_PARAMETER.name
_BYTES_COLUMN_NAMES_DEFECT = ("With use_unicode=False the C extension returns column names as bytes, which "
                              "DriverDialect.fetch_rows keys each row by, so the plugin's metadata read raises a bare "
                              "KeyError instead of InvalidAlgorithmError.")


def _opened(verifier: PayloadVerifier, name: str) -> List[Any]:
    """The values of the rows named ``name``, opened without the plugin."""
    return [opened[1] for opened in verifier.opened(_TABLE, "ssn", name) if opened is not None]


def _driver_error_chain(err: Optional[Exception]) -> str:
    """As ``error_chain``, with any driver error named "the driver's error"."""
    if isinstance(err, DB_ERRORS):
        return "the driver's error"
    if type(err) is AwsWrapperError and isinstance(err.__cause__, DB_ERRORS):
        return "AwsWrapperError from the driver's error"
    return type(err).__name__


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionLifecycle(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def key_rows(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                 kms_app_user: DbUser) -> Iterator[Dict[str, int]]:
        """The class's table, with ``ssn`` and ``dob`` encrypted under key rows of their own; yields their ids."""
        with encrypted_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}, dob {binary_type()}", ("ssn", "dob"),
                             kms_settings, kms_schema, kms_master_key_arn) as key_ids:
            yield key_ids

    @pytest.fixture(scope="class")
    def guarded(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                kms_app_user: DbUser) -> Iterator[None]:
        with encrypted_table(_GUARDED, f"name VARCHAR(100), ssn {binary_type()}", ("ssn",), kms_settings, kms_schema,
                             kms_master_key_arn, triggered=("ssn",)):
            yield

    @pytest.fixture(scope="class")
    def swe7_table(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                   kms_app_user: DbUser) -> Iterator[str]:
        """A MySQL table named with É whose encrypted column has the package's trigger."""
        try:
            with plain_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS `{_SWE7_TABLE}`")
                    cur.execute(f"CREATE TABLE `{_SWE7_TABLE}` (id INT AUTO_INCREMENT PRIMARY KEY, "
                                "name VARCHAR(100), ssn VARBINARY(1024))")
                conn.commit()
                encrypt_column(conn, kms_settings, kms_schema, _SWE7_TABLE, "ssn", kms_master_key_arn)
                install_trigger_sql(conn, kms_schema)
                create_column_trigger(conn, _SWE7_TABLE, "ssn")
            yield _SWE7_TABLE
        finally:
            with plain_connect() as conn, conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{_SWE7_TABLE}`")
                conn.commit()

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_psycopg_pipeline_reads_decrypt(self, build: str, key_rows: Dict[str, int], kms_settings: KmsSettings,
                                            kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                            kms_verifier: PayloadVerifier, kms_statement_log: str) -> None:
        reads: Dict[Any, List[Any]] = {}
        stored: Dict[Any, List[Any]] = {}
        expected: Dict[Any, List[Any]] = {}
        pids: List[int] = []
        for metadata in ("loaded", "loaded in the pipeline"):
            for read in ("fetchone", "fetchmany", "fetchall", "for"):
                name = f"pipeline-{len(reads)}"
                values = [f"pt-pipeline-{len(reads)}-{i}" for i in range(3)]
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                    pids.append(conn.target_connection.info.backend_pid)
                    if metadata == "loaded":
                        # The connection's first statement loads the metadata.
                        read_column(conn, _SELECT, "pipeline-nothing")
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
                stored[(metadata, read)] = _opened(kms_verifier, name)
                expected[(metadata, read)] = values
        lines = statement_log_lines(kms_statement_log)
        lookups = [line for line in lines if f"{kms_schema}.".encode() in line]
        in_pipeline = []
        for line in lookups:
            if any(f"[{pid}]".encode() in line for pid in pids):
                in_pipeline.append(line)
        own = []
        for line in lines:
            if _INSERT.split(" (")[0].encode() in line and any(f"[{pid}]".encode() in line for pid in pids):
                own.append(line)
        assert reads == expected
        assert stored == expected
        # The pid match finds the application's own INSERTs, so the log lines carry the pid.
        assert len(own) > 0
        # The plugin reads its metadata on its own connection, never in the application's pipeline session.
        assert len(lookups) > 0
        assert in_pipeline == []

    @kms_builds()
    def test_interleaved_cursors_keep_own_values(self, build: str, conn: AwsWrapperConnection,
                                                 key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        name = f"cursors-{build}"
        # Two unread results at once on one MySQL connection need buffered cursors.
        cursor_args = {} if is_pg() else {"buffered": True}
        with conn.cursor(**cursor_args) as first, conn.cursor(**cursor_args) as second:
            first.execute(_INSERT, (name, "pt-cursors-ssn-1"))
            second.execute(f"INSERT INTO {_TABLE} (dob, name, ssn) VALUES (%s, %s, %s)",
                           ("pt-cursors-dob-2", name, "pt-cursors-ssn-2"))
            conn.commit()
            first.execute(_SELECT, (name,))
            second.execute(f"SELECT name, dob, ssn FROM {_TABLE} WHERE name = %s ORDER BY id", (name,))
            first_rows = [tuple(row) for row in first.fetchall()]
            second_rows = [tuple(row) for row in second.fetchall()]
        assert first_rows == [("pt-cursors-ssn-1",), ("pt-cursors-ssn-2",)]
        assert second_rows == [(name, None, "pt-cursors-ssn-1"), (name, "pt-cursors-dob-2", "pt-cursors-ssn-2")]
        assert _opened(kms_verifier, name) == ["pt-cursors-ssn-1", "pt-cursors-ssn-2"]

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_psycopg_threads_keep_own_values(self, build: str, key_rows: Dict[str, int], kms_settings: KmsSettings,
                                             kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                             kms_verifier: PayloadVerifier) -> None:
        # Threads on one psycopg connection, half reading another column list.
        wrong: List[str] = []
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as shared:
            shared.autocommit = True

            def work(n: int) -> None:
                try:
                    with shared.cursor() as cur:
                        for i in range(5):
                            value = f"pt-thread-{n}-{i}"
                            cur.execute(_INSERT, (f"thread-{n}", value))
                            if n % 2:
                                cur.execute(f"{_SELECT} DESC LIMIT 1", (f"thread-{n}",))
                                got = cur.fetchone()[0]
                            else:
                                cur.execute(f"SELECT name, ssn FROM {_TABLE} WHERE name = %s ORDER BY id DESC LIMIT 1",
                                            (f"thread-{n}",))
                                got = cur.fetchone()[1]
                            if got != value:
                                wrong.append(f"thread {n} statement {i} read a {type(got).__name__}")
                except Exception as err:  # noqa: BLE001 - reported by class
                    wrong.append(f"thread {n} raised {type(err).__name__}")

            threads = [threading.Thread(target=work, args=(n,), name=f"kms-thread-{n}") for n in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(KMS_THREAD_JOIN_TIMEOUT_SEC)
        assert wrong == []
        assert [thread.name for thread in threads if thread.is_alive()] == []
        for n in range(8):
            assert _opened(kms_verifier, f"thread-{n}") == [f"pt-thread-{n}-{i}" for i in range(5)]

    @kms_builds()
    def test_text_decoding_off_raises(self, build: str, request: pytest.FixtureRequest, key_rows: Dict[str, int],
                                      kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                      kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
        if build == "mysql-cext":
            request.applymarker(pytest.mark.xfail(strict=True, raises=KeyError, reason=_BYTES_COLUMN_NAMES_DEFECT))
        name = f"decoding-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            with conn.cursor() as cur:
                cur.execute(_INSERT, (name, "pt-decoding-seed"))
            conn.commit()
        # Connection options that turn text decoding off, which the plugin's own metadata reads share. raw=True comes
        # first, so the C extension checks it before it meets the KeyError defect.
        if is_pg():
            options: List[Dict[str, Any]] = [{"client_encoding": "SQL_ASCII"}]
        else:
            options = [{"raw": True}, {"use_unicode": False}]
        for option in options:
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, **option) as off:
                # The plugin reads the metadata's algorithm name as bytes (the guide's SQL_ASCII note), on write and
                # on read; an error of another class goes past pytest.raises.
                with off.cursor() as cur:
                    with pytest.raises(InvalidAlgorithmError):
                        cur.execute(_INSERT, (f"{name}-off", "pt-decoding-off"))
                    off.rollback()
                with pytest.raises(InvalidAlgorithmError):
                    read_column(off, _SELECT, name)
                off.rollback()
        assert kms_verifier.stored(_SELECT, (f"{name}-off",)) == []

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    def test_mysql_swe7_session_stores_no_plaintext(self, build: str, swe7_table: str, kms_settings: KmsSettings,
                                                    kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                    kms_verifier: PayloadVerifier) -> None:
        name = f"swe7-{build}"
        insert = f"INSERT INTO `{swe7_table}` (name, ssn) VALUES (%s, %s)"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            # In the negotiated utf8mb4 session the plugin encrypts for the table.
            with conn.cursor() as cur:
                cur.execute(insert, (f"{name}-utf8", "pt-swe7-utf8"))
            conn.commit()
            # SQL as non-ASCII bytes runs unanalyzed on MySQL, so its plaintext reaches the trigger.
            with conn.cursor() as cur:
                as_bytes = raised(conn, lambda: cur.execute(insert.encode(), (f"{name}-bytes", "pt-swe7-bytes")))
            with conn.cursor() as cur:
                cur.execute("SET NAMES swe7")
            # The server reads this name as the table's own; the plugin reads it as a table without encrypted columns.
            with conn.cursor() as cur:
                swe7 = raised(conn, lambda: cur.execute(f"INSERT INTO `{_SWE7_SPELLING}` (name, ssn) VALUES (%s, %s)",
                                                        (f"{name}-swe7", "pt-swe7-swe7")))
        select = f"SELECT ssn FROM `{swe7_table}` WHERE name = %s"
        utf8_stored = [kms_verifier.decode(swe7_table, "ssn", payload)[1]
                       for payload in kms_verifier.stored(select, (f"{name}-utf8",))]
        assert utf8_stored == ["pt-swe7-utf8"]
        # The trigger rejects the plaintext of the unanalyzed bytes SQL.
        assert isinstance(as_bytes, mysql.connector.Error)
        assert kms_verifier.stored(select, (f"{name}-bytes",)) == []
        # After SET NAMES swe7 the plaintext write fails in the driver or at the trigger, not in the plugin.
        assert swe7 is not None
        assert not isinstance(swe7, EncryptionPluginError)
        assert kms_verifier.stored(select, (f"{name}-swe7",)) == []

    @kms_builds(exclude=("mysql-pure",))
    def test_connection_limit_without_room_raises_metadata_error(self, build: str, key_rows: Dict[str, int],
                                                                 kms_settings: KmsSettings, kms_schema: str,
                                                                 kms_master_key_arn: str) -> None:
        database = ConnectionUtils().dbname
        written = f"limit-write-{build}"
        outcomes = {}
        for limit in (1, 2):
            user = DbUser(f"kms_limit_{secrets.token_hex(4)}", secrets.token_hex(16))
            redact(user.password)
            try:
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
                with wrapper_connect(build, user, kms_settings, kms_schema, kms_master_key_arn) as limited:
                    outcomes[limit] = class_name(raised(limited, lambda: read_column(limited, _SELECT, "nothing")))
                    if limit == 1:
                        # The plugin can't read the metadata, so it can't tell whether ssn needs encrypting.
                        write_outcome = class_name(
                            raised(limited, lambda: write(limited, _INSERT, (written, "pt-limit-write"))))
            finally:
                drop_user(user.name)
        with plain_connect() as plain, plain.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {_TABLE} WHERE name = %s", (written,))
            (stored,) = cur.fetchone()
        # Room for one connection: the first statement raises; room for two: it runs.
        assert outcomes == {1: "MetadataError", 2: None}
        assert write_outcome == "MetadataError"
        assert stored == 0

    @kms_builds(exclude=("mysql-pure",))
    def test_trigger_rejection_error_class_with_failover(self, build: str, guarded: None, kms_settings: KmsSettings,
                                                         kms_schema: str, kms_master_key_arn: str,
                                                         kms_app_user: DbUser) -> None:
        # Without a column list the plugin refuses the write by default. With encryption_fail_on_unmapped_parameter
        # off it passes the write through with a warning, so the trigger sees the plaintext.
        unlisted = f"INSERT INTO {_GUARDED} VALUES (DEFAULT, %s, %s)"
        row = (f"rejection-{build}", "pt-rejection")
        chains: Dict[str, Dict[str, Any]] = {
            "kms_encryption": {"plugins": "kms_encryption"},
            "failover_v2,kms_encryption": {"plugins": "failover_v2,kms_encryption",
                                           "auto_sort_wrapper_plugin_order": False},
            "failover_v2,kms_encryption preserving the driver's class": {
                "plugins": "failover_v2,kms_encryption", "auto_sort_wrapper_plugin_order": False,
                "preserve_driver_exception_type": True}}
        refusals = {}
        rejected = {}
        for chain, props in chains.items():
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, **props) as conn:
                with conn.cursor() as cur:
                    refusals[(chain, "execute")] = raised(conn, lambda: cur.execute(unlisted, row))
                with conn.cursor() as cur:
                    refusals[(chain, "executemany")] = raised(conn, lambda: cur.executemany(unlisted, [row]))
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                 encryption_fail_on_unmapped_parameter=False, **props) as conn:
                with conn.cursor() as cur:
                    rejected[(chain, "execute")] = _driver_error_chain(raised(conn, lambda: cur.execute(unlisted, row)))
                with conn.cursor() as cur:
                    rejected[(chain, "executemany")] = _driver_error_chain(
                        raised(conn, lambda: cur.executemany(unlisted, [row])))
        with plain_connect() as plain, plain.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {_GUARDED} WHERE name = %s", (row[0],))
            (stored,) = cur.fetchone()
        refused = {key: error_chain(err) for key, err in refusals.items()}
        messages = {key: str(err if type(err) is MetadataError else getattr(err, "__cause__", None))
                    for key, err in refusals.items()}
        # By default the plugin refuses the unlisted INSERT with MetadataError, which failover_v2's execute() wraps in
        # AwsWrapperError unless preserve_driver_exception_type is on.
        assert refused == {
            ("kms_encryption", "execute"): "MetadataError",
            ("kms_encryption", "executemany"): "MetadataError",
            ("failover_v2,kms_encryption", "execute"): "AwsWrapperError from MetadataError",
            ("failover_v2,kms_encryption", "executemany"): "MetadataError",
            ("failover_v2,kms_encryption preserving the driver's class", "execute"): "MetadataError",
            ("failover_v2,kms_encryption preserving the driver's class", "executemany"): "MetadataError"}
        # Each refusal names the setting that turns it off and the reason.
        assert [key for key, message in messages.items() if _FAIL_ON_UNMAPPED not in message] == []
        assert [key for key, message in messages.items() if AnalysisGap.NO_COLUMN_LIST.value not in message] == []
        assert stored == 0
        # A trigger rejection reaches the application as the driver's error, except from execute() under
        # failover_v2, which wraps it in AwsWrapperError unless preserve_driver_exception_type is on.
        assert rejected == {
            ("kms_encryption", "execute"): "the driver's error",
            ("kms_encryption", "executemany"): "the driver's error",
            ("failover_v2,kms_encryption", "execute"): "AwsWrapperError from the driver's error",
            ("failover_v2,kms_encryption", "executemany"): "the driver's error",
            ("failover_v2,kms_encryption preserving the driver's class", "execute"): "the driver's error",
            ("failover_v2,kms_encryption preserving the driver's class", "executemany"): "the driver's error"}

    @kms_builds()
    def test_plugin_lookups_use_own_connection(self, build: str, key_rows: Dict[str, int],
                                               kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                               kms_app_user: DbUser, plugin_connection_opens: List[Any]) -> None:
        name = f"lookup-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            with conn.cursor() as cur:
                cur.execute(_INSERT, (name, "pt-lookup-1"))
            conn.commit()
        with plain_connect() as plain:
            key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(_TABLE, "ssn", kms_master_key_arn)
            plain.commit()
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            with conn.cursor() as cur:
                cur.execute(_INSERT, (name, "pt-lookup-2"))
            conn.commit()
        plugin_connection_opens.clear()
        if is_pg():
            session = "SET search_path TO public"
        else:
            session = "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, '')"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                             encryption_metadata_cache_enabled=False) as conn:
            with conn.cursor() as cur:
                # A session setting as the first statement, inside the application's transaction.
                cur.execute(session)
                before = len(plugin_connection_opens)
                cur.execute(_SELECT, (name,))
                at_execute = len(plugin_connection_opens) - before
                # The first row names the key row the rotation replaced, which the plugin looks up while the rest of
                # the result is still unread on the application's connection.
                first = cur.fetchone()[0]
                rest = [row[0] for row in cur.fetchall()]
                at_fetch = len(plugin_connection_opens) - before - at_execute
            names_after = read_column(conn, f"SELECT name FROM {_TABLE} WHERE name = %s", name)
            conn.commit()
        assert [first, *rest] == ["pt-lookup-1", "pt-lookup-2"]
        # With the metadata cache off, the execute and the fetch each open one plugin connection.
        assert at_execute == 1
        assert at_fetch == 1
        # The application's transaction goes on after the session setting and the lookups.
        assert names_after == [name, name]

    @kms_builds(exclude=("mysql-pure",))
    def test_release_resources_keeps_open_connections_working(self, build: str, conn: AwsWrapperConnection,
                                                              key_rows: Dict[str, int]) -> None:
        name = f"release-{build}"
        with conn.cursor() as cur:
            # Loads the metadata and ssn's data key, not dob's.
            cur.execute(_INSERT, (name, "pt-release-ssn"))
        conn.commit()
        aws_advanced_python_wrapper.release_resources()
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, "pt-release-ssn-after"))
            cur.execute(f"INSERT INTO {_TABLE} (name, dob) VALUES (%s, %s)", (name, "pt-release-dob"))
        conn.commit()
        assert read_column(conn, _SELECT, name) == ["pt-release-ssn", "pt-release-ssn-after", None]
        dob = read_column(conn, f"SELECT dob FROM {_TABLE} WHERE name = %s ORDER BY id", name)
        assert dob == [None, None, "pt-release-dob"]

    @kms_builds()
    def test_socket_timeout_worker_gets_encrypted_parameters(self, build: str, key_rows: Dict[str, int],
                                                             monkeypatch: pytest.MonkeyPatch,
                                                             kms_settings: KmsSettings, kms_schema: str,
                                                             kms_master_key_arn: str, kms_app_user: DbUser,
                                                             kms_verifier: PayloadVerifier) -> None:
        name = f"socket-{build}"
        calls: List[Tuple[bool, Any, Any]] = []
        # With socket_timeout the wrapper runs the driver call on a worker thread.
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                             socket_timeout=5) as conn:
            with conn.target_connection.cursor() as probe:
                driver_cursor = type(probe)
            original = driver_cursor.execute

            def recording(self: Any, *args: Any, **kwargs: Any) -> Any:
                # The plugin's own lookups run here too, so each call keeps its SQL and parameters.
                sql = args[0] if args else None
                params = args[1] if len(args) > 1 else None
                calls.append((threading.current_thread() is threading.main_thread(), sql, params))
                return original(self, *args, **kwargs)

            monkeypatch.setattr(driver_cursor, "execute", recording)
            write(conn, _INSERT, (name, "pt-socket"))
            got = read_column(conn, _SELECT, name)
        inserts = [(on_main, params) for on_main, sql, params in calls if str(sql).startswith(f"INSERT INTO {_TABLE}")]
        # The application's INSERT ran on the worker thread, with the encrypted value as its ssn parameter.
        assert [on_main for on_main, _ in inserts] == [False]
        assert [isinstance(params[1], (bytes, bytearray)) for _, params in inserts] == [True]
        assert _opened(kms_verifier, name) == ["pt-socket"]
        assert got == ["pt-socket"]
