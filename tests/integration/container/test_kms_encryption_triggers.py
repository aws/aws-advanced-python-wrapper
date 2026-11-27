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
import hashlib
import hmac
import os
import re
import secrets
import uuid
from typing import Any, Callable, Dict, Iterator, List, NamedTuple

# The C extension's own module, imported so that a host without it fails here, as the mysql-cext build does.
import _mysql_connector  # type: ignore[import-not-found]
import mysql.connector
import psycopg
import pytest

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.sql_analyzer import AnalysisGap
from aws_advanced_python_wrapper.errors import MetadataError
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from .utils.conditions import enable_on_engines, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (TRIGGER_REJECTION,
                                          KmsEncryptionTestBase, KmsSettings,
                                          PayloadVerifier, assert_rejected,
                                          binary_type, create_column_trigger,
                                          encrypt_column, install_trigger_sql,
                                          is_pg, key_management_utility,
                                          kms_builds, kms_client,
                                          plain_connect, raised, read_column,
                                          write, writes)
from .utils.retry_helper import retry_until
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import (KMS_METADATA_REFRESH_POLL_SEC,
                                 KMS_METADATA_REFRESH_WAIT_SEC)

# The trigger's other rejection (TRIGGER_REJECTION is the first). PostgreSQL's messages go on to name the table and
# column; MySQL's name neither.
_NO_KEY = "No HMAC key is configured"
# MySQL's error for a routine created by a user without SUPER while the server writes a binary log.
_ER_BINLOG_CREATE_ROUTINE_NEED_SUPER = 1419
# The routines trigger_mysql.sql creates.
_MYSQL_ROUTINE_NAMES = ("hmac_sha256", "validate_encrypted_data_hmac_before_insert",
                        "validate_encrypted_data_hmac_before_update", "verify_encrypted_data_hmac")
# A session user variable (@name, also quoted as @'name', @"name" or @`name`, not a system variable's @@name), which
# the writing session could set to steer a routine.
_SESSION_VARIABLE = re.compile(r"(?<!@)@(?!@)")
_SQL_COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)


class EncryptedTable(NamedTuple):
    """A table whose ``ssn`` and ``card`` columns are encrypted, each with its own key row, and triggered."""
    name: str
    ssn_key_id: int
    card_key_id: int

    def insert(self, column: str = "ssn") -> str:
        return f"INSERT INTO {self.name} (name, {column}) VALUES (%s, %s)"


def _plain_write(build: str, sql: str, params: Any = None) -> None:
    with plain_connect(build=build) as conn:
        write(conn, sql, params)


def _stored(verifier: PayloadVerifier, table: str, column: str, name: str) -> List[Any]:
    return verifier.stored(f"SELECT {column} FROM {table} WHERE name = %s ORDER BY id", (name,))


def _session_id(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()" if is_pg() else "SELECT CONNECTION_ID()")
        return cur.fetchone()[0]


def _drop_column_trigger(conn: Any, table: str, column: str) -> None:
    if is_pg():
        statements = [f"DROP TRIGGER {table}_{column}_encrypted ON {table}"]
    else:
        statements = [f"DROP TRIGGER {table}_{column}_hmac_insert", f"DROP TRIGGER {table}_{column}_hmac_update"]
    _run(conn, *statements)


def _run(conn: Any, *statements: str) -> None:
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionTriggers(KmsEncryptionTestBase):

    @pytest.fixture
    def table(self, kms_trigger_installed: None, kms_settings: KmsSettings, kms_schema: str,
              kms_master_key_arn: str) -> Iterator[EncryptedTable]:
        """A new table: ``ssn`` and ``card`` triggered and encrypted, ``pin`` binary and plain, ``note`` text."""
        name = f"kms_trg_{secrets.token_hex(4)}"
        id_type = "SERIAL PRIMARY KEY" if is_pg() else "INT AUTO_INCREMENT PRIMARY KEY"
        binary = binary_type()
        try:
            with plain_connect() as conn:
                _run(conn, f"CREATE TABLE {name} (id {id_type}, name VARCHAR(100), ssn {binary}, card {binary}, "
                           f"pin {binary}, note VARCHAR(100))")
                for column in ("ssn", "card"):
                    create_column_trigger(conn, name, column)
                ssn_key_id = encrypt_column(conn, kms_settings, kms_schema, name, "ssn", kms_master_key_arn)
                card_key_id = encrypt_column(conn, kms_settings, kms_schema, name, "card", kms_master_key_arn)
            yield EncryptedTable(name, ssn_key_id, card_key_id)
        finally:
            with plain_connect() as conn:
                _run(conn, f"DROP TABLE IF EXISTS {name}")

    @kms_builds()
    def test_trigger_after_rotation(self, build: str, table: EncryptedTable, kms_connect: Callable[..., Any],
                                    kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                    kms_verifier: PayloadVerifier) -> None:
        with kms_connect() as stale, kms_connect(encryption_metadata_cache_refresh_interval_sec=1) as refreshing:
            write(stale, table.insert(), ("before", "pt-rot-before"))
            write(refreshing, table.insert(), ("refreshing-before", "pt-rot-refreshing"))
            (old_payload,) = _stored(kms_verifier, table.name, "ssn", "before")
            with plain_connect() as admin:
                new_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                    table.name, "ssn", kms_master_key_arn)
                admin.commit()
            assert new_key_id != table.ssn_key_id

            assert_rejected(lambda: write(stale, table.insert(), ("stale", "pt-rot-stale")), TRIGGER_REJECTION, stale)
            # An UPDATE of another column on a row written before the rotation is accepted and keeps its value.
            write(stale, f"UPDATE {table.name} SET note = %s WHERE name = %s", ("touched", "before"))
            assert _stored(kms_verifier, table.name, "ssn", "before") == [old_payload]

            # A connection with a short refresh interval writes with the new key after its next refresh.
            assert retry_until(lambda: writes(refreshing, table.insert(), ("refreshed", "pt-rot-after")),
                               timeout_sec=KMS_METADATA_REFRESH_WAIT_SEC, delay_sec=KMS_METADATA_REFRESH_POLL_SEC)
            (new_payload,) = _stored(kms_verifier, table.name, "ssn", "refreshed")
            assert kms_verifier.decode(table.name, "ssn", new_payload) == (new_key_id, "pt-rot-after")
            with refreshing.cursor() as cur:
                cur.execute(f"SELECT ssn FROM {table.name} WHERE name = %s", ("before",))
                assert cur.fetchall() == [("pt-rot-before",)]

    @kms_builds()
    def test_writes_outside_plugin_reach_trigger(self, build: str, table: EncryptedTable,
                                                 kms_connect: Callable[..., Any],
                                                 kms_verifier: PayloadVerifier) -> None:
        no_list = f"INSERT INTO {table.name} VALUES ({'DEFAULT' if is_pg() else 'NULL'}, %s, %s, NULL, NULL, NULL)"
        copy_sql = f"COPY {table.name} (name, ssn) FROM STDIN"
        with kms_connect() as conn, conn.cursor() as cur:
            # By default the plugin refuses both before anything is sent: it can't pair the INSERT's parameters with
            # their columns, and a COPY's rows would reach the server as written.
            refused = [raised(conn, lambda: cur.execute(no_list, ("no-list-default", b"pt-unseen-default")))]
            if is_pg():
                refused.append(raised(conn, lambda: cur.copy(copy_sql)))
        assert [type(err) for err in refused] == [MetadataError] * len(refused)
        # The INSERT's refusal names the setting that turns it off and the reason.
        assert WrapperProperties.ENCRYPTION_FAIL_ON_UNMAPPED_PARAMETER.name in str(refused[0])
        assert AnalysisGap.NO_COLUMN_LIST.value in str(refused[0])
        if is_pg():
            # The COPY's refusal names the encrypted column it writes.
            assert (getattr(refused[1], "table_name", None), getattr(refused[1], "column_name", None)) == \
                (table.name, "ssn")
        # With the refusal off the INSERT runs unchanged, and the driver's own cursor runs the COPY, so each write
        # below reaches the trigger.
        with kms_connect(encryption_fail_on_unmapped_parameter=False) as conn:
            write(conn, table.insert(), ("seed", "pt-seen-seed"))
            with conn.cursor() as cur:
                assert_rejected(lambda: cur.execute(no_list, ("no-list", b"pt-unseen-no-list")), TRIGGER_REJECTION,
                                conn)
                if is_pg():
                    def copy_in() -> None:
                        with cur.target_cursor.copy(copy_sql) as copy:
                            copy.write_row(("copy", b"pt-unseen-copy"))

                    assert_rejected(copy_in, TRIGGER_REJECTION, conn)
                    assert_rejected(lambda: list(cur.stream(table.insert(), ("stream", b"pt-unseen-stream"))),
                                    TRIGGER_REJECTION, conn)
                    conn.target_connection.cursor_factory = psycopg.RawCursor
                    try:
                        raw = conn.cursor()
                    finally:
                        conn.target_connection.cursor_factory = psycopg.Cursor
                    with raw:
                        assert_rejected(lambda: raw.execute(
                            f"INSERT INTO {table.name} VALUES (DEFAULT, $1, $2, NULL, NULL, NULL)",
                            ("raw-no-list", b"pt-unseen-raw")), TRIGGER_REJECTION, conn)
                    cur.execute(f"PREPARE kms_insert (text, bytea) AS INSERT INTO {table.name} (name, ssn) "
                                "VALUES ($1, $2)")
                    assert_rejected(lambda: cur.execute("EXECUTE kms_insert ('prepared', 'pt-unseen-prepared')"),
                                    TRIGGER_REJECTION, conn)
                else:
                    with conn.cursor(prepared=True) as prepared:
                        assert_rejected(lambda: prepared.execute(
                            f"INSERT INTO {table.name} VALUES (NULL, ?, ?, NULL, NULL, NULL)",
                            ("prepared-no-list", b"pt-unseen-prepared")), TRIGGER_REJECTION, conn)
                    cur.execute(f"PREPARE kms_insert FROM 'INSERT INTO {table.name} (name, ssn) "
                                f"VALUES (''sql-prepare'', ''pt-unseen-sql'')'")
                    assert_rejected(lambda: cur.execute("EXECUTE kms_insert"), TRIGGER_REJECTION, conn)
                    assert_rejected(lambda: cur.execute(f"INSERT INTO {table.name} (name, ssn) VALUES ('bytes-é', %s)"
                                                        .encode("utf-8"), (b"pt-unseen-bytes",)), TRIGGER_REJECTION,
                                    conn)
        # No write the plugin can't see or doesn't analyze stored its plaintext.
        assert kms_verifier.stored(f"SELECT name FROM {table.name} ORDER BY id") == ["seed"]

    @kms_builds()
    def test_packaged_trigger_rejects_plaintext(self, build: str, table: EncryptedTable,
                                                kms_connect: Callable[..., Any], kms_schema: str,
                                                kms_verifier: PayloadVerifier) -> None:
        with plain_connect() as conn, conn.cursor() as cur:
            if is_pg():
                cur.execute("SELECT obj_description('enforce_encrypted_column'::regproc, 'pg_proc')")
            else:
                cur.execute("SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() "
                            "AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert'")
            (stamp,) = cur.fetchone()
            if not is_pg():
                cur.execute("SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM information_schema.ROUTINES "
                            "WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME IN (%s, %s, %s, %s)",
                            _MYSQL_ROUTINE_NAMES)
                routines: Dict[str, str] = dict(cur.fetchall())
                # All four routines are installed, and none reads a session user variable.
                assert sorted(routines) == sorted(_MYSQL_ROUTINE_NAMES)
                variables = {name: _SESSION_VARIABLE.findall(_SQL_COMMENT.sub("", body)) for name, body in routines.items()}
                assert variables == dict.fromkeys(routines, [])
        # The trigger SQL installed from the package records its version.
        assert stamp.startswith("aws-advanced-python-wrapper kms_encryption trigger v")

        with kms_connect() as conn:
            write(conn, f"INSERT INTO {table.name} (name, ssn, card) VALUES (%s, %s, %s)",
                  ("good", "pt-trg-ssn", "pt-trg-card"))
        _plain_write(build, table.insert(), ("null", None))
        (good_ssn,) = _stored(kms_verifier, table.name, "ssn", "good")
        (good_card,) = _stored(kms_verifier, table.name, "card", "good")
        (hmac_key,) = kms_verifier.stored(f"SELECT hmac_key FROM {kms_schema}.key_storage WHERE id = %s",
                                          (table.ssn_key_id,))
        good_ssn, good_card = bytes(good_ssn), bytes(good_card)
        rejected = {"plaintext": b"pt-trg-plaintext-" + b"x" * 64,
                    "short": os.urandom(64),
                    "random": os.urandom(80),
                    "tag-flipped": bytes([good_ssn[0] ^ 1]) + good_ssn[1:],
                    "ciphertext-flipped": good_ssn[:-1] + bytes([good_ssn[-1] ^ 1]),
                    "other-column": good_card}
        leaks = []
        tag_leaks = []
        with plain_connect(build=build) as conn:
            for label, value in rejected.items():
                inserted = assert_rejected(lambda: write(conn, table.insert(), (label, value)), TRIGGER_REJECTION, conn)
                updated = assert_rejected(lambda: write(conn, f"UPDATE {table.name} SET ssn = %s WHERE name = 'null'",
                                                        (value,)), TRIGGER_REJECTION, conn)
                if any("pt-trg" in message for message in (inserted, updated)):
                    leaks.append(label)
                # The value's own tag and the one the trigger computes, as hex in any case and as base64.
                tags = (value[:32], hmac.new(bytes(hmac_key), value[32:], hashlib.sha256).digest())
                for message in (inserted, updated):
                    if any(tag.hex() in message.lower() or base64.b64encode(tag).decode() in message for tag in tags):
                        tag_leaks.append(label)
                if is_pg():
                    # PostgreSQL's error names the table and column.
                    assert f"{table.name}.ssn" in inserted
            create_column_trigger(conn, table.name, "pin")
            assert_rejected(lambda: write(conn, f"INSERT INTO {table.name} (name, pin) VALUES (%s, %s)",
                                          ("no-key", good_ssn)), _NO_KEY, conn)
            write(conn, f"INSERT INTO {table.name} (name, pin) VALUES (%s, %s)", ("no-key-null", None))
        # The values whose trigger error carries one of the statement's values.
        assert leaks == []
        # The values whose trigger error carries an HMAC tag, stored or computed.
        assert tag_leaks == []
        # Only the plugin's payload and NULL are stored.
        assert kms_verifier.stored(f"SELECT name FROM {table.name} ORDER BY id") == ["good", "null", "no-key-null"]
        # A rejected UPDATE leaves the row as it was.
        assert _stored(kms_verifier, table.name, "ssn", "null") == [None]

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    @pytest.mark.parametrize("shadow", ["hmac", "operator"])
    def test_pg_trigger_ignores_hostile_search_path(self, build: str, shadow: str, table: EncryptedTable,
                                                    kms_verifier: PayloadVerifier) -> None:
        hostile = f"kms_hostile_{secrets.token_hex(4)}"
        if shadow == "hmac":
            shadows = [f"CREATE FUNCTION {hostile}.hmac(bytea, bytea, text) RETURNS bytea LANGUAGE sql IMMUTABLE "
                       "AS 'SELECT substring($1 from 1 for 32)'"]
            # The tag repeats, so a session hmac() that returns the body's first 32 bytes would match it.
            tag = os.urandom(32)
            value = tag + tag + os.urandom(16)
        else:
            shadows = [f"CREATE FUNCTION {hostile}.never_differs(bytea, bytea) RETURNS boolean LANGUAGE sql "
                       "IMMUTABLE AS 'SELECT false'",
                       f"CREATE OPERATOR {hostile}.<> (LEFTARG = bytea, RIGHTARG = bytea, "
                       f"FUNCTION = {hostile}.never_differs)"]
            value = os.urandom(80)
        try:
            with plain_connect() as conn:
                _run(conn, f"CREATE SCHEMA {hostile}", *shadows)
            with plain_connect() as conn:
                _run(conn, f"SET search_path = {hostile}, pg_catalog, public")
                assert_rejected(lambda: write(conn, table.insert(), ("hostile", value)), TRIGGER_REJECTION, conn)
        finally:
            with plain_connect() as conn:
                _run(conn, f"DROP SCHEMA IF EXISTS {hostile} CASCADE")
        # A session's own hmac() or <> ahead of pg_catalog gets no value past the trigger.
        assert kms_verifier.stored(f"SELECT name FROM {table.name}") == []

    @kms_builds(exclude=("mysql-pure",))
    def test_kept_trigger_after_removal_rejects_writes(self, build: str, table: EncryptedTable,
                                                       kms_connect: Callable[..., Any], kms_settings: KmsSettings,
                                                       kms_schema: str, kms_verifier: PayloadVerifier) -> None:
        with plain_connect() as conn:
            _drop_column_trigger(conn, table.name, "ssn")
            assert key_management_utility(conn, kms_settings, kms_schema).remove_encryption_for_column(
                table.name, "ssn") is True
            conn.commit()
            assert key_management_utility(conn, kms_settings, kms_schema).remove_encryption_for_column(
                table.name, "card") is True
            conn.commit()
        _plain_write(build, table.insert(), ("removed", b"pt-removed-ssn"))
        # The guide's removal (drop the triggers, remove the metadata, commit) leaves the column writable.
        assert _stored(kms_verifier, table.name, "ssn", "removed") == [b"pt-removed-ssn"]

        # With the metadata removed and the trigger kept, every non-NULL write fails with 'No HMAC key'.
        with plain_connect(build=build) as conn:
            assert_rejected(lambda: write(conn, table.insert("card"), ("kept-trigger", b"pt-kept-trigger")),
                            _NO_KEY, conn)
        with kms_connect() as conn:
            assert_rejected(lambda: write(conn, table.insert("card"), ("kept-plugin", "pt-kept-plugin")), _NO_KEY,
                            conn)
        assert kms_verifier.stored(f"SELECT name FROM {table.name} WHERE card IS NOT NULL") == []

    @kms_builds()
    def test_trigger_across_transactions_and_rotation(self, build: str, table: EncryptedTable,
                                                      kms_connect: Callable[..., Any], kms_settings: KmsSettings,
                                                      kms_schema: str, kms_master_key_arn: str,
                                                      kms_verifier: PayloadVerifier) -> None:
        # Without the metadata cache, each statement reads the column's current key row, so the writer is never stale.
        with kms_connect(encryption_metadata_cache_enabled=False) as writer:
            session_before = _session_id(writer)
            write(writer, table.insert(), ("tx1", "pt-tx1"))
            write(writer, table.insert(), ("tx2", "pt-tx2"))
            with plain_connect() as admin:
                new_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                    table.name, "ssn", kms_master_key_arn)
                admin.commit()
            write(writer, table.insert(), ("tx3", "pt-tx3"))
            session_after = _session_id(writer)
        # The three transactions ran in one server session.
        assert session_before == session_after
        decoded = [kms_verifier.decode(table.name, "ssn", payload)
                   for payload in kms_verifier.stored(f"SELECT ssn FROM {table.name} ORDER BY id")]
        # The trigger accepted every transaction's write, the last one under the rotated key.
        assert decoded == [(table.ssn_key_id, "pt-tx1"), (table.ssn_key_id, "pt-tx2"), (new_key_id, "pt-tx3")]

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    def test_mysql_trigger_install_without_trust_raises_1419(self, build: str, kms_trigger_installed: None,
                                                             kms_schema: str) -> None:
        with plain_connect(admin=True) as root, root.cursor() as cur:
            cur.execute("SELECT @@log_bin, @@GLOBAL.log_bin_trust_function_creators")
            log_bin, trust = cur.fetchone()
            # A binary log is what makes the trust setting count.
            assert log_bin == 1
            cur.execute("SET GLOBAL log_bin_trust_function_creators = 0")
        try:
            with plain_connect(build=build) as conn:
                # The C extension raises its own MySQLInterfaceError from inside the multi-statement script.
                with pytest.raises((mysql.connector.Error, _mysql_connector.MySQLInterfaceError),
                                   match="SUPER privilege and binary logging") as caught:
                    install_trigger_sql(conn, kms_schema)
                assert getattr(caught.value, "errno", None) == _ER_BINLOG_CREATE_ROUTINE_NEED_SUPER
        finally:
            with plain_connect(admin=True) as root, root.cursor() as cur:
                cur.execute("SET GLOBAL log_bin_trust_function_creators = %s", (trust,))
        # With the setting back, the install succeeds.
        with plain_connect(build=build) as conn:
            install_trigger_sql(conn, kms_schema)

    @kms_builds(exclude=("mysql-pure",))
    def test_copied_value_other_row_passes_other_column_rejected(self, build: str, table: EncryptedTable,
                                                                 kms_connect: Callable[..., Any]) -> None:
        with kms_connect() as conn:
            write(conn, f"INSERT INTO {table.name} (name, ssn, card) VALUES (%s, %s, %s)",
                  ("source", "pt-copy-ssn", "pt-copy-card"))
        with plain_connect(build=build) as conn:
            write(conn, f"INSERT INTO {table.name} (name, ssn) SELECT 'copy', ssn FROM {table.name} WHERE name = 'source'")
            assert_rejected(lambda: write(conn, f"UPDATE {table.name} SET card = ssn WHERE name = 'source'"),
                            TRIGGER_REJECTION, conn)
        with kms_connect() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT name, ssn, card FROM {table.name} ORDER BY id")
            rows = cur.fetchall()
        assert rows == [("source", "pt-copy-ssn", "pt-copy-card"), ("copy", "pt-copy-ssn", None)]

    @kms_builds()
    def test_trigger_rejects_update_to_previous_key_value(self, build: str, table: EncryptedTable,
                                                          kms_connect: Callable[..., Any], kms_settings: KmsSettings,
                                                          kms_schema: str, kms_master_key_arn: str,
                                                          kms_verifier: PayloadVerifier) -> None:
        update = f"UPDATE {table.name} SET ssn = %s WHERE name = 'a'"
        with kms_connect() as stale:
            write(stale, table.insert(), ("a", "pt-previous-a"))
            write(stale, table.insert(), ("b", "pt-previous-b"))
            (old_a,) = _stored(kms_verifier, table.name, "ssn", "a")
            (old_b,) = _stored(kms_verifier, table.name, "ssn", "b")
            with plain_connect() as admin:
                new_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                    table.name, "ssn", kms_master_key_arn)
                admin.commit()
            with kms_connect(encryption_metadata_cache_enabled=False) as fresh:
                write(fresh, table.insert(), ("c", "pt-previous-c"))
            (new_c,) = _stored(kms_verifier, table.name, "ssn", "c")
            with plain_connect(build=build) as conn:
                # Row b's value, under the previous key.
                assert_rejected(lambda: write(conn, update, (bytes(old_b),)), TRIGGER_REJECTION, conn)
                kept = _stored(kms_verifier, table.name, "ssn", "a")
                # Row c's value, under the current key.
                write(conn, update, (bytes(new_c),))
            moved = _stored(kms_verifier, table.name, "ssn", "a")
            # A plugin connection opened before the rotation still encrypts with the previous key.
            assert_rejected(lambda: write(stale, update, ("pt-previous-stale",)), TRIGGER_REJECTION, stale)
        # A rejected UPDATE leaves the stored value as it was.
        assert kept == [old_a]
        assert moved == [new_c]
        assert kms_verifier.decode(table.name, "ssn", moved[0]) == (new_key_id, "pt-previous-c")
        assert _stored(kms_verifier, table.name, "ssn", "a") == [new_c]

    @kms_builds()
    def test_trigger_accepts_shortest_payloads(self, build: str, table: EncryptedTable,
                                               kms_connect: Callable[..., Any],
                                               kms_verifier: PayloadVerifier) -> None:
        values: Dict[str, Any] = {"empty-text": "", "empty-bytes": b"", "true": True, "false": False}
        with kms_connect() as conn:
            for name, value in values.items():
                write(conn, table.insert(), (name, value))
            back = {name: read_column(conn, f"SELECT ssn FROM {table.name} WHERE name = %s", name) for name in values}
        lengths = {name: [len(bytes(payload)) for payload in _stored(kms_verifier, table.name, "ssn", name)]
                   for name in values}
        with plain_connect(build=build) as conn:
            assert_rejected(lambda: write(conn, table.insert(), (
                "one-short", os.urandom(EncryptionService.MIN_ENCRYPTED_LENGTH - 1))), TRIGGER_REJECTION, conn)
        shortest = EncryptionService.MIN_ENCRYPTED_LENGTH
        # An empty text or bytes value encrypts to the shortest payload, and a boolean to one byte more.
        assert lengths == {"empty-text": [shortest], "empty-bytes": [shortest], "true": [shortest + 1],
                           "false": [shortest + 1]}
        # repr shows both the type and the value.
        assert {name: [repr(value) for value in read] for name, read in back.items()} == \
            {name: [repr(value)] for name, value in values.items()}

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    def test_mysql_trigger_accepts_hmac_key_with_high_bytes(self, build: str, table: EncryptedTable,
                                                            kms_connect: Callable[..., Any],
                                                            kms_settings: KmsSettings, kms_schema: str,
                                                            kms_master_key_arn: str,
                                                            kms_verifier: PayloadVerifier) -> None:
        # Every byte at 128 or above, which the routines' byte arithmetic must keep.
        hmac_key = bytes(byte | 0x80 for byte in secrets.token_bytes(32))
        blob = kms_client(kms_settings).generate_data_key(KeyId=kms_master_key_arn, KeySpec="AES_256")["CiphertextBlob"]
        with plain_connect(build=build) as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {kms_schema}.key_storage (key_id, name, master_key_arn, encrypted_data_key, "
                            "hmac_key, key_spec) VALUES (%s, %s, %s, %s, %s, 'AES_256')",
                            (str(uuid.uuid4()), "high-bytes-pin", kms_master_key_arn,
                             base64.b64encode(blob).decode("ascii"), hmac_key))
                key_id = cur.lastrowid
                cur.execute(f"INSERT INTO {kms_schema}.encryption_metadata (table_name, column_name, "
                            "encryption_algorithm, key_id) VALUES (%s, 'pin', %s, %s)",
                            (table.name, EncryptionAlgorithm.DEFAULT, key_id))
            conn.commit()
            create_column_trigger(conn, table.name, "pin")
        with kms_connect() as conn:
            write(conn, f"INSERT INTO {table.name} (name, pin) VALUES (%s, %s)", ("high-bytes", "pt-high-bytes"))
        (payload,) = _stored(kms_verifier, table.name, "pin", "high-bytes")
        # The trigger accepted the payload signed with that key, and it opens with the hand-made key row.
        assert kms_verifier.decode(table.name, "pin", payload) == (key_id, "pt-high-bytes")
