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

"""The KMS Encryption Plugin's database-side trigger, installed from the package's trigger SQL, on a live server."""

from __future__ import annotations

import os
import secrets
from typing import Any, Callable, Iterator, NamedTuple

import mysql.connector
import psycopg
import pytest

from .utils.conditions import enable_on_engines, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (DB_ERRORS, TRIGGER_REJECTION,
                                          KmsSettings, PayloadVerifier,
                                          binary_type, create_column_trigger,
                                          encrypt_column, install_trigger_sql,
                                          is_pg, key_management_utility,
                                          kms_builds, plain_connect, write,
                                          writes)
from .utils.retry_helper import retry_until
from .utils.test_environment_features import TestEnvironmentFeatures

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

# The trigger's other rejection (TRIGGER_REJECTION is the first). PostgreSQL's messages go on to name the table and
# column; MySQL's name neither.
_NO_KEY = "No HMAC key is configured"


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


def _assert_rejected(write: Callable[[], Any], message: str, conn: Any = None) -> str:
    """Runs ``write``, which the trigger must reject with ``message``; rolls ``conn`` back and returns the error."""
    with pytest.raises(DB_ERRORS, match=message) as caught:
        write()
    if conn is not None:
        conn.rollback()
    return str(caught.value)


def _stored(verifier: PayloadVerifier, table: str, column: str, name: str) -> list:
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


@pytest.fixture
def table(kms_trigger_installed: None, kms_settings: KmsSettings, kms_schema: str,
          kms_master_key_arn: str) -> Iterator[EncryptedTable]:
    """A new table: ``ssn`` and ``card`` triggered and encrypted, ``pin`` binary and plain, ``note`` text."""
    name = f"kms_trg_{secrets.token_hex(4)}"
    id_type = "SERIAL PRIMARY KEY" if is_pg() else "INT AUTO_INCREMENT PRIMARY KEY"
    binary = binary_type()
    with plain_connect() as conn:
        _run(conn, f"CREATE TABLE {name} (id {id_type}, name VARCHAR(100), ssn {binary}, card {binary}, "
                   f"pin {binary}, note VARCHAR(100))")
        for column in ("ssn", "card"):
            create_column_trigger(conn, name, column)
        created = EncryptedTable(name, encrypt_column(conn, kms_settings, kms_schema, name, "ssn", kms_master_key_arn),
                                 encrypt_column(conn, kms_settings, kms_schema, name, "card", kms_master_key_arn))
    try:
        yield created
    finally:
        with plain_connect() as conn:
            _run(conn, f"DROP TABLE IF EXISTS {name}")


@kms_builds()
def test_trigger_after_rotation(build: str, table: EncryptedTable, kms_connect: Callable[..., Any],
                                kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                kms_verifier: PayloadVerifier) -> None:
    stale = kms_connect()
    refreshing = kms_connect(encryption_metadata_cache_refresh_interval_sec=1)
    try:
        write(stale, table.insert(), ("before", "pt-rot-before"))
        write(refreshing, table.insert(), ("refreshing-before", "pt-rot-refreshing"))
        (old_payload,) = _stored(kms_verifier, table.name, "ssn", "before")
        with plain_connect() as admin:
            new_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                table.name, "ssn", kms_master_key_arn)
            admin.commit()
        assert new_key_id != table.ssn_key_id, "rotation adds a key row"

        _assert_rejected(lambda: write(stale, table.insert(), ("stale", "pt-rot-stale")), TRIGGER_REJECTION, stale)
        write(stale, f"UPDATE {table.name} SET note = %s WHERE name = %s", ("touched", "before"))
        assert _stored(kms_verifier, table.name, "ssn", "before") == [old_payload], \
            "an UPDATE of another column on a row written before the rotation is accepted and keeps its value"

        assert retry_until(lambda: writes(refreshing, table.insert(), ("refreshed", "pt-rot-after")),
                           timeout_sec=15, delay_sec=0.25), \
            "a connection with a short refresh interval writes with the new key after its next refresh"
        (new_payload,) = _stored(kms_verifier, table.name, "ssn", "refreshed")
        assert kms_verifier.decode(table.name, "ssn", new_payload) == (new_key_id, "pt-rot-after"), \
            "the refreshed connection's write names the new key row"
        with refreshing.cursor() as cur:
            cur.execute(f"SELECT ssn FROM {table.name} WHERE name = %s", ("before",))
            assert cur.fetchall() == [("pt-rot-before",)], "a value written before the rotation still decrypts"
    finally:
        stale.close()
        refreshing.close()


@kms_builds()
def test_writes_outside_the_plugin_reach_the_trigger(build: str, table: EncryptedTable,
                                                     kms_connect: Callable[..., Any],
                                                     kms_verifier: PayloadVerifier) -> None:
    with kms_connect() as conn:
        write(conn, table.insert(), ("seed", "pt-seen-seed"))
        cur = conn.cursor()
        no_list = f"INSERT INTO {table.name} VALUES ({'DEFAULT' if is_pg() else 'NULL'}, %s, %s, NULL, NULL, NULL)"
        _assert_rejected(lambda: cur.execute(no_list, ("no-list", b"pt-unseen-no-list")), TRIGGER_REJECTION, conn)
        if is_pg():
            def copy_in() -> None:
                with cur.copy(f"COPY {table.name} (name, ssn) FROM STDIN") as copy:
                    copy.write_row(("copy", b"pt-unseen-copy"))
            _assert_rejected(copy_in, TRIGGER_REJECTION, conn)
            _assert_rejected(lambda: list(cur.stream(table.insert(), ("stream", b"pt-unseen-stream"))), TRIGGER_REJECTION, conn)
            conn.target_connection.cursor_factory = psycopg.RawCursor
            raw = conn.cursor()
            conn.target_connection.cursor_factory = psycopg.Cursor
            _assert_rejected(lambda: raw.execute(f"INSERT INTO {table.name} VALUES (DEFAULT, $1, $2, NULL, NULL, NULL)",
                                                 ("raw-no-list", b"pt-unseen-raw")), TRIGGER_REJECTION, conn)
            cur.execute(f"PREPARE kms_insert (text, bytea) AS INSERT INTO {table.name} (name, ssn) VALUES ($1, $2)")
            _assert_rejected(lambda: cur.execute("EXECUTE kms_insert ('prepared', 'pt-unseen-prepared')"),
                             TRIGGER_REJECTION, conn)
        else:
            prepared = conn.cursor(prepared=True)
            _assert_rejected(lambda: prepared.execute(f"INSERT INTO {table.name} VALUES (NULL, ?, ?, NULL, NULL, NULL)",
                                                      ("prepared-no-list", b"pt-unseen-prepared")), TRIGGER_REJECTION, conn)
            cur.execute(f"PREPARE kms_insert FROM 'INSERT INTO {table.name} (name, ssn) "
                        f"VALUES (''sql-prepare'', ''pt-unseen-sql'')'")
            _assert_rejected(lambda: cur.execute("EXECUTE kms_insert"), TRIGGER_REJECTION, conn)
            _assert_rejected(lambda: cur.execute(f"INSERT INTO {table.name} (name, ssn) VALUES ('bytes-é', %s)"
                                                 .encode("utf-8"), (b"pt-unseen-bytes",)), TRIGGER_REJECTION, conn)
        cur.close()
    names = kms_verifier.stored(f"SELECT name FROM {table.name} ORDER BY id")
    assert names == ["seed"], "no write the plugin can't see or doesn't analyze stored its plaintext"


@kms_builds()
def test_packaged_trigger_accepts_payloads_and_rejects_plaintext(build: str, table: EncryptedTable,
                                                                 kms_connect: Callable[..., Any],
                                                                 kms_verifier: PayloadVerifier) -> None:
    with plain_connect() as conn, conn.cursor() as cur:
        if is_pg():
            cur.execute("SELECT obj_description('enforce_encrypted_column'::regproc, 'pg_proc')")
        else:
            cur.execute("SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() "
                        "AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert'")
        (stamp,) = cur.fetchone()
    assert stamp.startswith("aws-advanced-python-wrapper kms_encryption trigger v"), \
        "the trigger SQL installed from the package records its version"

    with kms_connect() as conn:
        write(conn, f"INSERT INTO {table.name} (name, ssn, card) VALUES (%s, %s, %s)",
              ("good", "pt-trg-ssn", "pt-trg-card"))
    _plain_write(build, table.insert(), ("null", None))
    (good_ssn,) = _stored(kms_verifier, table.name, "ssn", "good")
    (good_card,) = _stored(kms_verifier, table.name, "card", "good")
    good_ssn, good_card = bytes(good_ssn), bytes(good_card)
    rejected = {"plaintext": b"pt-trg-plaintext-" + b"x" * 64,
                "short": os.urandom(64),
                "random": os.urandom(80),
                "tag-flipped": bytes([good_ssn[0] ^ 1]) + good_ssn[1:],
                "ciphertext-flipped": good_ssn[:-1] + bytes([good_ssn[-1] ^ 1]),
                "other-column": good_card}
    with plain_connect(build=build) as conn:
        for label, value in rejected.items():
            inserted = _assert_rejected(lambda: write(conn, table.insert(), (label, value)), TRIGGER_REJECTION, conn)
            updated = _assert_rejected(lambda: write(conn, f"UPDATE {table.name} SET ssn = %s WHERE name = 'null'",
                                                     (value,)), TRIGGER_REJECTION, conn)
            leaks = any("pt-trg" in message for message in (inserted, updated))
            assert not leaks, f"the trigger's error for the {label} value carries none of the statement's values"
            if is_pg():
                assert f"{table.name}.ssn" in inserted, "PostgreSQL's error names the table and column"
        create_column_trigger(conn, table.name, "pin")
        _assert_rejected(lambda: write(conn, f"INSERT INTO {table.name} (name, pin) VALUES (%s, %s)",
                                       ("no-key", good_ssn)), _NO_KEY, conn)
        write(conn, f"INSERT INTO {table.name} (name, pin) VALUES (%s, %s)", ("no-key-null", None))
    assert kms_verifier.stored(f"SELECT name FROM {table.name} ORDER BY id") == ["good", "null", "no-key-null"], \
        "only the plugin's payload and NULL are stored"
    assert _stored(kms_verifier, table.name, "ssn", "null") == [None], "a rejected UPDATE leaves the row as it was"


@enable_on_engines([DatabaseEngine.PG])
@kms_builds()
def test_pg_trigger_ignores_a_hostile_search_path(build: str, table: EncryptedTable,
                                                  kms_verifier: PayloadVerifier) -> None:
    hostile = f"kms_hostile_{secrets.token_hex(4)}"
    with plain_connect() as conn:
        _run(conn, f"CREATE SCHEMA {hostile}",
             f"CREATE FUNCTION {hostile}.never_differs(bytea, bytea) RETURNS boolean LANGUAGE sql IMMUTABLE "
             "AS 'SELECT false'",
             f"CREATE OPERATOR {hostile}.<> (LEFTARG = bytea, RIGHTARG = bytea, FUNCTION = {hostile}.never_differs)",
             f"CREATE FUNCTION {hostile}.hmac(bytea, bytea, text) RETURNS bytea LANGUAGE sql IMMUTABLE "
             "AS 'SELECT substring($1 from 1 for 32)'")
    try:
        with plain_connect() as conn:
            _run(conn, f"SET search_path = {hostile}, pg_catalog, public")
            _assert_rejected(lambda: write(conn, table.insert(), ("hostile", b"pt-hostile-" + b"x" * 80)),
                             TRIGGER_REJECTION, conn)
    finally:
        with plain_connect() as conn:
            _run(conn, f"DROP SCHEMA {hostile} CASCADE")
    assert kms_verifier.stored(f"SELECT name FROM {table.name}") == [], \
        "a session's own hmac() and <> ahead of pg_catalog get no plaintext past the trigger"


@kms_builds("mysql-pure")
def test_removing_a_column_without_dropping_its_trigger_fails_every_write(
        build: str, table: EncryptedTable, kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_verifier: PayloadVerifier) -> None:
    with plain_connect() as conn:
        _drop_column_trigger(conn, table.name, "ssn")
        assert key_management_utility(conn, kms_settings, kms_schema).remove_encryption_for_column(
            table.name, "ssn"), "the utility removes ssn's metadata row"
        conn.commit()
        assert key_management_utility(conn, kms_settings, kms_schema).remove_encryption_for_column(
            table.name, "card"), "the utility removes card's metadata row"
        conn.commit()
    _plain_write(build, table.insert(), ("removed", b"pt-removed-ssn"))
    assert _stored(kms_verifier, table.name, "ssn", "removed") == [b"pt-removed-ssn"], \
        "the guide's removal (drop the triggers, remove the metadata, commit) leaves the column writable"

    with plain_connect(build=build) as conn:
        _assert_rejected(lambda: write(conn, table.insert("card"), ("kept-trigger", b"pt-kept-trigger")),
                         _NO_KEY, conn)
    with kms_connect() as conn:
        _assert_rejected(lambda: write(conn, table.insert("card"), ("kept-plugin", "pt-kept-plugin")), _NO_KEY, conn)
    assert kms_verifier.stored(f"SELECT name FROM {table.name} WHERE card IS NOT NULL") == [], \
        "with the metadata removed and the trigger kept, every non-NULL write fails with 'No HMAC key'"


@kms_builds()
def test_trigger_across_transactions_and_rotation(build: str, table: EncryptedTable,
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
    assert session_before == session_after, "the three transactions ran in one server session"
    decoded = [kms_verifier.decode(table.name, "ssn", payload)
               for payload in kms_verifier.stored(f"SELECT ssn FROM {table.name} ORDER BY id")]
    assert decoded == [(table.ssn_key_id, "pt-tx1"), (table.ssn_key_id, "pt-tx2"), (new_key_id, "pt-tx3")], \
        "the trigger accepted every transaction's write, the last one under the rotated key"


@enable_on_engines([DatabaseEngine.MYSQL])
@kms_builds()
def test_mysql_trigger_install_without_trust_fails_with_1419(build: str, kms_trigger_installed: None,
                                                             kms_schema: str) -> None:
    # Imported here, so a host without mysql-connector's C extension skips this test instead of failing the module.
    _mysql_connector = pytest.importorskip("_mysql_connector")
    with plain_connect(admin=True) as root, root.cursor() as cur:
        cur.execute("SELECT @@log_bin")
        assert cur.fetchone()[0] == 1, "the server writes a binary log, which is what makes the trust setting count"
        cur.execute("SET GLOBAL log_bin_trust_function_creators = 0")
    try:
        with plain_connect(build=build) as conn:
            # The C extension raises its own MySQLInterfaceError from inside the multi-statement script.
            with pytest.raises((mysql.connector.Error, _mysql_connector.MySQLInterfaceError),
                               match="SUPER privilege and binary logging") as caught:
                install_trigger_sql(conn, kms_schema)
            assert getattr(caught.value, "errno", None) == 1419, \
                "a user without SUPER can't install the routines while log_bin_trust_function_creators is 0"
    finally:
        with plain_connect(admin=True) as root, root.cursor() as cur:
            cur.execute("SET GLOBAL log_bin_trust_function_creators = 1")
    with plain_connect(build=build) as conn:
        install_trigger_sql(conn, kms_schema)  # succeeds once the setting is 1 again


@kms_builds("mysql-pure")
def test_value_copied_to_another_row_passes_and_to_another_column_fails(
        build: str, table: EncryptedTable, kms_connect: Callable[..., Any]) -> None:
    with kms_connect() as conn:
        write(conn, f"INSERT INTO {table.name} (name, ssn, card) VALUES (%s, %s, %s)",
              ("source", "pt-copy-ssn", "pt-copy-card"))
    with plain_connect(build=build) as conn:
        write(conn, f"INSERT INTO {table.name} (name, ssn) SELECT 'copy', ssn FROM {table.name} WHERE name = 'source'")
        _assert_rejected(lambda: write(conn, f"UPDATE {table.name} SET card = ssn WHERE name = 'source'"),
                         TRIGGER_REJECTION, conn)
    with kms_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT name, ssn, card FROM {table.name} ORDER BY id")
        rows = cur.fetchall()
    assert rows == [("source", "pt-copy-ssn", "pt-copy-card"), ("copy", "pt-copy-ssn", None)], \
        "a value copied to another row of its column decrypts there; one copied to another column is rejected"
