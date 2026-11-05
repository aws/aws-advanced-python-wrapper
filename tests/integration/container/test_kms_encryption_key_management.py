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

"""The KMS Encryption Plugin's metadata tables, KeyManagementUtility and key rows on a live server and KMS."""

from __future__ import annotations

import base64
import secrets
import uuid
from typing import Any, Callable, Iterator, List, Optional

import pytest
import sqlalchemy

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from aws_advanced_python_wrapper.errors import (IntegrityCheckError,
                                                MetadataError)
from .utils.conditions import enable_on_features
from .utils.kms_encryption_helper import (DB_ERRORS, TRIGGER_REJECTION, DbUser,
                                          KmsSettings, PayloadVerifier,
                                          binary_type, create_column_trigger,
                                          encrypt_column, encryption_config,
                                          is_pg, key_management_utility,
                                          kms_builds, kms_client,
                                          plain_connect, run_script,
                                          wrapper_connect, write, writes)
from .utils.kms_encryption_schema import ddl_for
from .utils.retry_helper import retry_until
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]


def _read(conn: Any, sql: str, params: Any = None) -> List[Any]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _insert(table: str, column: str = "ssn") -> str:
    return f"INSERT INTO {table} (name, {column}) VALUES (%s, %s)"


def _metadata_key_ids(verifier: PayloadVerifier, schema: str, table: str) -> List[Any]:
    return verifier.stored(f"SELECT key_id FROM {schema}.encryption_metadata WHERE table_name = %s "
                           "ORDER BY column_name", (table,))


@pytest.fixture
def new_table(kms_trigger_installed: None) -> Iterator[Callable[..., str]]:
    """Creates tables ``(id, name, ssn, card)``, ``ssn`` of ``ssn_type`` and ``card`` binary, with the trigger on
    both when ``triggered``; nothing is encrypted. Every table is dropped afterwards."""
    created: List[str] = []

    def create(ssn_type: Optional[str] = None, *, triggered: bool = False) -> str:
        name = f"kms_km_{secrets.token_hex(4)}"
        id_type = "SERIAL PRIMARY KEY" if is_pg() else "INT AUTO_INCREMENT PRIMARY KEY"
        with plain_connect() as conn:
            write(conn, f"CREATE TABLE {name} (id {id_type}, name VARCHAR(100), ssn {ssn_type or binary_type()}, "
                        f"card {binary_type()})")
            created.append(name)
            if triggered:
                create_column_trigger(conn, name, "ssn")
                create_column_trigger(conn, name, "card")
        return name

    yield create
    with plain_connect() as conn:
        for name in created:
            write(conn, f"DROP TABLE IF EXISTS {name}")


@kms_builds("mysql-pure")
def test_metadata_ddl_from_the_guide_creates_tables_the_plugin_reads(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    columns = kms_verifier.stored("SELECT concat(table_name, '.', column_name) FROM information_schema.columns "
                                  "WHERE table_schema = %s ORDER BY 1", (kms_schema,))
    assert columns == sorted(
        [f"encryption_metadata.{c}" for c in ("column_name", "created_at", "encryption_algorithm", "key_id",
                                              "table_name", "updated_at")]
        + [f"key_storage.{c}" for c in ("created_at", "encrypted_data_key", "hmac_key", "id", "key_id", "key_spec",
                                        "last_used_at", "master_key_arn", "name")]), \
        "the guide's DDL creates the two metadata tables with the guide's columns"
    table = new_table()
    with plain_connect(build=build) as conn:
        key_id = encrypt_column(conn, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
    assert _metadata_key_ids(kms_verifier, kms_schema, table) == [key_id], \
        "KeyManagementUtility fills the tables, and encryption_metadata.key_id is the key row's integer id"
    with kms_connect() as conn:
        write(conn, _insert(table), ("ddl", "pt-ddl"))
        assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-ddl",)], "the plugin reads the metadata the DDL holds"
    (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
    assert kms_verifier.decode(table, "ssn", payload) == (key_id, "pt-ddl"), "the stored payload names the key row"


@kms_builds()
def test_least_privilege_grants_from_the_guide_are_enough_and_hold(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
    table = new_table(triggered=True)
    with plain_connect() as conn:
        encrypt_column(conn, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
    with kms_connect() as conn:
        write(conn, _insert(table), ("app", "pt-least"))
        assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-least",)], \
            "the guide's run-time grants (SELECT on the metadata, DML on the table) are enough for the plugin"

    if is_pg():
        refused = [f"ALTER TABLE {table} DISABLE TRIGGER {table}_ssn_encrypted",
                   f"DROP TRIGGER {table}_ssn_encrypted ON {table}"]
    else:
        refused = [f"DROP TRIGGER {table}_ssn_hmac_insert"]
    refused += [f"INSERT INTO {kms_schema}.key_storage (key_id, name, master_key_arn, encrypted_data_key, hmac_key) "
                "VALUES ('k', 'k', 'arn', 'e', 'h')",
                f"UPDATE {kms_schema}.key_storage SET name = 'changed'",
                f"DELETE FROM {kms_schema}.encryption_metadata"]
    with plain_connect(kms_app_user, build=build) as conn:
        for statement in refused:
            with pytest.raises(DB_ERRORS, match="permission denied|must be owner|command denied"):
                write(conn, statement)
            conn.rollback()
        with pytest.raises(DB_ERRORS, match=TRIGGER_REJECTION):
            write(conn, _insert(table), ("app-plain", b"pt-least-plain-" + b"x" * 64))
        conn.rollback()
    assert kms_verifier.stored(f"SELECT name FROM {table} ORDER BY id") == ["app"], \
        "the application user can't change the key rows or the trigger, so its plaintext is still rejected"


# IT-26 in three legs. Each leg checks "not before" on a connection with the default refresh interval (300 s) and "at
# refresh" on one that refreshes every second, so no check depends on where the refresh timer is.
@kms_builds()
def test_open_connections_pick_up_metadata_changes_at_refresh_register(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table(triggered=True)
    with plain_connect() as admin:
        encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
    with kms_connect() as stale, kms_connect(encryption_metadata_cache_refresh_interval_sec=1) as refreshing:
        write(stale, _insert(table), ("stale-loaded", "pt-refresh-ssn"))
        write(refreshing, _insert(table), ("refreshing-loaded", "pt-refresh-ssn"))
        with plain_connect() as admin:
            card_key_id = encrypt_column(admin, kms_settings, kms_schema, table, "card", kms_master_key_arn)
        assert not writes(stale, _insert(table, "card"), ("registered-early", "pt-refresh-early")), \
            "before its refresh, an open connection still treats the new column as plain, so the trigger rejects it"
        assert retry_until(lambda: writes(refreshing, _insert(table, "card"), ("registered", "pt-refresh-card")),
                           timeout_sec=15, delay_sec=0.25), \
            "after its refresh, an open connection encrypts the newly registered column"
    (payload,) = kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'registered'")
    assert kms_verifier.decode(table, "card", payload) == (card_key_id, "pt-refresh-card"), \
        "the refreshed connection's write names the new column's key row"


@kms_builds()
def test_open_connections_pick_up_metadata_changes_at_refresh_rotate(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table(triggered=True)
    with plain_connect() as admin:
        encrypt_column(admin, kms_settings, kms_schema, table, "card", kms_master_key_arn)
    with kms_connect() as stale, kms_connect(encryption_metadata_cache_refresh_interval_sec=1) as refreshing:
        write(stale, _insert(table, "card"), ("stale-loaded", "pt-refresh-card"))
        write(refreshing, _insert(table, "card"), ("refreshing-loaded", "pt-refresh-card"))
        with plain_connect() as admin:
            rotated_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                table, "card", kms_master_key_arn)
            admin.commit()
        assert not writes(stale, _insert(table, "card"), ("rotated-early", "pt-refresh-early")), \
            "before its refresh, an open connection still writes with the old key, which the trigger rejects"
        assert retry_until(lambda: writes(refreshing, _insert(table, "card"), ("rotated", "pt-refresh-rotated")),
                           timeout_sec=15, delay_sec=0.25), \
            "after its refresh, an open connection writes with the rotated key"
    (payload,) = kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'rotated'")
    assert kms_verifier.decode(table, "card", payload) == (rotated_key_id, "pt-refresh-rotated"), \
        "the refreshed connection's write names the rotated key row"


@kms_builds()
def test_open_connections_pick_up_metadata_changes_at_refresh_remove(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table(triggered=True)
    with plain_connect() as admin:
        card_key_id = encrypt_column(admin, kms_settings, kms_schema, table, "card", kms_master_key_arn)
    with kms_connect() as stale, kms_connect(encryption_metadata_cache_refresh_interval_sec=1) as refreshing:
        write(stale, _insert(table, "card"), ("stale-loaded", "pt-refresh-card"))
        write(refreshing, _insert(table, "card"), ("refreshing-loaded", "pt-refresh-card"))
        with plain_connect() as admin:
            with admin.cursor() as cur:
                if is_pg():
                    cur.execute(f"DROP TRIGGER {table}_card_encrypted ON {table}")
                else:
                    cur.execute(f"DROP TRIGGER {table}_card_hmac_insert")
                    cur.execute(f"DROP TRIGGER {table}_card_hmac_update")
            key_management_utility(admin, kms_settings, kms_schema).remove_encryption_for_column(table, "card")
            admin.commit()
        write(stale, _insert(table, "card"), ("removed-early", "pt-refresh-removed-early"))
        (early,) = kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'removed-early'")
        assert EncryptionService.key_id_from_payload(bytes(early)) == card_key_id, \
            "before its refresh, an open connection still encrypts the removed column"

        def stores_plaintext() -> bool:
            write(refreshing, f"DELETE FROM {table} WHERE name = 'removed'")
            write(refreshing, _insert(table, "card"), ("removed", b"pt-refresh-removed"))
            return kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'removed'") == [b"pt-refresh-removed"]
        assert retry_until(stores_plaintext, timeout_sec=15, delay_sec=0.25), \
            "after its refresh, an open connection writes the removed column as plain"


@kms_builds()
def test_missing_or_unreadable_metadata_refuses_every_statement(
        build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
        kms_master_key_arn: str, kms_app_user: DbUser) -> None:
    table = new_table()
    late = f"{kms_schema}_late"
    engine = TestEnvironment.get_current().get_engine()
    drop = f"DROP SCHEMA IF EXISTS {late} CASCADE" if is_pg() else f"DROP DATABASE IF EXISTS {late}"
    try:
        with wrapper_connect(build, kms_app_user, kms_settings, late, kms_master_key_arn) as conn:
            for sql, params in (("SELECT 1", None), (_insert(table), ("missing", "pt-missing"))):
                with pytest.raises(MetadataError):
                    _read(conn, sql, params) if params is None else write(conn, sql, params)
                conn.rollback()
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                if is_pg():
                    cur.execute(ddl_for(engine.value, late))
                    cur.execute(f"GRANT USAGE ON SCHEMA {late} TO {kms_app_user.name}")
                    cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {late} TO {kms_app_user.name}")
                else:
                    run_script(cur, ddl_for(engine.value, late))
                    cur.execute(f"GRANT SELECT ON {late}.* TO %s", (kms_app_user.name,))
                admin.commit()
            assert _read(conn, "SELECT 1") == [(1,)], "once the metadata tables exist, the same connection works"
    finally:
        with plain_connect(admin=True) as admin, admin.cursor() as cur:
            cur.execute(drop)
            admin.commit()

    grant = (f"SELECT ON ALL TABLES IN SCHEMA {kms_schema}" if is_pg() else f"SELECT ON {kms_schema}.*")
    user = kms_app_user.name if is_pg() else f"'{kms_app_user.name}'"
    with plain_connect(admin=True) as admin:
        write(admin, f"REVOKE {grant} FROM {user}")
    try:
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            for _ in range(2):
                with pytest.raises(MetadataError):
                    _read(conn, "SELECT 1")
                conn.rollback()
            with plain_connect(admin=True) as admin:
                write(admin, f"GRANT {grant} TO {user}")
            assert _read(conn, "SELECT 1") == [(1,)], "once SELECT on the metadata is granted, the connection works"
    finally:
        with plain_connect(admin=True) as admin:
            write(admin, f"GRANT {grant} TO {user}")


@kms_builds()
def test_initialize_encryption_for_column_then_round_trip(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table()
    client = kms_client(kms_settings)
    calls: List[str] = []
    client.meta.events.register("before-call.kms.GenerateDataKey", lambda **_: calls.append("GenerateDataKey"))
    with plain_connect(build=build) as conn:
        utility = KeyManagementUtility(conn, client, encryption_config(kms_settings, kms_schema))
        key_id = utility.initialize_encryption_for_column(table, "ssn", kms_master_key_arn)
        conn.commit()
    assert calls == ["GenerateDataKey"], "setting up a column calls GenerateDataKey once"

    (encrypted_data_key,) = kms_verifier.stored(
        f"SELECT encrypted_data_key FROM {kms_schema}.key_storage WHERE id = %s", (key_id,))
    (hmac_key,) = kms_verifier.stored(f"SELECT hmac_key FROM {kms_schema}.key_storage WHERE id = %s", (key_id,))
    plaintext = client.decrypt(CiphertextBlob=base64.b64decode(encrypted_data_key),
                               KeyId=kms_master_key_arn)["Plaintext"]
    stored_text = encrypted_data_key.encode() + bytes(hmac_key)
    leaked = any(form in stored_text for form in (plaintext, base64.b64encode(plaintext), plaintext.hex().encode()))
    plaintext = b""
    assert not leaked, "key_storage holds only the KMS-encrypted data key, never its plaintext"

    with kms_connect() as conn:
        write(conn, _insert(table), ("initialized", "pt-init"))
        assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-init",)], "the column round-trips through the plugin"
    (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
    assert kms_verifier.decode(table, "ssn", payload) == (key_id, "pt-init"), \
        "the stored payload names the key row the utility made"


@kms_builds()
def test_rotation_keeps_old_values_readable_and_new_writes_use_the_new_key(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table(triggered=True)
    with plain_connect() as admin:
        old_key_id = encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
    with kms_connect(encryption_metadata_cache_refresh_interval_sec=1) as open_conn:
        write(open_conn, _insert(table), ("old", "pt-rotate-old"))
        with plain_connect() as admin:
            new_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                table, "ssn", kms_master_key_arn)
            admin.commit()
        with kms_connect() as fresh:
            assert _read(fresh, f"SELECT ssn FROM {table} WHERE name = 'old'") == [("pt-rotate-old",)], \
                "a value written under the old key row still decrypts after the rotation"
            write(fresh, _insert(table), ("new", "pt-rotate-new"))
        assert retry_until(lambda: writes(open_conn, _insert(table), ("open", "pt-rotate-open")),
                           timeout_sec=15, delay_sec=0.25), \
            "an open plugin connection writes with the new key after its metadata refresh"
    decoded = [kms_verifier.decode(table, "ssn", payload)
               for payload in kms_verifier.stored(f"SELECT ssn FROM {table} ORDER BY id")]
    assert decoded == [(old_key_id, "pt-rotate-old"), (new_key_id, "pt-rotate-new"), (new_key_id, "pt-rotate-open")], \
        "old values keep their key row and new writes name the new one"


@kms_builds("mysql-pure")
def test_create_master_key_makes_a_symmetric_key_with_the_wrapper_alias(
        build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
        kms_app_user: DbUser) -> None:
    client = kms_client(kms_settings)
    with plain_connect(build=build) as conn:
        arn = key_management_utility(conn, kms_settings, kms_schema).create_master_key("kms container test key")
    metadata = client.describe_key(KeyId=arn)["KeyMetadata"]
    assert (metadata["KeySpec"], metadata["KeyUsage"]) == ("SYMMETRIC_DEFAULT", "ENCRYPT_DECRYPT"), \
        "create_master_key makes a symmetric encrypt-and-decrypt key"
    aliases = [alias["AliasName"] for page in client.get_paginator("list_aliases").paginate()
               for alias in page["Aliases"] if alias.get("TargetKeyId") == metadata["KeyId"]]
    assert len(aliases) == 1 and aliases[0].startswith(KeyManagementUtility.ALIAS_PREFIX), \
        "the new key gets one alias under the wrapper's prefix"

    table = new_table()
    with plain_connect() as conn:
        encrypt_column(conn, kms_settings, kms_schema, table, "ssn", arn)
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, arn) as conn:
        write(conn, _insert(table), ("new-key", "pt-master"))
        assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-master",)], "a column set up on the key round-trips"


@kms_builds()
def test_key_management_utility_on_a_caller_connection_rollback(
        build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
        kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table()
    key_rows = f"SELECT count(*) FROM {kms_schema}.key_storage"
    (key_rows_before,) = kms_verifier.stored(key_rows)
    with plain_connect(build=build) as conn:
        key_management_utility(conn, kms_settings, kms_schema).initialize_encryption_for_column(
            table, "ssn", kms_master_key_arn)
        conn.rollback()
    assert (_metadata_key_ids(kms_verifier, kms_schema, table), kms_verifier.stored(key_rows)) == \
        ([], [key_rows_before]), "the utility runs in the caller's transaction, so a rollback keeps nothing"


@kms_builds()
def test_key_management_utility_on_a_caller_connection_with_block(
        build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
        kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table()
    with plain_connect(build=build) as conn:
        key_management_utility(conn, kms_settings, kms_schema).initialize_encryption_for_column(
            table, "ssn", kms_master_key_arn)
    assert len(_metadata_key_ids(kms_verifier, kms_schema, table)) == (1 if is_pg() else 0), \
        "psycopg's with-block commits the utility's work; mysql-connector's closes without committing"


@kms_builds()
def test_key_management_utility_on_a_caller_connection_remove(
        build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
        kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table()
    with plain_connect(build=build) as conn:
        key_id = encrypt_column(conn, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
    if is_pg():
        triggers = kms_verifier.stored("SELECT count(*) FROM pg_trigger WHERE tgrelid = %s::regclass", (table,))
    else:
        triggers = kms_verifier.stored("SELECT count(*) FROM information_schema.TRIGGERS "
                                       "WHERE EVENT_OBJECT_TABLE = %s", (table,))
    assert triggers == [0], "the utility creates no trigger"

    with plain_connect(build=build) as conn:
        write(conn, f"INSERT INTO {kms_schema}.encryption_metadata (table_name, column_name, encryption_algorithm, "
                    "key_id) VALUES (%s, 'card', %s, %s)", (table, EncryptionAlgorithm.DEFAULT, key_id))
        utility = key_management_utility(conn, kms_settings, kms_schema)
        assert utility.columns_using_key(key_id) == [f"{table}.card", f"{table}.ssn"], \
            "columns_using_key lists every column on the key row"
        assert utility.remove_encryption_for_column(table, "card") is True, \
            "remove_encryption_for_column reports that it removed the column"
        conn.commit()
    assert kms_verifier.stored(f"SELECT column_name FROM {kms_schema}.encryption_metadata WHERE table_name = %s",
                               (table,)) == ["ssn"], \
        "remove_encryption_for_column deletes only that column's metadata row"
    assert kms_verifier.stored(f"SELECT count(*) FROM {kms_schema}.key_storage WHERE id = %s", (key_id,)) == [1], \
        "the key row stays, so values already written still decrypt"


@kms_builds()
def test_key_management_utility_on_a_caller_connection_sqlalchemy_raw_connection(
        build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
        kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table()
    url = "postgresql+psycopg://" if is_pg() else "mysql+mysqlconnector://"
    engine = sqlalchemy.create_engine(url, creator=lambda: plain_connect(build=build))
    try:
        raw = engine.raw_connection()
        try:
            key_management_utility(raw.driver_connection, kms_settings, kms_schema).initialize_encryption_for_column(
                table, "ssn", kms_master_key_arn)
            raw.commit()
        finally:
            raw.close()
    finally:
        engine.dispose()
    assert len(_metadata_key_ids(kms_verifier, kms_schema, table)) == 1, \
        "the utility works on SQLAlchemy's raw_connection().driver_connection"


@kms_builds()
def test_metadata_in_another_schema(build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
                                    kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                    kms_verifier: PayloadVerifier) -> None:
    schemas = ("SELECT nspname FROM pg_namespace WHERE nspname = %s" if is_pg()
               else "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s")
    assert kms_schema != "encrypt" and kms_verifier.stored(schemas, ("encrypt",)) == [], \
        "the module's metadata is in its own schema and there is no encrypt schema to fall back on"
    if not is_pg():
        (body,) = kms_verifier.stored("SELECT ROUTINE_DEFINITION FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA "
                                      "= DATABASE() AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert'")
        assert f"{kms_schema}.encryption_metadata" in body, "the MySQL trigger SQL reads the configured schema"
    table = new_table(triggered=True)
    with plain_connect() as admin:
        encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        rotated_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
            table, "ssn", kms_master_key_arn)
        admin.commit()
    with kms_connect() as conn:
        write(conn, _insert(table), ("other-schema", "pt-schema"))
        assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-schema",)], \
            "the plugin reads the metadata from encryption_metadata_schema"
    (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
    assert kms_verifier.decode(table, "ssn", payload)[0] == rotated_key_id, \
        "the utility's rotation in that schema is what the plugin and the trigger use"
    with plain_connect(build=build) as conn:
        assert not writes(conn, _insert(table), ("plain", b"pt-schema-plain-" + b"x" * 64)), \
            "the trigger reads its key from the configured schema and rejects plaintext"


@kms_builds()
def test_text_or_short_column_refuses_or_fails_closed(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    if is_pg():
        table = new_table("TEXT")
        with plain_connect() as admin:
            encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        with kms_connect() as conn:
            write(conn, _insert(table), ("text", "pt-text"))
            with pytest.raises(IntegrityCheckError, match="is not binary"):
                _read(conn, f"SELECT ssn FROM {table}")
        (stored,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
        assert isinstance(stored, str) and stored.startswith("\\x"), "a TEXT column stores the payload as hex text"
        return
    # The server's own refusal for each type, so a privilege or connection error doesn't pass for it.
    for column_type, refusal in (("TEXT", "Incorrect string value"), ("VARBINARY(16)", "Data too long")):
        table = new_table(column_type)
        with plain_connect() as admin:
            encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        with kms_connect() as conn:
            with pytest.raises(DB_ERRORS, match=refusal):
                write(conn, _insert(table), (column_type, "pt-text"))
            conn.rollback()
        assert kms_verifier.stored(f"SELECT name FROM {table}") == [], \
            f"a MySQL {column_type} column refuses the plugin's payload"


@kms_builds()
def test_hand_made_key_row_is_used_by_the_plugin_and_the_trigger(
        build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any], kms_settings: KmsSettings,
        kms_schema: str, kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    table = new_table(triggered=True)
    blob = kms_client(kms_settings).generate_data_key(KeyId=kms_master_key_arn, KeySpec="AES_256")["CiphertextBlob"]
    wrapped = base64.encodebytes(blob).decode("ascii")
    assert "\n" in wrapped.strip(), "the wrapped form spans more than one 76-column line"
    key_ids = {}
    with plain_connect(build=build) as conn:
        for column, encoded in (("ssn", base64.b64encode(blob).decode("ascii")), ("card", wrapped)):
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {kms_schema}.key_storage (key_id, name, master_key_arn, encrypted_data_key, "
                            "hmac_key, key_spec) VALUES (%s, %s, %s, %s, %s, 'AES_256')"
                            + (" RETURNING id" if is_pg() else ""),
                            (str(uuid.uuid4()), f"hand-made-{column}", kms_master_key_arn, encoded,
                             secrets.token_bytes(32)))
                key_ids[column] = cur.fetchone()[0] if is_pg() else cur.lastrowid
                cur.execute(f"INSERT INTO {kms_schema}.encryption_metadata (table_name, column_name, "
                            "encryption_algorithm, key_id) VALUES (%s, %s, %s, %s)",
                            (table, column, EncryptionAlgorithm.DEFAULT, key_ids[column]))
        conn.commit()
    with kms_connect() as conn:
        write(conn, f"INSERT INTO {table} (name, ssn, card) VALUES (%s, %s, %s)",
              ("hand-made", "pt-hand-ssn", "pt-hand-card"))
        assert _read(conn, f"SELECT ssn, card FROM {table}") == [("pt-hand-ssn", "pt-hand-card")], \
            "the plugin reads hand-made key rows, base64 on one line and wrapped, through the driver's column types"
    decoded = [kms_verifier.decode(table, column, kms_verifier.stored(f"SELECT {column} FROM {table}")[0])
               for column in ("ssn", "card")]
    assert decoded == [(key_ids["ssn"], "pt-hand-ssn"), (key_ids["card"], "pt-hand-card")], \
        "the trigger accepted the payloads signed with the hand-made rows' HMAC keys"
