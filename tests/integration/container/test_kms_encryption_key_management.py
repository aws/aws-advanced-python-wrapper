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
import secrets
import uuid
from contextlib import closing
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import pytest
import sqlalchemy

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from aws_advanced_python_wrapper.errors import (IntegrityCheckError,
                                                MetadataError,
                                                UnauthorizedMasterKeyError)
from .utils.conditions import enable_on_features
from .utils.connection_utils import ConnectionUtils
from .utils.kms_encryption_helper import (DB_ERRORS, TRIGGER_REJECTION, DbUser,
                                          KmsEncryptionTestBase, KmsSettings,
                                          PayloadVerifier, assert_rejected,
                                          binary_type, create_column_trigger,
                                          encrypt_column, encryption_config,
                                          is_pg, key_management_utility,
                                          kms_builds, kms_client,
                                          plain_connect, raised, run_script,
                                          wrapper_connect, write, writes)
from .utils.kms_encryption_schema import ddl_for
from .utils.retry_helper import retry_until
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import (KMS_METADATA_REFRESH_POLL_SEC,
                                 KMS_METADATA_REFRESH_WAIT_SEC)


def _read(conn: Any, sql: str, params: Any = None) -> List[Any]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _insert(table: str, column: str = "ssn") -> str:
    return f"INSERT INTO {table} (name, {column}) VALUES (%s, %s)"


def _metadata_key_ids(verifier: PayloadVerifier, schema: str, table: str) -> List[Any]:
    return verifier.stored(f"SELECT key_id FROM {schema}.encryption_metadata WHERE table_name = %s "
                           "ORDER BY column_name", (table,))


def _refreshed(condition: Callable[[], bool]) -> bool:
    """Whether ``condition`` holds once a connection that refreshes every second has refreshed its metadata."""
    return retry_until(condition, timeout_sec=KMS_METADATA_REFRESH_WAIT_SEC, delay_sec=KMS_METADATA_REFRESH_POLL_SEC)


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionKeyManagement(KmsEncryptionTestBase):

    @pytest.fixture
    def new_table(self, kms_trigger_installed: None) -> Iterator[Callable[..., str]]:
        """Creates tables ``(id, name, ssn, card)``, ``ssn`` of ``ssn_type`` and ``card`` binary, with the trigger on
        both when ``triggered``; nothing is encrypted. Every table is dropped afterwards."""
        created: List[str] = []

        def create(ssn_type: Optional[str] = None, *, triggered: bool = False) -> str:
            name = f"kms_km_{secrets.token_hex(4)}"
            id_type = "SERIAL PRIMARY KEY" if is_pg() else "INT AUTO_INCREMENT PRIMARY KEY"
            with plain_connect() as conn:
                write(conn, f"CREATE TABLE {name} (id {id_type}, name VARCHAR(100), "
                            f"ssn {ssn_type or binary_type()}, card {binary_type()})")
                created.append(name)
                if triggered:
                    create_column_trigger(conn, name, "ssn")
                    create_column_trigger(conn, name, "card")
            return name

        yield create
        with plain_connect() as conn:
            for name in created:
                write(conn, f"DROP TABLE IF EXISTS {name}")

    @kms_builds(exclude=("mysql-pure",))
    def test_guide_metadata_ddl_creates_plugin_tables(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
        columns = kms_verifier.stored("SELECT concat(table_name, '.', column_name) FROM information_schema.columns "
                                      "WHERE table_schema = %s ORDER BY 1", (kms_schema,))
        metadata_columns = ("column_name", "created_at", "encryption_algorithm", "key_id", "table_name", "updated_at")
        key_storage_columns = ("created_at", "encrypted_data_key", "hmac_key", "id", "key_id", "key_spec",
                               "last_used_at", "master_key_arn", "name")
        expected = [f"encryption_metadata.{column}" for column in metadata_columns]
        expected += [f"key_storage.{column}" for column in key_storage_columns]
        assert columns == sorted(expected)
        table = new_table()
        with plain_connect(build=build) as conn:
            key_id = encrypt_column(conn, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        assert _metadata_key_ids(kms_verifier, kms_schema, table) == [key_id]
        with kms_connect() as conn:
            write(conn, _insert(table), ("ddl", "pt-ddl"))
            assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-ddl",)]
        (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
        assert kms_verifier.decode(table, "ssn", payload) == (key_id, "pt-ddl")

    @kms_builds()
    def test_guide_least_privilege_grants_suffice(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
            kms_verifier: PayloadVerifier) -> None:
        table = new_table(triggered=True)
        with plain_connect() as conn:
            encrypt_column(conn, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        with kms_connect() as conn:
            write(conn, _insert(table), ("app", "pt-least"))
            assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-least",)]

        if is_pg():
            refused = [f"ALTER TABLE {table} DISABLE TRIGGER {table}_ssn_encrypted",
                       f"DROP TRIGGER {table}_ssn_encrypted ON {table}"]
        else:
            refused = [f"DROP TRIGGER {table}_ssn_hmac_insert"]
        refused += [f"INSERT INTO {kms_schema}.key_storage (key_id, name, master_key_arn, encrypted_data_key, "
                    "hmac_key) VALUES ('k', 'k', 'arn', 'e', 'h')",
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
        assert kms_verifier.stored(f"SELECT name FROM {table} ORDER BY id") == ["app"]

    # Each metadata refresh test checks "not before" on a connection with the default refresh interval (300 s) and
    # "at refresh" on one that refreshes every second, so no check depends on where the refresh timer is.
    @kms_builds()
    def test_metadata_refresh_registers_column(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
        table = new_table(triggered=True)
        with plain_connect() as admin:
            encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        with kms_connect() as stale, kms_connect(encryption_metadata_cache_refresh_interval_sec=1) as refreshing:
            write(stale, _insert(table), ("stale-loaded", "pt-refresh-ssn"))
            write(refreshing, _insert(table), ("refreshing-loaded", "pt-refresh-ssn"))
            with plain_connect() as admin:
                card_key_id = encrypt_column(admin, kms_settings, kms_schema, table, "card", kms_master_key_arn)
            # Before its refresh, an open connection still treats the new column as plain, so the trigger rejects it.
            assert_rejected(lambda: write(stale, _insert(table, "card"), ("registered-early", "pt-refresh-early")),
                            TRIGGER_REJECTION, stale)
            assert _refreshed(lambda: writes(refreshing, _insert(table, "card"), ("registered", "pt-refresh-card")))
        (payload,) = kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'registered'")
        assert kms_verifier.decode(table, "card", payload) == (card_key_id, "pt-refresh-card")

    @kms_builds()
    def test_metadata_refresh_rotates_key(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
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
            # Before its refresh, an open connection still writes with the old key, which the trigger rejects.
            assert_rejected(lambda: write(stale, _insert(table, "card"), ("rotated-early", "pt-refresh-early")),
                            TRIGGER_REJECTION, stale)
            assert _refreshed(lambda: writes(refreshing, _insert(table, "card"), ("rotated", "pt-refresh-rotated")))
        (payload,) = kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'rotated'")
        assert kms_verifier.decode(table, "card", payload) == (rotated_key_id, "pt-refresh-rotated")

    @kms_builds()
    def test_metadata_refresh_removes_column(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
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
            # Before its refresh, an open connection still encrypts the removed column.
            assert EncryptionService.key_id_from_payload(bytes(early)) == card_key_id

            def stores_plaintext() -> bool:
                write(refreshing, f"DELETE FROM {table} WHERE name = 'removed'")
                write(refreshing, _insert(table, "card"), ("removed", b"pt-refresh-removed"))
                stored = kms_verifier.stored(f"SELECT card FROM {table} WHERE name = 'removed'")
                return stored == [b"pt-refresh-removed"]

            assert _refreshed(stores_plaintext)

    @kms_builds()
    def test_missing_or_unreadable_metadata_raises_metadata_error(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        table = new_table()
        late = f"{kms_schema}_late"
        engine = TestEnvironment.get_current().get_engine()
        drop = f"DROP SCHEMA IF EXISTS {late} CASCADE" if is_pg() else f"DROP DATABASE IF EXISTS {late}"
        try:
            with wrapper_connect(build, kms_app_user, kms_settings, late, kms_master_key_arn) as conn:
                with pytest.raises(MetadataError):
                    _read(conn, "SELECT 1")
                conn.rollback()
                with pytest.raises(MetadataError):
                    write(conn, _insert(table), ("missing", "pt-missing"))
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
                # Once the metadata tables exist, the same connection works.
                assert _read(conn, "SELECT 1") == [(1,)]
        finally:
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                cur.execute(drop)
                admin.commit()

        if is_pg():
            grant = f"SELECT ON ALL TABLES IN SCHEMA {kms_schema}"
            user = kms_app_user.name
        else:
            grant = f"SELECT ON {kms_schema}.*"
            user = f"'{kms_app_user.name}'"
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
                # Once SELECT on the metadata is granted, the connection works.
                assert _read(conn, "SELECT 1") == [(1,)]
        finally:
            with plain_connect(admin=True) as admin:
                write(admin, f"GRANT {grant} TO {user}")

    @kms_builds()
    def test_initialize_encryption_for_column_round_trip(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
        table = new_table()
        client = kms_client(kms_settings)
        calls: List[str] = []
        client.meta.events.register("before-call.kms.GenerateDataKey", lambda **_: calls.append("GenerateDataKey"))
        with plain_connect(build=build) as conn:
            utility = KeyManagementUtility(conn, client, encryption_config(kms_settings, kms_schema))
            key_id = utility.initialize_encryption_for_column(table, "ssn", kms_master_key_arn)
            conn.commit()
        assert calls == ["GenerateDataKey"]
        (algorithm,) = kms_verifier.stored(f"SELECT encryption_algorithm FROM {kms_schema}.encryption_metadata "
                                           "WHERE table_name = %s AND column_name = 'ssn'", (table,))
        (master_key_arn,) = kms_verifier.stored(f"SELECT master_key_arn FROM {kms_schema}.key_storage WHERE id = %s",
                                                (key_id,))
        # The verifier reads the algorithm from this row, so a wrong but consistent value would still decode.
        assert algorithm == EncryptionAlgorithm.DEFAULT
        assert master_key_arn == kms_master_key_arn

        (encrypted_data_key,) = kms_verifier.stored(
            f"SELECT encrypted_data_key FROM {kms_schema}.key_storage WHERE id = %s", (key_id,))
        (hmac_key,) = kms_verifier.stored(f"SELECT hmac_key FROM {kms_schema}.key_storage WHERE id = %s", (key_id,))
        plaintext = client.decrypt(CiphertextBlob=base64.b64decode(encrypted_data_key),
                                   KeyId=kms_master_key_arn)["Plaintext"]
        stored_text = encrypted_data_key.encode() + bytes(hmac_key)
        leaked = any(form in stored_text for form in (plaintext, base64.b64encode(plaintext), plaintext.hex().encode()))
        plaintext = b""
        # A boolean, so a failure prints no data key.
        assert leaked is False, "key_storage holds a plaintext data key"

        with kms_connect() as conn:
            write(conn, _insert(table), ("initialized", "pt-init"))
            assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-init",)]
        (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
        assert kms_verifier.decode(table, "ssn", payload) == (key_id, "pt-init")

    @kms_builds()
    def test_rotation_keeps_old_values_readable(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
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
                assert _read(fresh, f"SELECT ssn FROM {table} WHERE name = 'old'") == [("pt-rotate-old",)]
                write(fresh, _insert(table), ("new", "pt-rotate-new"))
            assert _refreshed(lambda: writes(open_conn, _insert(table), ("open", "pt-rotate-open")))
        decoded = [kms_verifier.decode(table, "ssn", payload)
                   for payload in kms_verifier.stored(f"SELECT ssn FROM {table} ORDER BY id")]
        assert new_key_id != old_key_id
        assert decoded == [(old_key_id, "pt-rotate-old"), (new_key_id, "pt-rotate-new"),
                           (new_key_id, "pt-rotate-open")]

    @kms_builds(exclude=("mysql-pure",))
    def test_create_master_key_symmetric_with_alias(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_app_user: DbUser) -> None:
        client = kms_client(kms_settings)
        with plain_connect(build=build) as conn:
            arn = key_management_utility(conn, kms_settings, kms_schema).create_master_key("kms container test key")
        metadata = client.describe_key(KeyId=arn)["KeyMetadata"]
        assert metadata["KeySpec"] == "SYMMETRIC_DEFAULT"
        assert metadata["KeyUsage"] == "ENCRYPT_DECRYPT"
        aliases: List[str] = []
        for page in client.get_paginator("list_aliases").paginate():
            for alias in page["Aliases"]:
                if alias.get("TargetKeyId") == metadata["KeyId"]:
                    aliases.append(alias["AliasName"])
        assert len(aliases) == 1
        assert aliases[0].startswith(KeyManagementUtility.ALIAS_PREFIX)

        table = new_table()
        with plain_connect() as conn:
            encrypt_column(conn, kms_settings, kms_schema, table, "ssn", arn)
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, arn) as conn:
            write(conn, _insert(table), ("new-key", "pt-master"))
            assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-master",)]

    @kms_builds()
    def test_utility_rollback_keeps_nothing(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
        table = new_table()
        key_rows = f"SELECT count(*) FROM {kms_schema}.key_storage"
        (key_rows_before,) = kms_verifier.stored(key_rows)
        with plain_connect(build=build) as conn:
            key_management_utility(conn, kms_settings, kms_schema).initialize_encryption_for_column(
                table, "ssn", kms_master_key_arn)
            conn.rollback()
        # The utility runs in the caller's transaction.
        assert _metadata_key_ids(kms_verifier, kms_schema, table) == []
        assert kms_verifier.stored(key_rows) == [key_rows_before]

    @kms_builds()
    def test_utility_in_driver_with_block(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
        table = new_table()
        with plain_connect(build=build) as conn:
            key_management_utility(conn, kms_settings, kms_schema).initialize_encryption_for_column(
                table, "ssn", kms_master_key_arn)
        # psycopg's with-block commits the utility's work; mysql-connector's closes without committing.
        if is_pg():
            assert len(_metadata_key_ids(kms_verifier, kms_schema, table)) == 1
        else:
            assert _metadata_key_ids(kms_verifier, kms_schema, table) == []

    @kms_builds()
    def test_utility_remove_encryption_for_column(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
        table = new_table()
        with plain_connect(build=build) as conn:
            key_id = encrypt_column(conn, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        if is_pg():
            triggers = kms_verifier.stored("SELECT count(*) FROM pg_trigger WHERE tgrelid = %s::regclass", (table,))
        else:
            triggers = kms_verifier.stored("SELECT count(*) FROM information_schema.TRIGGERS "
                                           "WHERE EVENT_OBJECT_TABLE = %s", (table,))
        # The utility creates no trigger.
        assert triggers == [0]

        with plain_connect(build=build) as conn:
            write(conn, f"INSERT INTO {kms_schema}.encryption_metadata (table_name, column_name, "
                        "encryption_algorithm, key_id) VALUES (%s, 'card', %s, %s)",
                  (table, EncryptionAlgorithm.DEFAULT, key_id))
            utility = key_management_utility(conn, kms_settings, kms_schema)
            assert utility.columns_using_key(key_id) == [f"{table}.card", f"{table}.ssn"]
            assert utility.remove_encryption_for_column(table, "card") is True
            conn.commit()
        assert kms_verifier.stored(f"SELECT column_name FROM {kms_schema}.encryption_metadata WHERE table_name = %s",
                                   (table,)) == ["ssn"]
        # The key row stays, so values already written still decrypt.
        assert kms_verifier.stored(f"SELECT count(*) FROM {kms_schema}.key_storage WHERE id = %s", (key_id,)) == [1]

    @kms_builds()
    def test_utility_on_sqlalchemy_raw_connection(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
        table = new_table()
        url = "postgresql+psycopg://" if is_pg() else "mysql+mysqlconnector://"
        engine = sqlalchemy.create_engine(url, creator=lambda: plain_connect(build=build))
        try:
            raw = engine.raw_connection()
            try:
                utility = key_management_utility(raw.driver_connection, kms_settings, kms_schema)
                utility.initialize_encryption_for_column(table, "ssn", kms_master_key_arn)
                raw.commit()
            finally:
                raw.close()
        finally:
            engine.dispose()
        assert len(_metadata_key_ids(kms_verifier, kms_schema, table)) == 1

    @kms_builds()
    def test_metadata_in_another_schema(self, build: str, new_table: Callable[..., str],
                                        kms_connect: Callable[..., Any], kms_settings: KmsSettings, kms_schema: str,
                                        kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
        if is_pg():
            schemas = "SELECT nspname FROM pg_namespace WHERE nspname = %s"
        else:
            schemas = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s"
        # The class's metadata is in its own schema, and there is no encrypt schema to fall back on.
        assert kms_schema != "encrypt"
        assert kms_verifier.stored(schemas, ("encrypt",)) == []
        if not is_pg():
            (body,) = kms_verifier.stored("SELECT ROUTINE_DEFINITION FROM information_schema.ROUTINES "
                                          "WHERE ROUTINE_SCHEMA = DATABASE() "
                                          "AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert'")
            assert f"{kms_schema}.encryption_metadata" in body
        table = new_table(triggered=True)
        with plain_connect() as admin:
            encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
            rotated_key_id = key_management_utility(admin, kms_settings, kms_schema).rotate_data_key(
                table, "ssn", kms_master_key_arn)
            admin.commit()
        with kms_connect() as conn:
            write(conn, _insert(table), ("other-schema", "pt-schema"))
            assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-schema",)]
        (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
        assert kms_verifier.decode(table, "ssn", payload)[0] == rotated_key_id
        # The trigger reads its key from the configured schema and rejects plaintext.
        with plain_connect(build=build) as conn:
            assert_rejected(lambda: write(conn, _insert(table), ("plain", b"pt-schema-plain-" + b"x" * 64)),
                            TRIGGER_REJECTION, conn)

    @kms_builds()
    def test_text_or_short_column_refuses_payload(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
        if is_pg():
            table = new_table("TEXT")
            with plain_connect() as admin:
                encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
            with kms_connect() as conn:
                write(conn, _insert(table), ("text", "pt-text"))
                with pytest.raises(IntegrityCheckError, match="is not binary"):
                    _read(conn, f"SELECT ssn FROM {table}")
            # A TEXT column stores the payload as hex text.
            (stored,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
            assert isinstance(stored, str)
            assert stored.startswith("\\x")
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
            assert kms_verifier.stored(f"SELECT name FROM {table}") == []

    @kms_builds()
    def test_hand_made_key_row_used_by_plugin_and_trigger(
            self, build: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
        table = new_table(triggered=True)
        blob = kms_client(kms_settings).generate_data_key(KeyId=kms_master_key_arn, KeySpec="AES_256")["CiphertextBlob"]
        wrapped = base64.encodebytes(blob).decode("ascii")
        # The wrapped form spans more than one 76-column line.
        assert "\n" in wrapped.strip()
        insert_key = (f"INSERT INTO {kms_schema}.key_storage (key_id, name, master_key_arn, encrypted_data_key, "
                      "hmac_key, key_spec) VALUES (%s, %s, %s, %s, %s, 'AES_256')")
        if is_pg():
            insert_key += " RETURNING id"
        key_ids = {}
        with plain_connect(build=build) as conn:
            for column, encoded in (("ssn", base64.b64encode(blob).decode("ascii")), ("card", wrapped)):
                with conn.cursor() as cur:
                    cur.execute(insert_key, (str(uuid.uuid4()), f"hand-made-{column}", kms_master_key_arn, encoded,
                                             secrets.token_bytes(32)))
                    if is_pg():
                        key_ids[column] = cur.fetchone()[0]
                    else:
                        key_ids[column] = cur.lastrowid
                    cur.execute(f"INSERT INTO {kms_schema}.encryption_metadata (table_name, column_name, "
                                "encryption_algorithm, key_id) VALUES (%s, %s, %s, %s)",
                                (table, column, EncryptionAlgorithm.DEFAULT, key_ids[column]))
            conn.commit()
        with kms_connect() as conn:
            write(conn, f"INSERT INTO {table} (name, ssn, card) VALUES (%s, %s, %s)",
                  ("hand-made", "pt-hand-ssn", "pt-hand-card"))
            # Hand-made key rows, base64 on one line and wrapped, read through the driver's column types.
            assert _read(conn, f"SELECT ssn, card FROM {table}") == [("pt-hand-ssn", "pt-hand-card")]
        decoded = [kms_verifier.decode(table, column, kms_verifier.stored(f"SELECT {column} FROM {table}")[0])
                   for column in ("ssn", "card")]
        # The trigger accepted the payloads signed with the hand-made rows' HMAC keys.
        assert decoded == [(key_ids["ssn"], "pt-hand-ssn"), (key_ids["card"], "pt-hand-card")]

    @kms_builds()
    def test_rotation_onto_new_master_key_keeps_values_readable(
            self, build: str, new_table: Callable[..., str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
        table = new_table(triggered=True)
        second = kms_client(kms_settings).create_key(Description="kms container test second master key")
        second_arn = second["KeyMetadata"]["Arn"]
        both = f"{kms_master_key_arn},{second_arn}"
        select = f"SELECT ssn FROM {table} WHERE name = %s"
        key_rows = f"SELECT count(*) FROM {kms_schema}.key_storage"
        with plain_connect() as admin:
            old_key_id = encrypt_column(admin, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, both) as conn:
            write(conn, _insert(table), ("old", "pt-master-old"))
        (key_rows_before,) = kms_verifier.stored(key_rows)
        # A utility whose allow-list lacks the new master key refuses the rotation before KMS is called.
        with plain_connect() as admin:
            old_only = KeyManagementUtility(admin, kms_client(kms_settings),
                                            encryption_config(kms_settings, kms_schema, kms_master_key_arn))
            refused = raised(admin, lambda: old_only.rotate_data_key(table, "ssn", second_arn))
        (key_rows_after_refusal,) = kms_verifier.stored(key_rows)
        with plain_connect() as admin:
            utility = KeyManagementUtility(admin, kms_client(kms_settings),
                                           encryption_config(kms_settings, kms_schema, both))
            new_key_id = utility.rotate_data_key(table, "ssn", second_arn)
            admin.commit()
        # The trigger accepts the write under the new master key's data key.
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, both) as conn:
            write(conn, _insert(table), ("new", "pt-master-new"))
            both_read = _read(conn, f"SELECT ssn FROM {table} ORDER BY id")
        reads: Dict[Tuple[str, str], Any] = {}
        for label, allowed in (("old key", kms_master_key_arn), ("new key", second_arn)):
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as conn:
                for name in ("old", "new"):
                    rows: List[Any] = []
                    err = raised(conn, lambda: rows.extend(_read(conn, select, (name,))))
                    reads[(label, name)] = rows if err is None else type(err)
        master_keys = {key_id: kms_verifier.stored(f"SELECT master_key_arn FROM {kms_schema}.key_storage WHERE id = %s",
                                                   (key_id,)) for key_id in (old_key_id, new_key_id)}
        with closing(PayloadVerifier(kms_settings, kms_schema, both)) as verifier:
            decoded = [verifier.decode(table, "ssn", payload)
                       for payload in verifier.stored(f"SELECT ssn FROM {table} ORDER BY id")]
        assert type(refused) is UnauthorizedMasterKeyError
        assert key_rows_after_refusal == key_rows_before
        # The old key row keeps the old master key.
        assert master_keys == {old_key_id: [kms_master_key_arn], new_key_id: [second_arn]}
        assert both_read == [("pt-master-old",), ("pt-master-new",)]
        assert decoded == [(old_key_id, "pt-master-old"), (new_key_id, "pt-master-new")]
        # Each value needs its own master key on the allow-list.
        assert reads == {("old key", "old"): [("pt-master-old",)], ("old key", "new"): UnauthorizedMasterKeyError,
                         ("new key", "old"): UnauthorizedMasterKeyError, ("new key", "new"): [("pt-master-new",)]}

    @kms_builds()
    @pytest.mark.parametrize("plugins", ["kms_encryption", ""], ids=["with-plugin", "without-plugin"])
    def test_utility_on_wrapper_connection_initializes_column(
            self, build: str, plugins: str, new_table: Callable[..., str], kms_connect: Callable[..., Any],
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_verifier: PayloadVerifier) -> None:
        table = new_table()
        conn_utils = ConnectionUtils()
        test_user = DbUser(conn_utils.user, conn_utils.password)
        with wrapper_connect(build, test_user, kms_settings, kms_schema, kms_master_key_arn,
                             plugins=plugins) as conn:
            key_id = key_management_utility(conn, kms_settings, kms_schema).initialize_encryption_for_column(
                table, "ssn", kms_master_key_arn)
            conn.commit()
        assert _metadata_key_ids(kms_verifier, kms_schema, table) == [key_id]
        with kms_connect() as conn:
            write(conn, _insert(table), ("wrapper-utility", "pt-wrapper-utility"))
            assert _read(conn, f"SELECT ssn FROM {table}") == [("pt-wrapper-utility",)]
        (payload,) = kms_verifier.stored(f"SELECT ssn FROM {table}")
        assert kms_verifier.decode(table, "ssn", payload) == (key_id, "pt-wrapper-utility")
