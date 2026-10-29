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

import logging
from typing import Any, List, Tuple
from unittest.mock import MagicMock, create_autospec

import boto3
import pytest
from botocore.stub import Stubber

from aws_advanced_python_wrapper.encryption import \
    key_management_utility as kmu_module
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.connection_source import (
    CallerConnectionSource, PluginConnectionSource)
from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from aws_advanced_python_wrapper.encryption.key_manager import (
    GeneratedDataKey, KeyManager)
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.encryption.models import (
    ColumnEncryptionConfig, KeyMetadata, mask_arn)
from aws_advanced_python_wrapper.encryption.sql_dialect import (MYSQL, PG,
                                                                SqlDialect,
                                                                fetch_rows)
from aws_advanced_python_wrapper.errors import (EncryptionConfigError,
                                                InvalidAlgorithmError,
                                                KeyManagementError)

ARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
ALIAS_ARN = "arn:aws:kms:us-east-1:123456789012:alias/python-kms-encryption-1700000000000"
ROLE_SESSION_ARN = "arn:aws:sts::123456789012:assumed-role/kiro-admin-role/kiro-admin-session"


class RecordingConnection:
    """A raw-driver-shaped connection that records SQL and must never be
    committed, rolled back, or closed by the utility."""

    def __init__(self):
        self.executed: List[Tuple[str, Tuple[Any, ...]]] = []
        self.rowcount = 1
        self.rows: List[Tuple[Any, ...]] = []
        self.commit = MagicMock(side_effect=AssertionError("utility must not commit"))
        self.rollback = MagicMock(side_effect=AssertionError("utility must not roll back"))
        self.close = MagicMock(side_effect=AssertionError("utility must not close"))

    def cursor(self):
        conn = self

        class Cursor:
            description = [("table_name",), ("column_name",)]

            def execute(self, sql, params=()):
                conn.executed.append((sql, tuple(params)))
                self.rowcount = conn.rowcount

            def fetchall(self):
                return conn.rows

            def close(self):
                pass

        return Cursor()


@pytest.fixture
def conn():
    return RecordingConnection()


def config(restricted=False):
    return EncryptionConfig(kms_region="us-east-1",
                            allowed_master_key_arns=frozenset({ARN}) if restricted else frozenset())


def utility(conn, restricted=False, engine="postgresql"):
    kms = MagicMock()
    u = KeyManagementUtility(conn, kms, config(restricted), engine=engine)
    # Autospec, so a misspelled method fails here as it would in production.
    u._key_manager = create_autospec(KeyManager, instance=True)
    u._key_manager.generate_data_key.return_value = GeneratedDataKey(
        bytearray(b"P" * 32), "ZWRr", b"h" * 32, "AES_256")
    u._key_manager.store_key_metadata.side_effect = lambda km: KeyMetadata(**{**km.__dict__, "id": 11})
    u._metadata = create_autospec(MetadataManager, instance=True)
    u._metadata.is_column_encrypted.return_value = False
    return u, kms


class TestInitialize:
    def test_generates_stores_and_upserts_metadata(self, conn):
        u, _ = utility(conn)
        assert u.initialize_encryption_for_column("users", "ssn", ARN) == 11
        stored = u._key_manager.store_key_metadata.call_args[0][0]
        assert stored.master_key_arn == ARN and stored.key_spec == "AES_256"
        assert stored.key_name.startswith("key-users-ssn-")
        sql, params = conn.executed[-1]
        assert sql.startswith("INSERT INTO encrypt.encryption_metadata")
        assert "ON CONFLICT (table_name, column_name) DO UPDATE SET encryption_algorithm = " \
               "EXCLUDED.encryption_algorithm, key_id = EXCLUDED.key_id, updated_at = CURRENT_TIMESTAMP" in sql
        assert params == ("users", "ssn", "AES-256-GCM", 11)

    def test_wipes_the_generated_plaintext(self, conn):
        u, _ = utility(conn)
        u.initialize_encryption_for_column("users", "ssn", ARN)
        generated = u._key_manager.generate_data_key.return_value
        assert generated.plaintext == bytearray(32)

    def test_wipes_the_plaintext_even_when_storing_fails(self, conn):
        u, _ = utility(conn)
        u._key_manager.store_key_metadata.side_effect = KeyManagementError("x")
        with pytest.raises(KeyManagementError):
            u.initialize_encryption_for_column("users", "ssn", ARN)
        assert u._key_manager.generate_data_key.return_value.plaintext == bytearray(32)

    def test_mysql_upsert_grammar(self, conn):
        u, _ = utility(conn, engine="mysql")
        u.initialize_encryption_for_column("users", "ssn", ARN, "AES-128-GCM")
        sql = conn.executed[-1][0]
        assert "ON DUPLICATE KEY UPDATE encryption_algorithm = VALUES(encryption_algorithm), " \
               "key_id = VALUES(key_id), updated_at = CURRENT_TIMESTAMP" in sql
        u._key_manager.generate_data_key.assert_called_with(ARN, "AES-128-GCM")

    def test_refuses_an_already_encrypted_column(self, conn):
        u, _ = utility(conn)
        u._metadata.is_column_encrypted.return_value = True
        with pytest.raises(KeyManagementError, match="already encrypted"):
            u.initialize_encryption_for_column("users", "ssn", ARN)
        u._key_manager.generate_data_key.assert_not_called()

    def test_unsupported_algorithm(self, conn):
        u, _ = utility(conn)
        with pytest.raises(InvalidAlgorithmError):
            u.initialize_encryption_for_column("users", "ssn", ARN, "DES")

    @pytest.mark.parametrize("args", [(None, "c", ARN), ("t", "", ARN), ("t", "c", None)])
    def test_required_arguments(self, conn, args):
        u, _ = utility(conn)
        with pytest.raises(ValueError):
            u.initialize_encryption_for_column(*args)

    def test_statement_failure_is_wrapped(self, conn):
        u, _ = utility(conn)
        conn.cursor = MagicMock(side_effect=RuntimeError("fk violation"))
        with pytest.raises(KeyManagementError, match="fk violation"):
            u.initialize_encryption_for_column("users", "ssn", ARN)


class TestRotate:
    def _current(self, u, arn=ARN, algorithm="AES-256-GCM"):
        km = KeyMetadata(id=4, master_key_arn=arn, encrypted_data_key="e", hmac_key=b"h")
        u._metadata.column_config.return_value = ColumnEncryptionConfig(
            "users", "ssn", algorithm, 4, km if arn else None)

    def test_rotate_onto_the_current_master_key_needs_the_allow_list(self, conn):
        u, _ = utility(conn, restricted=False)
        with pytest.raises(ValueError, match="encryption_allowed_master_key_arns"):
            u.rotate_data_key("users", "ssn")

    def test_rotate_keeping_the_current_master_key(self, conn):
        u, _ = utility(conn, restricted=True)
        self._current(u, algorithm="AES-128-GCM")
        assert u.rotate_data_key("users", "ssn") == 11
        u._key_manager.generate_data_key.assert_called_with(ARN, "AES-128-GCM")
        sql, params = conn.executed[-1]
        assert sql.startswith("UPDATE encrypt.encryption_metadata SET key_id = %s")
        assert params == (11, "users", "ssn")

    def test_rotate_onto_a_new_master_key(self, conn):
        u, _ = utility(conn)
        self._current(u)
        other = ARN.replace("1234abcd", "eeeeeeee")
        u.rotate_data_key("users", "ssn", other)
        u._key_manager.generate_data_key.assert_called_with(other, "AES-256-GCM")

    def test_rotate_unknown_column(self, conn):
        u, _ = utility(conn)
        u._metadata.column_config.return_value = None
        with pytest.raises(KeyManagementError, match="No kms_encryption configuration"):
            u.rotate_data_key("users", "ssn", ARN)

    def test_rotate_with_missing_key_row_and_no_new_arn(self, conn):
        u, _ = utility(conn, restricted=True)
        self._current(u, arn=None)
        with pytest.raises(KeyManagementError, match="no current master key"):
            u.rotate_data_key("users", "ssn")

    def test_rotate_fails_if_no_row_was_updated(self, conn):
        u, _ = utility(conn)
        self._current(u)
        conn.rowcount = 0
        with pytest.raises(KeyManagementError):
            u.rotate_data_key("users", "ssn", ARN)


class TestRemoveAndQuery:
    def test_remove_deletes_only_the_metadata_row(self, conn):
        u, _ = utility(conn)
        assert u.remove_encryption_for_column("users", "ssn") is True
        assert len(conn.executed) == 1
        sql, params = conn.executed[0]
        assert sql.startswith("DELETE FROM encrypt.encryption_metadata ")
        assert "key_storage" not in sql
        assert params == ("users", "ssn")

    def test_remove_reports_nothing_removed(self, conn):
        u, _ = utility(conn)
        conn.rowcount = 0
        assert u.remove_encryption_for_column("users", "ssn") is False

    def test_columns_using_key(self, conn):
        u, _ = utility(conn)
        conn.rows = [("orders", "card"), ("users", "ssn")]
        assert u.columns_using_key(4) == ["orders.card", "users.ssn"]
        assert conn.executed[0][1] == (4,)


class TestMasterKeys:
    def test_create_master_key_with_alias(self, conn):
        u, kms = utility(conn)
        u._key_manager.create_master_key.return_value = ARN
        assert u.create_master_key("d") == ARN
        alias = kms.create_alias.call_args.kwargs
        assert alias["AliasName"].startswith("alias/python-kms-encryption-")
        assert alias["TargetKeyId"] == ARN

    def test_alias_failure_is_not_fatal(self, conn):
        u, kms = utility(conn)
        u._key_manager.create_master_key.return_value = ARN
        kms.create_alias.side_effect = RuntimeError("AlreadyExists")
        assert u.create_master_key("d") == ARN

    # AWS KMS's own texts: they name the alias, and an access denial names
    # the caller's role session.
    @pytest.mark.parametrize("code, text", [
        pytest.param("AccessDeniedException",
                     f"User: {ROLE_SESSION_ARN} is not authorized to perform: kms:CreateAlias on resource: "
                     f"{ALIAS_ARN} because no identity-based policy allows the kms:CreateAlias action",
                     id="access-denied"),
        pytest.param("AlreadyExistsException", f"An alias with the name {ALIAS_ARN} already exists",
                     id="already-exists"),
    ])
    def test_alias_failure_warning_masks_the_arns_kms_quotes(self, conn, caplog, code, text):
        kms = boto3.client("kms", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")
        u = KeyManagementUtility(conn, kms, config(), engine="postgresql")
        u._key_manager = create_autospec(KeyManager, instance=True)
        u._key_manager.create_master_key.return_value = ARN
        with Stubber(kms) as stubber, caplog.at_level(logging.WARNING, logger=kmu_module.__name__):
            stubber.add_client_error("create_alias", code, service_message=text, http_status_code=400)
            assert u.create_master_key("d") == ARN
            stubber.assert_no_pending_responses()
        # By logger name: threads other tests leave behind also log into caplog.
        warnings = [r.getMessage() for r in caplog.records
                    if r.name == kmu_module.__name__ and r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert f"({code})" in warnings[0] and mask_arn(ARN) in warnings[0]
        assert "arn:aws:kms:***:***:***" in warnings[0]
        for identifier in (ARN, ALIAS_ARN, ROLE_SESSION_ARN, "123456789012", "kiro-admin-role", "kiro-admin-session"):
            assert identifier not in warnings[0]

    def test_no_alias(self, conn):
        u, kms = utility(conn)
        u._key_manager.create_master_key.return_value = ARN
        u.create_master_key("d", create_alias=False)
        kms.create_alias.assert_not_called()


class TestEngineInference:
    def test_requires_arguments(self, conn):
        with pytest.raises(ValueError):
            KeyManagementUtility(None, MagicMock(), config())

    def test_unknown_driver(self, conn):
        with pytest.raises(EncryptionConfigError):
            KeyManagementUtility(conn, MagicMock(), config())

    def test_infers_from_driver_module_and_wrapper_connection(self):
        pg_conn = type("Connection", (), {"__module__": "psycopg"})()
        mysql_conn = type("CMySQLConnection", (), {"__module__": "mysql.connector.connection_cext"})()
        wrapper = type("AwsWrapperConnection", (), {"target_connection": mysql_conn})()
        assert SqlDialect.for_connection(pg_conn) is PG
        assert SqlDialect.for_connection(mysql_conn) is MYSQL
        assert SqlDialect.for_connection(wrapper) is MYSQL
        assert SqlDialect.for_name("PostgreSQL") is PG

    def test_from_driver_dialect(self):
        assert SqlDialect.for_driver_dialect(MagicMock(dialect_code="psycopg")) is PG
        assert SqlDialect.for_driver_dialect(MagicMock(dialect_code="mysql-connector-python")) is MYSQL
        with pytest.raises(EncryptionConfigError):
            SqlDialect.for_driver_dialect(MagicMock(dialect_code="generic"))


class TestConnectionSources:
    def test_caller_source_yields_the_same_connection(self, conn):
        with CallerConnectionSource(conn).connection() as c:
            assert c is conn

    def test_plugin_source_opens_an_independent_autocommit_connection_and_closes_it(self):
        service = MagicMock()
        service.props = {"plugins": "kms_encryption", "user": "u"}
        raw = MagicMock()
        service.force_connect.return_value = raw
        plugin = object()
        with PluginConnectionSource(service, plugin).connection() as c:
            assert c is raw
            host, props, skip = service.force_connect.call_args.args
            assert host is service.current_host_info and skip is plugin
            assert props == service.props and props is not service.props
            service.driver_dialect.set_autocommit.assert_called_once_with(raw, True)
        service.driver_dialect.execute.assert_called_once()  # close routed through the dialect

    def test_plugin_source_closes_on_error(self):
        service = MagicMock()
        service.props = {}
        with pytest.raises(RuntimeError):
            with PluginConnectionSource(service).connection():
                raise RuntimeError("query failed")
        service.driver_dialect.execute.assert_called_once()


def test_fetch_rows_lower_cases_keys_and_handles_no_result():
    conn = RecordingConnection()
    conn.rows = [("t", "c")]
    assert fetch_rows(conn, "SELECT") == [{"table_name": "t", "column_name": "c"}]


def test_fetch_rows_accepts_dict_rows():
    # A connection opened with a dict-like row factory (psycopg dict_row)
    # returns mappings; their keys are lower-cased like tuple rows' names.
    conn = RecordingConnection()
    conn.rows = [{"TABLE_NAME": "t", "column_name": "c"}]  # type: ignore[list-item]
    assert fetch_rows(conn, "SELECT") == [{"table_name": "t", "column_name": "c"}]


def test_columns_using_key_with_dict_rows(conn):
    conn.rows = [{"table_name": "users", "column_name": "ssn"}]  # type: ignore[list-item]
    u, _ = utility(conn)
    assert u.columns_using_key(3) == ["users.ssn"]
