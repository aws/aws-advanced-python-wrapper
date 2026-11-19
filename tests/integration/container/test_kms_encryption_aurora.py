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

"""The cluster's admin user is the test user; it sets up the metadata schema, the tables and the trigger, and the
plugin runs as a DML-only application user unless a test logs in another way.
"""

from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple
from uuid import uuid4

import boto3
import pytest

from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.errors import MetadataError
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_engines, enable_on_features,
                               enable_on_num_instances)
from .utils.connection_utils import ConnectionUtils
from .utils.database_engine import DatabaseEngine
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.kms_encryption_helper import (TRIGGER_REJECTION, DbUser,
                                          KmsEncryptionTestBase,
                                          PayloadVerifier, binary_type,
                                          create_column_trigger, create_table,
                                          drop_table, encrypt_column,
                                          install_trigger_sql, is_pg,
                                          kms_builds, plain_connect, raised,
                                          read_column, rotate_under_trigger,
                                          wrapper_connect, write)
from .utils.rds_test_utility import RdsTestUtility
from .utils.retry_helper import retry_until
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import (KMS_AURORA_READER_RETURN_POLL_SEC,
                                 KMS_AURORA_READER_RETURN_SEC,
                                 KMS_AURORA_REPLICATION_POLL_SEC,
                                 KMS_AURORA_REPLICATION_WAIT_SEC)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import KmsSettings

_TABLE = "kms_aurora_users"
_GUARDED_TABLE = "kms_aurora_guarded"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
# The secret holds the credentials under names other than the plugin's defaults, ``username`` and ``password``.
_SECRET_USER_KEY, _SECRET_PASSWORD_KEY = "aurora_user", "aurora_password"


@dataclass(frozen=True)
class Session:
    """The server's view of one connection."""
    user: str
    tls: bool
    tls_version: str
    instance: str


def _session(conn: Any) -> Session:
    """How the server sees the driver connection ``conn``; it leaves no transaction open."""
    with conn.cursor() as cur:
        if is_pg():
            cur.execute("SELECT current_user, s.ssl, COALESCE(s.version, '') FROM pg_catalog.pg_stat_ssl s "
                        "WHERE s.pid = pg_catalog.pg_backend_pid()")
            user, tls, version = cur.fetchall()[0]
        else:
            cur.execute("SELECT CURRENT_USER()")
            user = str(cur.fetchall()[0][0]).split("@")[0]
            cur.execute("SHOW SESSION STATUS LIKE 'Ssl_version'")
            version = cur.fetchall()[0][1] or ""
            tls = bool(version)
        cur.execute(RdsTestUtility.get_instance_id_query())
        instance = cur.fetchall()[0][0]
    if not getattr(conn, "autocommit", True):
        conn.commit()
    return Session(str(user), bool(tls), str(version), str(instance))


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY])
@disable_on_features([TestEnvironmentFeatures.KMS_LOCALSTACK])
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
class TestKmsEncryptionAurora(KmsEncryptionTestBase):

    def observe_plugin_connection(self, conn: Any) -> Session:
        """Each connection the plugin opens for itself, as the server sees it."""
        return _session(conn)

    @pytest.fixture(scope="class")
    def table(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
              kms_app_user: DbUser) -> Iterator[int]:
        """The class's table, ``ssn`` encrypted under the real key; yields its ``key_storage`` id."""
        create_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}")
        try:
            with plain_connect() as conn:
                key_storage_id = encrypt_column(conn, kms_settings, kms_schema, _TABLE, "ssn", kms_master_key_arn)
            yield key_storage_id
        finally:
            drop_table(_TABLE)

    @pytest.fixture(scope="class")
    def iam_user(self, table: int, kms_schema: str) -> Iterator[str]:
        """The harness's IAM database user, granted what the plugin and the test need for the class.
        On MySQL the grants are revoked at the end, since a schema grant outlives ``DROP DATABASE``; PostgreSQL's go
        with the schema and the table."""
        conn_utils = ConnectionUtils()
        name = conn_utils.iam_user
        revokes: List[str] = []
        if is_pg():
            statements = [f"GRANT USAGE ON SCHEMA {kms_schema} TO {name}",
                          f"GRANT SELECT ON ALL TABLES IN SCHEMA {kms_schema} TO {name}",
                          f"GRANT SELECT, INSERT, UPDATE ON {_TABLE} TO {name}",
                          f"GRANT USAGE, SELECT ON SEQUENCE {_TABLE}_id_seq TO {name}"]
        else:
            grants = [("SELECT", f"{kms_schema}.*"), ("SELECT, INSERT, UPDATE", f"{conn_utils.dbname}.{_TABLE}")]
            statements = [f"GRANT {privileges} ON {target} TO '{name}'@'%'" for privileges, target in grants]
            revokes = [f"REVOKE {privileges} ON {target} FROM '{name}'@'%'" for privileges, target in grants]
        try:
            with plain_connect() as conn, conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
                conn.commit()
            yield name
        finally:
            if revokes:
                with plain_connect() as conn, conn.cursor() as cur:
                    for statement in revokes:
                        cur.execute(statement)
                    conn.commit()

    @pytest.fixture(scope="class")
    def secret_arn(self) -> Iterator[str]:
        """A Secrets Manager secret of the class's own holding the admin user's credentials, deleted at the end, as
        test_aws_secrets_manager.py does."""
        conn_utils = ConnectionUtils()
        client = boto3.client("secretsmanager", region_name=TestEnvironment.get_current().get_info().get_region())
        secret = client.create_secret(Name=f"TestSecret-kms-{uuid4()}", SecretString=json.dumps({
            "engine": "postgres" if is_pg() else "mysql", "dbname": conn_utils.dbname,
            "host": conn_utils.writer_cluster_host, _SECRET_USER_KEY: conn_utils.user,
            _SECRET_PASSWORD_KEY: conn_utils.password}))
        try:
            yield secret["ARN"]
        finally:
            client.delete_secret(SecretId=secret["ARN"], ForceDeleteWithoutRecovery=True)

    @pytest.fixture(scope="class")
    def guarded_table(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                      kms_app_user: DbUser) -> Iterator[int]:
        """A table whose ``ssn`` is encrypted and guarded by the package's trigger SQL, installed by the admin user;
        yields the ``key_storage`` id."""
        create_table(_GUARDED_TABLE, f"name VARCHAR(100), ssn {binary_type()}")
        try:
            with plain_connect() as conn:
                key_storage_id = encrypt_column(conn, kms_settings, kms_schema, _GUARDED_TABLE, "ssn",
                                                kms_master_key_arn)
                install_trigger_sql(conn, kms_schema)
                create_column_trigger(conn, _GUARDED_TABLE, "ssn")
            yield key_storage_id
        finally:
            drop_table(_GUARDED_TABLE)

    @enable_on_features([TestEnvironmentFeatures.IAM, TestEnvironmentFeatures.SECRETS_MANAGER])
    @kms_builds()
    def test_plugin_connections_keep_application_auth_and_tls(
            self, build: str, table: int, iam_user: str, secret_arn: str, plugin_connection_opens: List[Session],
            monkeypatch: pytest.MonkeyPatch, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_app_user: DbUser) -> None:
        # libpq reads PGSSLMODE only for a connection that doesn't set sslmode itself, so a plugin connection that lost
        # the application's setting goes without TLS, or is refused where the cluster forces it.
        monkeypatch.setenv("PGSSLMODE", "disable")
        # mysql-connector negotiates the newest TLS version both sides offer, so only a pinned older one shows a plugin
        # connection that lost the application's TLS settings.
        if is_pg():
            tls: Dict[str, Any] = {"sslmode": "require"}
        else:
            tls = {"ssl_disabled": False, "tls_versions": ["TLSv1.2"]}
        region = TestEnvironment.get_current().get_info().get_region()
        # The Secrets Manager plugin reads the secret's key names at each connect, so a plugin connection without the
        # secrets_manager_* settings looks for ``username`` in the secret and fails to log in.
        logins = {"password": (kms_app_user, {}),
                  "secrets manager": (DbUser("IncorrectUser", "IncorrectPassword"),
                                      {"plugins": "aws_secrets_manager,kms_encryption",
                                       "secrets_manager_secret_id": secret_arn, "secrets_manager_region": region,
                                       "secrets_manager_secret_username_key": _SECRET_USER_KEY,
                                       "secrets_manager_secret_password_key": _SECRET_PASSWORD_KEY})}
        # The pure MySQL driver truncates IAM tokens (test_iam_authentication.py).
        if build != "mysql-pure":
            logins["iam"] = (DbUser(iam_user, ""), {"plugins": "iam,kms_encryption"})
        reads: Dict[str, List[Any]] = {}
        app_tls: Dict[str, bool] = {}
        plugin_opened: Dict[str, bool] = {}
        differing: Dict[str, List[Tuple[str, bool, str]]] = {}
        for login, (user, props) in logins.items():
            name, value = f"login-{login.replace(' ', '-')}-{build}", f"pt-login-{login}"
            plugin_connection_opens.clear()
            with wrapper_connect(build, user, kms_settings, kms_schema, kms_master_key_arn,
                                 encryption_metadata_cache_enabled=False, **tls, **props) as conn:
                app = _session(conn.target_connection)
                write(conn, _INSERT, (name, value))
                reads[login] = read_column(conn, _SELECT, name)
            app_tls[login] = app.tls
            plugin_opened[login] = len(plugin_connection_opens) > 0
            differing[login] = []
            for session in plugin_connection_opens:
                if (session.user, session.tls, session.tls_version) != (app.user, app.tls, app.tls_version):
                    differing[login].append((session.user, session.tls, session.tls_version))
        assert reads == {login: [f"pt-login-{login}"] for login in logins}
        assert app_tls == dict.fromkeys(logins, True)
        assert plugin_opened == dict.fromkeys(logins, True)
        # Every connection the plugin opens for itself logs in as the application's does, with the same TLS.
        assert differing == {login: [] for login in logins}

    @enable_on_num_instances(min_instances=2)
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    @kms_builds()
    def test_failover_and_read_write_splitting_keep_encrypting(
            self, build: str, table: int, plugin_connection_opens: List[Session], kms_settings: KmsSettings,
            kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        aurora = RdsTestUtility(TestEnvironment.get_current().get_info().get_region())
        before, during, after = (f"failover-{phase}-{build}" for phase in ("before", "during", "after"))
        initial_writer = aurora.get_cluster_writer_instance_id()

        def connect() -> AwsWrapperConnection:
            return wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                   plugins="read_write_splitting,failover_v2,kms_encryption", autocommit=True,
                                   connect_timeout=10, socket_timeout=10, encryption_metadata_cache_enabled=False)

        conn = connect()
        try:
            write(conn, _INSERT, (before, "pt-failover-before"))
            aurora.failover_cluster_and_wait_until_writer_changed()
            # conftest does this before each test; here it lets the verifier, a new connection and the class's teardown
            # reach the new writer.
            TestEnvironment.get_current().get_database_info().move_instance_first(
                aurora.get_cluster_writer_instance_id())
            failover: Optional[BaseException] = None
            try:
                write(conn, _INSERT, (during, "pt-failover-during"))
            except Exception as err:  # noqa: BLE001 - the test asserts on the error
                failover = err
            plugin_connection_opens.clear()
            if is_pg():
                # On psycopg the plugin's metadata lookup can still reach the old writer and raise MetadataError before
                # failover_v2 runs, unless the old writer is already back. The guide: a MetadataError doesn't start
                # failover, so close the connection and open a new one.
                try:
                    write(conn, _INSERT, (after, "pt-failover-after"))
                except MetadataError:
                    conn.close()
                    conn = connect()
                    plugin_connection_opens.clear()
                    write(conn, _INSERT, (after, "pt-failover-after"))
            else:
                write(conn, _INSERT, (after, "pt-failover-after"))
            writer_lookups = sorted({session.instance for session in plugin_connection_opens})
            writer = aurora.query_instance_id(conn)
            # On two instances the only reader is the old writer, which takes a while to come back.
            aurora.make_sure_instances_up([initial_writer])

            def switched_to_a_reader() -> bool:
                conn.read_only = False
                conn.read_only = True
                return aurora.query_instance_id(conn) != writer

            on_a_reader = retry_until(switched_to_a_reader, timeout_sec=KMS_AURORA_READER_RETURN_SEC,
                                      delay_sec=KMS_AURORA_READER_RETURN_POLL_SEC)
            expected = {before: ["pt-failover-before"], after: ["pt-failover-after"]}

            def replicated_to_reader() -> bool:
                return {name: read_column(conn, _SELECT, name) for name in expected} == expected

            replicated = retry_until(replicated_to_reader, timeout_sec=KMS_AURORA_REPLICATION_WAIT_SEC,
                                     delay_sec=KMS_AURORA_REPLICATION_POLL_SEC)
            conn.read_only = False
        finally:
            conn.close()
        # The statement that meets the failover raises an error that quotes no plaintext.
        assert failover is not None
        assert "pt-failover-during" not in f"{failover} {failover!r}"
        # After the failover the connection writes on the new writer, and the plugin's lookups go there too.
        assert writer != initial_writer
        assert retry_until(lambda: aurora.is_db_instance_writer(writer)) is True
        assert writer_lookups == [writer]
        # With read_only set, a reader decrypts the values written before and after the failover.
        assert on_a_reader is True
        assert replicated is True
        with closing(PayloadVerifier(kms_settings, kms_schema, kms_master_key_arn)) as verifier:
            opened = {name: verifier.opened(_TABLE, "ssn", name) for name in (before, after)}
        assert opened == {before: [(table, "pt-failover-before")], after: [(table, "pt-failover-after")]}

    @kms_builds(exclude=("mysql-pure",))
    def test_round_trip_on_aurora(self, build: str, guarded_table: int, kms_settings: KmsSettings, kms_schema: str,
                                  kms_master_key_arn: str, kms_app_user: DbUser,
                                  kms_verifier: PayloadVerifier) -> None:
        name, plain_name, value = f"aurora-{build}", f"aurora-plain-{build}", f"pt-aurora-{build}"
        copy_name, empty_name = f"aurora-copy-{build}", f"aurora-empty-{build}"
        insert = f"INSERT INTO {_GUARDED_TABLE} (name, ssn) VALUES (%s, %s)"
        select = f"SELECT ssn FROM {_GUARDED_TABLE} WHERE name = %s"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, insert, (name, value))
            read = read_column(conn, select, name)
            # Without parameters the plugin passes an INSERT ... SELECT to the database, which copies the payload.
            write(conn, f"INSERT INTO {_GUARDED_TABLE} (name, ssn) SELECT '{copy_name}', ssn FROM {_GUARDED_TABLE} "
                        f"WHERE name = '{name}'")
            copied = read_column(conn, select, copy_name)
            # The shortest payload passes the trigger.
            write(conn, insert, (empty_name, ""))
            empty = read_column(conn, select, empty_name)
        assert read == [value]
        assert kms_verifier.opened(_GUARDED_TABLE, "ssn", name) == [(guarded_table, value)]
        assert copied == [value]
        assert kms_verifier.stored(select, (copy_name,)) == kms_verifier.stored(select, (name,))
        assert empty == [""]
        assert [len(bytes(payload)) for payload in kms_verifier.stored(select, (empty_name,))] == \
            [EncryptionService.MIN_ENCRYPTED_LENGTH]
        # The trigger refuses a plaintext write that doesn't come through the plugin.
        with plain_connect() as conn:
            refused = raised(conn, lambda: write(conn, insert, (plain_name, value.encode())))
        assert TRIGGER_REJECTION in str(refused)
        assert kms_verifier.stored(select, (plain_name,)) == []

    @kms_builds()
    def test_rotation_on_aurora_keeps_old_values_readable(self, build: str, kms_trigger_installed: None,
                                                          kms_settings: KmsSettings, kms_schema: str,
                                                          kms_master_key_arn: str, kms_app_user: DbUser,
                                                          kms_verifier: PayloadVerifier) -> None:
        result = rotate_under_trigger(build, "aurora-rotation", kms_settings, kms_schema, kms_master_key_arn,
                                      kms_app_user, kms_verifier)
        assert result.read == ["pt-aurora-rotation-old", "pt-aurora-rotation-new"]
        assert result.decoded == [(result.old_key_id, "pt-aurora-rotation-old"),
                                  (result.new_key_id, "pt-aurora-rotation-new")]
        # A connection opened before the rotation still writes with the old key, which the trigger rejects.
        assert TRIGGER_REJECTION in str(result.stale_error)
        # Without the plugin, an UPDATE that keeps the old row's payload passes, and one that copies it into the new
        # row is rejected; the decoded rows above show that the new row kept its own payload.
        assert result.touched_error is None
        assert TRIGGER_REJECTION in str(result.previous_error)

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_psycopg_named_cursor_decrypts_on_aurora(self, build: str, table: int, kms_settings: KmsSettings,
                                                     kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                     kms_verifier: PayloadVerifier) -> None:
        name = f"named-{build}"
        values = [f"pt-named-{i:03d}" for i in range(250)]
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            with conn.cursor() as cur:
                cur.executemany(_INSERT, [(name, value) for value in values])
            conn.commit()
            with conn.cursor(name="kms_named_aurora") as cur:
                # On the driver's cursor: the wrapper's doesn't pass attribute writes on.
                driver_cursor: Any = cur.target_cursor
                driver_cursor.itersize = 50
                cur.execute(_SELECT, (name,))
                rows = cur.fetchmany(100)
                # Two whole pages, so the loop ends where a page ends.
                rows += list(islice(cur, 100))
                rows += cur.fetchall()
            conn.commit()
        assert [row[0] for row in rows] == values
        assert [value for _, value in kms_verifier.opened(_TABLE, "ssn", name)] == values
