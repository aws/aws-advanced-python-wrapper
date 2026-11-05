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

"""The KMS Encryption Plugin on an Aurora cluster with a real KMS key.

IAM and Secrets Manager authentication, required TLS, the default parameter group and real topology changes exist
only on AWS.
The cluster's admin user is the test user; it sets up the metadata schema, the tables and the trigger, and the
plugin runs as a DML-only application user unless a test logs in another way.
Each connection the plugin opens for itself is observed at ``PluginConnectionSource._open``, as the server sees it.
"""

from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional
from uuid import uuid4

import boto3
import pytest

from aws_advanced_python_wrapper.encryption.connection_source import \
    PluginConnectionSource
from aws_advanced_python_wrapper.errors import MetadataError
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_features, enable_on_num_instances)
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.kms_encryption_helper import (TRIGGER_REJECTION, DbUser,
                                          PayloadVerifier, binary_type,
                                          create_column_trigger, create_table,
                                          drop_table, encrypt_column,
                                          install_trigger_sql, is_pg,
                                          kms_builds, plain_connect, raised,
                                          read_column, wrapper_connect, write)
from .utils.rds_test_utility import RdsTestUtility
from .utils.retry_helper import retry_until
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.kms_encryption_helper import KmsSettings

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY]),
              disable_on_features([TestEnvironmentFeatures.KMS_LOCALSTACK]),
              enable_on_deployments([DatabaseEngineDeployment.AURORA])]

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


@pytest.fixture(scope="module")
def table(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[int]:
    """The module's table, ``ssn`` encrypted under the real key; yields its ``key_storage`` id."""
    create_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}")
    with plain_connect() as conn:
        key_storage_id = encrypt_column(conn, kms_settings, kms_schema, _TABLE, "ssn", kms_master_key_arn)
    try:
        yield key_storage_id
    finally:
        drop_table(_TABLE)


@pytest.fixture(scope="module")
def iam_user(table: int, kms_schema: str) -> Iterator[str]:
    """The harness's IAM database user, granted what the plugin and the test need for the module.
    On MySQL the grants are revoked at the end, since a schema grant outlives ``DROP DATABASE``; PostgreSQL's go with
    the schema and the table."""
    info = TestEnvironment.get_current().get_info()
    name, database = info.get_iam_user_name(), info.get_database_info().get_default_db_name()
    revokes: List[str] = []
    if is_pg():
        statements = [f"GRANT USAGE ON SCHEMA {kms_schema} TO {name}",
                      f"GRANT SELECT ON ALL TABLES IN SCHEMA {kms_schema} TO {name}",
                      f"GRANT SELECT, INSERT, UPDATE ON {_TABLE} TO {name}",
                      f"GRANT USAGE, SELECT ON SEQUENCE {_TABLE}_id_seq TO {name}"]
    else:
        grants = [("SELECT", f"{kms_schema}.*"), ("SELECT, INSERT, UPDATE", f"{database}.{_TABLE}")]
        statements = [f"GRANT {privileges} ON {target} TO '{name}'@'%'" for privileges, target in grants]
        revokes = [f"REVOKE {privileges} ON {target} FROM '{name}'@'%'" for privileges, target in grants]
    with plain_connect() as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
        conn.commit()
    try:
        yield name
    finally:
        if revokes:
            with plain_connect() as conn, conn.cursor() as cur:
                for statement in revokes:
                    cur.execute(statement)
                conn.commit()


@pytest.fixture(scope="module")
def secret_arn() -> Iterator[str]:
    """A Secrets Manager secret of the module's own holding the admin user's credentials, deleted at the end, as
    test_aws_secrets_manager.py does."""
    env = TestEnvironment.get_current()
    info = env.get_info().get_database_info()
    client = boto3.client("secretsmanager", region_name=env.get_info().get_region())
    secret = client.create_secret(Name=f"TestSecret-kms-{uuid4()}", SecretString=json.dumps({
        "engine": "postgres" if is_pg() else "mysql", "dbname": info.get_default_db_name(),
        "host": info.get_cluster_endpoint(), _SECRET_USER_KEY: info.get_username(),
        _SECRET_PASSWORD_KEY: info.get_password()}))
    try:
        yield secret["ARN"]
    finally:
        client.delete_secret(SecretId=secret["ARN"], ForceDeleteWithoutRecovery=True)


@pytest.fixture(scope="module")
def guarded_table(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser
                  ) -> Iterator[int]:
    """A table whose ``ssn`` is encrypted and guarded by the package's trigger SQL, installed by the admin user;
    yields the ``key_storage`` id."""
    create_table(_GUARDED_TABLE, f"name VARCHAR(100), ssn {binary_type()}")
    with plain_connect() as conn:
        key_storage_id = encrypt_column(conn, kms_settings, kms_schema, _GUARDED_TABLE, "ssn", kms_master_key_arn)
        install_trigger_sql(conn, kms_schema)
        create_column_trigger(conn, _GUARDED_TABLE, "ssn")
    try:
        yield key_storage_id
    finally:
        drop_table(_GUARDED_TABLE)


@pytest.fixture
def plugin_sessions(monkeypatch: pytest.MonkeyPatch) -> List[Session]:
    """The server's view of each connection the plugin opens for itself during the test."""
    seen: List[Session] = []
    original = PluginConnectionSource._open

    def observed(source: PluginConnectionSource) -> Any:
        conn = original(source)
        seen.append(_session(conn))
        return conn

    monkeypatch.setattr(PluginConnectionSource, "_open", observed)
    return seen


@enable_on_features([TestEnvironmentFeatures.IAM, TestEnvironmentFeatures.SECRETS_MANAGER])
@kms_builds()
def test_plugin_connections_carry_the_applications_auth_and_tls(
        build: str, table: int, iam_user: str, secret_arn: str, plugin_sessions: List[Session],
        monkeypatch: pytest.MonkeyPatch, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
        kms_app_user: DbUser) -> None:
    # libpq reads PGSSLMODE only for a connection that doesn't set sslmode itself, so a plugin connection that lost
    # the application's setting goes without TLS, or is refused where the cluster forces it.
    monkeypatch.setenv("PGSSLMODE", "disable")
    # mysql-connector negotiates the newest TLS version both sides offer, so only a pinned older one shows a plugin
    # connection that lost the application's TLS settings.
    tls: Dict[str, Any] = {"sslmode": "require"} if is_pg() else {"ssl_disabled": False, "tls_versions": ["TLSv1.2"]}
    region = TestEnvironment.get_current().get_info().get_region()
    # The Secrets Manager plugin reads the secret's key names at each connect, so a plugin connection without the
    # secrets_manager_* settings looks for ``username`` in the secret and fails to log in.
    logins = {"password": (kms_app_user, {}),
              "secrets manager": (DbUser("IncorrectUser", "IncorrectPassword"),
                                  {"plugins": "aws_secrets_manager,kms_encryption",
                                   "secrets_manager_secret_id": secret_arn, "secrets_manager_region": region,
                                   "secrets_manager_secret_username_key": _SECRET_USER_KEY,
                                   "secrets_manager_secret_password_key": _SECRET_PASSWORD_KEY})}
    if build != "mysql-pure":  # the pure MySQL driver truncates IAM tokens (test_iam_authentication.py)
        logins["iam"] = (DbUser(iam_user, ""), {"plugins": "iam,kms_encryption"})
    seen: Dict[str, Any] = {}
    for login, (user, props) in logins.items():
        name, value = f"it65-{login.replace(' ', '-')}-{build}", f"pt-it65-{login}"
        plugin_sessions.clear()
        with wrapper_connect(build, user, kms_settings, kms_schema, kms_master_key_arn,
                             encryption_metadata_cache_enabled=False, **tls, **props) as conn:
            app = _session(conn.target_connection)
            write(conn, _INSERT, (name, value))
            read = read_column(conn, _SELECT, name)
        differing = [(s.user, s.tls, s.tls_version) for s in plugin_sessions
                     if (s.user, s.tls, s.tls_version) != (app.user, app.tls, app.tls_version)]
        seen[login] = (read, app.tls, len(plugin_sessions) > 0, differing)
    assert seen == {login: ([f"pt-it65-{login}"], True, True, []) for login in logins}, \
        "each login's value round-trips over TLS, and every connection the plugin opens for itself logs in as the " \
        "application's does, with the same TLS"


@enable_on_num_instances(min_instances=2)
@enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
@pytest.mark.xfail(is_pg(), strict=True, raises=MetadataError, reason=(
    "on psycopg the plugin's metadata lookup on the dead writer raises MetadataError before failover_v2 runs, so "
    "failover never starts"))
@kms_builds()
def test_failover_and_read_write_splitting_keep_encrypting(
        build: str, table: int, plugin_sessions: List[Session], kms_settings: KmsSettings, kms_schema: str,
        kms_master_key_arn: str, kms_app_user: DbUser) -> None:
    aurora = RdsTestUtility(TestEnvironment.get_current().get_info().get_region())
    before, during, after = (f"it66-{phase}-{build}" for phase in ("before", "during", "after"))
    initial_writer = aurora.get_cluster_writer_instance_id()
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                         plugins="read_write_splitting,failover_v2,kms_encryption", autocommit=True,
                         connect_timeout=10, socket_timeout=10, encryption_metadata_cache_enabled=False) as conn:
        write(conn, _INSERT, (before, "pt-it66-before"))
        aurora.failover_cluster_and_wait_until_writer_changed()
        # conftest does this before each test; here it lets the verifier and the module's teardown reach the new writer.
        TestEnvironment.get_current().get_database_info().move_instance_first(aurora.get_cluster_writer_instance_id())
        failover: Optional[BaseException] = None
        try:
            write(conn, _INSERT, (during, "pt-it66-during"))
        except Exception as err:  # noqa: BLE001 - the test asserts on the error
            failover = err
        plugin_sessions.clear()
        write(conn, _INSERT, (after, "pt-it66-after"))
        writer_lookups = sorted({s.instance for s in plugin_sessions})
        writer = aurora.query_instance_id(conn)
        # On two instances the only reader is the old writer, which takes a while to come back.
        aurora.make_sure_instances_up([initial_writer])

        def switched_to_a_reader() -> bool:
            conn.read_only = False
            conn.read_only = True
            return aurora.query_instance_id(conn) != writer

        # Past the 5 minutes for which the wrapper keeps the old writer marked unavailable after the failover.
        on_a_reader = retry_until(switched_to_a_reader, timeout_sec=390, delay_sec=5)
        expected = {before: ["pt-it66-before"], after: ["pt-it66-after"]}
        replicated = retry_until(lambda: {name: read_column(conn, _SELECT, name) for name in expected} == expected,
                                 timeout_sec=30, delay_sec=1)
        conn.read_only = False
    assert failover is not None and "pt-it66-during" not in f"{failover} {failover!r}", \
        "the statement that meets the failover raises an error that quotes no plaintext"
    assert writer != initial_writer and retry_until(lambda: aurora.is_db_instance_writer(writer)), \
        "after the failover the connection writes on the new writer"
    assert writer_lookups == [writer], "after the failover the plugin's lookups go to the new writer"
    assert (on_a_reader, replicated) == (True, True), \
        "with read_only set, a reader decrypts the values written before and after the failover"
    with closing(PayloadVerifier(kms_settings, kms_schema, kms_master_key_arn)) as verifier:
        opened = {name: verifier.opened(_TABLE, "ssn", name) for name in (before, after)}
    assert opened == {before: [(table, "pt-it66-before")], after: [(table, "pt-it66-after")]}, \
        "the writer stored both values encrypted under the column's key row"


@kms_builds("mysql-pure")
def test_round_trip_on_aurora(build: str, guarded_table: int, kms_settings: KmsSettings, kms_schema: str,
                              kms_master_key_arn: str, kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
    """The packaged trigger installs as the admin user, and a value round-trips on Aurora."""
    name, plain_name, value = f"it67-{build}", f"it67-plain-{build}", f"pt-it67-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, f"INSERT INTO {_GUARDED_TABLE} (name, ssn) VALUES (%s, %s)", (name, value))
        read = read_column(conn, f"SELECT ssn FROM {_GUARDED_TABLE} WHERE name = %s", name)
    assert (read, kms_verifier.opened(_GUARDED_TABLE, "ssn", name)) == ([value], [(guarded_table, value)]), \
        "a value written through the plugin is stored encrypted under the column's key row and reads back"
    with plain_connect() as conn:
        refused = raised(conn, lambda: write(conn, f"INSERT INTO {_GUARDED_TABLE} (name, ssn) VALUES (%s, %s)",
                                             (plain_name, value.encode())))
    assert TRIGGER_REJECTION in str(refused) and kms_verifier.stored(
        f"SELECT ssn FROM {_GUARDED_TABLE} WHERE name = %s", (plain_name,)) == [], \
        "the trigger refuses a plaintext write that doesn't come through the plugin"
