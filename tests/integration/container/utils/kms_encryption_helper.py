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

"""Fixtures and helpers for the KMS Encryption Plugin container modules.

Each KMS module starts with::

    pytestmark = [pytest.mark.kms_encryption,
                  enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                      TestEnvironmentFeatures.KMS_LOCALSTACK])]

so that every other lane skips it, and ``conftest.py`` skips every other module in a KMS environment; a module that
needs a real key or Aurora names those features instead of ``KMS_LOCALSTACK``.
``conftest.py`` registers this module as a pytest plugin, so a module only names the fixtures as arguments.
The test user is the PostgreSQL superuser; on Docker MySQL the administrator is root, which has the test password,
and on Aurora it is the test user, the cluster's admin user.
Plugin connections use a DML-only application user (``kms_app_user``); the administrator only sets up and reads logs.
Connection failures become ``pytest.fail`` messages, because pytest's report prints a raising frame's arguments, and
the driver's connect frame holds the password.
The module's report hook prints the test password, each application user's password and a real key's account id and
key id as ``***`` in every report, captured output included, because CI logs are public.
"""

from __future__ import annotations

import io
import logging
import os
import re
import secrets
from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib.resources import files
from typing import (Any, Callable, Dict, Generator, Iterator, List, Optional,
                    Sequence, Set, Tuple)

import boto3
import django
import mysql.connector
import psycopg
import pytest
from django.conf import settings as django_settings

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.connection_source import \
    CallerConnectionSource
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.encryption.sql_dialect import SqlDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.properties import Properties
from .database_engine import DatabaseEngine
from .database_engine_deployment import DatabaseEngineDeployment
from .kms_encryption_schema import ddl_for
from .retry_helper import retry_until
from .test_environment import TestEnvironment
from .test_environment_features import TestEnvironmentFeatures

# The class of the driver connection each MySQL build must produce, so a missing C extension fails the run instead of
# running the pure build twice.
_MYSQL_CONNECTION_CLASS = {"mysql-cext": "CMySQLConnection", "mysql-pure": "MySQLConnection"}
_MYSQL_ROUTINES = ("FUNCTION hmac_sha256", "FUNCTION verify_encrypted_data_hmac",
                   "PROCEDURE validate_encrypted_data_hmac_before_insert",
                   "PROCEDURE validate_encrypted_data_hmac_before_update")
# The only AWS_* variables a LocalStack module keeps, so the plugin's session finds no credentials but the dummy ones.
_KEPT_AWS_VARIABLES = ("AWS_REGION", "AWS_DEFAULT_REGION")
# The trigger's rejection of a value that isn't a payload of the column's key, in PostgreSQL's and MySQL's messages.
TRIGGER_REJECTION = "does not carry a valid HMAC tag"
DB_ERRORS = (psycopg.Error, mysql.connector.Error)
# The values every test report prints as ``***``, added by the fixtures that come to know them.
_REDACTED: Set[str] = set()
# MySQL's "doesn't yet support" error, which Aurora MySQL raises for a session-level sql_log_off.
_ER_NOT_SUPPORTED_YET = 1235


@dataclass(frozen=True)
class KmsSettings:
    """Where KMS is: LocalStack (``endpoint_url`` set) or a real key (``key_arn`` set)."""
    region: str
    endpoint_url: Optional[str] = None
    key_arn: Optional[str] = None
    decrypt_only_role_arn: Optional[str] = None

    def props(self) -> Dict[str, str]:
        """The plugin's KMS connection properties."""
        props = {"encryption_kms_region": self.region}
        if self.endpoint_url:
            props["encryption_kms_endpoint"] = self.endpoint_url
        return props


@dataclass(frozen=True)
class DbUser:
    name: str
    password: str = field(repr=False)


def kms_builds(*excluded: str) -> pytest.MarkDecorator:
    """Parametrizes ``build`` with the current engine's builds: ``default`` on PostgreSQL, ``mysql-cext`` and
    ``mysql-pure`` on MySQL, less ``excluded``."""
    engine = TestEnvironment.get_current().get_engine()
    builds = ["default"] if engine == DatabaseEngine.PG else ["mysql-cext", "mysql-pure"]
    return pytest.mark.parametrize("build", [build for build in builds if build not in excluded])


def is_pg() -> bool:
    return TestEnvironment.get_current().get_engine() == DatabaseEngine.PG


def kms_client(settings: KmsSettings) -> Any:
    """A KMS client built the way the plugin builds its own."""
    config = EncryptionConfig.from_props(Properties(settings.props()), require_allowed_master_keys=False)
    return KeyManager.create_kms_client(boto3.Session(region_name=settings.region), config)


def encryption_config(settings: KmsSettings, schema: str, allowed_master_key_arns: Optional[str] = None
                      ) -> EncryptionConfig:
    props = Properties({**settings.props(), "encryption_metadata_schema": schema})
    if allowed_master_key_arns is None:
        return EncryptionConfig.from_props(props, require_allowed_master_keys=False)
    props["encryption_allowed_master_key_arns"] = allowed_master_key_arns
    return EncryptionConfig.from_props(props)


def plain_connect(user: Optional[DbUser] = None, *, admin: bool = False, build: str = "default") -> Any:
    """A driver connection without the wrapper: as ``user``, as the administrator, or else as the test user.

    The administrator is the test user, except on Docker MySQL, where it is root; on Aurora the test user is the
    cluster's admin user.
    """
    env = TestEnvironment.get_current()
    info, writer = env.get_database_info(), env.get_writer()
    if user is not None:
        name = user.name
    elif admin and not is_pg() and env.get_deployment() == DatabaseEngineDeployment.DOCKER:
        name = "root"
    else:
        name = info.get_username()
    try:
        if is_pg():
            return psycopg.connect(host=writer.get_host(), port=writer.get_port(), dbname=info.get_default_db_name(),
                                   user=name, password=user.password if user else info.get_password(),
                                   connect_timeout=10)
        return mysql.connector.connect(host=writer.get_host(), port=writer.get_port(),
                                       database=info.get_default_db_name(), user=name,
                                       password=user.password if user else info.get_password(),
                                       use_pure=build == "mysql-pure", connection_timeout=10)
    except (psycopg.Error, mysql.connector.Error) as err:
        failure = f"Couldn't connect as {name} to {writer.get_host()}:{writer.get_port()}: {type(err).__name__}: {err}"
    pytest.fail(failure)


def wrapper_connect(build: str, user: DbUser, settings: KmsSettings, schema: str, allowed_master_key_arns: str,
                    **props: Any) -> AwsWrapperConnection:
    """A connection through the wrapper with ``kms_encryption`` as its only plugin unless ``props`` says otherwise.

    On MySQL it checks that the driver connection is the build's class.
    """
    env = TestEnvironment.get_current()
    info, writer = env.get_database_info(), env.get_writer()
    props = {"plugins": "kms_encryption", "encryption_metadata_schema": schema,
             "encryption_allowed_master_key_arns": allowed_master_key_arns, **settings.props(), **props}
    try:
        if is_pg():
            conn = AwsWrapperConnection.connect(psycopg.Connection.connect, host=writer.get_host(),
                                                port=writer.get_port(), dbname=info.get_default_db_name(),
                                                user=user.name, password=user.password, **props)
        else:
            conn = AwsWrapperConnection.connect(mysql.connector.connect, host=writer.get_host(),
                                                port=writer.get_port(), database=info.get_default_db_name(),
                                                user=user.name, password=user.password,
                                                use_pure=build == "mysql-pure", **props)
    except (psycopg.Error, mysql.connector.Error, AwsWrapperError) as err:
        failure = (f"Couldn't connect as {user.name} through the wrapper ({build}) to "
                   f"{writer.get_host()}:{writer.get_port()}: {type(err).__name__}: {err}")
    else:
        driver_class = type(conn.target_connection).__name__
        if build not in _MYSQL_CONNECTION_CLASS or driver_class == _MYSQL_CONNECTION_CLASS[build]:
            return conn
        conn.close()
        failure = f"The {build} build connected with a {driver_class}, not a {_MYSQL_CONNECTION_CLASS[build]}."
    pytest.fail(failure)


def run_script(cur: Any, script: str) -> None:
    """Runs a MySQL script, DELIMITER lines included, in one execute(); the C extension needs every result read."""
    cur.execute(script)
    while cur.nextset():
        pass


def trigger_sql(schema: str) -> str:
    """The package's trigger SQL for the current engine, with its metadata schema replaced by ``schema``."""
    sql_folder = files("aws_advanced_python_wrapper.encryption") / "sql"
    if is_pg():
        return re.sub(r"\bencrypt(?=\.)", schema, (sql_folder / "trigger_pg.sql").read_text(encoding="utf-8"))
    return (sql_folder / "trigger_mysql.sql").read_text(encoding="utf-8").replace("SCHEMA_NAME", schema)


def install_trigger_sql(conn: Any, schema: str) -> None:
    """Installs the package's trigger SQL on ``conn`` (the test user's) and commits; on MySQL it first drops the
    routines a previous module installed."""
    with conn.cursor() as cur:
        if is_pg():
            cur.execute(trigger_sql(schema))  # no parameters, or psycopg parses the file's % signs
        else:
            for routine in _MYSQL_ROUTINES:
                cur.execute(f"DROP {routine.replace(' ', ' IF EXISTS ', 1)}")
            run_script(cur, trigger_sql(schema))
    conn.commit()


def create_column_trigger(conn: Any, table: str, column: str) -> None:
    """Creates the per-column trigger that calls the installed trigger SQL, and commits."""
    if is_pg():
        statements = [f"CREATE TRIGGER {table}_{column}_encrypted BEFORE INSERT OR UPDATE ON {table} "
                      f"FOR EACH ROW EXECUTE FUNCTION enforce_encrypted_column('{column}')"]
    else:
        statements = [f"CREATE TRIGGER {table}_{column}_hmac_insert BEFORE INSERT ON {table} FOR EACH ROW "
                      f"CALL validate_encrypted_data_hmac_before_insert('{table}', '{column}', NEW.{column})",
                      f"CREATE TRIGGER {table}_{column}_hmac_update BEFORE UPDATE ON {table} FOR EACH ROW "
                      f"CALL validate_encrypted_data_hmac_before_update('{table}', '{column}', NEW.{column}, "
                      f"OLD.{column})"]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()


def key_management_utility(conn: Any, settings: KmsSettings, schema: str) -> KeyManagementUtility:
    return KeyManagementUtility(conn, kms_client(settings), encryption_config(settings, schema))


def encrypt_column(conn: Any, settings: KmsSettings, schema: str, table: str, column: str,
                   master_key_arn: str) -> int:
    """Turns encryption on for ``table.column`` through KeyManagementUtility, commits, and returns the
    ``key_storage`` id."""
    key_storage_id = key_management_utility(conn, settings, schema).initialize_encryption_for_column(
        table, column, master_key_arn)
    conn.commit()
    return key_storage_id


def binary_type() -> str:
    """The binary column type the modules store payloads in."""
    return "BYTEA" if is_pg() else "VARBINARY(1024)"


def write(conn: Any, sql: Any, params: Any = None) -> None:
    """Runs one statement on ``conn`` and commits."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def writes(conn: Any, sql: str, params: Any) -> bool:
    """Whether ``conn`` writes ``params`` (the trigger accepts them); a rejected write is rolled back."""
    try:
        write(conn, sql, params)
    except DB_ERRORS:
        conn.rollback()
        return False
    return True


def read_column(conn: Any, sql: str, name: str) -> List[Any]:
    """The first column of each row ``sql`` returns for the rows named ``name``."""
    with conn.cursor() as cur:
        cur.execute(sql, (name,))
        return [row[0] for row in cur.fetchall()]


def raised(conn: Any, call: Callable[[], Any]) -> Optional[Exception]:
    """The error ``call`` raised, after a rollback that also discards an unread result, or None."""
    try:
        call()
    except Exception as err:  # noqa: BLE001 - the caller checks the class
        conn.rollback()
        return err
    return None


def create_table(table: str, columns: str) -> None:
    """Creates ``table`` with an id primary key and ``columns``, as the test user; a table a previous run left is
    dropped first."""
    id_type = "SERIAL PRIMARY KEY" if is_pg() else "INT AUTO_INCREMENT PRIMARY KEY"
    with plain_connect() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id {id_type}, {columns})")
        conn.commit()


def drop_table(table: str) -> None:
    with plain_connect() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()


@contextmanager
def encrypted_table(table: str, columns: str, encrypted: Sequence[str], settings: KmsSettings, schema: str,
                    master_key_arn: str, *, triggered: Sequence[str] = ()) -> Iterator[Dict[str, int]]:
    """Creates ``table`` (see ``create_table``), turns encryption on for each column in ``encrypted`` and puts the
    package's trigger on each column in ``triggered``; yields each encrypted column's ``key_storage`` id and drops the
    table at the end."""
    create_table(table, columns)
    try:
        with plain_connect() as conn:
            key_ids = {column: encrypt_column(conn, settings, schema, table, column, master_key_arn)
                       for column in encrypted}
            if triggered:
                install_trigger_sql(conn, schema)
            for column in triggered:
                create_column_trigger(conn, table, column)
        yield key_ids
    finally:
        drop_table(table)


class PayloadVerifier:
    """Opens stored payloads without the plugin: an administrator connection reads the key rows, and
    EncryptionService and a KeyManager of the verifier's own decrypt."""

    def __init__(self, settings: KmsSettings, schema: str, allowed_master_key_arns: str):
        self._conn = plain_connect(admin=True)
        self._conn.autocommit = True
        config = encryption_config(settings, schema, allowed_master_key_arns)
        source, dialect = CallerConnectionSource(self._conn), SqlDialect.for_connection(self._conn)
        # Never started, so every lookup reads the database.
        self._metadata = MetadataManager(source, dialect, config)
        self._cache = DataKeyCache(10, 60.0, start_cleanup_thread=False)
        self._keys = KeyManager(kms_client(settings), source, dialect, config, self._cache)

    def stored(self, sql: str, params: Any = None) -> List[Any]:
        """The first column of each row ``sql`` returns, as the server stores it."""
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return [row[0] for row in cur.fetchall()]

    def decode(self, table: str, column: str, payload: Any) -> Tuple[Optional[int], Any]:
        """The ``key_storage`` id that ``payload`` records and the value it decrypts to under that key row."""
        column_config = self._metadata.column_config(table, column)
        assert column_config is not None, f"{table}.{column} has no encryption_metadata row"
        data = bytes(payload)
        key_storage_id = EncryptionService.key_id_from_payload(data)
        key = None if key_storage_id is None else self._keys.key_metadata_by_id(key_storage_id)
        assert key is not None and key.hmac_key is not None, (
            f"the {table}.{column} payload records key_storage id {key_storage_id}, which has no usable key row")
        data_key = self._keys.decrypt_data_key(key.encrypted_data_key, key.master_key_arn)
        hmac_key = bytearray(key.hmac_key)
        try:
            return key_storage_id, EncryptionService.decrypt(data, data_key, hmac_key, column_config.algorithm)
        finally:
            EncryptionService.wipe(data_key)
            EncryptionService.wipe(hmac_key)

    def opened(self, table: str, column: str, name: str) -> List[Any]:
        """Each stored value of ``table.column`` in the rows named ``name``, opened: the ``key_storage`` id its payload
        records and the value, or None for a NULL."""
        sql = f"SELECT {column} FROM {table} WHERE name = %s ORDER BY id"
        return [None if payload is None else self.decode(table, column, payload)
                for payload in self.stored(sql, (name,))]

    def close(self) -> None:
        self._metadata.close()
        self._cache.close()
        self._conn.close()


def _statement_log(admin_conn: Any) -> List[bytes]:
    """The server's statement log, one entry per line, read once.

    The read's own statements stay out of the log: PostgreSQL's session sets ``log_statement = 'none'`` and MySQL's
    ``sql_log_off = 1``.
    """
    with admin_conn.cursor() as cur:
        if is_pg():
            cur.execute("SET log_statement = 'none'")
            cur.execute("SELECT pg_read_file(pg_current_logfile())")
            text = cur.fetchone()[0]
            if text is None:
                pytest.fail("PostgreSQL has no current log file; start it with logging_collector=on.")
            return [line.encode() for line in text.splitlines()]
        cur.execute("SET SESSION sql_log_off = 1")
        cur.execute("SELECT argument FROM mysql.general_log")
        return [value.encode() if isinstance(value, str) else bytes(value) for (value,) in cur.fetchall()]


def _log_marker(prefix: str) -> str:
    """Runs ``SELECT <marker>`` on a new administrator connection and returns the marker."""
    marker = prefix + secrets.token_hex(8)
    with plain_connect(admin=True) as writer, writer.cursor() as cur:
        cur.execute("SELECT %s", (marker,))
        cur.fetchall()
    return marker


def _wait_for_log_line(admin_conn: Any, marker: str) -> List[bytes]:
    """The statement log once it holds ``marker``, polled for up to 10 s: PostgreSQL's logging collector writes its
    file after the statement returns."""
    lines: List[bytes] = []

    def logged() -> bool:
        lines[:] = _statement_log(admin_conn)
        return any(marker.encode() in line for line in lines)
    if not retry_until(logged, timeout_sec=10, delay_sec=0.2):
        pytest.fail("The statement log doesn't hold the marker statement, so the log is off or unreadable.")
    return lines


def statement_log_lines(after: str) -> List[bytes]:
    """The statement log's lines after the last one that holds ``after``, once every statement before this call is
    in the log: it runs an end marker, waits for the log to show it, and stops there."""
    with plain_connect(admin=True) as reader:
        reader.autocommit = True
        end = _log_marker("kms_log_end_")
        lines = _wait_for_log_line(reader, end)
    starts = [i for i, line in enumerate(lines) if after.encode() in line]
    stops = [i for i, line in enumerate(lines) if end.encode() in line]
    if not starts:
        pytest.fail("The statement log no longer holds the test's start marker, so the scan can't cover the test.")
    return lines[starts[-1] + 1:stops[-1]]


def statement_log_scan(needles: Dict[str, str], after: str) -> Dict[str, int]:
    """For each needle, by its label, how many statement-log lines after the marker ``after`` hold it; the log is read
    once. Build each needle by concatenation, so the test's own source never holds it whole."""
    lines = statement_log_lines(after)
    return {label: sum(needle.encode() in line for line in lines) for label, needle in needles.items()}


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]
                              ) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """Prints each value in ``_REDACTED`` as ``***`` in the report: in the failure, whose traceback shows each frame's
    arguments (a connect frame's password, a fixture's key ARN), and in the captured output."""
    report = yield
    hidden = [value for value in _REDACTED if value]

    def redacted(text: str) -> str:
        for value in hidden:
            text = text.replace(value, "***")
        return text
    if hidden and report.longrepr is not None and not isinstance(report.longrepr, tuple):
        text = str(report.longrepr)
        if any(value in text for value in hidden):
            report.longrepr = redacted(text)
    if hidden:
        report.sections = [(title, redacted(content)) for title, content in report.sections]
    return report


# ---- Fixtures, registered by conftest.py ----

@pytest.fixture(scope="module")
def kms_settings() -> Iterator[KmsSettings]:
    """The KMS settings of a KMS environment, from ``KMS_ENDPOINT_URL``, ``KMS_REGION``, ``KMS_KEY_ARN`` and
    ``KMS_DECRYPT_ONLY_ROLE_ARN``. A missing value fails the module instead of skipping it.

    With LocalStack the module runs with dummy ``test`` keys and every other ``AWS_*`` variable removed.
    """
    localstack = TestEnvironmentFeatures.KMS_LOCALSTACK in TestEnvironment.get_current().get_features()
    required = ("KMS_ENDPOINT_URL", "KMS_REGION") if localstack else ("KMS_KEY_ARN", "KMS_REGION")
    missing = [name for name in required if not os.environ.get(name, "").strip()]
    if missing:
        pytest.fail(f"KMS environment without {', '.join(missing)}: set it to run the KMS encryption tests.")
    settings = KmsSettings(region=os.environ["KMS_REGION"].strip(),
                           endpoint_url=os.environ["KMS_ENDPOINT_URL"].strip() if localstack else None,
                           key_arn=None if localstack else os.environ["KMS_KEY_ARN"].strip(),
                           decrypt_only_role_arn=os.environ.get("KMS_DECRYPT_ONLY_ROLE_ARN", "").strip() or None)
    _REDACTED.add(TestEnvironment.get_current().get_database_info().get_password())
    if settings.key_arn is not None:
        # A real key's ARN, the role's and the role session ARNs KMS's errors name all hold the account id.
        _REDACTED.add(settings.key_arn.rsplit("/", 1)[-1])
        _REDACTED.update(arn.split(":")[4] for arn in (settings.key_arn, settings.decrypt_only_role_arn)
                         if arn is not None and arn.count(":") >= 5)
    with pytest.MonkeyPatch.context() as patch:
        if localstack:
            for name in [name for name in os.environ if name.startswith("AWS_") and name not in _KEPT_AWS_VARIABLES]:
                patch.delenv(name)
            for name, value in (("AWS_ACCESS_KEY_ID", "test"), ("AWS_SECRET_ACCESS_KEY", "test"),
                                ("AWS_CONFIG_FILE", os.devnull), ("AWS_SHARED_CREDENTIALS_FILE", os.devnull),
                                ("AWS_EC2_METADATA_DISABLED", "true")):
                patch.setenv(name, value)
        yield settings


@pytest.fixture(scope="module")
def kms_master_key_arn(request: pytest.FixtureRequest, kms_settings: KmsSettings) -> str:
    """The module's KMS key: on LocalStack a new key per module, on real KMS the administrator's key."""
    if kms_settings.key_arn is not None:
        return kms_settings.key_arn
    created = kms_client(kms_settings).create_key(Description=f"kms container test {request.module.__name__}")
    return created["KeyMetadata"]["Arn"]


@pytest.fixture(scope="module")
def kms_schema(request: pytest.FixtureRequest) -> Iterator[str]:
    """A metadata schema of the module's own (``kms_<module>_<random>``), made from the guide's DDL by the
    administrator, dropped before it is created and again at the end; on MySQL the test user gets DML on it."""
    module = request.module.__name__.rsplit(".", 1)[-1]
    short = module.replace("test_", "", 1).replace("kms_encryption", "").strip("_") or "module"
    schema = f"kms_{short[:40]}_{secrets.token_hex(4)}"
    drop = f"DROP SCHEMA IF EXISTS {schema} CASCADE" if is_pg() else f"DROP DATABASE IF EXISTS {schema}"
    with plain_connect(admin=True) as admin, admin.cursor() as cur:
        cur.execute(drop)
        if is_pg():
            cur.execute(ddl_for(DatabaseEngine.PG.value, schema))
        else:
            run_script(cur, ddl_for(DatabaseEngine.MYSQL.value, schema))
            cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {schema}.* TO %s",
                        (TestEnvironment.get_current().get_database_info().get_username(),))
        admin.commit()
    try:
        yield schema
    finally:
        with plain_connect(admin=True) as admin, admin.cursor() as cur:
            cur.execute(drop)
            admin.commit()


@pytest.fixture(scope="module")
def kms_app_user(kms_schema: str) -> Iterator[DbUser]:
    """A DML-only application user for plugin connections: DML on the database's tables, SELECT on the metadata
    schema. Its password is created here and kept out of both servers' statement logs and of every report.
    Aurora MySQL refuses a session-level ``sql_log_off``, so there the user is created only while the cluster keeps
    no general or slow query log; Aurora PostgreSQL's admin user is no superuser, so it takes the role before
    ``DROP OWNED BY``."""
    user = DbUser(f"kms_app_{secrets.token_hex(4)}", secrets.token_hex(16))
    _REDACTED.add(user.password)
    database = TestEnvironment.get_current().get_database_info().get_default_db_name()
    with plain_connect(admin=True) as admin, admin.cursor() as cur:
        if is_pg():
            cur.execute("SET log_statement = 'none'")
            cur.execute(f"CREATE ROLE {user.name} LOGIN PASSWORD '{user.password}'")  # the password is hex only
            for statement in (f"GRANT USAGE ON SCHEMA {kms_schema} TO {user.name}",
                              f"GRANT SELECT ON ALL TABLES IN SCHEMA {kms_schema} TO {user.name}",
                              f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {user.name}",
                              f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {user.name}",
                              f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                              f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {user.name}",
                              f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO {user.name}"):
                cur.execute(statement)
        else:
            try:
                cur.execute("SET SESSION sql_log_off = 1")
            except mysql.connector.Error as err:
                if err.errno != _ER_NOT_SUPPORTED_YET:
                    raise
                cur.execute("SELECT @@GLOBAL.general_log, @@GLOBAL.slow_query_log")
                if any(int(on) for on in cur.fetchall()[0]):
                    pytest.fail("The cluster keeps a general or slow query log, which would record the application "
                                "user's CREATE USER: turn both off to run the KMS Aurora tests.")
            cur.execute("CREATE USER %s IDENTIFIED BY %s", (user.name, user.password))
            cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {database}.* TO %s", (user.name,))
            cur.execute(f"GRANT SELECT ON {kms_schema}.* TO %s", (user.name,))
        admin.commit()
    try:
        yield user
    finally:
        with plain_connect(admin=True) as admin, admin.cursor() as cur:
            if is_pg():
                cur.execute(f"GRANT {user.name} TO CURRENT_USER")
                cur.execute(f"DROP OWNED BY {user.name}")
                cur.execute(f"DROP ROLE IF EXISTS {user.name}")
            else:
                cur.execute("DROP USER IF EXISTS %s", (user.name,))
            admin.commit()


@pytest.fixture(scope="module")
def kms_trigger_installed(kms_schema: str) -> None:
    """The package's trigger SQL, installed once for the module's schema."""
    with plain_connect() as conn:
        install_trigger_sql(conn, kms_schema)


@pytest.fixture
def kms_connect(build: str, kms_app_user: DbUser, kms_settings: KmsSettings, kms_schema: str,
                kms_master_key_arn: str) -> Callable[..., AwsWrapperConnection]:
    """Opens a plugin connection of the test's build as the application user, with extra connection properties."""
    def connect(**props: Any) -> AwsWrapperConnection:
        return wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, **props)
    return connect


@pytest.fixture(scope="session")
def kms_django_settings() -> None:
    """Django's settings for every KMS module that uses Django, configured once per process as
    ``test_django_basic.py`` does; the modules run the plugin only in aliases of their own."""
    if not django_settings.configured:
        env = TestEnvironment.get_current()
        info, writer = env.get_database_info(), env.get_writer()
        django_settings.configure(
            DEBUG=False,
            DATABASES={"default": {
                "ENGINE": "aws_advanced_python_wrapper.django.backends.mysql_connector",
                "NAME": info.get_default_db_name(),
                "USER": info.get_username(),
                "PASSWORD": info.get_password(),
                "HOST": writer.get_host(),  # the writer instance: a Docker server has no cluster endpoint
                "PORT": writer.get_port(),
                "OPTIONS": {"plugins": "failover_v2,aurora_connection_tracker", "connect_timeout": 10,
                            "autocommit": True},
            }},
            INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth"],
            DEFAULT_AUTO_FIELD="django.db.models.AutoField",
            SECRET_KEY="test-secret-key-for-django-tests",
            USE_TZ=True,
        )
    django.setup()


@pytest.fixture(scope="module")
def kms_verifier(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str) -> Iterator[PayloadVerifier]:
    verifier = PayloadVerifier(kms_settings, kms_schema, kms_master_key_arn)
    try:
        yield verifier
    finally:
        verifier.close()


@pytest.fixture
def kms_statement_log() -> Iterator[str]:
    """Yields a marker that a statement wrote after the fixture's negative check, once the log is seen to hold
    it. On MySQL it turns the general log on and restores its settings afterwards; PostgreSQL must start with
    ``logging_collector=on`` and ``log_statement=all``."""
    reader = plain_connect(admin=True)
    reader.autocommit = True
    saved = None
    try:
        if not is_pg():
            with reader.cursor() as cur:
                cur.execute("SELECT @@log_output, @@general_log")
                saved = cur.fetchone()
                cur.execute("SET GLOBAL log_output = 'TABLE', general_log = 'ON'")
        marker = "kms_log_" + secrets.token_hex(8)
        if any(marker.encode() in line for line in _statement_log(reader)):
            pytest.fail("The statement log holds the marker before its statement ran, so the scan matches itself.")
        with plain_connect(admin=True) as writer, writer.cursor() as cur:
            cur.execute("SELECT %s", (marker,))
            cur.fetchall()
        _wait_for_log_line(reader, marker)
        yield marker
    finally:
        if saved is not None:
            with reader.cursor() as cur:
                cur.execute("SET GLOBAL log_output = %s, general_log = %s", saved)
        reader.close()


@pytest.fixture
def kms_log_capture() -> Iterator[io.StringIO]:
    """Everything logged at DEBUG while the test runs, through the fixture's own handler on the root logger.

    It is the root logger's only handler meanwhile, so a data key that a positive control logs reaches no report.
    The harness runs pytest with ``-p no:logging``, so ``caplog`` doesn't exist there.
    """
    root = logging.getLogger()
    level, others = root.level, list(root.handlers)
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    for other in others:
        root.removeHandler(other)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        yield buffer
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
        for other in others:
            root.addHandler(other)
