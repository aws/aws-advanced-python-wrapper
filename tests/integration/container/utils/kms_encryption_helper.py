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

"""Fixtures and helpers for the KMS Encryption Plugin test classes, which inherit the fixtures from
``KmsEncryptionTestBase``.

The test user is the PostgreSQL superuser; on Docker MySQL the administrator is root, which has the test password,
and on Aurora it is the test user, the cluster's admin user.
Plugin connections use a DML-only application user (``kms_app_user``); the administrator only sets up and reads logs.
Connection failures become ``pytest.fail`` messages, because pytest's report prints a raising frame's arguments, and
the driver's connect frame holds the password.
The report hook in ``conftest.py`` prints each value passed to ``redact`` (the test password, each application user's
password, a real key's account id and key id) as ``***`` in the HTML and JUnit reports.
The live console output, which ``--capture=tee-sys`` copies, is not redacted.
"""

from __future__ import annotations

import io
import logging
import os
import secrets
from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib.resources import files
from typing import (Any, Callable, Dict, Iterator, List, NamedTuple, Optional,
                    Sequence, Set, Tuple)

import boto3
import django
import mysql.connector
import psycopg
import pytest
from botocore.client import BaseClient
from django.conf import settings as django_settings

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.connection_source import (
    CallerConnectionSource, PluginConnectionSource)
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.pg_driver_dialect import PgDriverDialect
from aws_advanced_python_wrapper.utils.properties import Properties
from .connection_utils import ConnectionUtils
from .database_engine import DatabaseEngine
from .driver_helper import DriverHelper
from .kms_encryption_schema import ddl_for
from .retry_helper import retry_until
from .test_driver import TestDriver
from .test_environment import TestEnvironment
from .test_environment_features import TestEnvironmentFeatures
from .test_timings import (KMS_STATEMENT_LOG_POLL_SEC,
                           KMS_STATEMENT_LOG_WAIT_SEC)

# The class of the driver connection each MySQL build must produce, so a missing C extension fails the run instead of
# running the pure build twice.
_MYSQL_CONNECTION_CLASS = {"mysql-cext": "CMySQLConnection", "mysql-pure": "MySQLConnection"}
_MYSQL_ROUTINES = ("FUNCTION hmac_sha256", "FUNCTION verify_encrypted_data_hmac",
                   "PROCEDURE validate_encrypted_data_hmac_before_insert",
                   "PROCEDURE validate_encrypted_data_hmac_before_update")
# The only AWS_* variables a LocalStack class keeps, so the plugin's session finds no credentials but the dummy ones.
_KEPT_AWS_VARIABLES = ("AWS_REGION", "AWS_DEFAULT_REGION")
# The trigger's rejection of a value that isn't a payload of the column's key, in PostgreSQL's and MySQL's messages.
TRIGGER_REJECTION = "does not carry a valid HMAC tag"
DB_ERRORS = (psycopg.Error, mysql.connector.Error)
# The values every test report prints as ``***``, added through ``redact`` by the fixtures that come to know them.
_REDACTED: Set[str] = set()
# MySQL's "doesn't yet support" error, which Aurora MySQL raises for a session-level sql_log_off.
_ER_NOT_SUPPORTED_YET = 1235
_CONNECT_TIMEOUT_SEC = 10
# The shortest text or bytes value whose plaintext PayloadVerifier looks for in the stored payload.
_PLAINTEXT_CHECK_MIN_BYTES = 4


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


def kms_builds(*, exclude: Sequence[str] = ()) -> pytest.MarkDecorator:
    """Parametrizes ``build`` with the current engine's builds: ``default`` on PostgreSQL, ``mysql-cext`` and
    ``mysql-pure`` on MySQL, less those in ``exclude``."""
    engine = TestEnvironment.get_current().get_engine()
    builds = ["default"] if engine == DatabaseEngine.PG else ["mysql-cext", "mysql-pure"]
    return pytest.mark.parametrize("build", [build for build in builds if build not in exclude])


def is_pg() -> bool:
    return TestEnvironment.get_current().get_engine() == DatabaseEngine.PG


def kms_test_driver() -> TestDriver:
    """The engine's sync driver, which every KMS connection uses (the KMS tasks run no async driver); ``build`` picks
    the MySQL driver's build."""
    return TestDriver.PG if is_pg() else TestDriver.MYSQL


def redact(*values: str) -> None:
    """Prints each value as ``***`` in every later test report."""
    _REDACTED.update(values)


def redact_report(report: pytest.TestReport) -> pytest.TestReport:
    """``report`` with each value given to ``redact`` printed as ``***``: in the failure, whose traceback shows each
    frame's arguments (a connect frame's password, a fixture's key ARN), and in the captured output."""
    hidden = [value for value in _REDACTED if value]
    if not hidden:
        return report

    def redacted(text: str) -> str:
        for value in hidden:
            text = text.replace(value, "***")
        return text

    if report.longrepr is not None and not isinstance(report.longrepr, tuple):
        text = str(report.longrepr)
        if any(value in text for value in hidden):
            report.longrepr = redacted(text)
    report.sections = [(title, redacted(content)) for title, content in report.sections]
    return report


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
    """A driver connection to the writer without the wrapper: as ``user``, as the administrator, or else as the test
    user. On MySQL ``build`` picks the build: the pure one for ``mysql-pure``, else the C extension."""
    conn_utils = ConnectionUtils()
    if user is not None:
        name, password = user.name, user.password
    elif admin:
        name, password = conn_utils.admin_user, conn_utils.password
    else:
        name, password = conn_utils.user, conn_utils.password
    test_driver = kms_test_driver()
    params = conn_utils.get_connect_params(user=name, password=password, test_driver=test_driver,
                                           use_pure=build == "mysql-pure")
    try:
        return DriverHelper.get_connect_func(test_driver)(**params, connect_timeout=_CONNECT_TIMEOUT_SEC)
    except DB_ERRORS as err:
        failure = f"Couldn't connect as {name} to {params['host']}:{params['port']}: {type(err).__name__}: {err}"
    pytest.fail(failure)


def wrapper_connect(build: str, user: DbUser, settings: KmsSettings, schema: str, allowed_master_key_arns: str,
                    **props: Any) -> AwsWrapperConnection:
    """A connection to the writer through the wrapper with ``kms_encryption`` as its only plugin unless ``props``
    says otherwise.

    On MySQL it checks that the driver connection is the build's class.
    """
    conn_utils = ConnectionUtils()
    test_driver = kms_test_driver()
    params = conn_utils.get_connect_params(user=user.name, password=user.password, test_driver=test_driver,
                                           use_pure=build == "mysql-pure")
    connect_props = {**params, "plugins": "kms_encryption", "encryption_metadata_schema": schema,
                     "encryption_allowed_master_key_arns": allowed_master_key_arns, **settings.props(), **props}
    try:
        conn = AwsWrapperConnection.connect(DriverHelper.get_connect_func(test_driver), **connect_props)
    except (psycopg.Error, mysql.connector.Error, AwsWrapperError) as err:
        failure = (f"Couldn't connect as {user.name} through the wrapper ({build}) to "
                   f"{params['host']}:{params['port']}: {type(err).__name__}: {err}")
    else:
        driver_class = type(conn.target_connection).__name__
        if build not in _MYSQL_CONNECTION_CLASS or driver_class == _MYSQL_CONNECTION_CLASS[build]:
            return conn
        conn.close()
        failure = f"The {build} build connected with a {driver_class}, not a {_MYSQL_CONNECTION_CLASS[build]}."
    pytest.fail(failure)


def drop_user(name: str) -> None:
    """Drops the database user ``name``, with everything it owns, when it exists."""
    with plain_connect(admin=True) as admin, admin.cursor() as cur:
        if is_pg():
            cur.execute("SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s", (name,))
            if cur.fetchone() is not None:
                # Aurora PostgreSQL's admin user is no superuser, so it takes the role before DROP OWNED BY.
                cur.execute(f"GRANT {name} TO CURRENT_USER")
                cur.execute(f"DROP OWNED BY {name}")
                cur.execute(f"DROP ROLE {name}")
        else:
            cur.execute("DROP USER IF EXISTS %s", (name,))
        admin.commit()


def run_script(cur: Any, script: str) -> None:
    """Runs a MySQL script, DELIMITER lines included, in one execute(); the C extension needs every result read."""
    cur.execute(script)
    while cur.nextset():
        pass


def trigger_sql(schema: str) -> str:
    """The package's trigger SQL for the current engine, with its metadata schema replaced by ``schema``."""
    sql_folder = files("aws_advanced_python_wrapper.encryption") / "sql"
    if is_pg():
        return (sql_folder / "trigger_pg.sql").read_text(encoding="utf-8").replace("SCHEMA_NAME", schema)
    return (sql_folder / "trigger_mysql.sql").read_text(encoding="utf-8").replace("SCHEMA_NAME", schema)


def install_trigger_sql(conn: Any, schema: str) -> None:
    """Installs the package's trigger SQL on ``conn`` (the test user's) and commits; on MySQL it first drops the
    routines a previous class installed."""
    with conn.cursor() as cur:
        if is_pg():
            # No parameters, or psycopg parses the file's % signs.
            cur.execute(trigger_sql(schema))
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
    """The binary column type the tests store payloads in."""
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


def assert_rejected(call: Callable[[], Any], message: str, conn: Any = None) -> str:
    """Runs ``call``, which the trigger must reject with ``message``; rolls ``conn`` back and returns the error."""
    with pytest.raises(DB_ERRORS, match=message) as caught:
        call()
    if conn is not None:
        conn.rollback()
    return str(caught.value)


def class_name(err: Optional[BaseException]) -> Optional[str]:
    return None if err is None else type(err).__name__


def error_chain(err: Optional[BaseException]) -> str:
    """The class of ``err``, or "AwsWrapperError from <class>" when failover_v2's execute() wrapped another error."""
    if type(err) is AwsWrapperError and err.__cause__ is not None:
        return f"AwsWrapperError from {type(err.__cause__).__name__}"
    return type(err).__name__


def outcome(err: Optional[BaseException]) -> str:
    """How a write ended: stored, rejected by the trigger (also when failover_v2 wrapped the driver's error), refused
    by the plugin, or another error by class and text."""
    if err is None:
        return "stored"
    if TRIGGER_REJECTION in str(err):
        return "rejected"
    if isinstance(err, AwsWrapperError):
        return "refused"
    return f"{type(err).__name__}: {err}"


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
        key_ids = {}
        with plain_connect() as conn:
            for column in encrypted:
                key_ids[column] = encrypt_column(conn, settings, schema, table, column, master_key_arn)
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
        config = encryption_config(settings, schema, allowed_master_key_arns)
        dialect = PgDriverDialect(Properties()) if is_pg() else MySQLDriverDialect(Properties())
        self._conn = plain_connect(admin=True)
        try:
            self._conn.autocommit = True
            source = CallerConnectionSource(self._conn)
            # Never started, so every lookup reads the database.
            self._metadata = MetadataManager(source, dialect, config)
            self._cache = DataKeyCache(10, 60.0, start_cleanup_thread=False)
            self._keys = KeyManager(kms_client(settings), source, dialect, config, self._cache)
        except Exception:
            self._conn.close()
            raise

    def stored(self, sql: str, params: Any = None) -> List[Any]:
        """The first column of each row ``sql`` returns, as the server stores it."""
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return [row[0] for row in cur.fetchall()]

    def decode(self, table: str, column: str, payload: Any) -> Tuple[Optional[int], Any]:
        """The ``key_storage`` id that ``payload`` records and the value it decrypts to under that key row."""
        column_config = self._metadata.column_config(table, column)
        if column_config is None:
            pytest.fail(f"{table}.{column} has no encryption_metadata row.")
        data = bytes(payload)
        key_storage_id = EncryptionService.key_id_from_payload(data)
        key = None if key_storage_id is None else self._keys.key_metadata_by_id(key_storage_id)
        if key is None or key.hmac_key is None:
            pytest.fail(f"The {table}.{column} payload records key_storage id {key_storage_id}, which has no usable "
                        "key row.")
        data_key = self._keys.decrypt_data_key(key.encrypted_data_key, key.master_key_arn)
        hmac_key = bytearray(key.hmac_key)
        try:
            value = EncryptionService.decrypt(data, data_key, hmac_key, column_config.algorithm)
        finally:
            EncryptionService.wipe(data_key)
            EncryptionService.wipe(hmac_key)
        plaintext = value.encode("utf-8") if isinstance(value, str) else value
        # Checked without the plugin's code; a shorter value can occur in the ciphertext by chance.
        if isinstance(plaintext, bytes) and len(plaintext) >= _PLAINTEXT_CHECK_MIN_BYTES and plaintext in data:
            pytest.fail(f"{table}.{column} stores the plaintext of a value.")
        return key_storage_id, value

    def opened(self, table: str, column: str, name: str) -> List[Any]:
        """Each stored value of ``table.column`` in the rows named ``name``, opened: the ``key_storage`` id its payload
        records and the value, or None for a NULL."""
        sql = f"SELECT {column} FROM {table} WHERE name = %s ORDER BY id"
        values: List[Any] = []
        for payload in self.stored(sql, (name,)):
            if payload is None:
                values.append(None)
            else:
                values.append(self.decode(table, column, payload))
        return values

    def close(self) -> None:
        self._metadata.close()
        self._cache.close()
        self._conn.close()


class RotationResult(NamedTuple):
    old_key_id: int
    new_key_id: int
    # The values a connection opened after the rotation reads, in row order.
    read: List[Any]
    # Each stored payload, opened as (key_storage id, value), in row order.
    decoded: List[Tuple[Optional[int], Any]]
    # The error of a write on a connection opened before the rotation.
    stale_error: Optional[Exception]
    # The error of an UPDATE, without the plugin, of another column of the row written before the rotation.
    touched_error: Optional[Exception]
    # The error of an UPDATE, without the plugin, that copies that row's previous-key payload into the newer row.
    previous_error: Optional[Exception]


def rotate_under_trigger(build: str, prefix: str, settings: KmsSettings, schema: str, master_key_arn: str,
                         user: DbUser, verifier: PayloadVerifier) -> RotationResult:
    """On a table of the build's own with the trigger on ``ssn`` (the trigger SQL must be installed): writes
    ``pt-<prefix>-old`` on a plugin connection, rotates the column's data key under ``master_key_arn``, writes
    ``pt-<prefix>-new`` on a new connection that then reads both, and tries ``pt-<prefix>-stale`` on the first
    connection. Then, without the plugin, it renames the old row and tries to copy the old row's payload into the new
    row. The table is dropped at the end."""
    table = f"kms_{prefix.replace('-', '_')}_{build.replace('-', '_')}"
    insert, select = f"INSERT INTO {table} (name, ssn) VALUES (%s, %s)", f"SELECT ssn FROM {table} ORDER BY id"
    create_table(table, f"name VARCHAR(100), ssn {binary_type()}")
    try:
        with plain_connect() as conn:
            old_key_id = encrypt_column(conn, settings, schema, table, "ssn", master_key_arn)
            create_column_trigger(conn, table, "ssn")
        with wrapper_connect(build, user, settings, schema, master_key_arn) as stale:
            write(stale, insert, ("old", f"pt-{prefix}-old"))
            with plain_connect() as conn:
                new_key_id = key_management_utility(conn, settings, schema).rotate_data_key(table, "ssn", master_key_arn)
                conn.commit()
            with wrapper_connect(build, user, settings, schema, master_key_arn) as fresh:
                write(fresh, insert, ("new", f"pt-{prefix}-new"))
                with fresh.cursor() as cur:
                    cur.execute(select)
                    read = [row[0] for row in cur.fetchall()]
            # Until its next metadata refresh, this connection writes with the old key.
            stale_error = raised(stale, lambda: write(stale, insert, ("stale", f"pt-{prefix}-stale")))
        (old_payload,) = verifier.stored(f"SELECT ssn FROM {table} WHERE name = %s", ("old",))
        with plain_connect(build=build) as conn:
            touched_error = raised(conn, lambda: write(conn, f"UPDATE {table} SET name = %s WHERE name = %s",
                                                       (f"{prefix}-old-touched", "old")))
            previous_error = raised(conn, lambda: write(conn, f"UPDATE {table} SET ssn = %s WHERE name = %s",
                                                        (bytes(old_payload), "new")))
        decoded = [verifier.decode(table, "ssn", payload) for payload in verifier.stored(select)]
    finally:
        drop_table(table)
    return RotationResult(old_key_id, new_key_id, read, decoded, stale_error, touched_error, previous_error)


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
    """The statement log once it holds ``marker``: PostgreSQL's logging collector writes its file after the statement
    returns."""
    lines: List[bytes] = []

    def logged() -> bool:
        lines[:] = _statement_log(admin_conn)
        return any(marker.encode() in line for line in lines)

    if not retry_until(logged, timeout_sec=KMS_STATEMENT_LOG_WAIT_SEC, delay_sec=KMS_STATEMENT_LOG_POLL_SEC):
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


class KmsEncryptionTestBase:
    """The KMS fixtures, which each KMS test class inherits."""

    @pytest.fixture(scope="class")
    def kms_settings(self) -> Iterator[KmsSettings]:
        """The KMS settings of a KMS environment, from ``KMS_ENDPOINT_URL``, ``KMS_REGION``, ``KMS_KEY_ARN`` and
        ``KMS_DECRYPT_ONLY_ROLE_ARN``. A missing value fails the class instead of skipping it.

        With LocalStack the class runs with dummy ``test`` keys and every other ``AWS_*`` variable removed.
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
        redact(ConnectionUtils().password)
        if settings.key_arn is not None:
            # A real key's ARN, the role's and the role session ARNs KMS's errors name all hold the account id.
            redact(settings.key_arn.rsplit("/", 1)[-1])
            for arn in (settings.key_arn, settings.decrypt_only_role_arn):
                if arn is not None and arn.count(":") >= 5:
                    redact(arn.split(":")[4])
        with pytest.MonkeyPatch.context() as patch:
            if localstack:
                for name in list(os.environ):
                    if name.startswith("AWS_") and name not in _KEPT_AWS_VARIABLES:
                        patch.delenv(name)
                for name, value in (("AWS_ACCESS_KEY_ID", "test"), ("AWS_SECRET_ACCESS_KEY", "test"),
                                    ("AWS_CONFIG_FILE", os.devnull), ("AWS_SHARED_CREDENTIALS_FILE", os.devnull),
                                    ("AWS_EC2_METADATA_DISABLED", "true")):
                    patch.setenv(name, value)
            yield settings

    @pytest.fixture(scope="class")
    def kms_master_key_arn(self, request: pytest.FixtureRequest, kms_settings: KmsSettings) -> str:
        """The class's KMS key: on LocalStack a new key per class, on real KMS ``KMS_KEY_ARN`` (an administrator's or the
        harness's)."""
        if kms_settings.key_arn is not None:
            return kms_settings.key_arn
        created = kms_client(kms_settings).create_key(Description=f"kms container test {request.module.__name__}")
        return created["KeyMetadata"]["Arn"]

    @pytest.fixture(scope="class")
    def kms_schema(self, request: pytest.FixtureRequest) -> Iterator[str]:
        """A metadata schema of the class's own (``kms_<module>_<random>``), made from the guide's DDL by the
        administrator, dropped before it is created and again at the end; on MySQL the test user gets DML on it."""
        module = request.module.__name__.rsplit(".", 1)[-1]
        short = module.replace("test_", "", 1).replace("kms_encryption", "").strip("_") or "module"
        schema = f"kms_{short[:40]}_{secrets.token_hex(4)}"
        drop = f"DROP SCHEMA IF EXISTS {schema} CASCADE" if is_pg() else f"DROP DATABASE IF EXISTS {schema}"
        try:
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                cur.execute(drop)
                if is_pg():
                    cur.execute(ddl_for(DatabaseEngine.PG.value, schema))
                else:
                    run_script(cur, ddl_for(DatabaseEngine.MYSQL.value, schema))
                    cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {schema}.* TO %s", (ConnectionUtils().user,))
                admin.commit()
            yield schema
        finally:
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                cur.execute(drop)
                admin.commit()

    @pytest.fixture(scope="class")
    def kms_app_user(self, kms_schema: str) -> Iterator[DbUser]:
        """A DML-only application user for plugin connections: DML on the database's tables, SELECT on the metadata
        schema. Its password is created here and kept out of both servers' statement logs and of every report.
        Aurora MySQL refuses a session-level ``sql_log_off``, so there the user is created only while the cluster keeps
        no general or slow query log."""
        user = DbUser(f"kms_app_{secrets.token_hex(4)}", secrets.token_hex(16))
        redact(user.password)
        database = ConnectionUtils().dbname
        try:
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                if is_pg():
                    cur.execute("SET log_statement = 'none'")
                    # The password is hex only.
                    cur.execute(f"CREATE ROLE {user.name} LOGIN PASSWORD '{user.password}'")
                    for statement in (f"GRANT USAGE ON SCHEMA {kms_schema} TO {user.name}",
                                      f"GRANT SELECT ON ALL TABLES IN SCHEMA {kms_schema} TO {user.name}",
                                      f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {user.name}",
                                      f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {user.name}",
                                      f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                                      f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {user.name}",
                                      f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES "
                                      f"TO {user.name}"):
                        cur.execute(statement)
                else:
                    try:
                        cur.execute("SET SESSION sql_log_off = 1")
                    except mysql.connector.Error as err:
                        if err.errno != _ER_NOT_SUPPORTED_YET:
                            raise
                        cur.execute("SELECT @@GLOBAL.general_log, @@GLOBAL.slow_query_log")
                        general_log, slow_query_log = cur.fetchall()[0]
                        if int(general_log) or int(slow_query_log):
                            pytest.fail("The cluster keeps a general or slow query log, which would record the "
                                        "application user's CREATE USER: turn both off to run the KMS Aurora tests.")
                    cur.execute("CREATE USER %s IDENTIFIED BY %s", (user.name, user.password))
                    cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {database}.* TO %s", (user.name,))
                    cur.execute(f"GRANT SELECT ON {kms_schema}.* TO %s", (user.name,))
                admin.commit()
            yield user
        finally:
            drop_user(user.name)

    @pytest.fixture(scope="class")
    def kms_trigger_installed(self, kms_schema: str) -> None:
        """The package's trigger SQL, installed once for the class's schema."""
        with plain_connect() as conn:
            install_trigger_sql(conn, kms_schema)

    @pytest.fixture(scope="class")
    def kms_verifier(self, kms_settings: KmsSettings, kms_schema: str,
                     kms_master_key_arn: str) -> Iterator[PayloadVerifier]:
        verifier = PayloadVerifier(kms_settings, kms_schema, kms_master_key_arn)
        try:
            yield verifier
        finally:
            verifier.close()

    @pytest.fixture(scope="class")
    def kms_django_settings(self) -> None:
        """Django's settings for every KMS class that uses Django, configured once per process as
        ``test_django_basic.py`` does; the classes run the plugin only in aliases of their own."""
        if not django_settings.configured:
            conn_utils = ConnectionUtils()
            django_settings.configure(
                DEBUG=False,
                DATABASES={"default": {
                    "ENGINE": "aws_advanced_python_wrapper.django.backends.mysql_connector",
                    "NAME": conn_utils.dbname,
                    "USER": conn_utils.user,
                    "PASSWORD": conn_utils.password,
                    # The writer instance: a Docker server has no cluster endpoint.
                    "HOST": conn_utils.writer_host,
                    "PORT": conn_utils.port,
                    "OPTIONS": {"plugins": "failover_v2,aurora_connection_tracker", "connect_timeout": 10,
                                "autocommit": True},
                }},
                INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth"],
                DEFAULT_AUTO_FIELD="django.db.models.AutoField",
                SECRET_KEY="test-secret-key-for-django-tests",
                USE_TZ=True,
            )
        django.setup()

    @pytest.fixture
    def kms_connect(self, build: str, kms_app_user: DbUser, kms_settings: KmsSettings, kms_schema: str,
                    kms_master_key_arn: str) -> Callable[..., AwsWrapperConnection]:
        """Opens a plugin connection of the test's build as the application user, with extra connection properties."""
        def connect(**props: Any) -> AwsWrapperConnection:
            return wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, **props)
        return connect

    @pytest.fixture
    def conn(self, kms_connect: Callable[..., AwsWrapperConnection]) -> Iterator[AwsWrapperConnection]:
        """A plugin connection of the test's build as the application user."""
        with kms_connect() as conn:
            yield conn

    @pytest.fixture
    def kms_statement_log(self) -> Iterator[str]:
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
    def kms_log_capture(self) -> Iterator[io.StringIO]:
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

    @pytest.fixture
    def plugin_connection_opens(self, monkeypatch: pytest.MonkeyPatch) -> List[Any]:
        """What ``observe_plugin_connection`` returns for each connection the plugin opens for itself during the test.

        ``PluginConnectionSource._open`` is private, but it is the one place where the plugin opens those connections.
        """
        opens: List[Any] = []
        open_connection = PluginConnectionSource._open

        def observed(source: PluginConnectionSource) -> Any:
            conn = open_connection(source)
            opens.append(self.observe_plugin_connection(conn))
            return conn

        monkeypatch.setattr(PluginConnectionSource, "_open", observed)
        return opens

    def observe_plugin_connection(self, conn: Any) -> Any:
        """What ``plugin_connection_opens`` records for a connection the plugin has just opened; a class that needs
        more than the connection overrides it."""
        return conn

    @pytest.fixture
    def kms_calls(self, monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Optional[str]]]:
        """Each KMS request this process sends while the test runs, one per attempt: the operation and its KeyId.

        ``BaseClient._make_api_call`` is private, but each KMS client, the plugin's included, sends every request
        through it.
        """
        calls: List[Tuple[str, Optional[str]]] = []
        make_api_call = BaseClient._make_api_call  # type: ignore[attr-defined]

        def recorded(client: Any, operation_name: str, api_params: Dict[str, Any]) -> Any:
            if client.meta.service_model.service_name == "kms":
                calls.append((operation_name, api_params.get("KeyId")))
            return make_api_call(client, operation_name, api_params)

        monkeypatch.setattr(BaseClient, "_make_api_call", recorded)
        return calls
