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

"""Live checks of the KMS Encryption Plugin on PostgreSQL 16, MySQL 8 and LocalStack's KMS, all on 127.0.0.1.

The module seeds both databases with the metadata tables, the trigger SQL the shared specification links and two
encrypted columns with key rows of their own: ``users.ssn``, which has the trigger, and ``users.dob``, which has none.
It first drops ``users``, ``addresses``, the ``encrypt`` schema and the trigger SQL's routines, and on MySQL turns on
``log_bin_trust_function_creators``, so point it only at servers it may reset. It runs the typical driver and ORM
shapes through the plugin, reads every stored value back with a plain driver, and counts plaintext in both servers'
own logs. It fails, never skips, when a server or a setting is missing, and reaches KMS only on LocalStack, with dummy
credentials. Give one run the two servers to itself: the log checks count every statement each server logs during the
run, and MySQL has one ``encrypt`` database per server, which every run drops and recreates whatever its ``DB_NAME``.
Run the module with::

    pytest tests/integration/test_kms_encryption_live.py

All of these environment variables are required. ``PG_PORT``, ``MYSQL_PORT`` and ``KMS_PORT`` are the local ports.
``DB_USER``, ``DB_PASSWORD`` and ``DB_NAME`` name a PostgreSQL superuser and a MySQL user with that password and
database; MySQL's root account uses ``DB_PASSWORD`` too. ``TRIGGER_SQL_DIR`` holds ``trigger_pg.sql`` and
``trigger_mysql.sql`` (see "Enforce encryption in the database" in
``docs/using-the-python-wrapper/using-plugins/UsingTheKmsEncryptionPlugin.md``). ``PG_LOG_COMMAND`` and
``MYSQL_LOG_COMMAND`` each hold a command that prints that server's statement log (PostgreSQL started with
``log_statement=all``, MySQL with its general log on); each is split into arguments and run without a shell. If
``AWS_PROFILE`` or ``AWS_DEFAULT_PROFILE`` is set, it must name a profile that exists, because importing the wrapper
reads it; the module removes both for its run. If SQLAlchemy or Django can't connect, pytest's failure report shows the
wrapper's connection properties, ``DB_PASSWORD`` included, so use a password that belongs to these local servers only.
"""

from __future__ import annotations

import contextlib
import copy
import io
import logging
import os
import re
import shlex
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum, IntEnum
from ipaddress import IPv4Address
from pathlib import Path
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List,
                    Optional, Set, Tuple, Type)

import boto3
import django
import mysql.connector
import psycopg
import pytest
from botocore.exceptions import (BotoCoreError, ClientError,
                                 EndpointConnectionError)
from django.conf import settings as django_settings
from django.db import connection as django_connection
from django.db import models
from django.test.utils import override_settings
from psycopg import sql as pgsql
from psycopg.rows import class_row, dict_row, namedtuple_row, scalar_row
from psycopg.types.json import Jsonb
from sqlalchemy import (Column, ForeignKey, Integer, MetaData, String, Table,
                        create_engine)
from sqlalchemy import exc as sa_exc
from sqlalchemy import insert, select, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import (DeclarativeBase, Session, joinedload,
                            mapped_column, relationship)
from sqlalchemy.types import UserDefinedType

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
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                EncryptionError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyMetadataError,
                                                KeyManagementError,
                                                MetadataError,
                                                UnauthorizedMasterKeyError)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.plugin_service import PluginManager
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from aws_advanced_python_wrapper.plugin_service import PluginService

_HOST = "127.0.0.1"
_REGION = "us-east-1"
_REQUIRED = ("PG_PORT", "MYSQL_PORT", "KMS_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "TRIGGER_SQL_DIR",
             "PG_LOG_COMMAND", "MYSQL_LOG_COMMAND")
_PORTS = ("PG_PORT", "MYSQL_PORT", "KMS_PORT")
_LOG_TIMEOUT = 120  # seconds for a log command
_STAMP = re.compile(r"kms_encryption trigger v(\d+)")
_INSERT = "INSERT INTO users (name, ssn) VALUES (%s, %s)"
_SELECT = "SELECT ssn FROM users WHERE name = %s ORDER BY id"
_INSERT_DOB = "INSERT INTO users (name, dob) VALUES (%s, %s)"
_SELECT_DOB = "SELECT dob FROM users WHERE name = %s ORDER BY id"
_COLUMN_SELECT = {"ssn": _SELECT, "dob": _SELECT_DOB}
_BUILDS = pytest.mark.parametrize("build", ["pg", "mysql-cext", "mysql-pure"])
_MYSQL_BUILDS = pytest.mark.parametrize("build", ["mysql-cext", "mysql-pure"])
_ENGINES = pytest.mark.parametrize("engine", ["pg", "mysql"])
_CURSOR_CLASS = {"mysql-cext": "CMySQLCursor", "mysql-pure": "MySQLCursor"}
_ENGINE = {"pg": "pg", "mysql-cext": "mysql", "mysql-pure": "mysql"}
_SERVER = {"pg": "PostgreSQL", "mysql": "MySQL"}

# The metadata DDL as the shared specification gives it.
_PG_METADATA_DDL = """
CREATE SCHEMA IF NOT EXISTS encrypt;

CREATE TABLE encrypt.key_storage (
    id SERIAL PRIMARY KEY,
    key_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key BYTEA NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
"""

_MYSQL_METADATA_DDL = """
CREATE SCHEMA IF NOT EXISTS encrypt;

CREATE TABLE encrypt.key_storage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    key_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key VARBINARY(32) NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
"""

_PG_TABLES = (
    "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, ssn BYTEA, dob BYTEA)",
    """CREATE TRIGGER users_ssn_encrypted
BEFORE INSERT OR UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION enforce_encrypted_column('ssn')""",
    "CREATE TABLE addresses (id SERIAL PRIMARY KEY, user_id INTEGER, email TEXT)",
)

_MYSQL_TABLES = (
    "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), ssn VARBINARY(1024), dob VARBINARY(1024))",
    """CREATE TRIGGER users_ssn_hmac_check
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END""",
    """CREATE TRIGGER users_ssn_hmac_check_update
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_update('users', 'ssn', NEW.ssn, OLD.ssn);
END""",
    "CREATE TABLE addresses (id INT AUTO_INCREMENT PRIMARY KEY, user_id INT, email VARCHAR(100))",
)

_MYSQL_RESET = ("DROP TABLE IF EXISTS addresses, users", "DROP DATABASE IF EXISTS encrypt",
                "DROP FUNCTION IF EXISTS hmac_sha256", "DROP FUNCTION IF EXISTS verify_encrypted_data_hmac",
                "DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_insert",
                "DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_update")


@dataclass(frozen=True)
class _Env:
    pg_port: int
    mysql_port: int
    kms_port: int
    user: str
    password: str = field(repr=False)
    dbname: str
    trigger_dir: Path
    pg_log_command: str = field(repr=False)
    mysql_log_command: str = field(repr=False)


@dataclass(frozen=True)
class _Bed:
    """The seeded databases: per engine, the master key ARN and ``key_storage`` id of ``users.ssn``, and what the
    standalone utility's DEBUG log showed while it gave ``users.dob`` its key rows (booleans only)."""
    env: _Env
    arns: Dict[str, str]
    key_ids: Dict[str, int]
    marker: str
    utility_log: Dict[str, Dict[str, bool]]


@pytest.fixture(scope="module", autouse=True)
def _localstack_only() -> Iterator[None]:
    # The plugin's AWS session reads the process environment, so this is what keeps ambient credentials out.
    with pytest.MonkeyPatch.context() as patch:
        for name, value in (("AWS_ACCESS_KEY_ID", "test"), ("AWS_SECRET_ACCESS_KEY", "test"),
                            ("AWS_CONFIG_FILE", os.devnull), ("AWS_SHARED_CREDENTIALS_FILE", os.devnull)):
            patch.setenv(name, value)
        for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
            patch.delenv(name, raising=False)
        yield


@pytest.fixture(scope="module")
def env() -> _Env:
    # Every value is read from os.environ by name, so no local holds the password and pytest's report can't show it.
    missing = [name for name in _REQUIRED if not os.environ.get(name, "").strip()]
    if missing:
        pytest.fail(f"Set {', '.join(missing)}: the module docstring says what each one holds.")
    not_ports = [name for name in _PORTS if not os.environ[name].strip().isdecimal()]
    if not_ports:
        pytest.fail(f"Set {', '.join(not_ports)} to a port number.")
    return _Env(pg_port=int(os.environ["PG_PORT"]), mysql_port=int(os.environ["MYSQL_PORT"]),
                kms_port=int(os.environ["KMS_PORT"]), user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
                dbname=os.environ["DB_NAME"], trigger_dir=Path(os.environ["TRIGGER_SQL_DIR"]),
                pg_log_command=os.environ["PG_LOG_COMMAND"], mysql_log_command=os.environ["MYSQL_LOG_COMMAND"])


def _plain(engine: str, env: _Env, *, root: bool = False) -> Any:
    """A driver connection without the wrapper."""
    # The driver's connect frame holds the password, and pytest's report prints a raising frame's arguments, so the
    # error is turned into a message here and the test fails outside the except block.
    port = env.pg_port if engine == "pg" else env.mysql_port
    try:
        if engine == "pg":
            return psycopg.connect(host=_HOST, port=port, user=env.user, password=env.password, dbname=env.dbname,
                                   connect_timeout=5)
        return mysql.connector.connect(host=_HOST, port=port, user="root" if root else env.user,
                                       password=env.password, database=env.dbname, connection_timeout=5)
    except (psycopg.Error, mysql.connector.Error) as err:
        failure = f"Couldn't connect to {_SERVER[engine]} on {_HOST}:{port}: {type(err).__name__}: {err}"
    pytest.fail(failure)


def _kms_props(env: _Env) -> Properties:
    return Properties({"encryption_kms_region": _REGION, "encryption_kms_endpoint": f"http://{_HOST}:{env.kms_port}"})


def _kms_client(env: _Env) -> Any:
    config = EncryptionConfig.from_props(_kms_props(env), require_allowed_master_keys=False)
    session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test", region_name=_REGION)
    return KeyManager.create_kms_client(session, config)


@pytest.fixture(scope="module", autouse=True)
def _services(env: _Env, _localstack_only: None) -> None:
    for engine in ("pg", "mysql"):
        _plain(engine, env).close()  # fails naming the server and its port
    try:
        _kms_client(env).list_keys()
        reached = True
    except (BotoCoreError, ClientError):
        reached = False
    if not reached:
        pytest.fail(f"LocalStack's KMS isn't reachable on {_HOST}:{env.kms_port}; start it before running this module.")


def _trigger_sql(env: _Env, name: str) -> str:
    path = env.trigger_dir / name
    if not path.is_file():
        pytest.fail(f"TRIGGER_SQL_DIR has no {name}.")
    return path.read_text(encoding="utf-8")


def _check_stamp(name: str, row: Any) -> None:
    if not _STAMP.search((row or [None])[0] or ""):
        pytest.fail(f"{name} installed without its version stamp; check TRIGGER_SQL_DIR.")


def _utility(conn: Any, env: _Env, allowed: Optional[str] = None) -> KeyManagementUtility:
    """A utility on a plain connection, with its KMS client built by ``create_kms_client`` as KeyManagementUtility's
    docstring shows."""
    props = _kms_props(env)
    if allowed is not None:
        props["encryption_allowed_master_key_arns"] = allowed
    config = EncryptionConfig.from_props(props, require_allowed_master_keys=False)
    return KeyManagementUtility(conn, _kms_client(env), config)


def _encrypt_column(conn: Any, env: _Env) -> Tuple[str, int]:
    """Turns encryption on for ``users.ssn`` under a new master key, then commits: the utility never does."""
    utility = _utility(conn, env)
    arn = utility.create_master_key("live module users.ssn")
    key_id = utility.initialize_encryption_for_column("users", "ssn", arn)
    conn.commit()
    return arn, key_id


def _encrypt_dob(engine: str, env: _Env, arn: str) -> Dict[str, bool]:
    """Gives ``users.dob`` its own key row under ``users.ssn``'s master key, with the root logger at DEBUG and no
    plugin connection opened yet in the process, then rotates it once with the plaintext filter removed. Returns
    booleans only: the captured text holds a data key, so it is never kept."""
    parsers = logging.getLogger("botocore.parsers")
    with _plain(engine, env) as conn:
        utility = _utility(conn, env)
        with _debug_log() as log:
            utility.initialize_encryption_for_column("users", "dob", arn)
        conn.commit()
        seen = {"plaintext": '"Plaintext"' in log.getvalue(),
                "kms-request": "TrentService.GenerateDataKey" in log.getvalue()}
        # The positive control: without the filter, generating the next data key logs it.
        removed = [f for f in parsers.filters if type(f).__name__ == "_NoKmsPlaintext"]
        for f in removed:
            parsers.removeFilter(f)
        try:
            with _debug_log() as log:
                utility.rotate_data_key("users", "dob", arn)
            conn.commit()
            seen.update({"filter-found": bool(removed), "control-plaintext": '"Plaintext"' in log.getvalue()})
        finally:
            for f in removed:
                parsers.addFilter(f)
    return seen


def _seed_pg(env: _Env) -> Tuple[str, int]:
    with _plain("pg", env) as conn:
        with conn.cursor() as cur:
            for statement in ("DROP TABLE IF EXISTS addresses, users CASCADE", "DROP SCHEMA IF EXISTS encrypt CASCADE",
                              "DROP FUNCTION IF EXISTS enforce_encrypted_column() CASCADE", _PG_METADATA_DDL):
                cur.execute(statement)
            cur.execute(_trigger_sql(env, "trigger_pg.sql"))  # no parameters, or psycopg parses the file's % signs
            cur.execute("SELECT obj_description('enforce_encrypted_column'::regproc, 'pg_proc')")
            _check_stamp("trigger_pg.sql", cur.fetchone())
            for statement in _PG_TABLES:
                cur.execute(statement)
        conn.commit()
        return _encrypt_column(conn, env)


def _run_script(cur: Any, script: str) -> None:
    # mysql-connector 9.7 runs a script, DELIMITER lines included, in one execute(); on its C extension every result
    # has to be read before the next statement.
    cur.execute(script)
    while cur.nextset():
        pass


def _seed_mysql(env: _Env) -> Tuple[str, int]:
    # Root runs only the administrator's steps; DB_USER does the rest. Without log_bin_trust_function_creators, a user
    # without SUPER can't create the routines or the triggers while binary logging is on.
    with _plain("mysql", env, root=True) as root, root.cursor() as cur:
        for statement in _MYSQL_RESET:
            cur.execute(statement)
        _run_script(cur, _MYSQL_METADATA_DDL)
        cur.execute("SET GLOBAL log_bin_trust_function_creators = 1")
        cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON encrypt.* TO %s", (env.user,))
    with _plain("mysql", env) as conn:
        with conn.cursor() as cur:
            # The file asks for SCHEMA_NAME to be replaced with the metadata schema.
            _run_script(cur, _trigger_sql(env, "trigger_mysql.sql").replace("SCHEMA_NAME", "encrypt"))
            cur.execute("SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() "
                        "AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert'")
            _check_stamp("trigger_mysql.sql", cur.fetchone())
            for statement in _MYSQL_TABLES:
                cur.execute(statement)
        conn.commit()
        return _encrypt_column(conn, env)


@pytest.fixture(scope="module")
def bed(env: _Env, _services: None) -> _Bed:
    seeds = {"pg": _seed_pg(env), "mysql": _seed_mysql(env)}
    utility_log = {engine: _encrypt_dob(engine, env, seed[0]) for engine, seed in seeds.items()}
    # The log checks count only what each server logs after this statement.
    marker = f"kms_live_{uuid.uuid4().hex}"
    for engine in seeds:
        with _plain(engine, env) as conn, conn.cursor() as cur:
            cur.execute("SELECT %s", (marker,))
            cur.fetchall()
    return _Bed(env, {e: s[0] for e, s in seeds.items()}, {e: s[1] for e, s in seeds.items()}, marker, utility_log)


def _plugin_props(engine: str, bed: _Bed) -> Dict[str, Any]:
    chain = WrapperProperties.DEFAULT_PLUGINS if engine == "pg" else WrapperProperties.MYSQL_CONNECTOR_DEFAULT_PLUGINS
    return {"plugins": f"kms_encryption,{chain}", "wrapper_dialect": engine,
            "encryption_allowed_master_key_arns": bed.arns[engine], **_kms_props(bed.env)}


def _connect(build: str, bed: _Bed, **overrides: Any) -> AwsWrapperConnection:
    # As in _plain: the wrapper's connect frames hold the password, so the test fails outside the except block.
    env, engine = bed.env, _ENGINE[build]
    props = {**_plugin_props(engine, bed), **overrides}
    port = env.pg_port if engine == "pg" else env.mysql_port
    try:
        if engine == "pg":
            return AwsWrapperConnection.connect(psycopg.Connection.connect, host=_HOST, port=port, user=env.user,
                                                password=env.password, dbname=env.dbname, **props)
        return AwsWrapperConnection.connect(mysql.connector.Connect, host=_HOST, port=port, user=env.user,
                                            password=env.password, database=env.dbname,
                                            use_pure=build == "mysql-pure", **props)
    except (psycopg.Error, mysql.connector.Error, AwsWrapperError) as err:
        failure = (f"Couldn't connect to {_SERVER[engine]} on {_HOST}:{port} through the wrapper ({build}): "
                   f"{type(err).__name__}: {err}")
    pytest.fail(failure)


class _Verifier:
    """Reads stored values with a plain driver and opens them with EncryptionService and a KeyManager of its own: a
    value counts as written only if a reader without the plugin finds a payload of the column's current key that
    decrypts to it."""

    def __init__(self, engine: str, bed: _Bed, column: str = "ssn"):
        self._conn = _plain(engine, bed.env)
        self._conn.autocommit = True
        self._select = _COLUMN_SELECT[column]
        config = EncryptionConfig.from_props(Properties(_kms_props(bed.env),
                                                        encryption_allowed_master_key_arns=bed.arns[engine]))
        source, dialect = CallerConnectionSource(self._conn), SqlDialect.for_connection(self._conn)
        metadata = MetadataManager(source, dialect, config)
        found = metadata.column_config("users", column)
        metadata.close()
        if (found is None or found.key_id is None or found.key_metadata is None
                or found.key_metadata.hmac_key is None):
            pytest.fail(f"users.{column} has no usable key row after seeding.")
        self._key_id = bed.key_ids[engine] if column == "ssn" else found.key_id
        self._key = found.key_metadata
        self._hmac_key = found.key_metadata.hmac_key
        self._algorithm = found.algorithm
        self._cache = DataKeyCache(10, 60.0, start_cleanup_thread=False)
        self._keys = KeyManager(_kms_client(bed.env), source, dialect, config, self._cache)

    def stored(self, name: str) -> List[Any]:
        with self._conn.cursor() as cur:
            cur.execute(self._select, (name,))
            return [row[0] for row in cur.fetchall()]

    def check(self, name: str, expected: List[Any]) -> None:
        stored = self.stored(name)
        assert len(stored) == len(expected)
        for value, want in zip(stored, expected):
            if want is None:
                assert value is None
                continue
            payload = bytes(value)
            assert len(payload) >= 65 and EncryptionService.key_id_from_payload(payload) == self._key_id
            data_key = self._keys.decrypt_data_key(self._key.encrypted_data_key, self._key.master_key_arn)
            hmac_key = bytearray(self._hmac_key)
            try:
                plain = EncryptionService.decrypt(payload, data_key, hmac_key, self._algorithm)
            finally:
                EncryptionService.wipe(data_key)
                EncryptionService.wipe(hmac_key)
            assert plain == want and type(plain) is type(want)

    def seal(self, value: Any) -> Optional[bytes]:
        """The payload a writer other than the plugin would store for ``value`` under this column's key."""
        data_key = self._keys.decrypt_data_key(self._key.encrypted_data_key, self._key.master_key_arn)
        hmac_key = bytearray(self._hmac_key)
        try:
            return EncryptionService.encrypt(value, data_key, hmac_key, self._algorithm, key_id=self._key_id)
        finally:
            EncryptionService.wipe(data_key)
            EncryptionService.wipe(hmac_key)

    def empty(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM addresses")
            cur.execute("DELETE FROM users")

    def close(self) -> None:
        self._cache.close()
        self._conn.close()


@pytest.fixture(scope="module")
def verifiers(bed: _Bed) -> Iterator[Dict[str, _Verifier]]:
    opened = {engine: _Verifier(engine, bed) for engine in ("pg", "mysql")}
    yield opened
    for verifier in opened.values():
        verifier.close()


@pytest.fixture(autouse=True)
def _empty_tables(verifiers: Dict[str, _Verifier]) -> None:
    # Each test writes the rows it reads, so no test depends on another one's.
    for verifier in verifiers.values():
        verifier.empty()


@dataclass(frozen=True)
class _Db:
    conn: AwsWrapperConnection
    verifier: _Verifier


@pytest.fixture
def db(build: str, bed: _Bed, verifiers: Dict[str, _Verifier]) -> Iterator[_Db]:
    with _connect(build, bed) as conn:
        if build in _CURSOR_CLASS:  # so a missing C extension fails instead of running the pure build twice
            with conn.cursor() as cur:
                driver_class = type(cur.target_cursor).__name__
            if driver_class != _CURSOR_CLASS[build]:
                pytest.fail(f"On {build} the driver's cursor is a {driver_class}, not a {_CURSOR_CLASS[build]}.")
        yield _Db(conn, verifiers[_ENGINE[build]])


def _write(conn: AwsWrapperConnection, sql: str, params: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def _read_one(conn: AwsWrapperConnection, name: str) -> Any:
    with conn.cursor() as cur:
        cur.execute(_SELECT, (name,))
        row = cur.fetchone()
        cur.fetchall()  # an unread row on mysql-connector's default cursor would fail the next statement
    return None if row is None else row[0]


def _write_many(conn: AwsWrapperConnection, name: str, as_generator: bool = False) -> List[str]:
    values = [f"pt-{name}-{i}" for i in range(3)]
    rows = [(name, value) for value in values]
    with conn.cursor() as cur:
        cur.executemany(_INSERT, (row for row in rows) if as_generator else rows)
    conn.commit()
    return values


# ---- DB-API: psycopg, and mysql-connector on its C extension and with use_pure=True ----

@_BUILDS
@pytest.mark.parametrize("sql, params, by_keyword, want", [
    pytest.param(_INSERT, ("d1", "pt-tuple-1"), False, "pt-tuple-1", id="insert-tuple"),
    pytest.param(_INSERT, ["d1", "pt-tuple-1"], False, "pt-tuple-1", id="caller-params-unchanged"),
    pytest.param("INSERT INTO users (name, ssn) VALUES (%(n)s, %(s)s)", {"n": "d1", "s": "pt-dict-2"}, False,
                 "pt-dict-2", id="insert-dict"),
    pytest.param(_INSERT, ("d1", "pt-kw-3"), True, "pt-kw-3", id="params-keyword"),
    pytest.param(_INSERT, ("d1", 42), False, 42, id="insert-int"),
    pytest.param(_INSERT, ("d1", None), False, None, id="insert-null")])
def test_write_stores_a_payload(db: _Db, sql: str, params: Any, by_keyword: bool, want: Any) -> None:
    caller = copy.copy(params)
    with db.conn.cursor() as cur:
        if by_keyword:
            cur.execute(sql, params=params)
        else:
            cur.execute(sql, params)
    db.conn.commit()
    assert params == caller
    value = _read_one(db.conn, "d1")
    assert value == want and type(value) is type(want)
    db.verifier.check("d1", [want])


@_BUILDS
@pytest.mark.parametrize("as_generator", [False, True], ids=["list", "generator"])
def test_executemany_stores_every_row(db: _Db, as_generator: bool) -> None:
    name = "g" if as_generator else "m"
    db.verifier.check(name, _write_many(db.conn, name, as_generator))


@_BUILDS
def test_update_stores_a_new_payload(db: _Db) -> None:
    _write(db.conn, _INSERT, ("d1", "pt-tuple-1"))
    _write(db.conn, "UPDATE users SET ssn = %s WHERE name = %s", ("pt-upd-1", "d1"))
    assert _read_one(db.conn, "d1") == "pt-upd-1"
    db.verifier.check("d1", ["pt-upd-1"])


@_BUILDS
@pytest.mark.parametrize("read", ["fetchall", "fetchmany", "fetchone", "iterate"])
def test_reads_decrypt_rows_in_order(db: _Db, read: str) -> None:
    want = _write_many(db.conn, "m")
    with db.conn.cursor() as cur:
        cur.execute(_SELECT, ("m",))
        if read == "fetchall":
            got = [row[0] for row in cur.fetchall()]
        elif read == "fetchmany":
            got = [row[0] for row in cur.fetchmany(2)] + [row[0] for row in cur.fetchmany(5)]
        elif read == "fetchone":
            got = [cur.fetchone()[0] for _ in want]
            assert cur.fetchone() is None
        else:
            got = [row[0] for row in cur]
    assert got == want
    db.verifier.check("m", want)


@_BUILDS
@pytest.mark.parametrize("sql, params, at, want", [
    pytest.param("SELECT * FROM users WHERE name = %s", ("d1",), 2, "pt-tuple-1", id="select-star"),
    pytest.param("SELECT count(*) FROM users WHERE ssn = %s", ("pt-tuple-1",), 0, 0, id="where-never-matches"),
    pytest.param("SELECT s.ssn FROM (SELECT ssn FROM users WHERE name = %s) s", ("d1",), 0, "pt-tuple-1",
                 id="derived-table"),
    pytest.param("WITH w AS (SELECT ssn FROM users WHERE name = %s) SELECT ssn FROM w", ("d1",), 0, "pt-tuple-1",
                 id="cte")])
def test_one_row_reads(db: _Db, sql: str, params: Tuple[str], at: int, want: Any) -> None:
    # An equality on the encrypted column compares payloads, so it never matches; the others decrypt.
    _write(db.conn, _INSERT, ("d1", "pt-tuple-1"))
    with db.conn.cursor() as cur:
        cur.execute(sql, params)
        assert cur.fetchone()[at] == want
    db.verifier.check("d1", ["pt-tuple-1"])


@pytest.mark.parametrize("build", ["pg"])
@pytest.mark.parametrize("read", ["chained-fetchone", "chained-iterate", "named-cursor", "dict-row"])
def test_psycopg_chained_named_and_dict_row_reads(db: _Db, read: str) -> None:
    values = _write_many(db.conn, "m")
    want = values
    cursor_args: Dict[str, Any] = {"named-cursor": {"name": "live_named"}, "dict-row": {"row_factory": dict_row}}
    with db.conn.cursor(**cursor_args.get(read, {})) as cur:
        chained = cur.execute(_SELECT, ("m",))
        if read == "chained-fetchone":
            got = [chained.fetchone()[0]]
            want = values[:1]
        elif read == "dict-row":
            got = [cur.fetchone()["ssn"]]
            want = values[:1]
        else:
            got = [row[0] for row in (chained if read == "chained-iterate" else cur)]
    db.conn.commit()
    assert got == want
    db.verifier.check("m", values)


@_MYSQL_BUILDS
@pytest.mark.parametrize("cursor_args", [{"dictionary": True}, {"buffered": True}], ids=["dictionary", "buffered"])
def test_mysql_dictionary_and_buffered_cursors(db: _Db, cursor_args: Dict[str, bool]) -> None:
    _write(db.conn, _INSERT, ("d2", "pt-dict-2"))
    with db.conn.cursor(**cursor_args) as cur:
        cur.execute(_SELECT, ("d2",))
        row = cur.fetchone()
    assert (row["ssn"] if "dictionary" in cursor_args else row[0]) == "pt-dict-2"
    db.verifier.check("d2", ["pt-dict-2"])


@_BUILDS
def test_literal_into_the_encrypted_column_is_refused(db: _Db) -> None:
    with db.conn.cursor() as cur, pytest.raises(MetadataError) as raised:
        cur.execute("INSERT INTO users (name, ssn) VALUES (%s, 'net-literal')", ("refused",))
    db.conn.rollback()
    err = raised.value
    assert type(err).__module__ == "aws_advanced_python_wrapper"
    assert (err.table_name, err.column_name) == ("users", "ssn")
    assert err.__cause__ is None and err.__context__ is None
    assert db.verifier.stored("refused") == []


@_BUILDS
def test_insert_without_a_column_list_reaches_the_trigger(db: _Db, build: str) -> None:
    # Passed through with a warning; the server log checks find each control value once.
    control = {"pg": "net-unanalysed", "mysql-cext": "net-unanalysed-cext", "mysql-pure": "net-unanalysed-pure"}[build]
    with db.conn.cursor() as cur, pytest.raises(AwsWrapperError) as raised:
        cur.execute("INSERT INTO users VALUES (DEFAULT, %s, %s, NULL)", ("netrow", control))
    db.conn.rollback()
    assert type(raised.value) is AwsWrapperError
    assert isinstance(raised.value.__cause__, (psycopg.Error, mysql.connector.Error))
    assert db.verifier.stored("netrow") == []


# ---- SQLAlchemy Core and ORM ----

class EncryptedValue(UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        return "BYTEA"


# The guide's pass-through type on PostgreSQL, where SQLAlchemy would cast a typed parameter that bytea rejects.
_SSN = EncryptedValue().with_variant(String(1024), "mysql")
_USERS = Table("users", MetaData(), Column("id", Integer, primary_key=True), Column("name", String(100)),
               Column("ssn", _SSN))


class _Base(DeclarativeBase):
    pass


class _User(_Base):
    __tablename__ = "users"
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String(100))
    ssn = mapped_column(_SSN)


class _JoinedBase(DeclarativeBase):
    pass


class _Owner(_JoinedBase):
    """``users`` with its addresses loaded by a join, so a LIMIT reads the encrypted column through a subquery."""
    __tablename__ = "users"
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String(100))
    ssn = mapped_column(_SSN)
    addresses = relationship("_Address", lazy="joined", order_by="_Address.id")


class _Address(_JoinedBase):
    __tablename__ = "addresses"
    id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(ForeignKey("users.id"))
    email = mapped_column(String(100))


@dataclass(frozen=True)
class _Orm:
    engine: Engine
    verifier: _Verifier


@pytest.fixture
def sa(engine: str, bed: _Bed, verifiers: Dict[str, _Verifier]) -> Iterator[_Orm]:
    env, pg = bed.env, engine == "pg"
    url = URL.create("postgresql+aws_wrapper_psycopg" if pg else "mysql+aws_wrapper_mysqlconnector",
                     username=env.user, password=env.password, host=_HOST,
                     port=env.pg_port if pg else env.mysql_port, database=env.dbname)
    sa_engine = create_engine(url, connect_args=_plugin_props(engine, bed))  # a ?plugins= in the URL would be dropped
    yield _Orm(sa_engine, verifiers[engine])
    sa_engine.dispose()


@_ENGINES
@pytest.mark.parametrize("shape", ["insert-values-select-scalar", "executemany-select-all", "result-iteration"])
def test_core_writes_and_reads(sa: _Orm, shape: str) -> None:
    want = ["pt-sa-core-1"] if shape == "insert-values-select-scalar" else ["pt-sa-many-0", "pt-sa-many-1"]
    with sa.engine.begin() as conn:
        if len(want) == 1:
            conn.execute(insert(_USERS).values(name="sa", ssn=want[0]))
        else:
            conn.execute(insert(_USERS), [{"name": "sa", "ssn": value} for value in want])
    query = select(_USERS.c.ssn).where(_USERS.c.name == "sa").order_by(_USERS.c.id)
    with sa.engine.connect() as conn:
        if len(want) == 1:
            got = [conn.execute(query).scalar()]
        else:
            got = [row.ssn if shape == "executemany-select-all" else row[0] for row in conn.execute(query)]
    assert got == want
    sa.verifier.check("sa", want)


def _orm_users(sa: _Orm, values: List[str]) -> List[int]:
    with Session(sa.engine) as session:
        session.add_all([_User(name="orm", ssn=value) for value in values])
        session.commit()
        return [user.id for user in session.scalars(select(_User).where(_User.name == "orm").order_by(_User.id))]


@_ENGINES
@pytest.mark.parametrize("read", ["add-all-scalars", "session-get", "legacy-filter-by-first"])
def test_orm_reads(sa: _Orm, read: str) -> None:
    values = [f"pt-orm-{i}" for i in range(3)]
    ids = _orm_users(sa, values)
    with Session(sa.engine) as session:
        if read == "add-all-scalars":
            users: List[Optional[_User]] = list(session.scalars(select(_User).where(_User.name == "orm")
                                                                .order_by(_User.id)))
            want = values
        elif read == "session-get":
            users = [session.get(_User, ids[1])]
            want = values[1:2]
        else:
            users = [session.query(_User).filter_by(id=ids[2]).first()]
            want = values[2:]
        assert [user.ssn if user else None for user in users] == want
    sa.verifier.check("orm", values)


@_ENGINES
def test_orm_attribute_update(sa: _Orm) -> None:
    ids = _orm_users(sa, [f"pt-orm-{i}" for i in range(3)])
    with Session(sa.engine) as session:
        user = session.get(_User, ids[0])
        assert user is not None
        user.ssn = "pt-orm-upd"
        session.commit()
    with Session(sa.engine) as session:
        user = session.get(_User, ids[0])
        assert user is not None and user.ssn == "pt-orm-upd"
    sa.verifier.check("orm", ["pt-orm-upd", "pt-orm-1", "pt-orm-2"])


@_ENGINES
@pytest.mark.parametrize("read", ["legacy-query-first", "joinedload-limit"])
def test_joined_eager_reads(sa: _Orm, read: str) -> None:
    with Session(sa.engine) as session:
        owner = _Owner(name="ow", ssn="pt-owner-1", addresses=[_Address(email="a1@example.com"),
                                                               _Address(email="a2@example.com")])
        session.add(owner)
        session.commit()
        owner_id = owner.id
    with Session(sa.engine) as session:
        if read == "legacy-query-first":
            found = session.query(_Owner).filter(_Owner.id == owner_id).first()
        else:
            found = session.scalars(select(_Owner).options(joinedload(_Owner.addresses)).limit(1)).unique().first()
        assert found is not None and found.ssn == "pt-owner-1" and len(found.addresses) == 2
    sa.verifier.check("ow", ["pt-owner-1"])


# ---- botocore at DEBUG ----

@contextlib.contextmanager
def _debug_log() -> Iterator[io.StringIO]:
    # While the root logger is at DEBUG, this handler is its only one, so the data key that the positive control logs
    # reaches no other handler, pytest's included, and so no report.
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


@_ENGINES
def test_debug_log_holds_no_data_key(engine: str, bed: _Bed, verifiers: Dict[str, _Verifier]) -> None:
    build = "pg" if engine == "pg" else "mysql-cext"
    with _connect(build, bed) as conn:
        _write(conn, _INSERT, ("dbg", "pt-debug-1"))
    verifiers[engine].check("dbg", ["pt-debug-1"])
    with _debug_log() as log, _connect(build, bed) as conn:  # a new connection decrypts its data key through KMS
        value = _read_one(conn, "dbg")
    holds_plaintext, logged_decrypt = '"Plaintext"' in log.getvalue(), "TrentService.Decrypt" in log.getvalue()
    assert value == "pt-debug-1" and not holds_plaintext and logged_decrypt
    # The positive control: without the filter the same read logs the data key.
    parsers = logging.getLogger("botocore.parsers")
    removed = [f for f in parsers.filters if type(f).__name__ == "_NoKmsPlaintext"]
    for f in removed:
        parsers.removeFilter(f)
    try:
        with _debug_log() as log, _connect(build, bed) as conn:
            _read_one(conn, "dbg")
        control_logged = '"Plaintext"' in log.getvalue()
    finally:
        for f in removed:
            parsers.addFilter(f)
    assert removed and control_logged


# ---- Django on MySQL ----

@pytest.fixture(scope="module")
def django_model(bed: _Bed) -> Iterator[Any]:
    env = bed.env
    if django_settings.configured:  # another module's settings would point these shapes at another database
        pytest.fail("Django settings are already configured in this process; run this module on its own.")
    django_settings.configure(
        DEBUG=False, USE_TZ=False, INSTALLED_APPS=[], DEFAULT_AUTO_FIELD="django.db.models.AutoField",
        DATABASES={"default": {"ENGINE": "aws_advanced_python_wrapper.django.backends.mysql_connector",
                               "NAME": env.dbname, "USER": env.user, "PASSWORD": env.password, "HOST": _HOST,
                               "PORT": env.mysql_port, "OPTIONS": _plugin_props("mysql", bed)}})
    django.setup()

    class LiveUser(models.Model):
        name: models.CharField[str, str] = models.CharField(max_length=100)
        ssn: models.CharField[Optional[str], Optional[str]] = models.CharField(max_length=100, null=True)

        class Meta:
            app_label = "kms_live"
            db_table = "users"
            managed = False

    yield LiveUser
    django_connection.close()


@pytest.mark.parametrize("write", ["create", "bulk-create", "save", "queryset-update"])
def test_django_writes(django_model: Any, verifiers: Dict[str, _Verifier], write: str) -> None:
    objects = django_model.objects
    if write == "bulk-create":
        want = ["pt-djb-0", "pt-djb-1"]
        assert len(objects.bulk_create([django_model(name="dj", ssn=value) for value in want])) == 2
    else:
        want = [{"create": "pt-dj-1", "save": "pt-dj-upd", "queryset-update": "pt-dj-qsupd"}[write]]
        assert objects.create(name="dj", ssn="pt-dj-1").ssn == "pt-dj-1"
        if write == "save":
            user = objects.get(name="dj")
            user.ssn = want[0]
            user.save()
        elif write == "queryset-update":
            objects.filter(name="dj").update(ssn=want[0])
        assert objects.get(name="dj").ssn == want[0]
    verifiers["mysql"].check("dj", want)


@pytest.mark.parametrize("read", ["get", "values-list", "iterator", "raw", "connection-cursor"])
def test_django_reads(django_model: Any, verifiers: Dict[str, _Verifier], read: str) -> None:
    want = ["pt-djb-0", "pt-djb-1"]
    django_model.objects.bulk_create([django_model(name="djb", ssn=value) for value in want])
    rows = django_model.objects.filter(name="djb").order_by("id")
    if read == "get":
        got = [django_model.objects.get(pk=pk).ssn for pk in rows.values_list("id", flat=True)]
    elif read == "values-list":
        got = list(rows.values_list("ssn", flat=True))
    elif read == "iterator":
        got = [user.ssn for user in rows.iterator(chunk_size=1)]
    elif read == "raw":
        got = [user.ssn for user in django_model.objects.raw("SELECT id, name, ssn FROM users WHERE name = %s "
                                                             "ORDER BY id", ["djb"])]
    else:
        with django_connection.cursor() as cur:
            cur.execute(_SELECT, ["djb"])
            got = [row[0] for row in cur]
    assert got == want
    verifiers["mysql"].check("djb", want)


# ---- The servers' own logs (after every test above, so they cover them) ----

def _log_lines(bed: _Bed, variable: str) -> List[bytes]:
    """The lines a server logged after this run's marker statement, from the command in ``variable``.

    A failure names the variable and holds neither the command nor its output; the tests report counts, never a line.
    """
    command = bed.env.pg_log_command if variable == "PG_LOG_COMMAND" else bed.env.mysql_log_command
    failure = ""
    try:
        done = subprocess.run(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              timeout=_LOG_TIMEOUT)
    except subprocess.TimeoutExpired:  # its text holds the command
        failure = f"{variable} didn't finish within {_LOG_TIMEOUT} seconds."
    except (OSError, ValueError):  # no such program, one that can't run, or an unclosed quote
        failure = f"{variable} couldn't be started; check its program and its quoting."
    if failure:
        pytest.fail(failure)
    if done.returncode != 0:
        pytest.fail(f"{variable} exited with status {done.returncode}.")
    lines = done.stdout.splitlines()
    starts = [i for i, line in enumerate(lines) if bed.marker.encode() in line]
    if not starts:
        pytest.fail(f"The output of {variable} has no line with this run's marker statement.")
    return lines[starts[-1] + 1:]


def _counts(lines: List[bytes], needles: Tuple[str, ...]) -> Dict[str, int]:
    return {needle: sum(needle.encode() in line for line in lines) for needle in needles}


def test_postgresql_log_holds_no_plaintext(bed: _Bed) -> None:
    lines = _log_lines(bed, "PG_LOG_COMMAND")
    total = len(lines)
    # PostgreSQL logs a bytea parameter in hex, so a "pt-" value bound as bytes would show as '\x70742d.
    # A read of PostgreSQL's container log now and then comes back without some of the lines the log holds. When a
    # PostgreSQL log test is the run's only failure and its only count off is one expected above 0 that came out lower,
    # re-run the module once: a wrong command misses the line again, and a missed read almost never does. A count
    # expected to be 0 that comes out above 0 is a leak even when it is the run's only failure.
    counts = _counts(lines, ("pt-", r"'\x70742d", "net-literal", r"'\x6e65742d"))
    assert total > 0
    assert counts == {"pt-": 0, r"'\x70742d": 0, "net-literal": 0, r"'\x6e65742d": 1}


def test_mysql_general_log_holds_no_plaintext(bed: _Bed) -> None:
    lines = _log_lines(bed, "MYSQL_LOG_COMMAND")
    total, writes = len(lines), sum(bool(re.search(rb"(?i)(INSERT INTO|UPDATE) `?users", line)) for line in lines)
    # The general log holds each encrypted value as raw bytes, so even a run without a leak can show "pt-" by chance,
    # though rarely. When this test is the run's only failure and "pt-" its only count off, re-run the module once: a
    # leak repeats and a chance match almost never does, so treat a second hit as a leak.
    counts = _counts(lines, ("pt-", "net-literal", "net-unanalysed-cext", "net-unanalysed-pure"))
    assert total > 0 and writes > 0
    assert counts == {"pt-": 0, "net-literal": 0, "net-unanalysed-cext": 1, "net-unanalysed-pure": 1}


# ---- Further checks: error classes, target_cursor, key rows, logging and driver quirks ----
# They run after the two log tests above, so those tests' counts don't include them. Plaintexts written through the
# plugin here start with "pt-x-", and values meant to reach a server unencrypted with "net-x-"; the file's last two
# tests count them, and count _NAIVE_DATETIME, which can't carry a prefix, by its time of day.

_LITERAL = "INSERT INTO users (name, ssn) VALUES (%s, 'net-x-literal')"
_SET_ARN = "UPDATE encrypt.key_storage SET master_key_arn = %s WHERE id = %s"
_FOREIGN_KEY_ID = 987654321  # no key_storage row has it
_READS = ("fetchone", "fetchmany", "fetchall", "iterate", "chained")  # mysql-connector's execute returns None


def _per_build(options: Tuple[str, ...], pg_only: Tuple[str, ...] = (), mysql_only: Tuple[str, ...] = ()) -> List[Any]:
    return [pytest.param(build, option, id=f"{build}-{option}") for build in _ENGINE for option in options
            if option not in (mysql_only if build == "pg" else pg_only)]


def _plain_rows(engine: str, env: _Env, sql: str, params: Any) -> List[Any]:
    """The first column of every row a plain driver reads."""
    with _plain(engine, env) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]


def _plain_write(engine: str, env: _Env, sql: str, params: Any) -> None:
    with _plain(engine, env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def _key_id(engine: str, env: _Env, column: str) -> int:
    """The column's current ``key_storage`` id, read when the test runs, since a rotation changes it."""
    return int(_plain_rows(engine, env, "SELECT key_id FROM encrypt.encryption_metadata WHERE table_name = %s "
                                        "AND column_name = %s", ("users", column))[0])


@contextlib.contextmanager
def _dob_verifier(engine: str, bed: _Bed) -> Iterator[_Verifier]:
    verifier = _Verifier(engine, bed, "dob")
    try:
        yield verifier
    finally:
        verifier.close()


def _read_all(conn: AwsWrapperConnection, sql: str, name: str) -> List[Any]:
    with conn.cursor() as cur:
        cur.execute(sql, (name,))
        return [row[0] for row in cur.fetchall()]


def _write_tampered(build: str, bed: _Bed) -> None:
    """Row ``x-tamper``: ``pt-x-dob`` written through the plugin, then its last byte flipped with a plain driver. The
    payload keeps the column's key id, so reading it fails the HMAC check and no other key row is looked up."""
    engine = _ENGINE[build]
    with _connect(build, bed) as conn:
        _write(conn, _INSERT_DOB, ("x-tamper", "pt-x-dob"))
    stored = bytes(_plain_rows(engine, bed.env, _SELECT_DOB, ("x-tamper",))[0])
    tampered = stored[:-1] + bytes([stored[-1] ^ 0x01])
    _plain_write(engine, bed.env, "UPDATE users SET dob = %s WHERE name = %s", (tampered, "x-tamper"))


def _read_tampered(cur: Any, read: str) -> Any:
    if read == "chained":
        return cur.execute(_SELECT_DOB, ("x-tamper",)).fetchone()
    cur.execute(_SELECT_DOB, ("x-tamper",))
    if read == "fetchone":
        return cur.fetchone()
    if read == "fetchmany":
        return cur.fetchmany(2)
    return cur.fetchall() if read == "fetchall" else list(cur)


@contextlib.contextmanager
def _refused(conn: AwsWrapperConnection, cls: Type[BaseException], kms_requests: Optional[int] = None) -> Iterator[Any]:
    """Expects exactly ``cls`` from the block, run with the root logger at DEBUG: no failover record names it, and if
    ``kms_requests`` is given, that many captured lines name a KMS operation (one request logs more than one; the
    captured text is only counted). After a rollback the connection still runs ``SELECT 1``. The rollback also
    discards rows a failed read left unread on mysql-connector's default cursor."""
    with _debug_log() as log:
        with pytest.raises(cls) as raised:
            yield raised
    lines = log.getvalue().splitlines()
    named = sum("failover" in line.lower() and cls.__name__ in line for line in lines)
    sent = sum("TrentService." in line for line in lines)
    assert type(raised.value) is cls and named == 0
    if kms_requests is not None:
        assert sent == kms_requests
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


# Error classes as the application sees them through the default chains, failover_v2 included.

@pytest.mark.parametrize("build, read", _per_build(_READS, pg_only=("chained",)))
def test_tampered_value_raises_integrity_check_error(db: _Db, bed: _Bed, build: str, read: str) -> None:
    _write_tampered(build, bed)
    with _refused(db.conn, IntegrityCheckError), db.conn.cursor() as cur:
        _read_tampered(cur, read)


@_ENGINES
def test_tampered_value_through_sqlalchemy_raises_integrity_error(sa: _Orm, bed: _Bed, engine: str) -> None:
    _write_tampered("pg" if engine == "pg" else "mysql-cext", bed)
    with sa.engine.connect() as conn, pytest.raises(sa_exc.IntegrityError) as raised:
        conn.execute(text("SELECT dob FROM users WHERE name = :n"), {"n": "x-tamper"}).scalar()
    assert type(raised.value) is sa_exc.IntegrityError and type(raised.value.orig) is IntegrityCheckError


@pytest.mark.parametrize("build, call", _per_build(("execute", "executemany")))
def test_unserializable_value_keeps_the_encryption_service_error(db: _Db, call: str) -> None:
    with pytest.raises(EncryptionError) as direct:  # what the encryption service raises for it on its own
        EncryptionService.encrypt(2 ** 70, bytearray(32), bytearray(32), "AES-256-GCM", key_id=1)
    expected = type(direct.value)
    with _refused(db.conn, expected), db.conn.cursor() as cur:
        if call == "execute":
            cur.execute(_INSERT, ("x-big", 2 ** 70))
        else:
            cur.executemany(_INSERT, [("x-big", 2 ** 70)])
    assert expected is EncryptionFailedError and db.verifier.stored("x-big") == []


@_BUILDS
def test_literal_through_executemany_is_refused(db: _Db) -> None:
    with _refused(db.conn, MetadataError) as raised, db.conn.cursor() as cur:
        cur.executemany(_LITERAL, [("x-lit",)])
    err = raised.value
    assert (err.table_name, err.column_name) == ("users", "ssn")
    assert err.__cause__ is None and err.__context__ is None
    assert db.verifier.stored("x-lit") == []


@_BUILDS
def test_hidden_placeholder_on_an_encrypted_table_is_refused(db: _Db) -> None:
    # Both drivers bind the %s in the comment too, so the plugin can't tell which value goes to ssn. Refused before
    # anything is sent, so the "pt-x-" count of the last two tests stays 0.
    with _refused(db.conn, MetadataError) as raised, db.conn.cursor() as cur:
        cur.execute("INSERT INTO users (name, ssn) VALUES (%s, %s) /* %s */", ("x-hidden", "pt-x-hidden", "x-comment"))
    err = raised.value
    assert str(err) == Messages.get_formatted("KmsEncryptionPlugin.HiddenPlaceholder", "users")
    assert err.__cause__ is None and err.__context__ is None
    assert db.verifier.stored("x-hidden") == []


def _refused_write_or_read(build: str, bed: _Bed, call: str, label: str, cls: Type[BaseException],
                           kms_requests: Optional[int] = None, **overrides: Any) -> Any:
    """A write, or a read of a row a normal connection wrote first, on a connection with ``overrides``."""
    name, value = f"x-{label}", f"pt-x-{label}"
    if call == "read":
        with _connect(build, bed) as conn:
            _write(conn, _INSERT, (name, value))
    with _connect(build, bed, **overrides) as conn:
        with _refused(conn, cls, kms_requests) as raised, conn.cursor() as cur:
            if call == "write":
                cur.execute(_INSERT, (name, value))
            else:
                cur.execute(_SELECT, (name,))
                cur.fetchone()
    assert len(_plain_rows(_ENGINE[build], bed.env, _SELECT, (name,))) == (1 if call == "read" else 0)
    return raised.value


@pytest.mark.parametrize("build, call", _per_build(("write", "read")))
def test_master_key_outside_the_allow_list_is_refused(bed: _Bed, build: str, call: str) -> None:
    other = bed.arns["mysql" if _ENGINE[build] == "pg" else "pg"]  # a LocalStack key this run made for the other engine
    # Refused before any KMS request: the captured DEBUG text names no TrentService operation.
    _refused_write_or_read(build, bed, call, "unauth", UnauthorizedMasterKeyError, kms_requests=0,
                           encryption_allowed_master_key_arns=other)


@pytest.mark.parametrize("build, call", _per_build(("write", "read")))
def test_unreachable_kms_raises_key_management_error(bed: _Bed, build: str, call: str) -> None:
    err = _refused_write_or_read(build, bed, call, "down", KeyManagementError,
                                 encryption_kms_endpoint=f"http://{_HOST}:1", encryption_key_management_max_retries=0)
    assert type(err.__cause__) is EndpointConnectionError


# target_cursor is the driver's cursor, which the plugin doesn't see.

@_BUILDS
def test_target_cursor_is_the_driver_cursor(db: _Db) -> None:
    with db.conn.cursor() as cur, db.conn.target_connection.cursor() as own:
        assert type(cur.target_cursor) is type(own)


def _closed(cursor: Any) -> bool:
    if isinstance(cursor, psycopg.Cursor):
        return cursor.closed
    return cursor._connection is None  # both mysql-connector builds clear it in close()


@_BUILDS
@pytest.mark.parametrize("how", ["close", "leave-with"])
def test_closing_the_cursor_closes_the_driver_cursor(db: _Db, how: str) -> None:
    if how == "close":
        cur = db.conn.cursor()
        target: Any = cur.target_cursor
        cur.close()
    else:
        with db.conn.cursor() as cur:
            target = cur.target_cursor
    assert _closed(target)


class _ArgsLoggerPlugin(Plugin):
    """The parameter-logging row's controls: a plugin that logs the arguments it receives."""

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.CURSOR_EXECUTE.method_name, DbApiMethod.CURSOR_EXECUTEMANY.method_name}

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        # stdlib logging: a test can't add a message key, and the record only has to reach the root handler.
        logging.getLogger(__name__).debug("args_logger %s %r", method_name, args)
        return execute_func()


class _ArgsLoggerFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return _ArgsLoggerPlugin()


class _LateArgsLoggerFactory(_ArgsLoggerFactory):
    """The same plugin at another weight, since a weight belongs to the factory type."""


@pytest.fixture
def args_loggers() -> Iterator[None]:
    # Weight 385 puts args_logger before kms_encryption (390), and 395 puts late_args_logger after it and before
    # failover_v2 (410).
    loggers = {"args_logger": (_ArgsLoggerFactory, 385), "late_args_logger": (_LateArgsLoggerFactory, 395)}
    for code, (factory, weight) in loggers.items():
        PluginManager.register_plugin(code, factory, weight)
    try:
        yield
    finally:
        for code, (factory, _) in loggers.items():
            PluginManager.PLUGIN_FACTORIES.pop(code, None)
            PluginManager.PLUGIN_FACTORY_WEIGHTS.pop(factory, None)


@_BUILDS
@pytest.mark.parametrize("extra", ["execute_time", "args_logger", "late_args_logger"])
def test_other_plugins_log_no_bound_value(bed: _Bed, verifiers: Dict[str, _Verifier], args_loggers: None, build: str,
                                          extra: str) -> None:
    # The plugins before kms_encryption receive the caller's values, and the plugins after it the payloads; the
    # built-in ones log neither. args_logger is the positive control; late_args_logger logs only payloads.
    value = f"pt-x-r7-{build}-{extra}"
    plugins = f"{_plugin_props(_ENGINE[build], bed)['plugins']},{extra}"
    with _debug_log() as log, _connect(build, bed, plugins=plugins) as conn:
        with conn.cursor() as cur:
            cur.execute(_INSERT, ("x-r7", value))
            cur.executemany(_INSERT, [("x-r7", f"{value}-m")])
        conn.commit()
        back = _read_all(conn, _SELECT, "x-r7")
    verifiers[_ENGINE[build]].check("x-r7", [value, f"{value}-m"])
    lines = log.getvalue().splitlines()
    logged = sum(value in line for line in lines)
    if extra == "args_logger":
        assert logged > 0
    else:
        assert logged == 0 and back == [value, f"{value}-m"]
    if extra == "late_args_logger":
        writes = [line for line in lines if line.startswith("args_logger ") and "INSERT INTO users" in line]
        assert len(writes) == 2 and all("b'" in line or 'b"' in line for line in writes)


@_BUILDS
def test_target_cursor_is_not_seen(db: _Db) -> None:
    # Its reads return the stored bytes, and the trigger rejects its plaintext write with the driver's own error.
    _write(db.conn, _INSERT, ("x-tc", "pt-x-tc"))
    with db.conn.cursor() as cur:
        target: Any = cur.target_cursor
        target.execute(_SELECT, ("x-tc",))
        stored = target.fetchone()[0]
        target.fetchall()
        with pytest.raises((psycopg.Error, mysql.connector.Error)):
            target.execute(_INSERT, ("x-tc-net", "net-x-target-cursor"))
    db.conn.rollback()
    db.verifier.check("x-tc", ["pt-x-tc"])
    assert bytes(stored) == bytes(db.verifier.stored("x-tc")[0]) and db.verifier.stored("x-tc-net") == []


# SQL that isn't str, and psycopg row factories.

@pytest.mark.parametrize("build", ["pg"])
def test_composable_insert_and_select(db: _Db) -> None:
    with db.conn.cursor() as cur:
        cur.execute(pgsql.SQL("INSERT INTO {} (name, ssn) VALUES ({}, {})").format(
            pgsql.Identifier("users"), pgsql.Placeholder(), pgsql.Placeholder()), ("x-comp", "pt-x-comp"))
        db.conn.commit()
        cur.execute(pgsql.SQL("SELECT ssn FROM users WHERE name = {}").format(pgsql.Placeholder()), ("x-comp",))
        got = cur.fetchone()[0]
    db.verifier.check("x-comp", ["pt-x-comp"])
    assert got == "pt-x-comp"


@pytest.mark.parametrize("build", ["pg"])
def test_composed_insert_with_percent_placeholders_is_encrypted(db: _Db) -> None:
    with db.conn.cursor() as cur:
        cur.execute(pgsql.SQL("INSERT INTO {} (name, ssn) VALUES (%s, %s)").format(pgsql.Identifier("users")),
                    ("x-composed", "pt-x-composed"))
    db.conn.commit()
    db.verifier.check("x-composed", ["pt-x-composed"])


@pytest.mark.parametrize("build", ["pg"])
def test_bytes_sql_is_analyzed_and_encrypted(db: _Db) -> None:
    with _debug_log() as log:
        for i in range(2):
            with db.conn.cursor() as cur:
                cur.execute(b"INSERT INTO users (name, ssn) VALUES (%s, %s)", (f"x-bytes{i}", f"pt-x-bytes{i}"))
            db.conn.commit()
    plugin_records = log.getvalue().count("[KmsEncryptionPlugin]")
    for i in range(2):
        db.verifier.check(f"x-bytes{i}", [f"pt-x-bytes{i}"])
    assert plugin_records == 0


@_BUILDS
def test_unanalysed_insert_control_reaches_the_trigger(db: _Db, build: str) -> None:
    # The positive control of the last two tests: passed through with a warning, found once in each server's log.
    control = {"pg": "net-x-unanalysed", "mysql-cext": "net-x-unanalysed-cext", "mysql-pure": "net-x-unanalysed-pure"}
    with db.conn.cursor() as cur, pytest.raises(AwsWrapperError) as raised:
        cur.execute("INSERT INTO users VALUES (DEFAULT, %s, %s, NULL)", ("x-netrow", control[build]))
    db.conn.rollback()
    assert type(raised.value) is AwsWrapperError
    assert isinstance(raised.value.__cause__, (psycopg.Error, mysql.connector.Error))
    assert db.verifier.stored("x-netrow") == []


def _first_row(cur: Any, read: str) -> Any:
    if read == "chained":
        return cur.execute(_SELECT, ("x-rf",)).fetchone()
    cur.execute(_SELECT, ("x-rf",))
    return cur.fetchone() if read == "fetchone" else list(cur)[0]


@dataclass(frozen=True)
class _NameRow:
    name: str


@pytest.mark.parametrize("build", ["pg"])
@pytest.mark.parametrize("factory", ["class_row", "scalar_row", "dict_row", "namedtuple_row"])
@pytest.mark.parametrize("read", ["fetchone", "iterate", "chained"])
def test_row_factories(db: _Db, factory: str, read: str) -> None:
    _write(db.conn, _INSERT, ("x-rf", "pt-x-rf"))
    db.verifier.check("x-rf", ["pt-x-rf"])
    made: List[int] = []

    class Counted:
        def __init__(self, **columns: Any) -> None:
            made.append(1)

    row_factory = {"class_row": class_row(Counted), "scalar_row": scalar_row, "dict_row": dict_row,
                   "namedtuple_row": namedtuple_row}[factory]
    with db.conn.cursor(row_factory=row_factory) as cur:
        if factory in ("class_row", "scalar_row"):  # refused before any row is built
            with pytest.raises(MetadataError) as raised:
                _first_row(cur, read)
            assert type(raised.value) is MetadataError and made == []
            return
        row = _first_row(cur, read)
    if factory == "dict_row":
        assert type(row) is dict and row["ssn"] == "pt-x-rf"
    else:
        assert type(row).__name__ == "Row" and row.ssn == "pt-x-rf"


@pytest.mark.parametrize("build", ["pg"])
def test_class_row_without_an_encrypted_column_is_not_refused(db: _Db) -> None:
    _write(db.conn, _INSERT, ("x-rf", "pt-x-rf"))
    db.verifier.check("x-rf", ["pt-x-rf"])
    with db.conn.cursor(row_factory=class_row(_NameRow)) as cur:
        cur.execute("SELECT name FROM users WHERE name = %s", ("x-rf",))
        row = cur.fetchone()
    assert row == _NameRow("x-rf")


# Key rows: rotation, two columns, and values whose key row is missing or unusable.

@pytest.mark.parametrize("build", ["pg"])
def test_rotation_keeps_values_under_the_old_key_readable(db: _Db, bed: _Bed) -> None:
    env = bed.env
    old = [f"pt-x-old-{i}" for i in range(200)]
    old_id = _key_id("pg", env, "dob")
    with db.conn.cursor() as cur:
        cur.executemany(_INSERT_DOB, [("x-old", value) for value in old])
    db.conn.commit()
    with _plain("pg", env) as conn:
        new_id = _utility(conn, env, allowed=bed.arns["pg"]).rotate_data_key("users", "dob")
        conn.commit()
    new = [f"pt-x-new-{i}" for i in range(200)]
    with _connect("pg", bed) as conn:  # an open connection keeps the old key row until its metadata refreshes
        old_back = _read_all(conn, _SELECT_DOB, "x-old")
        with conn.cursor() as cur:
            cur.executemany(_INSERT_DOB, [("x-new", value) for value in new])
        conn.commit()
        new_back = _read_all(conn, _SELECT_DOB, "x-new")
    with _dob_verifier("pg", bed) as verifier:  # built after the rotation, so it checks the new key id
        verifier.check("x-new", new)
        old_ids = {EncryptionService.key_id_from_payload(bytes(value)) for value in verifier.stored("x-old")}
    assert new_id != old_id and old_ids == {old_id} and old_back == old and new_back == new


@_BUILDS
def test_two_encrypted_columns_use_their_own_key_rows(db: _Db, bed: _Bed, build: str) -> None:
    engine = _ENGINE[build]
    _write(db.conn, "INSERT INTO users (name, ssn, dob) VALUES (%s, %s, %s)", ("x-two", "pt-x-two-ssn", "pt-x-two-dob"))
    db.verifier.check("x-two", ["pt-x-two-ssn"])
    with _dob_verifier(engine, bed) as verifier:
        verifier.check("x-two", ["pt-x-two-dob"])
    assert _key_id(engine, bed.env, "dob") != bed.key_ids[engine]


def _fake_payload(key_id: int) -> bytes:
    """81 bytes in the payload layout naming ``key_id``; the test owns every byte, and none of them is a key."""
    return bytes(32) + key_id.to_bytes(4, "big") + bytes([1]) + bytes(44)


@_BUILDS
def test_foreign_key_id_is_refused_in_strict_mode(bed: _Bed, build: str) -> None:
    payload = _fake_payload(_FOREIGN_KEY_ID)
    _plain_write(_ENGINE[build], bed.env, _INSERT_DOB, ("x-foreign", payload))
    with _connect(build, bed) as conn, conn.cursor() as cur, pytest.raises(IntegrityCheckError) as raised:
        cur.execute(_SELECT_DOB, ("x-foreign",))
        cur.fetchone()
    err = raised.value
    shown = " ".join((str(err), repr(err), repr(err.args)))
    forms = (str(_FOREIGN_KEY_ID), payload[32:36].hex(), repr(payload[32:36])[2:-1], payload.hex())
    leaked = [form for form in forms if form in shown]
    assert type(err) is IntegrityCheckError and "users.dob" in str(err) and leaked == []
    assert err.__cause__ is None and err.__context__ is None


@_BUILDS
def test_foreign_key_id_comes_back_as_stored_in_lenient_mode(bed: _Bed, build: str) -> None:
    payload = _fake_payload(_FOREIGN_KEY_ID)
    _plain_write(_ENGINE[build], bed.env, _INSERT_DOB, ("x-foreign", payload))
    with _connect(build, bed, encryption_return_unverified_data=True) as conn:
        back = _read_all(conn, _SELECT_DOB, "x-foreign")
    as_stored = [bytes(value) for value in back] == [payload]
    assert as_stored


@pytest.mark.parametrize("build, mode", _per_build(("strict", "lenient")))
def test_unusable_key_row_of_the_column_raises_in_both_modes(bed: _Bed, build: str, mode: str) -> None:
    engine, env = _ENGINE[build], bed.env
    key_id = _key_id(engine, env, "dob")
    _plain_write(engine, env, _INSERT_DOB, ("x-own", _fake_payload(key_id)))
    arn = _plain_rows(engine, env, "SELECT master_key_arn FROM encrypt.key_storage WHERE id = %s", (key_id,))[0]
    _plain_write(engine, env, _SET_ARN, ("", key_id))  # a key row with no master key ARN is unusable
    try:
        with _connect(build, bed, encryption_return_unverified_data=mode == "lenient") as conn:
            with conn.cursor() as cur, pytest.raises(InvalidKeyMetadataError) as raised:
                cur.execute(_SELECT_DOB, ("x-own",))
                cur.fetchone()
    finally:
        _plain_write(engine, env, _SET_ARN, (arn, key_id))
    assert type(raised.value) is InvalidKeyMetadataError


@_ENGINES
def test_standalone_utility_at_debug_logs_no_data_key(bed: _Bed, engine: str) -> None:
    # The bed fixture gave users.dob its key row with the root logger at DEBUG, before any plugin connection existed.
    assert bed.utility_log[engine] == {"plaintext": False, "kms-request": True, "filter-found": True,
                                       "control-plaintext": True}


# The worker thread socket_timeout uses, named-cursor paging, and kms_encryption listed after failover_v2.

@_BUILDS
def test_socket_timeout_hands_the_driver_the_encrypted_copy(bed: _Bed, verifiers: Dict[str, _Verifier],
                                                            build: str) -> None:
    with _connect(build, bed, socket_timeout=5) as conn:  # the driver call then runs on a worker thread
        _write(conn, _INSERT, ("x-sock", "pt-x-sock"))
        got = _read_one(conn, "x-sock")
    verifiers[_ENGINE[build]].check("x-sock", ["pt-x-sock"])
    assert got == "pt-x-sock"


@pytest.mark.parametrize("build", ["pg"])
def test_named_cursor_pages_through_250_rows(db: _Db) -> None:
    # Its three FETCH FORWARD 100 lines are counted by the PostgreSQL log test at the end of the file, once the
    # container's log has had time to arrive.
    values = [f"pt-x-page-{i}" for i in range(250)]
    with db.conn.cursor() as cur:
        cur.executemany(_INSERT, [("x-page", value) for value in values])
    db.conn.commit()
    with db.conn.cursor(name="live_paging") as cur:
        cur.execute(_SELECT, ("x-page",))
        got = [row[0] for row in cur]
    db.conn.commit()
    db.verifier.check("x-page", values)
    assert got == values


_FAILOVER_FIRST: Dict[str, Type[AwsWrapperError]] = {
    "execute": AwsWrapperError, "fetchone": AwsWrapperError, "fetchmany": AwsWrapperError,
    "fetchall": AwsWrapperError, "executemany": MetadataError, "iterate": IntegrityCheckError,
    "chained": AwsWrapperError}  # psycopg's chained fetch is the wrapper cursor's fetchone, which failover_v2 handles


@pytest.mark.parametrize("build, call", _per_build(tuple(_FAILOVER_FIRST), pg_only=("chained",)))
def test_failover_first_order_wraps_execute_and_fetch_errors(bed: _Bed, build: str, call: str) -> None:
    engine = _ENGINE[build]
    chain = WrapperProperties.DEFAULT_PLUGINS if engine == "pg" else WrapperProperties.MYSQL_CONNECTOR_DEFAULT_PLUGINS
    if call not in ("execute", "executemany"):
        _write_tampered(build, bed)
    with _connect(build, bed, plugins=f"{chain},kms_encryption", auto_sort_wrapper_plugin_order=False) as conn:
        with conn.cursor() as cur, pytest.raises(AwsWrapperError) as raised:
            if call == "execute":
                cur.execute(_LITERAL, ("x-lit",))
            elif call == "executemany":
                cur.executemany(_LITERAL, [("x-lit",)])
            else:
                _read_tampered(cur, call)
        conn.rollback()
    err = raised.value
    assert type(err) is _FAILOVER_FIRST[call]
    if type(err) is AwsWrapperError:  # the failover plugin's wrapping keeps the plugin's error as its cause
        assert type(err.__cause__) is (MetadataError if call == "execute" else IntegrityCheckError)


# psycopg's pipeline mode, where a result and its cursor.description arrive only when a fetch syncs the pipeline.

def _pipelined_reads(cur: Any, read: str, count: int) -> List[Any]:
    if read == "fetchone":
        got = [cur.fetchone()[0] for _ in range(count)]
        assert cur.fetchone() is None
        return got
    if read == "fetchmany":
        return [row[0] for row in cur.fetchmany(2)] + [row[0] for row in cur.fetchmany(5)]
    return [row[0] for row in (cur.fetchall() if read == "fetchall" else cur)]


@pytest.mark.parametrize("build", ["pg"])
@pytest.mark.parametrize("metadata", ["loaded", "new-connection"])
@pytest.mark.parametrize("read", ["fetchone", "fetchmany", "fetchall", "iterate"])
def test_psycopg_pipeline_reads_decrypt(db: _Db, metadata: str, read: str) -> None:
    values = [f"pt-x-pipe-{i}" for i in range(3)]
    if metadata == "loaded":
        _write(db.conn, _INSERT, ("x-pipe-warm", "pt-x-pipe-warm"))  # the connection's first statement loads it
    with db.conn.cursor() as cur:
        with db.conn.pipeline():
            for value in values:
                cur.execute(_INSERT, ("x-pipe", value))
            cur.execute(_SELECT, ("x-pipe",))
            got = _pipelined_reads(cur, read, len(values))
    db.conn.commit()
    assert got == values
    assert _read_all(db.conn, _SELECT, "x-pipe") == values  # the same rows, read outside a pipeline
    db.verifier.check("x-pipe", values)


# MySQL's _binary introducer, on both builds and under Django.

def _warnings_1300(cur: Any) -> int:
    cur.execute("SHOW WARNINGS")
    return sum(row[1] == 1300 for row in cur.fetchall())


@_MYSQL_BUILDS
def test_mysql_binary_prefix_keeps_payloads_binary(db: _Db, bed: _Bed, build: str) -> None:
    one, many = f"x-bin-{build}", f"x-binm-{build}"
    values = [f"pt-x-binm-{i}" for i in range(3)]
    with db.conn.cursor() as cur:
        cur.execute(_INSERT, (one, "pt-x-bin"))
        warned = _warnings_1300(cur)
        cur.executemany(_INSERT, [(many, value) for value in values])
        warned += _warnings_1300(cur)
    db.conn.commit()
    db.verifier.check(one, ["pt-x-bin"])
    db.verifier.check(many, values)
    lines = [line for line in _log_lines(bed, "MYSQL_LOG_COMMAND") if b"INSERT INTO users" in line]
    prefixed = {name: sum(line.count(b"_binary '") for line in lines if f"'{name}'".encode() in line)
                for name in (one, many)}
    assert warned == 0 and prefixed == {one: 1, many: 3}


@_MYSQL_BUILDS
def test_mysql_writes_succeed_with_raise_on_warnings(bed: _Bed, verifiers: Dict[str, _Verifier], build: str) -> None:
    values = [f"pt-x-strict-{i}" for i in range(4)]
    with _connect(build, bed, raise_on_warnings=True) as conn:
        _write(conn, _INSERT, ("x-strict", values[0]))
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [("x-strict", value) for value in values[1:]])
        conn.commit()
    verifiers["mysql"].check("x-strict", values)


@_MYSQL_BUILDS
def test_mysql_null_and_mixed_batches_are_stored(db: _Db) -> None:
    _write(db.conn, _INSERT, ("x-null", None))
    with db.conn.cursor() as cur:  # a set with None keeps that placeholder unprefixed in every set (Warning 1300)
        cur.executemany(_INSERT, [("x-mixed", None), ("x-mixed", "pt-x-mixed")])
    db.conn.commit()
    db.verifier.check("x-null", [None])
    db.verifier.check("x-mixed", [None, "pt-x-mixed"])


@_MYSQL_BUILDS
def test_mysql_prepared_cursor_gets_no_binary_prefix(db: _Db, bed: _Bed) -> None:
    with db.conn.cursor(prepared=True) as cur:
        cur.execute("INSERT INTO users (ssn, name) VALUES (%s, %s)", ("pt-x-prep", "x-prep"))
    db.conn.commit()
    db.verifier.check("x-prep", ["pt-x-prep"])
    # The Prepare line is the SQL text the server received; the Execute line shows the server's own rendering.
    prepared = [line for line in _log_lines(bed, "MYSQL_LOG_COMMAND")
                if b"Prepare" in line and b"INSERT INTO users (ssn, name)" in line]
    assert prepared and not any(b"_binary" in line for line in prepared)


@pytest.fixture(scope="module")
def django_binary_model(django_model: Any) -> Any:
    class LiveBinaryUser(models.Model):  # a second unmanaged model over users needs a class name of its own
        name: models.CharField[str, str] = models.CharField(max_length=100)
        ssn: models.BinaryField[Optional[bytes], Optional[bytes]] = models.BinaryField(null=True)

        class Meta:
            app_label = "kms_live"
            db_table = "users"
            managed = False

    return LiveBinaryUser


def test_django_binary_field_under_debug_gets_one_binary_prefix(django_model: Any, django_binary_model: Any, bed: _Bed,
                                                                verifiers: Dict[str, _Verifier]) -> None:
    with override_settings(DEBUG=True):
        # The backend sets raise_on_warnings from DEBUG when it connects, so the row needs a connection of its own.
        django_connection.close()
        try:
            django_connection.ensure_connection()
            strict = django_connection.connection.target_connection.raise_on_warnings is True
            logged = django_connection.queries_logged
            django_model.objects.create(name="x-djchar", ssn="pt-x-djchar")  # the plugin adds the prefix
            django_binary_model.objects.create(name="x-djbin", ssn=b"pt-x-djbin")  # Django writes "_binary %s"
            got = django_binary_model.objects.get(name="x-djbin").ssn
        finally:
            django_connection.close()  # later statements reconnect under DEBUG=False
    lines = _log_lines(bed, "MYSQL_LOG_COMMAND")
    doubled = sum(b"_binary _binary" in line for line in lines)
    prefixes = {name: [line.count(b"_binary ") for line in lines if b"INSERT INTO" in line and name in line]
                for name in (b"'x-djchar'", b"'x-djbin'")}
    verifiers["mysql"].check("x-djchar", ["pt-x-djchar"])
    verifiers["mysql"].check("x-djbin", [b"pt-x-djbin"])
    assert strict and logged and bytes(got) == b"pt-x-djbin"
    assert doubled == 0 and prefixes == {b"'x-djchar'": [1], b"'x-djbin'": [1]}


# Values with no exact stored form are refused; the rest still encrypt.

class _Shade(Enum):
    TEAL = "pt-x-enum"


class _Size(IntEnum):
    LARGE = 7


class _Color(str, Enum):
    RED = "pt-x-strenum"


# Each refused value, and text of it that no part of the error may show.
_NO_EXACT_FORM: Dict[str, Tuple[Any, str]] = {
    "uuid": (uuid.UUID("0c0ffee0-0000-4000-8000-00000000c0de"), "0c0ffee0-0000-4000-8000-00000000c0de"),
    "dict": ({"k": "pt-x-dict"}, "pt-x-dict"), "list": (["pt-x-list"], "pt-x-list"),
    "ipv4": (IPv4Address("192.0.2.7"), "192.0.2.7"), "timedelta": (timedelta(hours=1, minutes=2, seconds=3), "1:02:03"),
    "enum": (_Shade.TEAL, "pt-x-enum"), "jsonb": (Jsonb({"k": "pt-x-jsonb"}), "pt-x-jsonb"),
    "psycopg-binary": (psycopg.Binary(b"pt-x-binary"), "pt-x-binary")}

# Each accepted value, and what it reads back as.
_EXACT_FORM: Dict[str, Tuple[Any, Any]] = {
    "none": (None, None), "int-enum": (_Size.LARGE, 7), "decimal": (Decimal("12.50"), Decimal("12.50")),
    "str-enum": (_Color.RED, "pt-x-strenum"),
    "mysql-binary": (mysql.connector.Binary(b"pt-x-mysql-binary"), b"pt-x-mysql-binary")}

# A naive datetime with microseconds. A datetime can't carry the "pt-x-" prefix, so the file's last two tests count
# this one's time of day.
_NAIVE_DATETIME = datetime(2026, 8, 11, 12, 30, 45, 123456)


@pytest.mark.parametrize("build, kind", _per_build(tuple(_NO_EXACT_FORM), pg_only=("jsonb", "psycopg-binary")))
def test_value_with_no_exact_stored_form_is_refused(db: _Db, kind: str) -> None:
    value, value_text = _NO_EXACT_FORM[kind]
    with db.conn.cursor() as cur, pytest.raises(EncryptionFailedError) as raised:
        cur.execute(_INSERT, ("x-generic", value))
    db.conn.rollback()
    err = raised.value
    shown = " ".join((str(err), repr(err), repr(err.args)))
    assert type(err) is EncryptionFailedError and err.data_type == "GENERIC"
    assert type(value).__name__ in str(err) and "users.ssn" in str(err)
    leaked = [text for text in (value_text, str(value), repr(value)) if text in shown]
    assert leaked == [] and err.__cause__ is None and err.__context__ is None
    assert db.verifier.stored("x-generic") == []


@pytest.mark.parametrize("build, kind", _per_build(tuple(_EXACT_FORM), mysql_only=("mysql-binary",)))
def test_value_with_an_exact_stored_form_is_still_encrypted(db: _Db, kind: str) -> None:
    value, want = _EXACT_FORM[kind]
    _write(db.conn, _INSERT, ("x-exact", value))
    got = _read_one(db.conn, "x-exact")
    db.verifier.check("x-exact", [want])
    assert got == want and type(got) is type(want)


@_BUILDS
def test_naive_datetime_reads_back_with_its_microseconds(db: _Db) -> None:
    _write(db.conn, _INSERT, ("x-naive", _NAIVE_DATETIME))
    got = _read_one(db.conn, "x-naive")
    assert got == _NAIVE_DATETIME and type(got) is datetime and got.tzinfo is None
    db.verifier.check("x-naive", [_NAIVE_DATETIME])
    assert bytes(db.verifier.stored("x-naive")[0])[36] == 13  # LOCAL_DATE_TIME, not an instant under TIMESTAMP


@_BUILDS
def test_stored_generic_payload_reads_back_as_text(db: _Db, bed: _Bed, build: str) -> None:
    value = {"k": "pt-x-generic"}
    with _dob_verifier(_ENGINE[build], bed) as verifier:
        payload = verifier.seal(value)  # the encryption service with users.dob's keys, as another writer would
    is_generic = payload is not None and payload[36] == 99  # the GENERIC type marker
    assert is_generic
    _plain_write(_ENGINE[build], bed.env, _INSERT_DOB, ("x-generic", payload))
    assert _read_all(db.conn, _SELECT_DOB, "x-generic") == [str(value)]


# ---- The servers' own logs for the further checks (last, so they cover every test above) ----

def _lines_with(lines: List[bytes], *needles: str) -> int:
    return sum(any(needle.encode() in line for needle in needles) for line in lines)


def test_mysql_general_log_holds_no_further_plaintext(bed: _Bed) -> None:
    lines = _log_lines(bed, "MYSQL_LOG_COMMAND")
    writes = sum(bool(re.search(rb"(?i)(INSERT INTO|UPDATE) `?users", line)) for line in lines)
    naive = _NAIVE_DATETIME.time().isoformat()
    counts = _counts(lines, ("pt-x-", "net-x-literal", "net-x-unanalysed-cext", "net-x-unanalysed-pure", naive))
    assert len(lines) > 0 and writes > 0
    assert counts == {"pt-x-": 0, "net-x-literal": 0, "net-x-unanalysed-cext": 1, "net-x-unanalysed-pure": 1, naive: 0}


def test_postgresql_log_holds_no_further_plaintext(bed: _Bed) -> None:
    # The file's last test: PostgreSQL's container log can lag its newest lines by a moment, and here even the
    # named-cursor row's FETCH lines have had the rest of the run to arrive. A count expected above 0 that comes out
    # lower gets the one re-run described in test_postgresql_log_holds_no_plaintext.
    lines = _log_lines(bed, "PG_LOG_COMMAND")
    naive = _NAIVE_DATETIME.time().isoformat()
    counts = {"pt-x-": _lines_with(lines, "pt-x-", r"'\x70742d782d"),
              "net-x-literal": _lines_with(lines, "net-x-literal", r"'\x6e65742d782d6c"),
              "net-x-unanalysed": _lines_with(lines, "net-x-unanalysed", r"'\x6e65742d782d756e616e616c79736564"),
              "naive-datetime": _lines_with(lines, naive, naive.encode().hex()),
              "live_paging-fetches": sum(b"FETCH FORWARD 100" in line and b"live_paging" in line for line in lines)}
    assert len(lines) > 0
    assert counts == {"pt-x-": 0, "net-x-literal": 0, "net-x-unanalysed": 1, "naive-datetime": 0, "live_paging-fetches": 3}
