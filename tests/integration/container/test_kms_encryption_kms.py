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

"""The class changes the state of KMS keys it creates, so it runs on LocalStack only."""

from __future__ import annotations

import logging
import secrets
import socket
import time
from itertools import product
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List,
                    Optional, Tuple)

import pytest
from botocore.exceptions import NoCredentialsError, ProfileNotFound
from django.db import connections
from sqlalchemy import create_engine
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy.engine import URL

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.errors import (EncryptionConfigError,
                                                KeyManagementError,
                                                UnauthorizedMasterKeyError)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from .utils.conditions import enable_on_engines, enable_on_features
from .utils.connection_utils import ConnectionUtils
from .utils.database_engine import DatabaseEngine
from .utils.driver_helper import DriverHelper
from .utils.kms_encryption_helper import (KmsEncryptionTestBase, binary_type,
                                          create_table, drop_table,
                                          encrypt_column, encrypted_table,
                                          is_pg, key_management_utility,
                                          kms_builds, kms_client,
                                          kms_test_driver, plain_connect,
                                          raised, read_column, wrapper_connect,
                                          write)
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import KMS_ATTEMPT_BOUND_SEC

if TYPE_CHECKING:
    import io
    from pathlib import Path

    from .utils.kms_encryption_helper import DbUser, KmsSettings

_TABLE = "kms_kms_users"
# One column per KMS key whose state a test changes; each key is the class's own.
_STATES = "kms_kms_states"
_STATE_COLUMNS = ("repointed", "disabled", "pending", "warm")
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_CLOSED_PORT = "http://127.0.0.1:1"
_ALLOW_LIST = WrapperProperties.ENCRYPTION_ALLOWED_MASTER_KEY_ARNS.name
# Values of the allow-list that list no key; None leaves the parameter out.
_NO_ALLOW_LIST: Dict[str, Any] = {"absent": None, "empty": "", "blank-entries": " , ", "empty-list": []}
_PROFILE = "kms-it"


def _stored_names(name: str) -> List[Any]:
    with plain_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT name FROM {_TABLE} WHERE name = %s", (name,))
        rows = cur.fetchall()
        cur.execute(f"SELECT name FROM {_STATES} WHERE name = %s", (name,))
        return rows + cur.fetchall()


def _props_without_allow_list(settings: KmsSettings, schema: str, allowed: str) -> Dict[str, Any]:
    """The plugin's connection properties with the allow-list value ``_NO_ALLOW_LIST[allowed]``."""
    props: Dict[str, Any] = {"plugins": "kms_encryption", "encryption_metadata_schema": schema, **settings.props()}
    if _NO_ALLOW_LIST[allowed] is not None:
        props[_ALLOW_LIST] = _NO_ALLOW_LIST[allowed]
    return props


def _connect_error(connect: Callable[[], Any]) -> Optional[Exception]:
    """The error ``connect`` raised, or None after closing what it returned."""
    try:
        opened = connect()
    except Exception as err:  # noqa: BLE001 - the caller checks the class
        return err
    if opened is not None:
        opened.close()
    return None


def _filters_named(logger_names: Tuple[str, ...], filter_name: str) -> List[Tuple[logging.Logger, Any]]:
    """Each filter of class ``filter_name`` on the named loggers, with its logger. The plugin's filter classes are
    private, so the test finds them by name."""
    found = []
    for logger_name in logger_names:
        logger = logging.getLogger(logger_name)
        for log_filter in logger.filters:
            if type(log_filter).__name__ == filter_name:
                found.append((logger, log_filter))
    return found


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionKms(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def key_row(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                kms_app_user: DbUser) -> Iterator[int]:
        """The class's users table with ``ssn`` encrypted under the class's KMS key; yields the key row's id."""
        with encrypted_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}", ("ssn",), kms_settings, kms_schema,
                             kms_master_key_arn) as key_ids:
            yield key_ids["ssn"]

    @pytest.fixture(scope="class")
    def state_keys(self, request: pytest.FixtureRequest, kms_settings: KmsSettings, kms_schema: str,
                   kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[Dict[str, str]]:
        """A KMS key of the class's own for each column of the states table, each column encrypted under its key
        and holding a value in the row named ``seed``. Then ``repointed``'s key row names the class's main KMS key
        instead, ``disabled``'s key is disabled and ``pending``'s is scheduled for deletion. Yields each column's key."""
        client = kms_client(kms_settings)
        keys = {}
        for column in _STATE_COLUMNS:
            created = client.create_key(Description=f"kms container test {request.module.__name__} {column}")
            keys[column] = created["KeyMetadata"]["Arn"]
        create_table(_STATES, "name VARCHAR(100), " + ", ".join(f"{column} {binary_type()}" for column in _STATE_COLUMNS))
        try:
            key_rows = {}
            with plain_connect() as conn:
                for column, arn in keys.items():
                    key_rows[column] = encrypt_column(conn, kms_settings, kms_schema, _STATES, column, arn)
            allowed = ",".join([kms_master_key_arn, *keys.values()])
            with wrapper_connect("default" if is_pg() else "mysql-pure", kms_app_user, kms_settings, kms_schema,
                                 allowed) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"INSERT INTO {_STATES} (name, {', '.join(_STATE_COLUMNS)}) VALUES (%s, %s, %s, %s, %s)",
                                ("seed", *(f"pt-seed-{column}" for column in _STATE_COLUMNS)))
                conn.commit()
            with plain_connect() as conn, conn.cursor() as cur:
                cur.execute(f"UPDATE {kms_schema}.key_storage SET master_key_arn = %s WHERE id = %s",
                            (kms_master_key_arn, key_rows["repointed"]))
                conn.commit()
            client.disable_key(KeyId=keys["disabled"])
            client.schedule_key_deletion(KeyId=keys["pending"], PendingWindowInDays=7)
            yield keys
        finally:
            drop_table(_STATES)

    @kms_builds()
    def test_unlisted_master_key_refused_before_kms_call(self, build: str, key_row: int, state_keys: Dict[str, str],
                                                         kms_settings: KmsSettings, kms_schema: str,
                                                         kms_master_key_arn: str, kms_app_user: DbUser,
                                                         kms_calls: List[Tuple[str, Optional[str]]]) -> None:
        name, refused = f"unlisted-{build}", f"unlisted-refused-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (name, "pt-unlisted"))
        kms_calls.clear()
        outcomes = {}
        for endpoint in (kms_settings.endpoint_url, _CLOSED_PORT):
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, state_keys["warm"],
                                 encryption_kms_endpoint=endpoint) as conn:
                write_error = raised(conn, lambda: write(conn, _INSERT, (refused, "pt-unlisted-refused")))
                read_error = raised(conn, lambda: read_column(conn, _SELECT, name))
            outcomes[(endpoint, "write")] = type(write_error)
            outcomes[(endpoint, "read")] = type(read_error)
        # On write and read, also with KMS unreachable.
        assert outcomes == dict.fromkeys(outcomes, UnauthorizedMasterKeyError)
        assert kms_calls == []
        assert _stored_names(refused) == []

    @kms_builds(exclude=("mysql-pure",))
    def test_debug_log_holds_no_data_key(self, build: str, key_row: int, kms_settings: KmsSettings, kms_schema: str,
                                         kms_master_key_arn: str, kms_app_user: DbUser,
                                         kms_log_capture: io.StringIO) -> None:
        name = f"log-key-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (name, "pt-log-key"))
        start = len(kms_log_capture.getvalue())
        # A new connection, so the read starts with a cold data key cache.
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            value = read_column(conn, _SELECT, name)
        logged = kms_log_capture.getvalue()[start:]
        # Booleans only, so a failing assertion can't print a logged data key.
        decrypt_logged = "TrentService.Decrypt" in logged
        plaintext_logged = '"Plaintext"' in logged
        removed = _filters_named(("botocore.parsers",), "_NoKmsPlaintext")
        for logger, log_filter in removed:
            logger.removeFilter(log_filter)
        try:
            start = len(kms_log_capture.getvalue())
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                read_column(conn, _SELECT, name)
            control_logged = '"Plaintext"' in kms_log_capture.getvalue()[start:]
        finally:
            for logger, log_filter in removed:
                logger.addFilter(log_filter)
        assert value == ["pt-log-key"]
        assert decrypt_logged is True, "a cold read at DEBUG doesn't log the Decrypt request"
        assert plaintext_logged is False, "a cold read at DEBUG logs the response body that holds the data key"
        assert len(removed) == 1
        assert control_logged is True, "without the plugin's filter the read doesn't log the data key either"

    @kms_builds()
    def test_kms_refusals_fail_closed(self, build: str, key_row: int, state_keys: Dict[str, str],
                                      kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                      kms_app_user: DbUser) -> None:
        name, seed = f"refusal-{build}", f"refusal-seed-{build}"
        arns = [kms_master_key_arn, *state_keys.values()]
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (seed, "pt-refusal-seed"))
        # Each case runs on new connections, so no data key is cached: a key row repointed to another allowed KMS key,
        # a disabled KMS key, one pending deletion, an unlisted KMS key and an unreachable endpoint.
        cases: List[Tuple[str, str, str, str, str, Dict[str, Any]]] = []
        for column in ("repointed", "disabled", "pending"):
            cases.append((column, f"INSERT INTO {_STATES} (name, {column}) VALUES (%s, %s)",
                          f"SELECT {column} FROM {_STATES} WHERE name = %s", "seed", ",".join(arns), {}))
        cases.append(("unlisted", _INSERT, _SELECT, seed, state_keys["warm"], {}))
        cases.append(("unreachable", _INSERT, _SELECT, seed, kms_master_key_arn,
                      {"encryption_kms_endpoint": _CLOSED_PORT, "encryption_key_management_max_retries": 0}))
        errors: Dict[Tuple[str, str], Optional[Exception]] = {}
        for case, insert, select, row, allowed, props in cases:
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed, **props) as conn:
                errors[(case, "write")] = raised(conn, lambda: write(conn, insert, (name, f"pt-refusal-{case}")))
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed, **props) as conn:
                errors[(case, "read")] = raised(conn, lambda: read_column(conn, select, row))
        classes = {key: type(err).__name__ for key, err in errors.items()}
        assert classes == {("repointed", "write"): "KeyManagementError", ("repointed", "read"): "KeyManagementError",
                           ("disabled", "write"): "KeyManagementError", ("disabled", "read"): "KeyManagementError",
                           ("pending", "write"): "KeyManagementError", ("pending", "read"): "KeyManagementError",
                           ("unlisted", "write"): "UnauthorizedMasterKeyError",
                           ("unlisted", "read"): "UnauthorizedMasterKeyError",
                           ("unreachable", "write"): "KeyManagementError",
                           ("unreachable", "read"): "KeyManagementError"}
        # The cases whose error shows a master key ARN unmasked.
        shown = []
        for key, err in errors.items():
            if any(arn in f"{err} {err!r}" for arn in arns):
                shown.append(key)
        assert shown == []
        assert _stored_names(name) == []

    @kms_builds(exclude=("mysql-pure",))
    def test_disabled_key_open_connection_keeps_data_key(self, build: str, state_keys: Dict[str, str],
                                                         kms_settings: KmsSettings, kms_schema: str,
                                                         kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        name = f"disabled-{build}"
        allowed = ",".join([kms_master_key_arn, *state_keys.values()])
        insert = f"INSERT INTO {_STATES} (name, warm) VALUES (%s, %s)"
        select = f"SELECT warm FROM {_STATES} WHERE name = %s ORDER BY id"
        client = kms_client(kms_settings)
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as warm:
            # The connection now holds the column's data key.
            before = read_column(warm, select, "seed")
            client.disable_key(KeyId=state_keys["warm"])
            try:
                write(warm, insert, (name, "pt-disabled"))
                after = read_column(warm, select, name)
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as cold:
                    refused = raised(cold, lambda: read_column(cold, select, "seed"))
            finally:
                client.enable_key(KeyId=state_keys["warm"])
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as again:
            recovered = read_column(again, select, "seed")
        assert before == ["pt-seed-warm"]
        assert after == ["pt-disabled"]
        assert type(refused) is KeyManagementError
        # The failure isn't kept: after EnableKey a new connection reads.
        assert recovered == ["pt-seed-warm"]

    @kms_builds(exclude=("mysql-pure",))
    def test_plugin_calls_only_kms_decrypt(self, build: str, key_row: int, kms_settings: KmsSettings,
                                           kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                           kms_calls: List[Tuple[str, Optional[str]]]) -> None:
        name = f"decrypt-only-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (name, "pt-decrypt-only-1"))
            read_column(conn, _SELECT, name)
        sent = len(kms_calls)
        with plain_connect() as plain:
            key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(_TABLE, "ssn", kms_master_key_arn)
            plain.commit()
        # The utility's own requests.
        del kms_calls[sent:]
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (name, "pt-decrypt-only-2"))
            back = read_column(conn, _SELECT, name)
        # Values under the old and the rotated key row.
        assert back == ["pt-decrypt-only-1", "pt-decrypt-only-2"]
        assert len(kms_calls) >= 2
        assert set(kms_calls) == {("Decrypt", kms_master_key_arn)}

    @kms_builds()
    def test_unreachable_kms_raises_key_management_error(
            self, build: str, key_row: int, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_app_user: DbUser, kms_calls: List[Tuple[str, Optional[str]]]) -> None:
        seed = f"unreachable-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (seed, "pt-unreachable"))
        # A loopback socket that listens but never accepts: the kernel completes the handshake, and nothing answers.
        silent = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        silent.bind(("127.0.0.1", 0))
        silent.listen(8)
        silent_endpoint = f"http://127.0.0.1:{silent.getsockname()[1]}"
        results = {}
        try:
            for label, endpoint, retries in (("closed port", _CLOSED_PORT, 1), ("silent listener", silent_endpoint, 0)):
                for call in ("write", "read"):
                    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                         encryption_kms_endpoint=endpoint,
                                         encryption_key_management_max_retries=retries) as conn:
                        kms_calls.clear()
                        started = time.monotonic()
                        if call == "write":
                            err = raised(conn, lambda: write(conn, _INSERT, (f"{seed}-x", "pt-unreachable-x")))
                        else:
                            err = raised(conn, lambda: read_column(conn, _SELECT, seed))
                        # The guide's bound for 1 + max_retries attempts.
                        within = time.monotonic() - started <= (1 + retries) * KMS_ATTEMPT_BOUND_SEC
                    cause = type(getattr(err, "__cause__", None)).__name__
                    results[(label, call)] = (type(err).__name__, cause, len(kms_calls), within)
        finally:
            silent.close()
        assert results == {("closed port", "write"): ("KeyManagementError", "EndpointConnectionError", 2, True),
                           ("closed port", "read"): ("KeyManagementError", "EndpointConnectionError", 2, True),
                           ("silent listener", "write"): ("KeyManagementError", "ReadTimeoutError", 1, True),
                           ("silent listener", "read"): ("KeyManagementError", "ReadTimeoutError", 1, True)}

    @kms_builds(exclude=("mysql-pure",))
    def test_debug_log_holds_no_aws_credentials(self, build: str, monkeypatch: pytest.MonkeyPatch, key_row: int,
                                                kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                                kms_app_user: DbUser, kms_log_capture: io.StringIO) -> None:
        name = f"log-credentials-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (name, "pt-log-credentials"))
        key_id, token = "AKIAKMSTEST" + secrets.token_hex(6).upper(), "kmstest-token-" + secrets.token_hex(8)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", key_id)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "kmstest-secret-" + secrets.token_hex(8))
        monkeypatch.setenv("AWS_SESSION_TOKEN", token)
        # So the plugin's next session reads these marker credentials.
        AwsCredentialsManager.release_resources()
        try:
            start = len(kms_log_capture.getvalue())
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                value = read_column(conn, _SELECT, name)
            logged = kms_log_capture.getvalue()[start:]
            leaked = [marker for marker in (key_id, token, "Signature:") if marker in logged]
            removed = _filters_named(("botocore.endpoint", "botocore.auth"), "_NoKmsRequestCredentials")
            for logger, log_filter in removed:
                logger.removeFilter(log_filter)
            try:
                start = len(kms_log_capture.getvalue())
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                    read_column(conn, _SELECT, name)
                shown = [marker for marker in (key_id, token) if marker in kms_log_capture.getvalue()[start:]]
            finally:
                for logger, log_filter in removed:
                    logger.addFilter(log_filter)
        finally:
            # The next session reads the class's credentials again.
            AwsCredentialsManager.release_resources()
        assert value == ["pt-log-credentials"]
        assert leaked == []
        # Without the plugin's filters the same read logs both markers, so the check above can find them.
        assert len(removed) == 2
        assert shown == [key_id, token]

    @kms_builds()
    def test_unlisted_master_key_refused_strict_and_lenient(
            self, build: str, key_row: int, state_keys: Dict[str, str], kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_app_user: DbUser, kms_calls: List[Tuple[str, Optional[str]]]) -> None:
        name, refused = f"lenient-{build}", f"lenient-refused-{build}"
        # A table of the build's own, whose key row the test repoints: its metadata row outlives the table.
        repointed = f"kms_kms_repointed_{build.replace('-', '_')}"
        repointed_select = f"SELECT ssn FROM {repointed} WHERE name = %s"
        repointed_insert = f"INSERT INTO {repointed} (name, ssn) VALUES (%s, %s)"
        unlisted_arn = f"arn:aws:kms:{kms_settings.region}:000000000000:key/00000000-0000-0000-0000-000000000000"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, _INSERT, (name, "pt-lenient"))
        outcomes = {}
        with encrypted_table(repointed, f"name VARCHAR(100), ssn {binary_type()}", ("ssn",), kms_settings,
                             kms_schema, kms_master_key_arn) as key_ids:
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                write(conn, repointed_insert, (name, "pt-lenient-repointed"))
            repoint = f"UPDATE {kms_schema}.key_storage SET master_key_arn = %s WHERE id = %s"
            with plain_connect() as admin:
                write(admin, repoint, (unlisted_arn, key_ids["ssn"]))
            try:
                kms_calls.clear()
                # The class's key outside the allow-list, and a key row repointed to an ARN outside it.
                cases = (("unlisted key", state_keys["warm"], _INSERT, _SELECT),
                         ("repointed row", kms_master_key_arn, repointed_insert, repointed_select))
                for (case, allowed, insert, select), lenient in product(cases, (False, True)):
                    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed,
                                         encryption_return_unverified_data=lenient) as conn:
                        write_error = raised(conn, lambda: write(conn, insert, (refused, "pt-lenient-refused")))
                        read_error = raised(conn, lambda: read_column(conn, select, name))
                    outcomes[(case, lenient, "write")] = type(write_error)
                    outcomes[(case, lenient, "read")] = type(read_error)
                with plain_connect() as plain, plain.cursor() as cur:
                    cur.execute(f"SELECT name FROM {repointed} WHERE name = %s", (refused,))
                    repointed_rows = cur.fetchall()
            finally:
                with plain_connect() as admin:
                    write(admin, repoint, (kms_master_key_arn, key_ids["ssn"]))
        assert outcomes == dict.fromkeys(outcomes, UnauthorizedMasterKeyError)
        assert kms_calls == []
        assert _stored_names(refused) == []
        assert repointed_rows == []

    @kms_builds()
    @pytest.mark.parametrize("allowed", list(_NO_ALLOW_LIST))
    def test_connect_without_allow_list_raises_config_error(
            self, build: str, allowed: str, kms_settings: KmsSettings, kms_schema: str, kms_app_user: DbUser,
            kms_calls: List[Tuple[str, Optional[str]]]) -> None:
        props = _props_without_allow_list(kms_settings, kms_schema, allowed)
        conn_utils = ConnectionUtils()
        test_driver = kms_test_driver()
        params = conn_utils.get_connect_params(user=kms_app_user.name, password=kms_app_user.password,
                                               test_driver=test_driver, use_pure=build == "mysql-pure")
        errors = {"dbapi": _connect_error(lambda: AwsWrapperConnection.connect(
            DriverHelper.get_connect_func(test_driver), **params, **props))}
        url = URL.create("postgresql+aws_wrapper_psycopg" if is_pg() else "mysql+aws_wrapper_mysqlconnector",
                         username=kms_app_user.name, password=kms_app_user.password, host=conn_utils.writer_host,
                         port=conn_utils.port, database=conn_utils.dbname)
        driver_args: Dict[str, Any] = {}
        if not is_pg():
            driver_args["use_pure"] = build == "mysql-pure"
        engines = {"sqlalchemy connect_args": create_engine(url, connect_args={**props, **driver_args})}
        # A URL query value is text, so the URL form carries every value but the list.
        if not isinstance(_NO_ALLOW_LIST[allowed], list):
            query = {name: str(value) for name, value in props.items() if name != "plugins"}
            engines["sqlalchemy url"] = create_engine(url.update_query_dict({**query, "wrapper_plugins": "kms_encryption"}),
                                                      connect_args=driver_args)
        try:
            for label, engine in engines.items():
                errors[label] = _connect_error(engine.connect)
        finally:
            for engine in engines.values():
                engine.dispose()
        # SQLAlchemy wraps the plugin's error and keeps it as orig.
        plugin_errors = {label: getattr(err, "orig", err) for label, err in errors.items()}
        classes = {label: (type(err), type(plugin_errors[label])) for label, err in errors.items()}
        expected: Dict[str, Tuple[type, type]] = {"dbapi": (EncryptionConfigError, EncryptionConfigError)}
        for label in engines:
            expected[label] = (sqlalchemy_exc.InterfaceError, EncryptionConfigError)
        assert classes == expected
        assert [label for label, err in plugin_errors.items() if _ALLOW_LIST not in str(err)] == []
        assert kms_calls == []

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    @pytest.mark.parametrize("allowed", list(_NO_ALLOW_LIST))
    def test_django_connect_without_allow_list_raises_config_error(
            self, build: str, allowed: str, kms_django_settings: None, kms_settings: KmsSettings, kms_schema: str,
            kms_app_user: DbUser) -> None:
        conn_utils = ConnectionUtils()
        alias = f"kms_kms_{build.replace('-', '_')}"
        connections.databases[alias] = {
            **connections["default"].settings_dict, "NAME": conn_utils.dbname, "HOST": conn_utils.writer_host,
            "PORT": conn_utils.port, "USER": kms_app_user.name, "PASSWORD": kms_app_user.password,
            "OPTIONS": {**_props_without_allow_list(kms_settings, kms_schema, allowed),
                        "use_pure": build == "mysql-pure"}}
        try:
            error = _connect_error(connections[alias].ensure_connection)
        finally:
            connections[alias].close()
            del connections[alias]
            del connections.databases[alias]
        # Django doesn't translate the plugin's error.
        assert type(error) is EncryptionConfigError
        assert _ALLOW_LIST in str(error)

    @kms_builds()
    def test_aws_profile_selects_plugin_credentials(
            self, build: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key_row: int,
            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        config_file, credentials_file = tmp_path / "aws-config", tmp_path / "aws-credentials"
        config_file.write_text(f"[profile {_PROFILE}]\nregion = {kms_settings.region}\n")
        credentials_file.write_text(f"[{_PROFILE}]\naws_access_key_id = test\naws_secret_access_key = test\n")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config_file))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_file))
        # The profile's keys are then the only credentials the plugin can find.
        for variable in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            monkeypatch.delenv(variable)
        name, missing = f"profile-{build}", f"profile-missing-{build}"
        # So the plugin's sessions read the files above, and no later session keeps them.
        AwsCredentialsManager.release_resources()
        try:
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                 aws_profile=_PROFILE) as conn:
                write(conn, _INSERT, (name, "pt-profile"))
                back = read_column(conn, _SELECT, name)
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                 aws_profile=f"{_PROFILE}-missing") as conn:
                missing_error = raised(conn, lambda: write(conn, _INSERT, (missing, "pt-profile-missing")))
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                default_error = raised(conn, lambda: read_column(conn, _SELECT, name))
        finally:
            AwsCredentialsManager.release_resources()
        assert back == ["pt-profile"]
        # The guide: a missing profile raises the AWS SDK's own error at the first statement.
        assert type(missing_error) is ProfileNotFound
        assert _stored_names(missing) == []
        # Without aws_profile the default chain finds no credentials, so the round trip above ran on the profile's.
        assert type(default_error) is KeyManagementError
        assert type(getattr(default_error, "__cause__", None)) is NoCredentialsError
