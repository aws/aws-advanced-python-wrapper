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

"""What the KMS Encryption Plugin asks of KMS, and how it fails when KMS refuses or doesn't answer.

The module changes the state of KMS keys it creates, so it runs on LocalStack only. KMS requests are recorded at
botocore's ``BaseClient._make_api_call``, once per attempt.
"""

from __future__ import annotations

import logging
import secrets
import socket
import time
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

import pytest
from botocore.client import BaseClient

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from .utils.conditions import enable_on_features
from .utils.kms_encryption_helper import (binary_type, create_table,
                                          drop_table, encrypt_column,
                                          encrypted_table, is_pg,
                                          key_management_utility, kms_builds,
                                          kms_client, plain_connect, raised,
                                          read_column, wrapper_connect, write)
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    import io

    from .utils.kms_encryption_helper import DbUser, KmsSettings

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

_TABLE = "kms_kms_users"
# One column per KMS key whose state a test changes; each key is the module's own.
_STATES = "kms_kms_states"
_STATE_COLUMNS = ("repointed", "disabled", "pending", "warm")
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_CLOSED_PORT = "http://127.0.0.1:1"
# The guide's bound: each attempt waits up to 5 s to connect and 10 s for KMS to answer.
_ATTEMPT_BOUND_SEC = 15.0


@pytest.fixture(scope="module")
def key_row(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[int]:
    """The module's users table with ``ssn`` encrypted under the module's KMS key; yields the key row's id."""
    with encrypted_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}", ("ssn",), kms_settings, kms_schema,
                         kms_master_key_arn) as key_ids:
        yield key_ids["ssn"]


@pytest.fixture(scope="module")
def state_keys(request: pytest.FixtureRequest, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
               kms_app_user: DbUser) -> Iterator[Dict[str, str]]:
    """A KMS key of the module's own for each column of the states table, each column encrypted under its key
    and holding a value in the row named ``seed``. Then ``repointed``'s key row names the module's main KMS key
    instead, ``disabled``'s key is disabled and ``pending``'s is scheduled for deletion. Yields each column's key."""
    client = kms_client(kms_settings)
    keys = {column: client.create_key(Description=f"kms container test {request.module.__name__} {column}")
            ["KeyMetadata"]["Arn"] for column in _STATE_COLUMNS}
    create_table(_STATES, "name VARCHAR(100), " + ", ".join(f"{column} {binary_type()}" for column in _STATE_COLUMNS))
    with plain_connect() as conn:
        key_rows = {column: encrypt_column(conn, kms_settings, kms_schema, _STATES, column, arn)
                    for column, arn in keys.items()}
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
    try:
        yield keys
    finally:
        drop_table(_STATES)


@pytest.fixture
def kms_calls(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Optional[str]]]:
    """Each KMS request this process sends while the test runs, one per attempt: the operation and its KeyId."""
    calls: List[Tuple[str, Optional[str]]] = []
    make_api_call = BaseClient._make_api_call  # type: ignore[attr-defined]

    def recorded(client: Any, operation_name: str, api_params: Dict[str, Any]) -> Any:
        if client.meta.service_model.service_name == "kms":
            calls.append((operation_name, api_params.get("KeyId")))
        return make_api_call(client, operation_name, api_params)

    monkeypatch.setattr(BaseClient, "_make_api_call", recorded)
    return calls


def _stored_names(name: str) -> List[Any]:
    with plain_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT name FROM {_TABLE} WHERE name = %s", (name,))
        rows = cur.fetchall()
        cur.execute(f"SELECT name FROM {_STATES} WHERE name = %s", (name,))
        return rows + cur.fetchall()


@kms_builds()
def test_unlisted_master_key_is_refused_before_any_kms_call(build: str, key_row: int, state_keys: Dict[str, str],
                                                            kms_settings: KmsSettings, kms_schema: str,
                                                            kms_master_key_arn: str, kms_app_user: DbUser,
                                                            kms_calls: List[Tuple[str, Optional[str]]]) -> None:
    name, refused = f"it35-{build}", f"it35-refused-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (name, "pt-it35"))
    kms_calls.clear()
    outcomes = {}
    for endpoint in (kms_settings.endpoint_url, _CLOSED_PORT):
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, state_keys["warm"],
                             encryption_kms_endpoint=endpoint) as conn:
            outcomes[(endpoint, "write")] = type(raised(conn, lambda: write(conn, _INSERT, (refused, "pt-it35-x"))))
            outcomes[(endpoint, "read")] = type(raised(conn, lambda: read_column(conn, _SELECT, name)))
    assert {key: cls.__name__ for key, cls in outcomes.items()} == \
        {key: "UnauthorizedMasterKeyError" for key in outcomes}, \
        "a key row whose master key isn't allowed is refused on write and read, also with KMS unreachable"
    assert (kms_calls, _stored_names(refused)) == ([], []), "the refusal comes before any KMS request and writes nothing"


@kms_builds("mysql-pure")
def test_debug_log_holds_no_data_key(build: str, key_row: int, kms_settings: KmsSettings, kms_schema: str,
                                     kms_master_key_arn: str, kms_app_user: DbUser,
                                     kms_log_capture: io.StringIO) -> None:
    name = f"it36-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (name, "pt-it36"))
    start = len(kms_log_capture.getvalue())
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:  # a cold cache
        value = read_column(conn, _SELECT, name)
    logged = kms_log_capture.getvalue()[start:]
    # Booleans only, so a failing assertion can't print a logged data key.
    decrypt_logged, plaintext_logged = "TrentService.Decrypt" in logged, '"Plaintext"' in logged
    parsers = logging.getLogger("botocore.parsers")
    removed = [f for f in parsers.filters if type(f).__name__ == "_NoKmsPlaintext"]
    for f in removed:
        parsers.removeFilter(f)
    try:
        start = len(kms_log_capture.getvalue())
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            read_column(conn, _SELECT, name)
        control_logged = '"Plaintext"' in kms_log_capture.getvalue()[start:]
    finally:
        for f in removed:
            parsers.addFilter(f)
    assert (value, decrypt_logged, plaintext_logged) == (["pt-it36"], True, False), \
        "a cold read at DEBUG logs the Decrypt request but no response body that holds the data key"
    assert (len(removed), control_logged) == (1, True), \
        "without the plugin's filter the same read logs the data key, so the check above can find one"


@kms_builds()
def test_kms_refusals_fail_closed(build: str, key_row: int, state_keys: Dict[str, str], kms_settings: KmsSettings,
                                  kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> None:
    name, seed = f"it37-{build}", f"it37-seed-{build}"
    arns = [kms_master_key_arn, *state_keys.values()]
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (seed, "pt-it37-seed"))
    errors: Dict[Tuple[str, str], Optional[Exception]] = {}
    # Each case runs on new connections, so no data key is cached: a key row repointed to another allowed KMS key,
    # a disabled KMS key, one pending deletion, an unlisted KMS key and an unreachable endpoint.
    cases: List[Tuple[str, str, str, str, str, Dict[str, Any]]] = [
        (column, f"INSERT INTO {_STATES} (name, {column}) VALUES (%s, %s)",
         f"SELECT {column} FROM {_STATES} WHERE name = %s", "seed", ",".join(arns), {})
        for column in ("repointed", "disabled", "pending")]
    cases += [("unlisted", _INSERT, _SELECT, seed, state_keys["warm"], {}),
              ("unreachable", _INSERT, _SELECT, seed, kms_master_key_arn,
               {"encryption_kms_endpoint": _CLOSED_PORT, "encryption_key_management_max_retries": 0})]
    for case, insert, select, row, allowed, props in cases:
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed, **props) as conn:
            errors[(case, "write")] = raised(conn, lambda: write(conn, insert, (name, f"pt-it37-{case}")))
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed, **props) as conn:
            errors[(case, "read")] = raised(conn, lambda: read_column(conn, select, row))
    assert {key: type(err).__name__ for key, err in errors.items()} == \
        {key: "UnauthorizedMasterKeyError" if key[0] == "unlisted" else "KeyManagementError" for key in errors}, \
        "each KMS refusal raises KeyManagementError on write and read, and an unlisted master key is refused"
    shown = [key for key, err in errors.items() if any(arn in f"{err} {err!r}" for arn in arns)]
    assert (shown, _stored_names(name)) == ([], []), "no error shows a master key ARN unmasked, and nothing is written"


@kms_builds("mysql-pure")
def test_open_connection_keeps_its_data_key_after_key_disabled(build: str, state_keys: Dict[str, str],
                                                               kms_settings: KmsSettings, kms_schema: str,
                                                               kms_master_key_arn: str, kms_app_user: DbUser) -> None:
    name = f"it38-{build}"
    allowed = ",".join([kms_master_key_arn, *state_keys.values()])
    insert = f"INSERT INTO {_STATES} (name, warm) VALUES (%s, %s)"
    select = f"SELECT warm FROM {_STATES} WHERE name = %s ORDER BY id"
    client = kms_client(kms_settings)
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as warm:
        before = read_column(warm, select, "seed")  # the connection now holds the column's data key
        client.disable_key(KeyId=state_keys["warm"])
        try:
            write(warm, insert, (name, "pt-it38"))
            after = read_column(warm, select, name)
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as cold:
                refused = type(raised(cold, lambda: read_column(cold, select, "seed"))).__name__
        finally:
            client.enable_key(KeyId=state_keys["warm"])
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, allowed) as again:
        recovered = read_column(again, select, "seed")
    assert (before, after) == (["pt-seed-warm"], ["pt-it38"]), \
        "a connection that holds the data key keeps reading and writing after the master key is disabled"
    assert (refused, recovered) == ("KeyManagementError", ["pt-seed-warm"]), \
        "a new connection fails while the key is disabled, and the failure isn't kept: after EnableKey one reads"


@kms_builds("mysql-pure")
def test_plugin_connections_call_only_kms_decrypt(build: str, key_row: int, kms_settings: KmsSettings,
                                                  kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                  kms_calls: List[Tuple[str, Optional[str]]]) -> None:
    name = f"it39-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (name, "pt-it39-1"))
        read_column(conn, _SELECT, name)
    sent = len(kms_calls)
    with plain_connect() as plain:
        key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(_TABLE, "ssn", kms_master_key_arn)
        plain.commit()
    del kms_calls[sent:]  # the utility's own requests
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (name, "pt-it39-2"))
        back = read_column(conn, _SELECT, name)
    assert back == ["pt-it39-1", "pt-it39-2"], "values under the old and the rotated key row read back"
    assert len(kms_calls) >= 2 and set(kms_calls) == {("Decrypt", kms_master_key_arn)}, \
        f"every KMS request of a plugin connection is a Decrypt naming the row's master key: {sorted(set(kms_calls))}"


@kms_builds()
def test_unreachable_kms_fails_with_key_management_error_within_the_bound(
        build: str, key_row: int, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
        kms_app_user: DbUser, kms_calls: List[Tuple[str, Optional[str]]]) -> None:
    seed = f"it40-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (seed, "pt-it40"))
    # A loopback socket that listens but never accepts: the kernel completes the handshake, and nothing answers.
    silent = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)
    results = {}
    try:
        endpoints = {"closed port": (_CLOSED_PORT, 1), "silent listener": (f"http://127.0.0.1:{silent.getsockname()[1]}", 0)}
        for label, (endpoint, retries) in endpoints.items():
            for call in ("write", "read"):
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                     encryption_kms_endpoint=endpoint,
                                     encryption_key_management_max_retries=retries) as conn:
                    kms_calls.clear()
                    started = time.monotonic()
                    err = raised(conn, (lambda: write(conn, _INSERT, (f"{seed}-x", "pt-it40-x"))) if call == "write"
                                 else (lambda: read_column(conn, _SELECT, seed)))
                    within = time.monotonic() - started <= (1 + retries) * _ATTEMPT_BOUND_SEC
                results[(label, call)] = (type(err).__name__, type(getattr(err, "__cause__", None)).__name__,
                                          len(kms_calls), within)
    finally:
        silent.close()
    expected = {("closed port", call): ("KeyManagementError", "EndpointConnectionError", 2, True)
                for call in ("write", "read")}
    expected.update({("silent listener", call): ("KeyManagementError", "ReadTimeoutError", 1, True)
                     for call in ("write", "read")})
    assert results == expected, \
        "an unreachable KMS raises KeyManagementError from the connection or read timeout, after 1 + max_retries " \
        "attempts and within the documented bound"


@kms_builds("mysql-pure")
def test_debug_log_holds_no_aws_credentials(build: str, monkeypatch: pytest.MonkeyPatch, key_row: int,
                                            kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                            kms_app_user: DbUser, kms_log_capture: io.StringIO) -> None:
    name = f"it41-{build}"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        write(conn, _INSERT, (name, "pt-it41"))
    key_id, token = "AKIAKMSIT" + secrets.token_hex(6).upper(), "kmsit-token-" + secrets.token_hex(8)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", key_id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "kmsit-secret-" + secrets.token_hex(8))
    monkeypatch.setenv("AWS_SESSION_TOKEN", token)
    AwsCredentialsManager.release_resources()  # so the plugin's next session reads these marker credentials
    try:
        start = len(kms_log_capture.getvalue())
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            value = read_column(conn, _SELECT, name)
        logged = kms_log_capture.getvalue()[start:]
        leaked = [marker for marker in (key_id, token, "Signature:") if marker in logged]
        loggers = [logging.getLogger(logger_name) for logger_name in ("botocore.endpoint", "botocore.auth")]
        removed = [(lg, f) for lg in loggers for f in lg.filters if type(f).__name__ == "_NoKmsRequestCredentials"]
        for lg, f in removed:
            lg.removeFilter(f)
        try:
            start = len(kms_log_capture.getvalue())
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                read_column(conn, _SELECT, name)
            shown = [marker for marker in (key_id, token) if marker in kms_log_capture.getvalue()[start:]]
        finally:
            for lg, f in removed:
                lg.addFilter(f)
    finally:
        AwsCredentialsManager.release_resources()  # the next session reads the module's credentials again
    assert (value, leaked) == (["pt-it41"], []), \
        "a cold read at DEBUG logs neither the access key id, the session token nor a signature record"
    assert (len(removed), shown) == (2, [key_id, token]), \
        "without the plugin's filter the same read logs both markers, so the check above can find them"
