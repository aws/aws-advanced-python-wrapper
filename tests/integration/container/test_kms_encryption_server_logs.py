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

"""What the servers' statement logs hold after the KMS Encryption Plugin's statements.

PostgreSQL logs every statement (``log_statement=all``) and MySQL's general log is switched on by the
``kms_statement_log`` fixture, which first proves the scan finds its own marker. Needles are built by concatenation,
so the test's source never holds them whole.
"""

from __future__ import annotations

import base64
import logging
import secrets
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List,
                    Optional, Set, Tuple)

import pytest

from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.plugin_service import PluginManager
from .utils.conditions import enable_on_features
from .utils.kms_encryption_helper import (TRIGGER_REJECTION, binary_type,
                                          create_table, drop_table,
                                          encrypt_column, encrypted_table,
                                          key_management_utility, kms_builds,
                                          plain_connect, raised,
                                          statement_log_scan, wrapper_connect)
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    import io

    from aws_advanced_python_wrapper import AwsWrapperConnection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from .utils.kms_encryption_helper import DbUser, KmsSettings

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

_TABLE = "kms_logs_users"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_COLUMNS = f"name VARCHAR(100), ssn {binary_type()}"
# Just below kms_encryption's weight (390), so the logging plugin would come before it even in a sorted chain; IT-52
# turns sorting off and lists the chain itself.
_ARGS_LOGGER_WEIGHT = 385


class _ArgsLoggerPlugin(Plugin):
    """IT-52's positive control: a plugin that logs, at DEBUG, the arguments it receives."""

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.CURSOR_EXECUTE.method_name, DbApiMethod.CURSOR_EXECUTEMANY.method_name}

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        logging.getLogger(__name__).debug("args_logger %s %r", method_name, args)
        return execute_func()


class _ArgsLoggerFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return _ArgsLoggerPlugin()


@pytest.fixture(scope="module")
def key_row(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[int]:
    """The module's users table with ``ssn`` encrypted; yields the key row's id."""
    with encrypted_table(_TABLE, _COLUMNS, ("ssn",), kms_settings, kms_schema, kms_master_key_arn) as key_ids:
        yield key_ids["ssn"]


@pytest.fixture
def args_logger() -> Iterator[str]:
    """The code of a plugin, registered for the test, that logs the arguments it receives."""
    PluginManager.register_plugin("kms_args_logger", _ArgsLoggerFactory, _ARGS_LOGGER_WEIGHT)
    try:
        yield "kms_args_logger"
    finally:
        PluginManager.PLUGIN_FACTORIES.pop("kms_args_logger", None)
        PluginManager.PLUGIN_FACTORY_WEIGHTS.pop(_ArgsLoggerFactory, None)


@pytest.fixture
def conn(build: str, key_row: int, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
         kms_app_user: DbUser) -> Iterator[AwsWrapperConnection]:
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        yield conn


@pytest.fixture
def data_keys(monkeypatch: pytest.MonkeyPatch) -> List[bytes]:
    """Every plaintext data key KMS hands out while the test runs, through KeyManager, in memory only."""
    keys: List[bytes] = []
    generate, decrypt = KeyManager.generate_data_key, KeyManager.decrypt_data_key

    def generated(self: KeyManager, *args: Any, **kwargs: Any) -> Any:
        out = generate(self, *args, **kwargs)
        keys.append(bytes(out.plaintext))
        return out

    def decrypted(self: KeyManager, *args: Any, **kwargs: Any) -> Any:
        out = decrypt(self, *args, **kwargs)
        keys.append(bytes(out))
        return out

    monkeypatch.setattr(KeyManager, "generate_data_key", generated)
    monkeypatch.setattr(KeyManager, "decrypt_data_key", decrypted)
    return keys


def _key_forms(keys: List[bytes]) -> Dict[str, str]:
    """The text forms each data key would take in a statement log, by a label that shows no key: base64 and hex."""
    return {f"data key {i} {kind}": form for i, key in enumerate(keys)
            for kind, form in (("base64", base64.b64encode(key).decode()), ("hex", key.hex()),
                               ("HEX", key.hex().upper()))}


def _error_class(err: Optional[Exception]) -> str:
    """The class of ``err``, or of the error it wraps when failover_v2's execute() wrapped it in AwsWrapperError."""
    if type(err) is AwsWrapperError and err.__cause__ is not None:
        return f"AwsWrapperError from {type(err.__cause__).__name__}"
    return type(err).__name__


@kms_builds()
def test_statement_logs_hold_no_written_plaintext(build: str, conn: AwsWrapperConnection, key_row: int,
                                                  kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                                  kms_statement_log: str, data_keys: List[bytes]) -> None:
    tag = build.replace("-", "")
    values = {shape: "pt-it49-" + shape + tag for shape in ("one", "many-1", "many-2", "multi-1", "multi-2",
                                                            "update", "compared")}
    name = "it49-name-" + tag
    with conn.cursor() as cur:
        cur.execute(_INSERT, (name, values["one"]))
        cur.executemany(_INSERT, [(name, values["many-1"]), (name, values["many-2"])])
        cur.execute(f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s), (%s, %s)",
                    (name, values["multi-1"], name, values["multi-2"]))
        cur.execute(f"UPDATE {_TABLE} SET ssn = %s WHERE name = %s AND ssn IS NULL", (values["update"], name))
        cur.execute(f"SELECT name FROM {_TABLE} WHERE ssn = %s", (values["compared"],))
        cur.fetchall()
    conn.commit()
    setup_table = f"kms_logs_setup_{tag}"
    create_table(setup_table, _COLUMNS)
    try:
        with plain_connect() as plain:  # the utility's setup of a column, under the same log
            encrypt_column(plain, kms_settings, kms_schema, setup_table, "ssn", kms_master_key_arn)
            key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(setup_table, "ssn",
                                                                                    kms_master_key_arn)
            plain.commit()
    finally:
        drop_table(setup_table)
    key_needles = _key_forms(list(dict.fromkeys(data_keys)))
    seen_keys = len(set(data_keys))  # counts only, so no assertion can print a key
    # PostgreSQL logs a parameter sent as bytes in hex.
    hexed = {f"{label} hex": value.encode().hex() for label, value in values.items()}
    counts = statement_log_scan({**values, **hexed, "name": name, **key_needles}, kms_statement_log)
    assert {label: counts[label] for label in [*values, *hexed]} == {label: 0 for label in [*values, *hexed]}, \
        "no written or compared plaintext value reaches the server's statement log"
    assert seen_keys >= 2 and [label for label in key_needles if counts[label]] == [], \
        "no plaintext data key, of the plugin's or the utility's, reaches the statement log"
    assert counts["name"] >= 4, "the plain name the statements bound does reach the log, so the scan reads it"


@kms_builds()
def test_compared_parameter_in_statement_logs(build: str, conn: AwsWrapperConnection, kms_statement_log: str) -> None:
    tag = build.replace("-", "")
    direct, through = "pt-it50-direct-" + tag, "pt-it50-coalesce-" + tag
    with conn.cursor() as cur:
        cur.execute(f"SELECT name FROM {_TABLE} WHERE ssn = %s", (direct,))
        direct_rows = cur.fetchall()
        # The plugin can't tie a value compared with an expression to the column, so it sends it as written.
        cur.execute(f"SELECT name FROM {_TABLE} WHERE COALESCE(ssn, ssn) = %s", (through,))
        through_rows = cur.fetchall()
    conn.commit()
    # PostgreSQL logs a parameter sent as bytes in hex.
    counts = statement_log_scan({"direct": direct, "direct hex": direct.encode().hex(),
                                 "coalesce": through, "coalesce hex": through.encode().hex()}, kms_statement_log)
    assert (direct_rows, through_rows) == ([], []), "neither comparison matches a row"
    assert counts["coalesce"] + counts["coalesce hex"] >= 1, \
        "a value compared through COALESCE is sent as written and reaches the log"
    assert counts["direct"] + counts["direct hex"] == 0, \
        "a value compared with the encrypted column is encrypted before it is sent"


@kms_builds("mysql-cext")
def test_no_plaintext_data_key_reaches_the_database(build: str, key_row: int, kms_settings: KmsSettings,
                                                    kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                    kms_statement_log: str, data_keys: List[bytes]) -> None:
    tag = build.replace("-", "")
    table = f"kms_logs_keys_{tag}"
    create_table(table, _COLUMNS)
    try:
        with plain_connect() as plain:  # setup and rotation: GenerateDataKey twice
            encrypt_column(plain, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
        for i in range(2):  # use: Decrypt, under the first key row and then under the rotated one
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"INSERT INTO {table} (name, ssn) VALUES (%s, %s)", ("it51", f"pt-it51-{i}"))
                    cur.execute(f"SELECT ssn FROM {table} WHERE name = %s ORDER BY id", ("it51",))
                    back = [row[0] for row in cur.fetchall()]
                conn.commit()
            if i == 0:
                with plain_connect() as plain:
                    key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(table, "ssn",
                                                                                            kms_master_key_arn)
                    plain.commit()
        keys = list(dict.fromkeys(data_keys))
        needles = _key_forms(keys)
        # The positive control: a random key of a data key's size, written on purpose in base64, is found by the
        # same scan; no real data key is written.
        control = {f"control {label}": form for label, form in _key_forms([secrets.token_bytes(32)]).items()}
        with plain_connect() as plain, plain.cursor() as cur:
            cur.execute(f"INSERT INTO {table} (name) VALUES (%s)",
                        ("it51-control-" + control["control data key 0 base64"],))
            plain.commit()
        with plain_connect(admin=True) as admin, admin.cursor() as cur:
            cur.execute(f"SELECT id, key_id, name, master_key_arn, encrypted_data_key, hmac_key, key_spec "
                        f"FROM {kms_schema}.key_storage")
            rows = cur.fetchall()
    finally:
        drop_table(table)
    # Counts and labels only, so no assertion can print a key.
    cells = [bytes(c) if isinstance(c, (bytes, bytearray, memoryview)) else str(c).encode() for r in rows for c in r]
    stored = sum(any(key in cell for cell in cells) for key in keys) + \
        sum(any(form.encode() in cell for cell in cells) for form in needles.values())
    in_wrapped = sum(key in base64.b64decode("".join(str(r[4]).split())) for key in keys for r in rows)
    seen_keys = len(keys)
    logged = {label: count for label, count in statement_log_scan({**needles, **control}, kms_statement_log).items()
              if count}
    assert back == ["pt-it51-0", "pt-it51-1"] and seen_keys >= 2, \
        "setup and rotation handed out two data keys, use decrypted them, and the values read back"
    assert (stored, in_wrapped) == (0, 0), "key_storage holds no plaintext data key, only KMS's encrypted form"
    assert logged == {"control data key 0 base64": 1}, \
        "no plaintext data key reaches the statement log; the control key written on purpose is found once"


@kms_builds()
def test_failover_debug_log_holds_no_bound_value(build: str, key_row: int, kms_settings: KmsSettings, kms_schema: str,
                                                 kms_master_key_arn: str, kms_app_user: DbUser,
                                                 kms_log_capture: io.StringIO, data_keys: List[bytes],
                                                 args_logger: str) -> None:
    tag = build.replace("-", "")
    guarded = f"kms_logs_guarded_{tag}"
    values = {"refused": "pt-it52-refused-" + tag, "rejected": "pt-it52-rejected-" + tag}
    logged: Dict[str, int] = {}
    outcomes: Dict[str, Tuple[str, str]] = {}
    with encrypted_table(guarded, _COLUMNS, ("ssn",), kms_settings, kms_schema, kms_master_key_arn,
                         triggered=("ssn",)):
        for chain in ("failover_v2,kms_encryption", f"{args_logger},failover_v2,kms_encryption"):
            start = len(kms_log_capture.getvalue())
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, plugins=chain,
                                 auto_sort_wrapper_plugin_order=False) as conn:
                with conn.cursor() as cur:
                    # Refused by the plugin before anything is sent.
                    refused = raised(conn, lambda: cur.execute(f"DELETE FROM {_TABLE} WHERE ssn <> %s",
                                                               (values["refused"],)))
                    # No column list: passed through, and the trigger rejects the plaintext.
                    rejected = raised(conn, lambda: cur.execute(f"INSERT INTO {guarded} VALUES (DEFAULT, %s, %s)",
                                                                ("it52", values["rejected"])))
            outcomes[chain] = (_error_class(refused), "rejected" if TRIGGER_REJECTION in str(rejected) else
                               f"{type(rejected).__name__}: {rejected}")
            text = kms_log_capture.getvalue()[start:]
            keys = _key_forms(list(dict.fromkeys(data_keys)))
            logged[chain] = sum(value in text for value in values.values())
            logged[f"{chain} data keys"] = sum(form in text for form in keys.values())
    assert outcomes == {"failover_v2,kms_encryption": ("AwsWrapperError from MetadataError", "rejected"),
                        f"{args_logger},failover_v2,kms_encryption": ("AwsWrapperError from MetadataError", "rejected")}, \
        "the plugin refuses the DELETE (failover_v2's execute() wraps the MetadataError) and the trigger rejects the " \
        "unlisted INSERT, so the log covers both paths"
    assert logged == {"failover_v2,kms_encryption": 0, "failover_v2,kms_encryption data keys": 0,
                      f"{args_logger},failover_v2,kms_encryption": 2,
                      f"{args_logger},failover_v2,kms_encryption data keys": 0}, \
        "with failover_v2 before kms_encryption, no DEBUG record holds a bound plaintext or a data key, while a " \
        "plugin that logs its arguments shows the scan finds both values"
