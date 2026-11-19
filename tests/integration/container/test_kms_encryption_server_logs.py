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

"""PostgreSQL logs every statement (``log_statement=all``) and MySQL's general log is switched on by the
``kms_statement_log`` fixture, which first proves the scan finds its own marker. Needles are built by concatenation,
so the test's source never holds them whole.
"""

from __future__ import annotations

import base64
import logging
import secrets
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Set,
                    Tuple)

import psycopg
import pytest

from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.plugin_service import PluginManager
from .utils.conditions import enable_on_engines, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (KmsEncryptionTestBase, binary_type,
                                          create_table, drop_table,
                                          encrypt_column, encrypted_table,
                                          error_chain, is_pg,
                                          key_management_utility, kms_builds,
                                          outcome, plain_connect, raised,
                                          redact, statement_log_scan,
                                          wrapper_connect)
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    import io

    from aws_advanced_python_wrapper import AwsWrapperConnection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from .utils.kms_encryption_helper import (DbUser, KmsSettings,
                                              PayloadVerifier)

_TABLE = "kms_logs_users"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_COLUMNS = f"name VARCHAR(100), ssn {binary_type()}"
_ARGS_LOGGER = "kms_args_logger"


class _ArgsLoggerPlugin(Plugin):
    """A plugin that logs, at DEBUG, the arguments it receives."""

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


def _key_forms(keys: List[bytes]) -> Dict[str, str]:
    """The text forms each data key would take in a statement log, by a label that shows no key: base64 and hex."""
    forms = {}
    for i, key in enumerate(keys):
        forms[f"data key {i} base64"] = base64.b64encode(key).decode()
        forms[f"data key {i} hex"] = key.hex()
        forms[f"data key {i} HEX"] = key.hex().upper()
    return forms


def _hexed(values: Dict[str, str]) -> Dict[str, str]:
    """Each value's hex form, by its label plus " hex": PostgreSQL logs a parameter sent as bytes in hex."""
    return {f"{label} hex": value.encode().hex() for label, value in values.items()}


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionServerLogs(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def key_row(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                kms_app_user: DbUser) -> Iterator[int]:
        """The class's users table with ``ssn`` encrypted; yields the key row's id."""
        with encrypted_table(_TABLE, _COLUMNS, ("ssn",), kms_settings, kms_schema, kms_master_key_arn) as key_ids:
            yield key_ids["ssn"]

    @pytest.fixture
    def args_logger(self) -> Iterator[str]:
        """The code of a plugin, registered for the test, that logs the arguments it receives."""
        kms_weight = PluginManager.PLUGIN_FACTORY_WEIGHTS[PluginManager.PLUGIN_FACTORIES["kms_encryption"]]
        # Below kms_encryption's weight, so this plugin runs before it; the test turns sorting off and lists the chain.
        PluginManager.register_plugin(_ARGS_LOGGER, _ArgsLoggerFactory, kms_weight - 5)
        try:
            yield _ARGS_LOGGER
        finally:
            PluginManager.PLUGIN_FACTORIES.pop(_ARGS_LOGGER, None)
            PluginManager.PLUGIN_FACTORY_WEIGHTS.pop(_ArgsLoggerFactory, None)

    @pytest.fixture
    def data_keys(self, monkeypatch: pytest.MonkeyPatch) -> List[bytes]:
        """Every plaintext data key KMS hands out while the test runs, through KeyManager, in memory only. A failed
        test's traceback shows this list, so the report prints each key's repr, base64 and hex as ``***``."""
        keys: List[bytes] = []
        generate, decrypt = KeyManager.generate_data_key, KeyManager.decrypt_data_key

        def keep(key: bytes) -> None:
            keys.append(key)
            redact(repr(key), *_key_forms([key]).values())

        def generated(self: KeyManager, *args: Any, **kwargs: Any) -> Any:
            out = generate(self, *args, **kwargs)
            keep(bytes(out.plaintext))
            return out

        def decrypted(self: KeyManager, *args: Any, **kwargs: Any) -> Any:
            out = decrypt(self, *args, **kwargs)
            keep(bytes(out))
            return out

        monkeypatch.setattr(KeyManager, "generate_data_key", generated)
        monkeypatch.setattr(KeyManager, "decrypt_data_key", decrypted)
        return keys

    @kms_builds()
    def test_statement_log_holds_no_written_plaintext(self, build: str, conn: AwsWrapperConnection, key_row: int,
                                                      kms_settings: KmsSettings, kms_schema: str,
                                                      kms_master_key_arn: str, kms_statement_log: str,
                                                      data_keys: List[bytes]) -> None:
        tag = build.replace("-", "")
        shapes = ["one", "many-1", "many-2", "multi-1", "multi-2", "update", "compared"]
        if is_pg():
            shapes.append("client")
        values = {shape: "pt-written-" + shape + tag for shape in shapes}
        name = "written-name-" + tag
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, values["one"]))
            cur.executemany(_INSERT, [(name, values["many-1"]), (name, values["many-2"])])
            cur.execute(f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s), (%s, %s)",
                        (name, values["multi-1"], name, values["multi-2"]))
            cur.execute(f"UPDATE {_TABLE} SET ssn = %s WHERE name = %s AND ssn IS NULL", (values["update"], name))
            cur.execute(f"SELECT name FROM {_TABLE} WHERE ssn = %s", (values["compared"],))
            cur.fetchall()
        payload_needles: Dict[str, str] = {}
        if is_pg():
            # psycopg's ClientCursor puts the values into the statement text, so the log shows what it was given.
            conn.target_connection.cursor_factory = psycopg.ClientCursor
            try:
                client = conn.cursor()
            finally:
                conn.target_connection.cursor_factory = psycopg.Cursor
            client_name = "written-client-" + tag
            with client:
                client.execute(_INSERT, (client_name, values["client"]))
            conn.commit()
            with plain_connect() as plain, plain.cursor() as cur:
                cur.execute(f"SELECT ssn FROM {_TABLE} WHERE name = %s", (client_name,))
                payload_needles = {"client payload": bytes(cur.fetchone()[0]).hex()}
        conn.commit()
        setup_table = f"kms_logs_setup_{tag}"
        create_table(setup_table, _COLUMNS)
        try:
            # The utility's setup of a column, under the same log.
            with plain_connect() as plain:
                encrypt_column(plain, kms_settings, kms_schema, setup_table, "ssn", kms_master_key_arn)
                key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(setup_table, "ssn",
                                                                                        kms_master_key_arn)
                plain.commit()
        finally:
            drop_table(setup_table)
        key_needles = _key_forms(list(dict.fromkeys(data_keys)))
        # Counts only, so no assertion can print a key.
        seen_keys = len(set(data_keys))
        hexed = _hexed(values)
        counts = statement_log_scan({**values, **hexed, "name": name, **key_needles, **payload_needles},
                                    kms_statement_log)
        assert {label: counts[label] for label in [*values, *hexed]} == {label: 0 for label in [*values, *hexed]}
        # The ClientCursor's statement text holds the stored payload, so the plugin encrypted the value first.
        assert {label: counts[label] >= 1 for label in payload_needles} == {label: True for label in payload_needles}
        assert seen_keys >= 2
        logged_keys = [label for label in key_needles if counts[label]]
        # No plaintext data key, of the plugin's or the utility's, reaches the statement log.
        assert logged_keys == []
        # The plain name the statements bound does reach the log, so the scan reads it.
        assert counts["name"] >= 4

    @kms_builds()
    def test_compared_parameter_in_statement_log(self, build: str, conn: AwsWrapperConnection, key_row: int,
                                                 kms_statement_log: str) -> None:
        tag = build.replace("-", "")
        direct, through = "pt-compared-direct-" + tag, "pt-compared-coalesce-" + tag
        with conn.cursor() as cur:
            cur.execute(f"SELECT name FROM {_TABLE} WHERE ssn = %s", (direct,))
            direct_rows = cur.fetchall()
            # The plugin can't tie a value compared with an expression to the column, so it sends it as written.
            cur.execute(f"SELECT name FROM {_TABLE} WHERE COALESCE(ssn, ssn) = %s", (through,))
            through_rows = cur.fetchall()
        conn.commit()
        needles = {"direct": direct, "coalesce": through}
        counts = statement_log_scan({**needles, **_hexed(needles)}, kms_statement_log)
        assert direct_rows == []
        assert through_rows == []
        assert counts["coalesce"] + counts["coalesce hex"] >= 1
        # A value compared with the encrypted column is encrypted before it is sent.
        assert counts["direct"] + counts["direct hex"] == 0

    @kms_builds(exclude=("mysql-pure",))
    def test_plaintext_data_key_never_reaches_database(self, build: str, key_row: int, kms_settings: KmsSettings,
                                                       kms_schema: str, kms_master_key_arn: str,
                                                       kms_app_user: DbUser, kms_statement_log: str,
                                                       data_keys: List[bytes]) -> None:
        tag = build.replace("-", "")
        table = f"kms_logs_keys_{tag}"
        create_table(table, _COLUMNS)
        try:
            # Setup and rotation: GenerateDataKey twice.
            with plain_connect() as plain:
                encrypt_column(plain, kms_settings, kms_schema, table, "ssn", kms_master_key_arn)
            # Use: Decrypt, under the first key row and then under the rotated one.
            for i in range(2):
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"INSERT INTO {table} (name, ssn) VALUES (%s, %s)", ("keys", f"pt-keys-{i}"))
                        cur.execute(f"SELECT ssn FROM {table} WHERE name = %s ORDER BY id", ("keys",))
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
                            ("keys-control-" + control["control data key 0 base64"],))
                plain.commit()
            with plain_connect(admin=True) as admin, admin.cursor() as cur:
                cur.execute(f"SELECT id, key_id, name, master_key_arn, encrypted_data_key, hmac_key, key_spec "
                            f"FROM {kms_schema}.key_storage")
                rows = cur.fetchall()
        finally:
            drop_table(table)
        # Counts and labels only, so no assertion can print a key.
        cells: List[bytes] = []
        wrapped_keys: List[bytes] = []
        for row in rows:
            for cell in row:
                if isinstance(cell, (bytes, bytearray, memoryview)):
                    cells.append(bytes(cell))
                else:
                    cells.append(str(cell).encode())
            wrapped_keys.append(base64.b64decode("".join(str(row[4]).split())))
        stored = 0
        for key in keys:
            stored += any(key in cell for cell in cells)
            stored += sum(key in wrapped for wrapped in wrapped_keys)
        for form in needles.values():
            stored += any(form.encode() in cell for cell in cells)
        scan = statement_log_scan({**needles, **control}, kms_statement_log)
        logged = {label: count for label, count in scan.items() if count}
        # Setup and rotation handed out two data keys, and use decrypted them.
        assert back == ["pt-keys-0", "pt-keys-1"]
        assert len(keys) >= 2
        assert stored == 0, "key_storage holds a plaintext data key, not only KMS's encrypted form"
        # The control key written on purpose is found once.
        assert logged == {"control data key 0 base64": 1}

    @kms_builds()
    def test_failover_debug_log_holds_no_bound_value(self, build: str, key_row: int, kms_settings: KmsSettings,
                                                     kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                     kms_log_capture: io.StringIO, data_keys: List[bytes],
                                                     args_logger: str) -> None:
        tag = build.replace("-", "")
        guarded = f"kms_logs_guarded_{tag}"
        values = {"refused": "pt-debug-refused-" + tag, "rejected": "pt-debug-rejected-" + tag,
                  "unmapped": "pt-debug-unmapped-" + tag}
        unlisted = f"INSERT INTO {guarded} VALUES (DEFAULT, %s, %s)"
        logged: Dict[str, int] = {}
        outcomes: Dict[str, Tuple[str, str, str]] = {}
        with encrypted_table(guarded, _COLUMNS, ("ssn",), kms_settings, kms_schema, kms_master_key_arn,
                             triggered=("ssn",)):
            for chain in ("failover_v2,kms_encryption", f"{args_logger},failover_v2,kms_encryption"):
                start = len(kms_log_capture.getvalue())
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, plugins=chain,
                                     auto_sort_wrapper_plugin_order=False) as conn:
                    with conn.cursor() as cur:
                        # No column list: by default refused by the plugin before anything is sent.
                        unmapped = raised(conn, lambda: cur.execute(unlisted, ("debug", values["unmapped"])))
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn, plugins=chain,
                                     auto_sort_wrapper_plugin_order=False,
                                     encryption_fail_on_unmapped_parameter=False) as conn:
                    with conn.cursor() as cur:
                        # Refused by the plugin before anything is sent.
                        refused = raised(conn, lambda: cur.execute(f"DELETE FROM {_TABLE} WHERE ssn <> %s",
                                                                   (values["refused"],)))
                        # No column list, with the refusal off: passed through, and the trigger rejects the plaintext.
                        rejected = raised(conn, lambda: cur.execute(unlisted, ("debug", values["rejected"])))
                outcomes[chain] = (error_chain(refused), outcome(rejected), error_chain(unmapped))
                text = kms_log_capture.getvalue()[start:]
                keys = _key_forms(list(dict.fromkeys(data_keys)))
                logged[chain] = sum(value in text for value in values.values())
                logged[f"{chain} data keys"] = sum(form in text for form in keys.values())
            with plain_connect() as plain, plain.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {guarded}")
                (stored,) = cur.fetchone()
        # The plugin refuses the DELETE and, by default, the unlisted INSERT (failover_v2's execute() wraps the
        # MetadataError); with the refusal off the trigger rejects the unlisted INSERT, so the log covers both paths.
        refusal = "AwsWrapperError from MetadataError"
        assert outcomes == {"failover_v2,kms_encryption": (refusal, "rejected", refusal),
                            f"{args_logger},failover_v2,kms_encryption": (refusal, "rejected", refusal)}
        assert stored == 0
        # With failover_v2 before kms_encryption no DEBUG record holds a bound plaintext or a data key, while a plugin
        # that logs its arguments shows the scan finds the values.
        assert logged == {"failover_v2,kms_encryption": 0, "failover_v2,kms_encryption data keys": 0,
                          f"{args_logger},failover_v2,kms_encryption": 3,
                          f"{args_logger},failover_v2,kms_encryption data keys": 0}

    @kms_builds()
    def test_executable_comments_on_encrypted_table(self, build: str, conn: AwsWrapperConnection, key_row: int,
                                                    kms_verifier: PayloadVerifier, kms_statement_log: str) -> None:
        tag = build.replace("-", "")
        plain_table = f"kms_logs_comment_{tag}"
        target = "comment-target-" + tag
        note = "comment-note-" + tag
        keyword_only = f"INSERT /*!50000 LOW_PRIORITY */ INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
        # MySQL runs the body of a /*! ... */ comment; PostgreSQL reads it as a plain comment.
        shapes = {"keyword-only": (keyword_only, "comment-keyword-" + tag)}
        if not is_pg():
            shapes["hidden-table"] = (f"UPDATE {plain_table} /*! JOIN {_TABLE} ON {_TABLE}.name = {plain_table}.name */ "
                                      f"SET {_TABLE}.ssn = %s WHERE {plain_table}.note = %s", note)
            shapes["written-inside"] = (f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s /*!, %s */)",
                                        "comment-written-" + tag)
            shapes["compared-inside"] = (f"SELECT name FROM {_TABLE} WHERE name = %s /*! AND ssn = %s */",
                                         "comment-compared-" + tag)
        values = {shape: "pt-comment-" + shape + tag for shape in shapes}
        create_table(plain_table, "name VARCHAR(100), note VARCHAR(100)")
        try:
            with plain_connect() as plain, plain.cursor() as cur:
                cur.execute(f"INSERT INTO {plain_table} (name, note) VALUES (%s, %s)", (target, note))
                plain.commit()
            with conn.cursor() as cur:
                cur.execute(_INSERT, (target, "pt-comment-before" + tag))
            conn.commit()
            outcomes = {}
            for shape, (sql, other) in shapes.items():
                # The value is the first parameter in hidden-table and the second in the others.
                if shape == "hidden-table":
                    params = (values[shape], other)
                else:
                    params = (other, values[shape])
                with conn.cursor() as cur:
                    err = raised(conn, lambda: cur.execute(sql, params))
                    if err is None and cur.description is not None:
                        cur.fetchall()
                conn.commit()
                outcomes[shape] = None if err is None else type(err).__name__
            stored = {"keyword-only": kms_verifier.opened(_TABLE, "ssn", shapes["keyword-only"][1]),
                      "target": kms_verifier.opened(_TABLE, "ssn", target)}
        finally:
            drop_table(plain_table)
        hexed = _hexed(values)
        counts = statement_log_scan({**values, **hexed, "target": target}, kms_statement_log)
        if is_pg():
            # On PostgreSQL /*! ... */ is a plain comment, and the value is encrypted.
            assert outcomes == {"keyword-only": None}
            assert stored == {"keyword-only": [(key_row, values["keyword-only"])],
                              "target": [(key_row, "pt-comment-before" + tag)]}
        else:
            # On MySQL the plugin refuses every statement with an executable comment on a table with encrypted
            # columns, so none stores a row or changes the payload of the row the comment names.
            assert outcomes == {shape: "MetadataError" for shape in shapes}
            assert stored == {"keyword-only": [], "target": [(key_row, "pt-comment-before" + tag)]}
        assert {label: counts[label] for label in [*values, *hexed]} == {label: 0 for label in [*values, *hexed]}
        # The plain name the setup bound does reach the log, so the scan reads it.
        assert counts["target"] >= 1

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    def test_multi_table_update_and_delete(self, build: str, conn: AwsWrapperConnection, key_row: int,
                                           kms_verifier: PayloadVerifier, kms_statement_log: str) -> None:
        tag = build.replace("-", "")
        other = f"kms_logs_other_{tag}"
        shapes = {
            "join": f"UPDATE {_TABLE} JOIN {other} ON {other}.name = {_TABLE}.name SET {_TABLE}.ssn = %s "
                    f"WHERE {other}.note = %s",
            "comma": f"UPDATE {_TABLE}, {other} SET {_TABLE}.ssn = %s WHERE {other}.name = {_TABLE}.name "
                     f"AND {other}.note = %s",
            "alias": f"UPDATE {_TABLE} AS u JOIN {other} AS o ON o.name = u.name SET u.ssn = %s WHERE o.note = %s",
            "delete-join": f"DELETE {_TABLE} FROM {_TABLE} JOIN {other} ON {other}.name = {_TABLE}.name "
                           f"WHERE {_TABLE}.ssn = %s AND {other}.note = %s",
            "delete-alias": f"DELETE u FROM {_TABLE} AS u, {other} AS o WHERE o.name = u.name AND u.ssn = %s "
                            f"AND o.note = %s",
            # The plugin can't pair these with their columns, so it refuses them.
            "paren-update": f"UPDATE ({_TABLE} AS u) SET u.ssn = %s WHERE u.name = %s",
            "paren-join-update": f"UPDATE ({_TABLE} JOIN {other} ON {other}.name = {_TABLE}.name) "
                                 f"SET {_TABLE}.ssn = %s WHERE {other}.note = %s",
            "paren-delete": f"DELETE u FROM ({_TABLE} AS u) WHERE u.ssn = %s AND u.name = %s",
            "unqualified": f"UPDATE {_TABLE} JOIN {other} ON {other}.name = {_TABLE}.name SET ssn = %s "
                           f"WHERE {other}.note = %s"}
        names = {shape: f"multi-{shape}-{tag}" for shape in shapes}
        notes = {shape: f"multi-note-{shape}-{tag}" for shape in shapes}
        values = {shape: "pt-multi-" + shape + tag for shape in shapes}
        create_table(other, "name VARCHAR(100), note VARCHAR(100)")
        try:
            with plain_connect() as plain, plain.cursor() as cur:
                cur.executemany(f"INSERT INTO {other} (name, note) VALUES (%s, %s)",
                                [(names[shape], notes[shape]) for shape in shapes])
                plain.commit()
            with conn.cursor() as cur:
                cur.executemany(_INSERT, [(names[shape], "pt-multi-before" + tag) for shape in shapes])
            conn.commit()
            before = {shape: kms_verifier.opened(_TABLE, "ssn", names[shape]) for shape in shapes}
            outcomes = {}
            for shape, sql in shapes.items():
                # The second parameter picks the shape's own row, by its note or, with one table, by its name.
                if shape in ("paren-update", "paren-delete"):
                    params = (values[shape], names[shape])
                else:
                    params = (values[shape], notes[shape])
                with conn.cursor() as cur:
                    err = raised(conn, lambda: cur.execute(sql, params))
                conn.commit()
                outcomes[shape] = None if err is None else type(err).__name__
            after = {shape: kms_verifier.opened(_TABLE, "ssn", names[shape]) for shape in shapes}
        finally:
            drop_table(other)
        hexed = _hexed(values)
        note_needles = {f"{shape} note": note for shape, note in notes.items()}
        counts = statement_log_scan({**values, **hexed, **note_needles}, kms_statement_log)
        updates, deletes = ("join", "comma", "alias"), ("delete-join", "delete-alias")
        refused = ("paren-update", "paren-join-update", "paren-delete", "unqualified")
        assert outcomes == {**dict.fromkeys(updates + deletes), **dict.fromkeys(refused, "MetadataError")}
        # Each multi-table UPDATE stores a payload under the encrypted table's key row.
        assert {shape: after[shape] for shape in updates} == {shape: [(key_row, values[shape])] for shape in updates}
        # A DELETE that compares the encrypted column matches no row, and no refused statement changes a row.
        assert {shape: after[shape] for shape in deletes + refused} == \
            {shape: before[shape] for shape in deletes + refused}
        assert {label: counts[label] for label in [*values, *hexed]} == {label: 0 for label in [*values, *hexed]}
        # The plain notes the setup bound do reach the log, so the scan reads it.
        assert [shape for shape in shapes if counts[f"{shape} note"] == 0] == []
