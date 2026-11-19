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

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from itertools import islice, product
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Tuple

import mysql.connector
import pytest
from psycopg.rows import class_row, dict_row, namedtuple_row, tuple_row

from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (EncryptionError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyMetadataError,
                                                MetadataError)
from .utils.conditions import enable_on_engines, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (KmsEncryptionTestBase, binary_type,
                                          encrypted_table, is_pg,
                                          key_management_utility, kms_builds,
                                          kms_client, plain_connect, raised,
                                          read_column, statement_log_scan,
                                          wrapper_connect, write)
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import (DbUser, KmsSettings,
                                              PayloadVerifier)

_TABLE = "kms_round_trip_users"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_INSERT_DOB = f"INSERT INTO {_TABLE} (name, dob) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_SELECT_DOB = f"SELECT dob FROM {_TABLE} WHERE name = %s ORDER BY id"
_FETCHMANY_DEFECT = ("AwsWrapperCursor.fetchmany() passes size=0 to the driver when no size is given; "
                     "the C extension then returns no rows.")
# Added to the class's last key_storage id, so the payload names an id no key row has.
_UNKNOWN_KEY_ID_OFFSET = 1000
# The mysql-connector cursors each read test opens, by name: conn.cursor(**options).
_MYSQL_CURSOR_KINDS: Dict[str, Dict[str, bool]] = {
    "dictionary": {"dictionary": True}, "buffered": {"buffered": True}, "raw": {"raw": True},
    "prepared": {"prepared": True}, "prepared-dictionary": {"prepared": True, "dictionary": True}}
# A payload's type marker follows the 32-byte HMAC tag and the 4-byte key id (encryption_service.py's layout).
_TYPE_MARKER_OFFSET = 36
# An int too large for the encryption service to serialize.
_TOO_LARGE_INT = 2 ** 70


class _Size(IntEnum):
    LARGE = 7


class _Color(str, Enum):
    RED = "pt-strenum"


# Values the plugin stores in an exact form of another type: (the value written, the value read back).
_EXACT_FORM: Dict[str, Tuple[Any, Any]] = {
    "none": (None, None), "int-enum": (_Size.LARGE, 7), "decimal": (Decimal("12.50"), Decimal("12.50")),
    "str-enum": (_Color.RED, "pt-strenum"),
    "mysql-binary": (mysql.connector.Binary(b"pt-mysql-binary"), b"pt-mysql-binary")}


def _exact_form_kinds() -> List[str]:
    """The kinds of ``_EXACT_FORM`` the engine's driver takes: psycopg can't adapt mysql.connector.Binary."""
    kinds = list(_EXACT_FORM)
    if is_pg():
        kinds.remove("mysql-binary")
    return kinds


def _fake_payload(key_id: int) -> bytes:
    """81 bytes laid out as a payload that names ``key_id``; the test chose every byte, and none is key material."""
    return bytes(32) + key_id.to_bytes(4, "big") + bytes([1]) + bytes(44)


@dataclass
class _UserRow:
    """A row for psycopg's class_row, a row factory the plugin refuses."""
    name: str
    ssn: Any


def _fetched(cur: Any, path: str, size: int = 2) -> List[Any]:
    """Every row of ``cur``'s result, read with ``path``; fetchmany reads two pages of ``size`` rows."""
    if path == "fetchone":
        return list(iter(cur.fetchone, None))
    if path == "fetchmany":
        return cur.fetchmany(size) + cur.fetchmany(size)
    if path == "fetchall":
        return cur.fetchall()
    if path == "next":
        return list(iter(lambda: next(cur, None), None))
    return [row for row in cur]


def _name_and_ssn(row: Any) -> Tuple[Any, Any]:
    """The name and ssn of a dict or namedtuple row."""
    if isinstance(row, dict):
        return row["name"], row["ssn"]
    return row.name, row.ssn


def _ssn(row: Any) -> Any:
    """The ssn of a dict or tuple row."""
    if isinstance(row, dict):
        return row["ssn"]
    return row[0]


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionRoundTrip(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def key_rows(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                 kms_app_user: DbUser) -> Iterator[Dict[str, int]]:
        """The class's table, with ``ssn`` and ``dob`` encrypted under key rows of their own; yields each column's
        ``key_storage`` id."""
        with encrypted_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}, dob {binary_type()}", ("ssn", "dob"),
                             kms_settings, kms_schema, kms_master_key_arn) as key_ids:
            yield key_ids

    @kms_builds()
    def test_bound_write_stores_payload(self, build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                        kms_verifier: PayloadVerifier) -> None:
        one, multi, mapping, keyword, updated = (f"bound-{shape}-{build}"
                                                 for shape in ("one", "multi", "mapping", "keyword", "updated"))
        with conn.cursor() as cur:
            cur.execute(_INSERT, (one, "pt-bound-one"))
            cur.execute(f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s), (%s, %s)",
                        (multi, "pt-bound-multi-1", multi, "pt-bound-multi-2"))
            cur.execute(f"INSERT INTO {_TABLE} (ssn, name) VALUES (%(ssn)s, %(name)s)",
                        {"ssn": "pt-bound-mapping", "name": mapping})
            cur.execute(_INSERT, params=(keyword, "pt-bound-keyword"))
            cur.execute(_INSERT, (updated, "pt-bound-before"))
            cur.execute(f"UPDATE {_TABLE} SET ssn = %s WHERE name = %s", ("pt-bound-after", updated))
        conn.commit()
        ssn = key_rows["ssn"]
        stored = {name: kms_verifier.opened(_TABLE, "ssn", name) for name in (one, multi, mapping, keyword, updated)}
        assert stored == {one: [(ssn, "pt-bound-one")],
                          multi: [(ssn, "pt-bound-multi-1"), (ssn, "pt-bound-multi-2")],
                          mapping: [(ssn, "pt-bound-mapping")], keyword: [(ssn, "pt-bound-keyword")],
                          updated: [(ssn, "pt-bound-after")]}

    @kms_builds()
    def test_fetch_paths_decrypt_rows_in_order(self, build: str, conn: AwsWrapperConnection,
                                               key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        name = f"fetch-{build}"
        values = [f"pt-fetch-{i}" for i in range(4)]
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(name, value) for value in values])
        conn.commit()
        reads: Dict[str, List[Any]] = {}
        for path in ("fetchone", "fetchmany", "fetchall", "next", "for"):
            with conn.cursor() as cur:
                cur.execute(_SELECT, (name,))
                reads[path] = [row[0] for row in _fetched(cur, path, size=3)]
        assert reads == {path: values for path in reads}
        assert [value for _, value in kms_verifier.opened(_TABLE, "ssn", name)] == values

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    def test_mysql_cursor_kinds_and_binary_prefix(self, build: str, conn: AwsWrapperConnection,
                                                  key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                  kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                  kms_verifier: PayloadVerifier, kms_statement_log: str) -> None:
        kinds, strict, prepared, nulls = (f"cursor-{part}-{build}" for part in ("kinds", "strict", "prepared", "nulls"))
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(kinds, "pt-cursor-kinds-1"), (kinds, "pt-cursor-kinds-2")])
            cur.execute(_INSERT, (nulls, None))
            cur.executemany(_INSERT, [(nulls, None), (nulls, "pt-cursor-nulls")])
        with conn.cursor(prepared=True) as cur:
            cur.execute(f"INSERT INTO {_TABLE} (ssn, name) VALUES (%s, %s)", ("pt-cursor-prepared", prepared))
        conn.commit()
        reads: Dict[str, List[Any]] = {}
        # A prepared cursor reads rows over the binary protocol, and with dictionary=True it is another cursor class.
        for kind, options in _MYSQL_CURSOR_KINDS.items():
            with conn.cursor(**options) as cur:
                cur.execute(_SELECT, (kinds,))
                rows = cur.fetchall()
            reads[kind] = [row["ssn"] if isinstance(row, dict) else row[0] for row in rows]
        assert reads == {kind: ["pt-cursor-kinds-1", "pt-cursor-kinds-2"] for kind in _MYSQL_CURSOR_KINDS}
        # Under raise_on_warnings, a Warning 1300 on any of these writes raises.
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                             raise_on_warnings=True) as warned:
            with warned.cursor() as cur:
                cur.execute(_INSERT, (strict, "pt-cursor-strict-1"))
                cur.executemany(_INSERT, [(strict, "pt-cursor-strict-2"), (strict, "pt-cursor-strict-3")])
            warned.commit()
        ssn = key_rows["ssn"]
        stored = {name: kms_verifier.opened(_TABLE, "ssn", name) for name in (strict, prepared, nulls)}
        assert stored == {strict: [(ssn, "pt-cursor-strict-1"), (ssn, "pt-cursor-strict-2"),
                                   (ssn, "pt-cursor-strict-3")],
                          prepared: [(ssn, "pt-cursor-prepared")], nulls: [None, None, (ssn, "pt-cursor-nulls")]}
        # A prepared cursor's SQL reaches the server with ? markers (the Prepare entry). The Execute entry is the
        # server's rendering of the values, and the other writes' entries carry _binary before quoted values.
        logged = statement_log_scan({"prepared statement": f"INSERT INTO {_TABLE} (ssn, name) VALUES (?, ?)",
                                     "_binary before a ? marker": "_bin" + "ary ?"}, kms_statement_log)
        assert logged == {"prepared statement": 1, "_binary before a ? marker": 0}

    @kms_builds()
    def test_executemany_reads_generator_once(self, build: str, conn: AwsWrapperConnection,
                                              key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        listed, generated = f"batch-list-{build}", f"batch-generator-{build}"
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(listed, f"pt-batch-list-{i}") for i in range(3)])
            cur.executemany(_INSERT, ((generated, f"pt-batch-generator-{i}") for i in range(3)))
            cur.executemany(_INSERT, iter(()))
        conn.commit()
        ssn = key_rows["ssn"]
        stored = {name: kms_verifier.opened(_TABLE, "ssn", name) for name in (listed, generated)}
        assert stored == {listed: [(ssn, f"pt-batch-list-{i}") for i in range(3)],
                          generated: [(ssn, f"pt-batch-generator-{i}") for i in range(3)]}

    @kms_builds()
    def test_python_type_values_round_trip(self, build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                           kms_verifier: PayloadVerifier) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        # Each value written, and what it reads back as: an aware datetime as UTC milliseconds, a time as its ISO text.
        cases = [("nul\x00 and non-BMP \U0001F600\U00010348", "nul\x00 and non-BMP \U0001F600\U00010348"),
                 (b"\x00\x01\xff", b"\x00\x01\xff"), (bytearray(b"bytearray\x00"), b"bytearray\x00"),
                 (memoryview(b"memoryview\x80"), b"memoryview\x80"), (-2 ** 63, -2 ** 63), (2 ** 63 - 1, 2 ** 63 - 1),
                 (0.1, 0.1), (-0.0, -0.0), (5e-324, 5e-324), (True, True), (False, False),
                 (Decimal("12.50"), Decimal("12.50")),
                 (datetime(2026, 8, 11, 12, 30, 45, 123456, tzinfo=ist),
                  datetime(2026, 8, 11, 7, 0, 45, 123000, tzinfo=timezone.utc)),
                 (datetime(2026, 8, 11, 12, 30, 45, 123456), datetime(2026, 8, 11, 12, 30, 45, 123456)),
                 (date(2024, 2, 29), date(2024, 2, 29)), (time(9, 26, 53, 120000), "09:26:53.120000")]
        names = [f"type-{i}-{build}" for i in range(len(cases))]
        with conn.cursor() as cur:
            for name, (value, _) in zip(names, cases):
                cur.execute(_INSERT, (name, value))
        conn.commit()
        read = [read_column(conn, _SELECT, name)[0] for name in names]
        opened = [kms_verifier.opened(_TABLE, "ssn", name)[0][1] for name in names]
        # repr shows both the type and the value.
        expected = [repr(back) for _, back in cases]
        assert [repr(value) for value in read] == expected
        assert [repr(value) for value in opened] == expected
        if not is_pg():
            # A prepared cursor reads each value over the binary protocol.
            with conn.cursor(prepared=True) as cur:
                prepared_read = []
                for name in names:
                    cur.execute(_SELECT, (name,))
                    prepared_read.append(cur.fetchall()[0][0])
            assert [repr(value) for value in prepared_read] == expected

    @kms_builds()
    def test_encrypted_columns_use_own_key_rows(self, build: str, conn: AwsWrapperConnection,
                                                kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                                kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
        before, after = f"columns-before-{build}", f"columns-after-{build}"
        insert_both = f"INSERT INTO {_TABLE} (name, ssn, dob) VALUES (%s, %s, %s)"
        with conn.cursor() as cur:
            cur.execute(insert_both, (before, "pt-columns-ssn-1", "pt-columns-dob-1"))
        conn.commit()
        ssn_before = kms_verifier.opened(_TABLE, "ssn", before)
        dob_before = kms_verifier.opened(_TABLE, "dob", before)
        with plain_connect() as plain:
            rotated = key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(
                _TABLE, "dob", kms_master_key_arn)
            plain.commit()
        # A new connection, so the plugin reads the metadata the rotation changed.
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as later:
            ssn_back = read_column(later, _SELECT, before)
            dob_back = read_column(later, _SELECT_DOB, before)
            with later.cursor() as cur:
                cur.execute(insert_both, (after, "pt-columns-ssn-2", "pt-columns-dob-2"))
            later.commit()
        ssn_id, dob_id = ssn_before[0][0], dob_before[0][0]
        assert ssn_id != dob_id
        assert ssn_before == [(ssn_id, "pt-columns-ssn-1")]
        assert dob_before == [(dob_id, "pt-columns-dob-1")]
        assert ssn_back == ["pt-columns-ssn-1"]
        assert dob_back == ["pt-columns-dob-1"]
        assert kms_verifier.opened(_TABLE, "ssn", after) == [(ssn_id, "pt-columns-ssn-2")]
        assert kms_verifier.opened(_TABLE, "dob", after) == [(rotated, "pt-columns-dob-2")]

    @kms_builds()
    def test_later_result_sets_use_own_columns(self, build: str, conn: AwsWrapperConnection,
                                               key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        name = f"sets-{build}"
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, "pt-sets"))
        conn.commit()
        # No parameters: psycopg's default cursor refuses several statements with parameters. Each set is read to its
        # end before nextset(), as mysql-connector's pure build requires.
        with conn.cursor() as cur:
            cur.execute(f"SELECT ssn FROM {_TABLE} WHERE name = '{name}'; "
                        f"SELECT name FROM {_TABLE} WHERE name = '{name}'")
            first = cur.fetchall()
            moved = bool(cur.nextset())
            second = cur.fetchall()
        assert first == [("pt-sets",)]
        assert moved is True
        assert second == [(name,)]
        values = [f"pt-sets-returning-{i}" for i in range(3)]
        with conn.cursor() as cur:
            if is_pg():
                cur.executemany(f"{_INSERT} RETURNING ssn", [(name, value) for value in values], returning=True)
                sets = [cur.fetchall()]
            else:
                # mysql-connector runs a string that isn't an INSERT through execute() for each parameter set.
                cur.executemany(f"SELECT ssn FROM {_TABLE} WHERE name = %s; "
                                f"SELECT name, ssn FROM {_TABLE} WHERE name = %s", [(name, name)])
                # The first set, whose rows the driver has already read when executemany returns.
                cur.fetchall()
                sets = []
            while cur.nextset():
                sets.append(cur.fetchall())
        conn.commit()
        if is_pg():
            assert sets == [[(value,)] for value in values]
        else:
            assert sets == [[(name, "pt-sets")]]

    @kms_builds()
    def test_fetchmany_without_size_reads_arraysize_rows(self, build: str, request: pytest.FixtureRequest,
                                                         conn: AwsWrapperConnection,
                                                         key_rows: Dict[str, int]) -> None:
        if build == "mysql-cext":
            request.applymarker(pytest.mark.xfail(strict=True, reason=_FETCHMANY_DEFECT))
        name = f"page-{build}"
        values = [f"pt-page-{i}" for i in range(5)]
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(name, value) for value in values])
        conn.commit()
        arraysize = 3
        with plain_connect(build=build) as plain, plain.cursor() as cur:
            cur.arraysize = arraysize
            cur.execute(_SELECT, (name,))
            plain_page = len(cur.fetchmany())
            cur.fetchall()
        # psycopg and the pure build read arraysize rows; the C extension's fetchmany() reads one, whatever arraysize
        # says.
        if build == "mysql-cext":
            assert plain_page == 1
        else:
            assert plain_page == arraysize
        with conn.cursor() as cur:
            # The wrapper's arraysize is read-only.
            driver_cursor: Any = cur.target_cursor
            driver_cursor.arraysize = arraysize
            cur.execute(_SELECT, (name,))
            page = [row[0] for row in cur.fetchmany()]
            cur.fetchall()
        assert page == values[:plain_page]

    @kms_builds()
    def test_unknown_key_id_strict_and_lenient(self, build: str, key_rows: Dict[str, int],
                                               kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                               kms_app_user: DbUser, plugin_connection_opens: List[Any]) -> None:
        foreign = f"foreign-{build}"
        with plain_connect() as plain, plain.cursor() as cur:
            cur.execute(f"SELECT max(id) FROM {kms_schema}.key_storage")
            (last_key_id,) = cur.fetchone()
            payload = _fake_payload(last_key_id + _UNKNOWN_KEY_ID_OFFSET)
            cur.execute(_INSERT_DOB, (foreign, payload))
            plain.commit()
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as strict:
            # Loads the column metadata, so the read below opens a plugin connection only to look up key_storage.
            read_column(strict, _SELECT, foreign)
            before = len(plugin_connection_opens)
            with pytest.raises(IntegrityCheckError):
                read_column(strict, _SELECT_DOB, foreign)
            strict_lookups = len(plugin_connection_opens) - before
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                             encryption_return_unverified_data=True) as lenient:
            read_column(lenient, _SELECT, foreign)
            before = len(plugin_connection_opens)
            lenient_back = read_column(lenient, _SELECT_DOB, foreign)
            lenient_lookups = len(plugin_connection_opens) - before
        assert [bytes(value) for value in lenient_back] == [payload]
        assert strict_lookups == 1
        assert lenient_lookups == 1

    @kms_builds()
    def test_unusable_key_row_raises_invalid_key_metadata(self, build: str, key_rows: Dict[str, int],
                                                          kms_settings: KmsSettings, kms_schema: str,
                                                          kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        own = f"own-{build}"
        with plain_connect() as plain, plain.cursor() as cur:
            # The column's current key row, which an earlier test may have rotated away from key_rows["dob"].
            cur.execute(f"SELECT key_id FROM {kms_schema}.encryption_metadata "
                        "WHERE table_name = %s AND column_name = %s", (_TABLE, "dob"))
            (dob_key_id,) = cur.fetchone()
            cur.execute(f"SELECT master_key_arn FROM {kms_schema}.key_storage WHERE id = %s", (dob_key_id,))
            (dob_arn,) = cur.fetchone()
            cur.execute(_INSERT_DOB, (own, _fake_payload(dob_key_id)))
            cur.execute(f"UPDATE {kms_schema}.key_storage SET master_key_arn = %s WHERE id = %s", ("", dob_key_id))
            plain.commit()
        try:
            for lenient_mode in (False, True):
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                     encryption_return_unverified_data=lenient_mode) as unusable:
                    with pytest.raises(InvalidKeyMetadataError):
                        read_column(unusable, _SELECT_DOB, own)
        finally:
            with plain_connect() as plain, plain.cursor() as cur:
                cur.execute(f"UPDATE {kms_schema}.key_storage SET master_key_arn = %s WHERE id = %s",
                            (dob_arn, dob_key_id))
                plain.commit()

    @kms_builds()
    @pytest.mark.parametrize("call", ["execute", "executemany"])
    def test_unserializable_value_raises_encryption_failed(self, build: str, call: str, conn: AwsWrapperConnection,
                                                           key_rows: Dict[str, int],
                                                           kms_verifier: PayloadVerifier) -> None:
        # What the encryption service raises for the value on its own.
        with pytest.raises(EncryptionError) as direct:
            EncryptionService.encrypt(_TOO_LARGE_INT, bytearray(32), bytearray(32), key_id=1)
        name = f"big-{build}-{call}"
        with conn.cursor() as cur:
            if call == "execute":
                err = raised(conn, lambda: cur.execute(_INSERT, (name, _TOO_LARGE_INT)))
            else:
                err = raised(conn, lambda: cur.executemany(_INSERT, [(name, _TOO_LARGE_INT)]))
        assert type(direct.value) is EncryptionFailedError
        assert type(err) is EncryptionFailedError
        assert kms_verifier.stored(_SELECT, (name,)) == []

    @kms_builds()
    @pytest.mark.parametrize("kind", _exact_form_kinds())
    def test_exact_form_value_stored_encrypted(self, build: str, kind: str, conn: AwsWrapperConnection,
                                               key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        value, want = _EXACT_FORM[kind]
        name = f"exact-{build}-{kind}"
        write(conn, _INSERT, (name, value))
        got = read_column(conn, _SELECT, name)
        if want is None:
            assert kms_verifier.opened(_TABLE, "ssn", name) == [None]
        else:
            assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows["ssn"], want)]
        assert got == [want]
        assert [type(item) for item in got] == [type(want)]

    @kms_builds()
    def test_generic_payload_reads_back_as_text(self, build: str, conn: AwsWrapperConnection,
                                                key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                kms_schema: str, kms_master_key_arn: str) -> None:
        value = {"k": "pt-generic"}
        name = f"generic-{build}"
        with plain_connect() as plain, plain.cursor() as cur:
            # The column's current key row, which an earlier test may have rotated away from key_rows["dob"].
            cur.execute(f"SELECT key_id FROM {kms_schema}.encryption_metadata "
                        "WHERE table_name = %s AND column_name = %s", (_TABLE, "dob"))
            (dob_key_id,) = cur.fetchone()
            cur.execute(f"SELECT encrypted_data_key, hmac_key FROM {kms_schema}.key_storage WHERE id = %s",
                        (dob_key_id,))
            encrypted_data_key, hmac_key = cur.fetchone()
        # The encryption service with dob's keys, as a writer other than the plugin would store the value.
        data_key = bytearray(kms_client(kms_settings).decrypt(CiphertextBlob=base64.b64decode(encrypted_data_key),
                                                              KeyId=kms_master_key_arn)["Plaintext"])
        hmac = bytearray(hmac_key)
        try:
            payload = EncryptionService.encrypt(value, data_key, hmac, key_id=dob_key_id)
        finally:
            EncryptionService.wipe(data_key)
            EncryptionService.wipe(hmac)
        assert payload is not None
        assert payload[_TYPE_MARKER_OFFSET] == TypeMarker.GENERIC
        with plain_connect() as plain:
            write(plain, _INSERT_DOB, (name, payload))
        assert read_column(conn, _SELECT_DOB, name) == [str(value)]

    @kms_builds()
    def test_legacy_and_tampered_values_strict_and_lenient(self, build: str, conn: AwsWrapperConnection,
                                                           key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                           kms_schema: str, kms_master_key_arn: str,
                                                           kms_app_user: DbUser) -> None:
        names = {kind: f"legacy-{kind}-{build}" for kind in ("tampered", "genuine", "short", "random")}
        write(conn, _INSERT_DOB, (names["tampered"], "pt-legacy-tampered"))
        write(conn, _INSERT_DOB, (names["genuine"], "pt-legacy-genuine"))
        # A value shorter than a key id, one that names an id no key row has, and a payload of the column's own key
        # with its last byte changed.
        stored = {"short": b"pt-legacy-short", "random": b"x" * 80}
        with plain_connect() as plain, plain.cursor() as cur:
            cur.execute(_SELECT_DOB, (names["tampered"],))
            payload = bytes(cur.fetchone()[0])
            stored["tampered"] = payload[:-1] + bytes([payload[-1] ^ 1])
            cur.execute(f"UPDATE {_TABLE} SET dob = %s WHERE name = %s", (stored["tampered"], names["tampered"]))
            for kind in ("short", "random"):
                cur.execute(_INSERT_DOB, (names[kind], stored[kind]))
            plain.commit()
        errors = {}
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as strict:
            for kind in stored:
                errors[kind] = raised(strict, lambda: read_column(strict, _SELECT_DOB, names[kind]))
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                             encryption_return_unverified_data=True) as lenient, lenient.cursor() as cur:
            cur.execute(f"SELECT name, dob FROM {_TABLE} WHERE name IN (%s, %s, %s, %s) ORDER BY id",
                        tuple(names.values()))
            lenient_rows = cur.fetchall()
        lenient_back: Dict[str, Any] = {}
        for name, value in lenient_rows:
            if isinstance(value, str):
                lenient_back[name] = value
            else:
                lenient_back[name] = bytes(value)
        leaked = []
        for kind, err in errors.items():
            if any(needle in f"{err} {err!r}" for needle in ("pt-legacy", "x" * 8, stored["tampered"].hex())):
                leaked.append(kind)
        assert {kind: type(err) for kind, err in errors.items()} == dict.fromkeys(stored, IntegrityCheckError)
        # The values whose error quotes the stored value or the plaintext.
        assert leaked == []
        # Lenient mode returns the genuine value decrypted and each other value as stored, in one result.
        assert lenient_back == {names["tampered"]: stored["tampered"], names["genuine"]: "pt-legacy-genuine",
                                names["short"]: stored["short"], names["random"]: stored["random"]}

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_psycopg_row_factories_decrypt(self, build: str, conn: AwsWrapperConnection,
                                           key_rows: Dict[str, int]) -> None:
        name = f"factory-{build}"
        values = [f"pt-factory-{i}" for i in range(3)]
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(name, value) for value in values])
        conn.commit()
        select = f"SELECT name, ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
        reads: Dict[Tuple[str, str, str], List[Any]] = {}
        for factory, how, path in product((dict_row, namedtuple_row), ("cursor", "connection"),
                                          ("fetchone", "fetchmany", "fetchall", "for")):
            if how == "cursor":
                cur = conn.cursor(row_factory=factory)
            else:
                conn.target_connection.row_factory = factory
                try:
                    cur = conn.cursor()
                finally:
                    conn.target_connection.row_factory = tuple_row
            with cur:
                cur.execute(select, (name,))
                reads[(factory.__name__, how, path)] = [_name_and_ssn(row) for row in _fetched(cur, path)]
        refused_rows: List[Any] = []

        def read_with_class_row() -> None:
            with conn.cursor(row_factory=class_row(_UserRow)) as cur:
                cur.execute(select, (name,))
                refused_rows.extend(cur.fetchall())

        refused = raised(conn, read_with_class_row)
        assert reads == dict.fromkeys(reads, [(name, value) for value in values])
        # Another row factory is refused before any row is returned.
        assert type(refused) is MetadataError
        assert refused_rows == []

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_psycopg_named_cursor_decrypts_page_by_page(self, build: str, conn: AwsWrapperConnection,
                                                        key_rows: Dict[str, int],
                                                        kms_verifier: PayloadVerifier) -> None:
        name = f"named-{build}"
        values = [f"pt-named-{i:03d}" for i in range(250)]
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(name, value) for value in values])
        conn.commit()
        reads = {}
        for factory in (tuple_row, dict_row):
            with conn.cursor(name=f"kms_named_{factory.__name__}", row_factory=factory) as cur:
                # On the driver's cursor: the wrapper's doesn't pass attribute writes on.
                driver_cursor: Any = cur.target_cursor
                driver_cursor.itersize = 50
                cur.execute(_SELECT, (name,))
                rows = cur.fetchmany(100)
                # Two whole pages, so the loop ends where a page ends.
                rows += list(islice(cur, 100))
                rows += cur.fetchall()
            conn.commit()
            reads[factory.__name__] = [_ssn(row) for row in rows]
        with conn.cursor() as cur:
            streamed = [bytes(row[0]) for row in cur.stream(_SELECT, (name,))]
        conn.commit()
        assert reads == {"tuple_row": values, "dict_row": values}
        # stream() isn't seen by the plugin, so its rows are the stored payloads.
        assert streamed == [bytes(payload) for payload in kms_verifier.stored(_SELECT, (name,))]
