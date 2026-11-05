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

"""Round trips through the KMS Encryption Plugin on a real server.

Each write is checked against the bytes the server stored, opened by a decoder outside the plugin with the column's
keys, and each read against the values written.
"""

from __future__ import annotations

import base64
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Tuple

import mysql.connector
import pytest

from aws_advanced_python_wrapper.encryption.connection_source import \
    PluginConnectionSource
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (EncryptionError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyMetadataError)
from .utils.conditions import disable_on_engines, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (binary_type, encrypted_table, is_pg,
                                          key_management_utility, kms_builds,
                                          kms_client, plain_connect, raised,
                                          read_column, statement_log_scan,
                                          wrapper_connect, write)
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import (DbUser, KmsSettings,
                                              PayloadVerifier)

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

_TABLE = "kms_round_trip_users"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_INSERT_DOB = f"INSERT INTO {_TABLE} (name, dob) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_SELECT_DOB = f"SELECT dob FROM {_TABLE} WHERE name = %s ORDER BY id"
_FETCHMANY_DEFECT = ("AwsWrapperCursor.fetchmany passes size=0 to the driver when no size is given "
                     "(aws_advanced_python_wrapper/wrapper.py:433-435): the C extension's fetchmany(0) returns no "
                     "rows, where its fetchmany() returns one")
# Added to the module's last key_storage id, so the payload names an id no key row has.
_UNKNOWN_KEY_ID_OFFSET = 1000
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


@pytest.fixture(scope="module")
def key_rows(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
             kms_app_user: DbUser) -> Iterator[Dict[str, int]]:
    """The module's table, with ``ssn`` and ``dob`` encrypted under key rows of their own; yields each column's
    ``key_storage`` id."""
    with encrypted_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}, dob {binary_type()}", ("ssn", "dob"),
                         kms_settings, kms_schema, kms_master_key_arn) as key_ids:
        yield key_ids


@pytest.fixture
def conn(build: str, key_rows: Dict[str, int], kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
         kms_app_user: DbUser) -> Iterator[AwsWrapperConnection]:
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        yield conn


def _fake_payload(key_id: int) -> bytes:
    """81 bytes laid out as a payload that names ``key_id``; the test chose every byte, and none is key material."""
    return bytes(32) + key_id.to_bytes(4, "big") + bytes([1]) + bytes(44)


@kms_builds()
def test_bound_write_stores_a_payload(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                      kms_verifier: PayloadVerifier) -> None:
    one, multi, mapping, keyword, updated = (f"it1-{shape}-{build}"
                                             for shape in ("one", "multi", "mapping", "keyword", "updated"))
    with conn.cursor() as cur:
        cur.execute(_INSERT, (one, "pt-it1-one"))
        cur.execute(f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s), (%s, %s)",
                    (multi, "pt-it1-multi-1", multi, "pt-it1-multi-2"))
        cur.execute(f"INSERT INTO {_TABLE} (ssn, name) VALUES (%(ssn)s, %(name)s)",
                    {"ssn": "pt-it1-mapping", "name": mapping})
        cur.execute(_INSERT, params=(keyword, "pt-it1-keyword"))
        cur.execute(_INSERT, (updated, "pt-it1-before"))
        cur.execute(f"UPDATE {_TABLE} SET ssn = %s WHERE name = %s", ("pt-it1-after", updated))
    conn.commit()
    ssn = key_rows["ssn"]
    stored = {name: kms_verifier.opened(_TABLE, "ssn", name) for name in (one, multi, mapping, keyword, updated)}
    assert stored == {one: [(ssn, "pt-it1-one")], multi: [(ssn, "pt-it1-multi-1"), (ssn, "pt-it1-multi-2")],
                      mapping: [(ssn, "pt-it1-mapping")], keyword: [(ssn, "pt-it1-keyword")],
                      updated: [(ssn, "pt-it1-after")]}, \
        "each bound INSERT and UPDATE stores, in the row its other values name, a payload under ssn's key row " \
        "that opens to the bound value"


@kms_builds()
def test_every_fetch_path_decrypts_rows_in_order(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                                 kms_verifier: PayloadVerifier) -> None:
    name = f"it2-{build}"
    values = [f"pt-it2-{i}" for i in range(4)]
    with conn.cursor() as cur:
        cur.executemany(_INSERT, [(name, value) for value in values])
    conn.commit()
    reads: Dict[str, List[Any]] = {}
    for path in ("fetchone", "fetchmany", "fetchall", "next", "for"):
        with conn.cursor() as cur:
            cur.execute(_SELECT, (name,))
            if path == "fetchone":
                rows = list(iter(cur.fetchone, None))
            elif path == "fetchmany":
                rows = cur.fetchmany(3) + cur.fetchmany(3)
            elif path == "fetchall":
                rows = cur.fetchall()
            elif path == "next":
                rows = list(iter(lambda: next(cur, None), None))
            else:
                rows = [row for row in cur]
        reads[path] = [row[0] for row in rows]
    assert reads == {path: values for path in reads}, \
        "fetchone, fetchmany(n), fetchall, next() and a for loop each return every row decrypted, once and in order"
    assert [value for _, value in kms_verifier.opened(_TABLE, "ssn", name)] == values, "the rows read are the rows stored"


@disable_on_engines([DatabaseEngine.PG])
@kms_builds()
def test_mysql_cursor_kinds_and_binary_prefix(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                              kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                                              kms_app_user: DbUser, kms_verifier: PayloadVerifier,
                                              kms_statement_log: str) -> None:
    kinds, strict, prepared, nulls = (f"it3-{part}-{build}" for part in ("kinds", "strict", "prepared", "nulls"))
    with conn.cursor() as cur:
        cur.executemany(_INSERT, [(kinds, "pt-it3-kinds-1"), (kinds, "pt-it3-kinds-2")])
        cur.execute(_INSERT, (nulls, None))
        cur.executemany(_INSERT, [(nulls, None), (nulls, "pt-it3-nulls")])
    with conn.cursor(prepared=True) as cur:
        cur.execute(f"INSERT INTO {_TABLE} (ssn, name) VALUES (%s, %s)", ("pt-it3-prepared", prepared))
    conn.commit()
    reads: Dict[str, List[Any]] = {}
    for kind in ("dictionary", "buffered", "raw"):
        with conn.cursor(**{kind: True}) as cur:
            cur.execute(_SELECT, (kinds,))
            reads[kind] = [row["ssn"] if kind == "dictionary" else row[0] for row in cur.fetchall()]
    assert reads == {kind: ["pt-it3-kinds-1", "pt-it3-kinds-2"] for kind in reads}, \
        "dictionary, buffered and raw cursors return the stored values decrypted"
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                         raise_on_warnings=True) as warned:
        with warned.cursor() as cur:
            cur.execute(_INSERT, (strict, "pt-it3-strict-1"))
            cur.executemany(_INSERT, [(strict, "pt-it3-strict-2"), (strict, "pt-it3-strict-3")])
        warned.commit()
    ssn = key_rows["ssn"]
    stored = {name: kms_verifier.opened(_TABLE, "ssn", name) for name in (strict, prepared, nulls)}
    assert stored == {strict: [(ssn, "pt-it3-strict-1"), (ssn, "pt-it3-strict-2"), (ssn, "pt-it3-strict-3")],
                      prepared: [(ssn, "pt-it3-prepared")], nulls: [None, None, (ssn, "pt-it3-nulls")]}, \
        "writes under raise_on_warnings raise no Warning 1300, a prepared cursor's write stores a payload, and a " \
        "NULL or a batch mixing NULL and a value is stored as written"
    # A prepared cursor's SQL reaches the server with ? markers (the Prepare entry). The Execute entry is the server's
    # rendering of the values, and the other writes' entries carry _binary before quoted values.
    logged = statement_log_scan({"prepared statement": f"INSERT INTO {_TABLE} (ssn, name) VALUES (?, ?)",
                                 "_binary before a ? marker": "_bin" + "ary ?"}, kms_statement_log)
    assert logged == {"prepared statement": 1, "_binary before a ? marker": 0}, \
        "a prepared cursor's statement reaches the server once, with no _binary before its markers"


@kms_builds()
def test_executemany_reads_a_generator_once(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                            kms_verifier: PayloadVerifier) -> None:
    listed, generated = f"it4-list-{build}", f"it4-generator-{build}"
    with conn.cursor() as cur:
        cur.executemany(_INSERT, [(listed, f"pt-it4-list-{i}") for i in range(3)])
        cur.executemany(_INSERT, ((generated, f"pt-it4-generator-{i}") for i in range(3)))
        cur.executemany(_INSERT, iter(()))
    conn.commit()
    ssn = key_rows["ssn"]
    stored = {name: kms_verifier.opened(_TABLE, "ssn", name) for name in (listed, generated)}
    assert stored == {listed: [(ssn, f"pt-it4-list-{i}") for i in range(3)],
                      generated: [(ssn, f"pt-it4-generator-{i}") for i in range(3)]}, \
        "executemany stores every row of a list and of a one-shot generator encrypted, and an empty batch raises nothing"


@kms_builds()
def test_values_of_each_python_type_round_trip(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
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
    names = [f"it5-{i}-{build}" for i in range(len(cases))]
    with conn.cursor() as cur:
        for name, (value, _) in zip(names, cases):
            cur.execute(_INSERT, (name, value))
    conn.commit()
    read = [read_column(conn, _SELECT, name)[0] for name in names]
    opened = [kms_verifier.opened(_TABLE, "ssn", name)[0][1] for name in names]
    expected = [repr(back) for _, back in cases]
    assert [repr(value) for value in read] == expected, \
        "each Python type reads back through the plugin with its type and value (repr shows both)"
    assert [repr(value) for value in opened] == expected, "each stored payload opens to the same typed value"


@kms_builds()
def test_two_encrypted_columns_use_their_own_key_rows(build: str, conn: AwsWrapperConnection,
                                                      kms_settings: KmsSettings, kms_schema: str,
                                                      kms_master_key_arn: str, kms_app_user: DbUser,
                                                      kms_verifier: PayloadVerifier) -> None:
    before, after = f"it6-before-{build}", f"it6-after-{build}"
    insert_both = f"INSERT INTO {_TABLE} (name, ssn, dob) VALUES (%s, %s, %s)"
    with conn.cursor() as cur:
        cur.execute(insert_both, (before, "pt-it6-ssn-1", "pt-it6-dob-1"))
    conn.commit()
    ssn_before, dob_before = kms_verifier.opened(_TABLE, "ssn", before), kms_verifier.opened(_TABLE, "dob", before)
    with plain_connect() as plain:
        rotated = key_management_utility(plain, kms_settings, kms_schema).rotate_data_key(_TABLE, "dob", kms_master_key_arn)
        plain.commit()
    # A new connection, so the plugin reads the metadata the rotation changed.
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as later:
        old_back = (read_column(later, _SELECT, before), read_column(later, _SELECT_DOB, before))
        with later.cursor() as cur:
            cur.execute(insert_both, (after, "pt-it6-ssn-2", "pt-it6-dob-2"))
        later.commit()
    ssn_id, dob_id = ssn_before[0][0], dob_before[0][0]
    assert ssn_id != dob_id and [value for _, value in ssn_before + dob_before] == ["pt-it6-ssn-1", "pt-it6-dob-1"], \
        "each encrypted column's payload records its own key_storage id and opens to its own value"
    assert old_back == (["pt-it6-ssn-1"], ["pt-it6-dob-1"]), \
        "after dob's data key is rotated, the values written under the old key rows still read back"
    assert (kms_verifier.opened(_TABLE, "ssn", after), kms_verifier.opened(_TABLE, "dob", after)) == \
        ([(ssn_id, "pt-it6-ssn-2")], [(rotated, "pt-it6-dob-2")]), \
        "after the rotation, a new dob value uses the new key row and a new ssn value keeps ssn's"


@kms_builds()
def test_later_result_sets_use_their_own_columns(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                                 kms_verifier: PayloadVerifier) -> None:
    name = f"it7-{build}"
    with conn.cursor() as cur:
        cur.execute(_INSERT, (name, "pt-it7"))
    conn.commit()
    # No parameters: psycopg's default cursor refuses several statements with parameters. Each set is read to its end
    # before nextset(), as mysql-connector's pure build requires.
    with conn.cursor() as cur:
        cur.execute(f"SELECT ssn FROM {_TABLE} WHERE name = '{name}'; SELECT name FROM {_TABLE} WHERE name = '{name}'")
        first = cur.fetchall()
        moved = bool(cur.nextset())
        second = cur.fetchall()
    assert (first, moved, second) == ([("pt-it7",)], True, [(name,)]), \
        "a later result set of one execute() is never decoded with the first set's columns"
    want: List[List[Any]]
    with conn.cursor() as cur:
        if is_pg():
            values = [f"pt-it7-returning-{i}" for i in range(3)]
            cur.executemany(f"{_INSERT} RETURNING ssn", [(name, value) for value in values], returning=True)
            sets = [cur.fetchall()]
            while cur.nextset():
                sets.append(cur.fetchall())
            want = [[(value,)] for value in values]
        else:
            # mysql-connector runs a string that isn't an INSERT through execute() for each parameter set.
            cur.executemany(f"SELECT ssn FROM {_TABLE} WHERE name = %s; SELECT name, ssn FROM {_TABLE} WHERE name = %s",
                            [(name, name)])
            cur.fetchall()  # the first set, whose rows the driver has already read when executemany returns
            sets = []
            while cur.nextset():
                sets.append(cur.fetchall())
            want = [[(name, "pt-it7")]]
    conn.commit()
    assert sets == want, "each result set of an executemany is decoded by its own columns"


@kms_builds()
def test_fetchmany_without_a_size_reads_arraysize_rows(build: str, request: pytest.FixtureRequest,
                                                       conn: AwsWrapperConnection, key_rows: Dict[str, int]) -> None:
    """fetchmany() without a size returns arraysize decrypted rows, as a plain connection does."""
    if build == "mysql-cext":
        request.applymarker(pytest.mark.xfail(strict=True, reason=_FETCHMANY_DEFECT))
    name = f"it8-{build}"
    values = [f"pt-it8-{i}" for i in range(5)]
    with conn.cursor() as cur:
        cur.executemany(_INSERT, [(name, value) for value in values])
    conn.commit()
    arraysize = 3
    with plain_connect(build=build) as plain, plain.cursor() as cur:
        cur.arraysize = arraysize
        cur.execute(_SELECT, (name,))
        plain_page = len(cur.fetchmany())
        cur.fetchall()
    # psycopg and the pure build read arraysize rows; the C extension's fetchmany() reads one, whatever arraysize says.
    assert plain_page == (1 if build == "mysql-cext" else arraysize), \
        "the plain driver's fetchmany() with no size reads arraysize rows, or one on the C extension"
    with conn.cursor() as cur:
        driver_cursor: Any = cur.target_cursor  # the wrapper's arraysize is read-only
        driver_cursor.arraysize = arraysize
        cur.execute(_SELECT, (name,))
        page = [row[0] for row in cur.fetchmany()]
        cur.fetchall()
    assert (len(page), page) == (plain_page, values[:plain_page]), \
        "fetchmany() with no size returns as many rows as the plain driver's fetchmany(), decrypted"


@kms_builds()
def test_legacy_and_foreign_key_id_values_strict_and_lenient(build: str, monkeypatch: pytest.MonkeyPatch,
                                                             key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                             kms_schema: str, kms_master_key_arn: str,
                                                             kms_app_user: DbUser) -> None:
    foreign = f"it9-foreign-{build}"
    with plain_connect() as plain, plain.cursor() as cur:
        cur.execute(f"SELECT max(id) FROM {kms_schema}.key_storage")
        (last_key_id,) = cur.fetchone()
        payload = _fake_payload(last_key_id + _UNKNOWN_KEY_ID_OFFSET)
        cur.execute(_INSERT_DOB, (foreign, payload))
        plain.commit()
    opened: List[Any] = []
    open_connection = PluginConnectionSource._open

    def counted_open(source: PluginConnectionSource) -> Any:
        opened.append(source)
        return open_connection(source)

    monkeypatch.setattr(PluginConnectionSource, "_open", counted_open)
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as strict:
        read_column(strict, _SELECT, foreign)  # loads the column metadata, so the read below opens a plugin connection only to look up key_storage
        before = len(opened)
        with pytest.raises(IntegrityCheckError):
            read_column(strict, _SELECT_DOB, foreign)
        strict_lookups = len(opened) - before
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                         encryption_return_unverified_data=True) as lenient:
        read_column(lenient, _SELECT, foreign)
        before = len(opened)
        lenient_back = read_column(lenient, _SELECT_DOB, foreign)
        lenient_lookups = len(opened) - before
    assert [bytes(value) for value in lenient_back] == [payload], \
        "in lenient mode a value naming a key_storage id that doesn't exist comes back as stored"
    assert (strict_lookups, lenient_lookups) == (1, 1), "each lookup of a stored key_storage id opens one plugin connection"


@kms_builds()
def test_legacy_and_foreign_key_id_values_unusable_key_row(build: str, key_rows: Dict[str, int],
                                                           kms_settings: KmsSettings, kms_schema: str,
                                                           kms_master_key_arn: str, kms_app_user: DbUser) -> None:
    own = f"it9-own-{build}"
    with plain_connect() as plain, plain.cursor() as cur:
        # The column's current key row, which an earlier test may have rotated away from key_rows["dob"].
        cur.execute(f"SELECT key_id FROM {kms_schema}.encryption_metadata WHERE table_name = %s AND column_name = %s",
                    (_TABLE, "dob"))
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
            cur.execute(f"UPDATE {kms_schema}.key_storage SET master_key_arn = %s WHERE id = %s", (dob_arn, dob_key_id))
            plain.commit()


@kms_builds()
@pytest.mark.parametrize("call", ["execute", "executemany"])
def test_unserializable_value_keeps_the_encryption_service_error(build: str, call: str, conn: AwsWrapperConnection,
                                                                 key_rows: Dict[str, int],
                                                                 kms_verifier: PayloadVerifier) -> None:
    with pytest.raises(EncryptionError) as direct:  # what the encryption service raises for it on its own
        EncryptionService.encrypt(_TOO_LARGE_INT, bytearray(32), bytearray(32), key_id=1)
    name = f"big-{build}-{call}"
    with conn.cursor() as cur:
        err = raised(conn, (lambda: cur.execute(_INSERT, (name, _TOO_LARGE_INT))) if call == "execute"
                     else (lambda: cur.executemany(_INSERT, [(name, _TOO_LARGE_INT)])))
    assert (type(direct.value), type(err)) == (EncryptionFailedError, EncryptionFailedError), \
        "an int too large to encrypt fails with the encryption service's own EncryptionFailedError"
    assert kms_verifier.stored(_SELECT, (name,)) == [], "nothing is stored"


@kms_builds()
@pytest.mark.parametrize("kind", [kind for kind in _EXACT_FORM if kind != "mysql-binary" or not is_pg()])
def test_value_with_an_exact_stored_form_is_still_encrypted(build: str, kind: str, conn: AwsWrapperConnection,
                                                            key_rows: Dict[str, int],
                                                            kms_verifier: PayloadVerifier) -> None:
    value, want = _EXACT_FORM[kind]
    name = f"exact-{build}-{kind}"
    write(conn, _INSERT, (name, value))
    got = read_column(conn, _SELECT, name)
    assert kms_verifier.opened(_TABLE, "ssn", name) == [None if want is None else (key_rows["ssn"], want)], \
        "the value is stored as a payload of its exact form (and None as NULL)"
    assert (got, [type(v) for v in got]) == ([want], [type(want)]), "it reads back as that form, with its type"


@kms_builds()
def test_stored_generic_payload_reads_back_as_text(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                                   kms_settings: KmsSettings, kms_schema: str,
                                                   kms_master_key_arn: str) -> None:
    value = {"k": "pt-generic"}
    name = f"generic-{build}"
    with plain_connect() as plain, plain.cursor() as cur:
        # The column's current key row, which an earlier test may have rotated away from key_rows["dob"].
        cur.execute(f"SELECT key_id FROM {kms_schema}.encryption_metadata WHERE table_name = %s AND column_name = %s",
                    (_TABLE, "dob"))
        (dob_key_id,) = cur.fetchone()
        cur.execute(f"SELECT encrypted_data_key, hmac_key FROM {kms_schema}.key_storage WHERE id = %s", (dob_key_id,))
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
    assert payload is not None and payload[_TYPE_MARKER_OFFSET] == TypeMarker.GENERIC, "the service stores a dict under the GENERIC type marker"
    with plain_connect() as plain:
        write(plain, _INSERT_DOB, (name, payload))
    assert read_column(conn, _SELECT_DOB, name) == [str(value)], "a GENERIC payload reads back as its text"
