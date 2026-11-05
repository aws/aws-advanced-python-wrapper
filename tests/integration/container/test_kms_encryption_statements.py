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

"""Statements whose text decides what the KMS Encryption Plugin does, on a real server.

The plugin's reading of the SQL has to agree with the server's, so each test checks what the server stored or
returned: a plain read of the table, or the stored bytes opened by a decoder outside the plugin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Tuple

import pytest
from django.db import connections, models
from psycopg import sql as pgsql
from sqlalchemy import (Column, Integer, MetaData, String, Table,
                        create_engine, select)
from sqlalchemy.engine import URL
from sqlalchemy.types import UserDefinedType

from aws_advanced_python_wrapper.errors import AwsWrapperError, MetadataError
from .utils.conditions import disable_on_engines, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (DB_ERRORS, TRIGGER_REJECTION,
                                          binary_type, create_table,
                                          drop_table, encrypted_table, is_pg,
                                          kms_builds, plain_connect, raised,
                                          wrapper_connect, write)
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    import io

    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import (DbUser, KmsSettings,
                                              PayloadVerifier)

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

_TABLE = "kms_statements_users"
# Two encrypted tables and a plain one for the recursive CTE and the outer join.
_FIRST, _SECOND, _PLAIN = "kms_statements_first", "kms_statements_second", "kms_statements_plain"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"


class _EncryptedValue(UserDefinedType):
    """The guide's pass-through type on PostgreSQL, where SQLAlchemy would cast a typed parameter that bytea
    rejects."""
    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        return "BYTEA"


_USERS = Table(_TABLE, MetaData(), Column("id", Integer, primary_key=True), Column("name", String(100)),
               Column("ssn", _EncryptedValue().with_variant(String(1024), "mysql")))


@pytest.fixture(scope="module")
def key_rows(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
             kms_app_user: DbUser) -> Iterator[Dict[str, int]]:
    """The module's tables: ``ssn`` encrypted in the users table, whose trigger refuses plaintext, and in the two CTE
    tables, under key rows of their own; yields each table's ``key_storage`` id."""
    binary = binary_type()
    with encrypted_table(_TABLE, f"name VARCHAR(100) UNIQUE, note VARCHAR(100), ssn {binary}, alt {binary}", ("ssn",),
                         kms_settings, kms_schema, kms_master_key_arn, triggered=("ssn",)) as users, \
            encrypted_table(_FIRST, f"ssn {binary}", ("ssn",), kms_settings, kms_schema, kms_master_key_arn) as first, \
            encrypted_table(_SECOND, f"ssn {binary}", ("ssn",), kms_settings, kms_schema, kms_master_key_arn) as second:
        create_table(_PLAIN, "ssn VARCHAR(100)")
        try:
            yield {_TABLE: users["ssn"], _FIRST: first["ssn"], _SECOND: second["ssn"]}
        finally:
            drop_table(_PLAIN)


@pytest.fixture(scope="module")
def django_users(kms_django_settings: None, key_rows: Dict[str, int], kms_settings: KmsSettings, kms_schema: str,
                 kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[Dict[str, Any]]:
    """A Django model of the users table and, for each MySQL build, a database alias of the module's own with the
    plugin; ``default`` is never changed."""
    env = TestEnvironment.get_current()
    info, writer = env.get_database_info(), env.get_writer()
    database = {"ENGINE": "aws_advanced_python_wrapper.django.backends.mysql_connector",
                "NAME": info.get_default_db_name(), "HOST": writer.get_host(), "PORT": writer.get_port()}

    class StatementsUser(models.Model):
        name: models.CharField[str, str] = models.CharField(max_length=100)
        ssn: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "kms_statements"
            db_table = _TABLE
            managed = False

    aliases = {build: f"kms_statements_{build.replace('-', '_')}" for build in ("mysql-cext", "mysql-pure")}
    for build, alias in aliases.items():
        connections.databases[alias] = {
            **connections["default"].settings_dict, **database, "USER": kms_app_user.name,
            "PASSWORD": kms_app_user.password,
            "OPTIONS": {"plugins": "kms_encryption", "encryption_metadata_schema": kms_schema,
                        "encryption_allowed_master_key_arns": kms_master_key_arn, "use_pure": build == "mysql-pure",
                        **kms_settings.props()}}
    try:
        yield {"model": StatementsUser, "aliases": aliases}
    finally:
        for alias in aliases.values():
            connections[alias].close()
            del connections[alias]
            del connections.databases[alias]


@pytest.fixture
def conn(build: str, key_rows: Dict[str, int], kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
         kms_app_user: DbUser) -> Iterator[AwsWrapperConnection]:
    with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
        yield conn


def _table_rows(build: str) -> List[Tuple[Any, ...]]:
    """Every row of the users table as the server stores it."""
    with plain_connect(build=build) as plain, plain.cursor() as cur:
        cur.execute(f"SELECT id, name, note, ssn FROM {_TABLE} ORDER BY id")
        return [tuple(bytes(v) if isinstance(v, (bytes, bytearray, memoryview)) else v for v in row)
                for row in cur.fetchall()]


def _plaintext_stored(build: str, values: List[str]) -> List[str]:
    """The values whose plaintext a stored ``ssn`` holds."""
    stored = [bytes(row[3]) for row in _table_rows(build) if row[3] is not None]
    return [value for value in values if any(value.encode() in payload for payload in stored)]


def _literal_shapes() -> Dict[str, str]:
    """Literals and comments to put before a parameter, each ended by the server's own lexing rules."""
    shapes = {"doubled-quote": "'it''s'", "semicolon": "'a;b'", "block-comment": "/* it's; */ 'c'",
              "line-comment": "-- it's;\n 'd'"}
    if is_pg():
        shapes.update({"escape-string": "E'it\\'s'", "dollar": "$$it's$$", "tagged-dollar": "$tag$a;b$tag$"})
    else:
        shapes.update({"backslash-escape": "'it\\'s'", "hash-comment": "# it's;\n 'e'"})
    return shapes


def _upserts(build: str) -> List[Tuple[str, str, Any, str]]:
    """Upserts: (name, SQL, parameters, the value the row ends with); a name ending in -existing is written first."""
    t = _TABLE
    if is_pg():
        return [(f"it10-conflict-new-{build}", f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) "
                 f"ON CONFLICT (name) DO UPDATE SET ssn = %s", ("pt-it10-ins", "pt-it10-upd"), "pt-it10-ins"),
                (f"it10-conflict-existing-{build}", f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) "
                 f"ON CONFLICT (name) DO UPDATE SET ssn = %s", ("pt-it10-ins", "pt-it10-upd"), "pt-it10-upd"),
                (f"it10-merge-new-{build}", f"MERGE INTO {t} USING (SELECT %s::text AS n) s ON {t}.name = s.n "
                 f"WHEN MATCHED THEN UPDATE SET ssn = %s WHEN NOT MATCHED THEN INSERT (name, ssn) VALUES (s.n, %s)",
                 ("pt-it10-upd", "pt-it10-ins"), "pt-it10-ins"),
                (f"it10-merge-existing-{build}", f"MERGE INTO {t} USING (SELECT %s::text AS n) s ON {t}.name = s.n "
                 f"WHEN MATCHED THEN UPDATE SET ssn = %s WHEN NOT MATCHED THEN INSERT (name, ssn) VALUES (s.n, %s)",
                 ("pt-it10-upd", "pt-it10-ins"), "pt-it10-upd"),
                (f"it10-cte-{build}", f"WITH w AS (INSERT INTO {t} (name, ssn) VALUES (%s, %s) RETURNING id) "
                 f"SELECT id FROM w", ("pt-it10-ins",), "pt-it10-ins")]
    odku = f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) ON DUPLICATE KEY UPDATE ssn = %s"
    return [(f"it10-odku-new-{build}", odku, ("pt-it10-ins", "pt-it10-upd"), "pt-it10-ins"),
            (f"it10-odku-existing-{build}", odku, ("pt-it10-ins", "pt-it10-upd"), "pt-it10-upd")]


def _non_default_quoting() -> Dict[str, Tuple[str, List[str]]]:
    """Each session setting that changes how the server reads quotes: the statement that sets it, and writes that the
    default rules read differently (each binds name and ssn). Under ANSI_QUOTES, ``"name"`` in the value list is the
    name column, set by the value before it, so the server runs the write."""
    t = _TABLE
    if is_pg():
        return {"standard_conforming_strings=off": (
            "SET standard_conforming_strings = off",
            [f"INSERT INTO {t} (name, note, ssn) VALUES (%s, 'it\\'s', %s)",
             f"INSERT INTO {t} (name, note, ssn) VALUES (%s, 'a\\\\', %s)"])}
    return {"NO_BACKSLASH_ESCAPES": (
        "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_BACKSLASH_ESCAPES')",
        [f"INSERT INTO {t} (name, note, ssn) VALUES (%s, 'a\\', %s)",
         f"INSERT INTO {t} (name, note, ssn) VALUES (%s, 'a\\', %s) /* ' */"]),
        "ANSI_QUOTES": (
        "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',ANSI_QUOTES')",
        [f'INSERT INTO {t} ("name", "ssn") VALUES (%s, %s)', f'INSERT INTO {t} (name, note, ssn) VALUES (%s, "name", %s)'])}


@kms_builds()
def test_statement_text_the_server_reads_its_own_way(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                                     kms_settings: KmsSettings, kms_schema: str,
                                                     kms_master_key_arn: str, kms_app_user: DbUser,
                                                     kms_verifier: PayloadVerifier) -> None:
    want: Dict[str, str] = {}
    with conn.cursor() as cur:
        for label, literal in _literal_shapes().items():
            name, value = f"it10-{label}-{build}", f"pt-it10-{label}"
            cur.execute(f"INSERT INTO {_TABLE} (name, note, ssn) VALUES (%s, {literal}, %s)", (name, value))
            want[name] = value
        if is_pg():
            for label, sql in (("b", f"INSERT INTO {_TABLE} (name, ssn, note) VALUES (%s, %b, %s)"),
                               ("t", f"INSERT INTO {_TABLE} (name, ssn, note) VALUES (%t, %t, %b)")):
                name, value = f"it10-placeholder-{label}-{build}", f"pt-it10-placeholder-{label}"
                cur.execute(sql, (name, value, "note"))
                want[name] = value
        for name, sql, params, ends_with in _upserts(build):
            if name.endswith(f"existing-{build}"):
                cur.execute(_INSERT, (name, "pt-it10-before"))
            cur.execute(sql, (name, *params))
            if cur.description is not None:
                cur.fetchall()
            want[name] = ends_with
    conn.commit()
    opened = {name: [value for _, value in kms_verifier.opened(_TABLE, "ssn", name)] for name in want}
    assert opened == {name: [value] for name, value in want.items()}, \
        "a parameter after a literal or comment the server ends its own way, a %b or %t placeholder, and an " \
        "upsert's values are each stored as a payload of the value bound"


@kms_builds()
def test_write_under_non_default_quoting_never_stores_plaintext(build: str, key_rows: Dict[str, int],
                                                                kms_settings: KmsSettings, kms_schema: str,
                                                                kms_master_key_arn: str, kms_app_user: DbUser) -> None:
    # The plugin reads SQL with the default quoting rules, so it can miss these writes' values (the guide lists the
    # settings as unsupported); the trigger refuses the plaintext they would store.
    sent: List[str] = []
    outcomes: Dict[str, str] = {}
    for setting, (statement, shapes) in _non_default_quoting().items():
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as quoted:
            quoted.autocommit = True  # so a failed write keeps the session's setting on PostgreSQL
            with quoted.cursor() as cur:
                cur.execute(statement)
                for i, sql in enumerate(shapes):
                    name, value = f"it10-{setting}-{i}-{build}", f"pt-it10-{setting}-{i}-{build}"
                    sent.append(value)
                    try:
                        cur.execute(sql, (name, value))
                        outcomes[name] = "stored"
                    except Exception as err:  # noqa: BLE001 - the outcome is checked below
                        if isinstance(err, AwsWrapperError):
                            outcomes[name] = "refused"
                        elif TRIGGER_REJECTION in str(err):
                            outcomes[name] = "rejected"
                        else:
                            outcomes[name] = f"{type(err).__name__}: {err}"
    assert _plaintext_stored(build, sent) == [], \
        f"a write under non-default quoting never stores plaintext (outcomes: {outcomes})"
    assert set(outcomes.values()) <= {"stored", "refused", "rejected"}, \
        f"each write is stored, refused by the plugin or rejected by the trigger, never failed otherwise: {outcomes}"


@kms_builds()
def test_refused_statements_leave_no_row(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int]) -> None:
    """Statements refused before sending leave no row written, changed or deleted."""
    with conn.cursor() as cur:
        cur.executemany(_INSERT, [(f"it11-1-{build}", "pt-it11-1"), (f"it11-2-{build}", "pt-it11-2")])
    conn.commit()
    t, before = _TABLE, _table_rows(build)
    statements = [(f"INSERT INTO {t} (name, ssn) VALUES (%s, 'pt-it11-literal')", (f"it11-literal-{build}",)),
                  (f"DELETE FROM {t} WHERE ssn <> %s", ("pt-it11-neq",)),
                  (f"DELETE FROM {t} WHERE ssn != %s", ("pt-it11-bang",)),
                  (f"UPDATE {t} SET note = %s WHERE ssn < %s", ("it11-changed", "pt-it11-lt")),
                  (f"DELETE FROM {t} WHERE ssn > %s", ("pt-it11-gt",)),
                  (f"DELETE FROM {t} WHERE ssn BETWEEN %s AND %s", ("pt-it11-low", "pt-it11-high")),
                  (f"DELETE FROM {t} WHERE ssn NOT IN (%s)", ("pt-it11-not-in",)),
                  (f"DELETE FROM {t} WHERE ssn NOT LIKE %s", ("pt-it11-not-like",)),
                  (f"DELETE FROM {t} WHERE NOT (ssn = %s)", ("pt-it11-not",)),
                  (f"UPDATE {t} SET note = %s WHERE id IN (SELECT id FROM (SELECT u.id FROM {_PLAIN} p "
                   f"RIGHT JOIN {t} u ON u.ssn = %s) j)", ("it11-changed", "pt-it11-outer-join"))]
    not_refused: List[str] = []
    for sql, params in statements:
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        except MetadataError:
            conn.rollback()
            continue
        conn.commit()
        not_refused.append(sql)
    assert _table_rows(build) == before, "a refused statement leaves no row written, changed or deleted"
    assert not_refused == [], "a literal in an encrypted column and each comparison the plugin can't make safe raise MetadataError"


@kms_builds()
def test_equality_on_an_encrypted_column_matches_no_row(build: str, conn: AwsWrapperConnection,
                                                        key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                        kms_schema: str, kms_master_key_arn: str,
                                                        kms_app_user: DbUser) -> None:
    name, value = f"it12-{build}", "pt-it12"
    with conn.cursor() as cur:
        cur.execute(_INSERT, (name, value))
        conn.commit()
        cur.execute(f"SELECT name FROM {_TABLE} WHERE ssn = %s", (value,))
        dbapi = cur.fetchall()
    env = TestEnvironment.get_current()
    info, writer = env.get_database_info(), env.get_writer()
    url = URL.create("postgresql+aws_wrapper_psycopg" if is_pg() else "mysql+aws_wrapper_mysqlconnector",
                     username=kms_app_user.name, password=kms_app_user.password, host=writer.get_host(),
                     port=writer.get_port(), database=info.get_default_db_name())
    props: Dict[str, Any] = {"plugins": "kms_encryption", "encryption_metadata_schema": kms_schema,
                             "encryption_allowed_master_key_arns": kms_master_key_arn, **kms_settings.props()}
    if not is_pg():
        props["use_pure"] = build == "mysql-pure"
    engine = create_engine(url, connect_args=props)  # a ?plugins= in the URL would be dropped
    try:
        with engine.connect() as sa_conn:
            orm = sa_conn.execute(select(_USERS.c.name).filter_by(ssn=value)).all()
            by_name = sa_conn.execute(select(_USERS.c.ssn).filter_by(name=name)).all()
    finally:
        engine.dispose()
    assert (dbapi, orm) == ([], []), \
        "an equality on an encrypted column, through DB-API and SQLAlchemy filter_by, is sent encrypted and matches no row"
    assert [tuple(row) for row in by_name] == [(value,)], "the row is there, and SQLAlchemy reads it decrypted"


@disable_on_engines([DatabaseEngine.PG])
@kms_builds()
def test_django_equality_on_an_encrypted_column_matches_no_row(build: str, conn: AwsWrapperConnection,
                                                               django_users: Dict[str, Any]) -> None:
    name, value = f"it12-django-{build}", "pt-it12-django"
    with conn.cursor() as cur:
        cur.execute(_INSERT, (name, value))
    conn.commit()
    users = django_users["model"].objects.using(django_users["aliases"][build])
    assert (users.filter(ssn=value).count(), users.filter(name=name).count()) == (0, 1), \
        "a Django filter on an encrypted column is sent encrypted and matches no row, while the row is there"


@kms_builds()
def test_annotated_placeholder_is_stored_encrypted(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                                   kms_verifier: PayloadVerifier) -> None:
    plain, mapped, overriding = (f"it13-{shape}-{build}" for shape in ("plain", "mapped", "overriding"))
    t = _TABLE
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {t} (name, ssn) VALUES (%s, /*@encrypt:{t}.ssn*/ %s)", (plain, "pt-it13-plain"))
        cur.execute(f"INSERT INTO {t} (name, note, ssn) VALUES ('{mapped}', %s, /*@encrypt:{t}.ssn*/ %s)",
                    ("it13-note", "pt-it13-mapped"))
        # The column list maps this value to the plain column alt; the annotation names ssn.
        cur.execute(f"INSERT INTO {t} (name, alt) VALUES (%s, /*@encrypt:{t}.ssn*/ %s)",
                    (overriding, "pt-it13-overriding"))
    conn.commit()
    ssn = key_rows[_TABLE]
    # alt has no metadata row of its own, so its payload is opened as ssn's.
    overriding_stored = [kms_verifier.decode(_TABLE, "ssn", payload) for payload in
                         kms_verifier.stored(f"SELECT alt FROM {_TABLE} WHERE name = %s ORDER BY id", (overriding,))]
    assert (kms_verifier.opened(_TABLE, "ssn", plain), kms_verifier.opened(_TABLE, "ssn", mapped),
            overriding_stored) == \
        ([(ssn, "pt-it13-plain")], [(ssn, "pt-it13-mapped")], [(ssn, "pt-it13-overriding")]), \
        "a placeholder annotated /*@encrypt:table.ssn*/ is stored as a payload under ssn's key row, also where the " \
        "column list names a plain column"


@kms_builds()
def test_prepared_statements_and_other_placeholder_styles(build: str, conn: AwsWrapperConnection,
                                                          key_rows: Dict[str, int],
                                                          kms_verifier: PayloadVerifier) -> None:
    prepared, numbered = f"it14-prepared-{build}", f"it14-numbered-{build}"
    if is_pg():
        with conn.cursor() as cur:
            cur.execute(_INSERT, (prepared, "pt-it14-prepared"), prepare=True)
        numbered_sql = f"INSERT INTO {_TABLE} (name, ssn) VALUES ($1, $2)"
        numbered_cursor: Dict[str, Any] = {}
    else:
        with conn.cursor(prepared=True) as cur:
            cur.execute(_INSERT, (prepared, "pt-it14-prepared"))
        numbered_sql = f"INSERT INTO {_TABLE} (name, ssn) VALUES (?, ?)"
        numbered_cursor = {"prepared": True}
    conn.commit()
    with pytest.raises(MetadataError), conn.cursor(**numbered_cursor) as cur:
        cur.execute(numbered_sql, (numbered, "pt-it14-numbered"))
    conn.rollback()
    assert kms_verifier.opened(_TABLE, "ssn", prepared) == [(key_rows[_TABLE], "pt-it14-prepared")], \
        "a server-side prepared write with %s placeholders stores a payload, as a plain execute does"
    assert kms_verifier.stored(_SELECT, (numbered,)) == [], \
        "a write naming an encrypted column with $1 or ? placeholders is refused before anything is sent"


@kms_builds()
def test_recursive_cte_named_like_a_table(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int]) -> None:
    with plain_connect(build=build) as plain, plain.cursor() as cur:
        for table in (_FIRST, _SECOND, _PLAIN):
            cur.execute(f"DELETE FROM {table}")
        cur.execute(f"INSERT INTO {_PLAIN} (ssn) VALUES ('it15-plain')")
        plain.commit()
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {_FIRST} (ssn) VALUES (%s)", ("pt-it15-first",))
        cur.execute(f"INSERT INTO {_SECOND} (ssn) VALUES (%s)", ("pt-it15-second",))
    conn.commit()
    with plain_connect(build=build) as plain, plain.cursor() as cur:
        cur.execute(f"SELECT ssn FROM {_FIRST}")
        (first_stored,) = [bytes(value) for (value,) in cur.fetchall()]
        cur.execute(f"SELECT ssn FROM {_SECOND}")
        (second_stored,) = [bytes(value) for (value,) in cur.fetchall()]
    # a reads the relation named like the later CTE: the CTE on PostgreSQL, the table on MySQL.
    reads: Dict[str, List[Any]] = {}
    for source in (_SECOND, _PLAIN):
        with conn.cursor() as cur:
            cur.execute(f"WITH RECURSIVE a AS (SELECT ssn FROM {_FIRST}), {_FIRST} AS (SELECT ssn FROM {source}) "
                        f"SELECT ssn FROM a")
            reads[source] = [bytes(v) if isinstance(v, (bytes, bytearray)) else v for (v,) in cur.fetchall()]
    # Nested under an outer CTE of the same name, c reads the inner, later one on PostgreSQL and the outer one on MySQL;
    # the statement also reads the users table, so by name alone ssn matches two encrypted columns.
    with conn.cursor() as cur:
        cur.execute(f"WITH {_FIRST} AS (SELECT ssn FROM {_SECOND}) SELECT z.ssn FROM (WITH RECURSIVE c AS "
                    f"(SELECT ssn FROM {_FIRST}), {_FIRST} AS (SELECT ssn FROM {_PLAIN}) SELECT ssn FROM c) z "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {_TABLE} WHERE 1 = 0)")
        reads["nested"] = [bytes(v) if isinstance(v, (bytes, bytearray)) else v for (v,) in cur.fetchall()]
    if is_pg():
        assert reads == {_SECOND: ["pt-it15-second"], _PLAIN: ["it15-plain"], "nested": ["it15-plain"]}, \
            "on PostgreSQL a reads the later CTE, and its value comes back decrypted, or as written from a plain " \
            "table; nested, the plugin doesn't take c for the outer CTE"
    else:
        assert reads == {_SECOND: ["pt-it15-first"], _PLAIN: [first_stored], "nested": [second_stored]}, \
            "on MySQL a reads the table: its value decrypts by name when exactly one other table read has an " \
            "encrypted ssn, and comes back as stored bytes when none has or two have"


# target_cursor is the driver's cursor, which the plugin doesn't see.

@kms_builds()
def test_target_cursor_is_the_driver_cursor(build: str, conn: AwsWrapperConnection) -> None:
    with conn.cursor() as cur, conn.target_connection.cursor() as own:
        assert type(cur.target_cursor) is type(own), "a wrapper cursor's target_cursor is the driver's own cursor"


@kms_builds()
@pytest.mark.parametrize("how", ["close", "leave-with"])
def test_closing_the_cursor_closes_the_driver_cursor(build: str, how: str, conn: AwsWrapperConnection) -> None:
    if how == "close":
        cur = conn.cursor()
        target: Any = cur.target_cursor
        cur.close()
    else:
        with conn.cursor() as cur:
            target = cur.target_cursor
    closed = isinstance(raised(conn, lambda: target.execute("SELECT 1")), DB_ERRORS)
    assert closed, "closing the wrapper cursor, or leaving its with-block, closes the driver cursor"


@kms_builds()
def test_target_cursor_is_not_seen(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                   kms_verifier: PayloadVerifier) -> None:
    name, unseen = f"target-{build}", f"target-unseen-{build}"
    write(conn, _INSERT, (name, "pt-target"))
    with conn.cursor() as cur:
        target: Any = cur.target_cursor
        target.execute(_SELECT, (name,))
        stored = target.fetchone()[0]
        target.fetchall()
        with pytest.raises(DB_ERRORS, match=TRIGGER_REJECTION):
            target.execute(_INSERT, (unseen, "pt-target-unseen-" + "x" * 64))
    conn.rollback()
    assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows[_TABLE], "pt-target")], \
        "the wrapper cursor's write is stored as a payload"
    assert bytes(stored) == bytes(kms_verifier.stored(_SELECT, (name,))[0]), \
        "a read on the driver cursor returns the stored payload, which the plugin doesn't decrypt"
    assert kms_verifier.stored(_SELECT, (unseen,)) == [], \
        "a write on the driver cursor isn't encrypted, so the trigger rejects its plaintext with the driver's error"


# SQL that isn't str.

@disable_on_engines([DatabaseEngine.MYSQL])
@kms_builds()
def test_composable_insert_and_select(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                      kms_verifier: PayloadVerifier) -> None:
    name = f"composable-{build}"
    with conn.cursor() as cur:
        cur.execute(pgsql.SQL("INSERT INTO {} (name, ssn) VALUES ({}, {})").format(
            pgsql.Identifier(_TABLE), pgsql.Placeholder(), pgsql.Placeholder()), (name, "pt-composable"))
        conn.commit()
        select = pgsql.SQL("SELECT ssn FROM {} WHERE name = {}").format(pgsql.Identifier(_TABLE), pgsql.Placeholder())
        cur.execute(select, (name,))
        got = cur.fetchone()[0]
    assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows[_TABLE], "pt-composable")], \
        "a psycopg Composable INSERT stores a payload"
    assert got == "pt-composable", "a psycopg Composable SELECT reads the value decrypted"


@disable_on_engines([DatabaseEngine.MYSQL])
@kms_builds()
def test_composed_insert_with_percent_placeholders_is_encrypted(build: str, conn: AwsWrapperConnection,
                                                                key_rows: Dict[str, int],
                                                                kms_verifier: PayloadVerifier) -> None:
    name = f"composed-{build}"
    with conn.cursor() as cur:
        cur.execute(pgsql.SQL("INSERT INTO {} (name, ssn) VALUES (%s, %s)").format(pgsql.Identifier(_TABLE)),
                    (name, "pt-composed"))
    conn.commit()
    assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows[_TABLE], "pt-composed")], \
        "a Composed statement with %s placeholders stores a payload"


@disable_on_engines([DatabaseEngine.MYSQL])
@kms_builds()
def test_bytes_sql_is_analyzed_and_encrypted(build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                             kms_verifier: PayloadVerifier, kms_log_capture: io.StringIO) -> None:
    names = [f"bytes-{build}-{i}" for i in range(2)]
    start = len(kms_log_capture.getvalue())
    for i, name in enumerate(names):
        with conn.cursor() as cur:
            cur.execute(_INSERT.encode(), (name, f"pt-bytes-{i}"))
        conn.commit()
    plugin_records = kms_log_capture.getvalue()[start:].count("[KmsEncryptionPlugin]")
    assert [kms_verifier.opened(_TABLE, "ssn", name) for name in names] == \
        [[(key_rows[_TABLE], f"pt-bytes-{i}")] for i in range(2)], \
        "SQL given as bytes is analyzed, so each write stores a payload"
    assert plugin_records == 0, "the plugin logs no pass-through warning for SQL given as ASCII bytes"
