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

import secrets
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List,
                    Optional, Tuple)

import psycopg
import pytest
from django.db import connections, models
from psycopg import sql as pgsql
from sqlalchemy import (Column, Integer, MetaData, String, Table,
                        create_engine, select)
from sqlalchemy.engine import URL
from sqlalchemy.types import UserDefinedType

from aws_advanced_python_wrapper.encryption.sql_analyzer import AnalysisGap
from aws_advanced_python_wrapper.errors import MetadataError
from .utils.conditions import enable_on_engines, enable_on_features
from .utils.connection_utils import ConnectionUtils
from .utils.database_engine import DatabaseEngine
from .utils.kms_encryption_helper import (DB_ERRORS, TRIGGER_REJECTION,
                                          KmsEncryptionTestBase, binary_type,
                                          create_table, drop_table,
                                          encrypted_table, is_pg, kms_builds,
                                          outcome, plain_connect, raised,
                                          read_column, statement_log_scan,
                                          wrapper_connect, write)
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    import io

    from aws_advanced_python_wrapper import AwsWrapperConnection
    from .utils.kms_encryption_helper import (DbUser, KmsSettings,
                                              PayloadVerifier)

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


def _table_rows(build: str) -> List[Tuple[Any, ...]]:
    """Every row of the users table as the server stores it."""
    with plain_connect(build=build) as plain, plain.cursor() as cur:
        cur.execute(f"SELECT id, name, note, ssn FROM {_TABLE} ORDER BY id")
        rows = []
        for row in cur.fetchall():
            rows.append(tuple(bytes(v) if isinstance(v, (bytes, bytearray, memoryview)) else v for v in row))
        return rows


def _refusal(err: Optional[Exception]) -> Tuple[type, Optional[str], Optional[str]]:
    """The class of ``err`` and the table and column it names, which a refusal for an encrypted column carries."""
    return type(err), getattr(err, "table_name", None), getattr(err, "column_name", None)


def _plaintext_stored(build: str, values: List[str]) -> List[str]:
    """The values whose plaintext a stored ``ssn`` holds."""
    stored = [bytes(row[3]) for row in _table_rows(build) if row[3] is not None]
    return [value for value in values if any(value.encode() in payload for payload in stored)]


def _run_every_statement(cur: Any, sql: str, params: Any) -> None:
    """Runs a multi-statement string on ``cur`` and reads each of its result sets to the end."""
    cur.execute(sql, params)
    while True:
        if cur.description is not None:
            cur.fetchall()
        if not cur.nextset():
            return


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
        conflict = f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET ssn = %s"
        merge = (f"MERGE INTO {t} USING (SELECT %s::text AS n) s ON {t}.name = s.n "
                 f"WHEN MATCHED THEN UPDATE SET ssn = %s WHEN NOT MATCHED THEN INSERT (name, ssn) VALUES (s.n, %s)")
        return [(f"upsert-conflict-new-{build}", conflict, ("pt-upsert-ins", "pt-upsert-upd"), "pt-upsert-ins"),
                (f"upsert-conflict-existing-{build}", conflict, ("pt-upsert-ins", "pt-upsert-upd"), "pt-upsert-upd"),
                (f"upsert-merge-new-{build}", merge, ("pt-upsert-upd", "pt-upsert-ins"), "pt-upsert-ins"),
                (f"upsert-merge-existing-{build}", merge, ("pt-upsert-upd", "pt-upsert-ins"), "pt-upsert-upd"),
                (f"upsert-cte-{build}", f"WITH w AS (INSERT INTO {t} (name, ssn) VALUES (%s, %s) RETURNING id) "
                 f"SELECT id FROM w", ("pt-upsert-ins",), "pt-upsert-ins")]
    odku = f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) ON DUPLICATE KEY UPDATE ssn = %s"
    return [(f"upsert-odku-new-{build}", odku, ("pt-upsert-ins", "pt-upsert-upd"), "pt-upsert-ins"),
            (f"upsert-odku-existing-{build}", odku, ("pt-upsert-ins", "pt-upsert-upd"), "pt-upsert-upd")]


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


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
class TestKmsEncryptionStatements(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def key_rows(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                 kms_app_user: DbUser) -> Iterator[Dict[str, int]]:
        """The class's tables: ``ssn`` encrypted in the users table, whose trigger refuses plaintext, and in the two
        CTE tables, under key rows of their own; yields each table's ``key_storage`` id."""
        binary = binary_type()
        with encrypted_table(_TABLE, f"name VARCHAR(100) UNIQUE, note VARCHAR(100), ssn {binary}, alt {binary}",
                             ("ssn",), kms_settings, kms_schema, kms_master_key_arn, triggered=("ssn",)) as users, \
                encrypted_table(_FIRST, f"ssn {binary}", ("ssn",), kms_settings, kms_schema,
                                kms_master_key_arn) as first, \
                encrypted_table(_SECOND, f"ssn {binary}", ("ssn",), kms_settings, kms_schema,
                                kms_master_key_arn) as second:
            create_table(_PLAIN, "ssn VARCHAR(100)")
            try:
                yield {_TABLE: users["ssn"], _FIRST: first["ssn"], _SECOND: second["ssn"]}
            finally:
                drop_table(_PLAIN)

    @pytest.fixture(scope="class")
    def django_users(self, kms_django_settings: None, key_rows: Dict[str, int], kms_settings: KmsSettings,
                     kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser) -> Iterator[Dict[str, Any]]:
        """A Django model of the users table and, for each MySQL build, a database alias of the class's own with the
        plugin; ``default`` is never changed."""
        conn_utils = ConnectionUtils()
        database = {"ENGINE": "aws_advanced_python_wrapper.django.backends.mysql_connector",
                    "NAME": conn_utils.dbname, "HOST": conn_utils.writer_host, "PORT": conn_utils.port}

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
                            "encryption_allowed_master_key_arns": kms_master_key_arn,
                            "use_pure": build == "mysql-pure", **kms_settings.props()}}
        try:
            yield {"model": StatementsUser, "aliases": aliases}
        finally:
            for alias in aliases.values():
                connections[alias].close()
                del connections[alias]
                del connections.databases[alias]

    @kms_builds()
    def test_server_lexed_statement_text_stores_payloads(self, build: str, conn: AwsWrapperConnection,
                                                         key_rows: Dict[str, int],
                                                         kms_verifier: PayloadVerifier) -> None:
        want: Dict[str, str] = {}
        with conn.cursor() as cur:
            for label, literal in _literal_shapes().items():
                name, value = f"lexed-{label}-{build}", f"pt-lexed-{label}"
                cur.execute(f"INSERT INTO {_TABLE} (name, note, ssn) VALUES (%s, {literal}, %s)", (name, value))
                want[name] = value
            if is_pg():
                for label, sql in (("b", f"INSERT INTO {_TABLE} (name, ssn, note) VALUES (%s, %b, %s)"),
                                   ("t", f"INSERT INTO {_TABLE} (name, ssn, note) VALUES (%t, %t, %b)")):
                    name, value = f"lexed-placeholder-{label}-{build}", f"pt-lexed-placeholder-{label}"
                    cur.execute(sql, (name, value, "note"))
                    want[name] = value
            for name, sql, params, ends_with in _upserts(build):
                if name.endswith(f"existing-{build}"):
                    cur.execute(_INSERT, (name, "pt-upsert-before"))
                cur.execute(sql, (name, *params))
                if cur.description is not None:
                    cur.fetchall()
                want[name] = ends_with
        conn.commit()
        opened = {name: [value for _, value in kms_verifier.opened(_TABLE, "ssn", name)] for name in want}
        assert opened == {name: [value] for name, value in want.items()}

    @kms_builds()
    def test_non_default_quoting_stores_no_plaintext(self, build: str, key_rows: Dict[str, int],
                                                     kms_settings: KmsSettings, kms_schema: str,
                                                     kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        # The plugin reads SQL with the default quoting rules, so it can miss these writes' values (the guide lists
        # the settings as unsupported); the trigger refuses the plaintext they would store.
        sent: List[str] = []
        outcomes: Dict[str, str] = {}
        for setting, (statement, shapes) in _non_default_quoting().items():
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as quoted:
                # So a failed write keeps the session's setting on PostgreSQL.
                quoted.autocommit = True
                with quoted.cursor() as cur:
                    cur.execute(statement)
                    for i, sql in enumerate(shapes):
                        name, value = f"quoting-{setting}-{i}-{build}", f"pt-quoting-{setting}-{i}-{build}"
                        sent.append(value)
                        try:
                            cur.execute(sql, (name, value))
                            outcomes[name] = "stored"
                        except Exception as err:  # noqa: BLE001 - the outcome is checked below
                            outcomes[name] = outcome(err)
        assert _plaintext_stored(build, sent) == []
        # Each write is stored, refused by the plugin or rejected by the trigger, never failed otherwise.
        unexpected = {name: ended for name, ended in outcomes.items() if ended not in ("stored", "refused", "rejected")}
        assert unexpected == {}

    @kms_builds()
    def test_refused_statements_leave_rows_unchanged(self, build: str, conn: AwsWrapperConnection,
                                                     key_rows: Dict[str, int]) -> None:
        with conn.cursor() as cur:
            cur.executemany(_INSERT, [(f"refused-1-{build}", "pt-refused-1"), (f"refused-2-{build}", "pt-refused-2")])
        conn.commit()
        t, before = _TABLE, _table_rows(build)
        # A literal in an encrypted column, and each comparison the plugin can't make safe.
        statements = [(f"INSERT INTO {t} (name, ssn) VALUES (%s, 'pt-refused-literal')", (f"refused-literal-{build}",)),
                      (f"DELETE FROM {t} WHERE ssn <> %s", ("pt-refused-neq",)),
                      (f"DELETE FROM {t} WHERE ssn != %s", ("pt-refused-bang",)),
                      (f"UPDATE {t} SET note = %s WHERE ssn < %s", ("refused-changed", "pt-refused-lt")),
                      (f"DELETE FROM {t} WHERE ssn > %s", ("pt-refused-gt",)),
                      (f"DELETE FROM {t} WHERE ssn BETWEEN %s AND %s", ("pt-refused-low", "pt-refused-high")),
                      (f"DELETE FROM {t} WHERE ssn NOT IN (%s)", ("pt-refused-not-in",)),
                      (f"DELETE FROM {t} WHERE ssn NOT LIKE %s", ("pt-refused-not-like",)),
                      (f"DELETE FROM {t} WHERE NOT (ssn = %s)", ("pt-refused-not",)),
                      (f"UPDATE {t} SET note = %s WHERE id IN (SELECT id FROM (SELECT u.id FROM {_PLAIN} p "
                       f"RIGHT JOIN {t} u ON u.ssn = %s) j)", ("refused-changed", "pt-refused-outer-join"))]
        refusals = {}
        for sql, params in statements:
            with conn.cursor() as cur:
                err = raised(conn, lambda: cur.execute(sql, params))
            if err is None:
                conn.commit()
            refusals[sql] = _refusal(err)
        # The literal through executemany.
        with conn.cursor() as cur:
            batch = raised(conn, lambda: cur.executemany(f"INSERT INTO {t} (name, ssn) VALUES (%s, 'pt-refused-batch')",
                                                         [(f"refused-batch-1-{build}",), (f"refused-batch-2-{build}",)]))
        assert _table_rows(build) == before
        # Each refusal names the encrypted column.
        assert refusals == dict.fromkeys(refusals, (MetadataError, t, "ssn"))
        assert _refusal(batch) == (MetadataError, t, "ssn")

    @kms_builds()
    def test_equality_on_encrypted_column_matches_no_row(self, build: str, conn: AwsWrapperConnection,
                                                         key_rows: Dict[str, int], kms_settings: KmsSettings,
                                                         kms_schema: str, kms_master_key_arn: str,
                                                         kms_app_user: DbUser) -> None:
        name, value = f"equality-{build}", "pt-equality"
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, value))
            conn.commit()
            cur.execute(f"SELECT name FROM {_TABLE} WHERE ssn = %s", (value,))
            dbapi = cur.fetchall()
        conn_utils = ConnectionUtils()
        url = URL.create("postgresql+aws_wrapper_psycopg" if is_pg() else "mysql+aws_wrapper_mysqlconnector",
                         username=kms_app_user.name, password=kms_app_user.password, host=conn_utils.writer_host,
                         port=conn_utils.port, database=conn_utils.dbname)
        props: Dict[str, Any] = {"plugins": "kms_encryption", "encryption_metadata_schema": kms_schema,
                                 "encryption_allowed_master_key_arns": kms_master_key_arn, **kms_settings.props()}
        if not is_pg():
            props["use_pure"] = build == "mysql-pure"
        # A ?plugins= in the URL would be dropped.
        engine = create_engine(url, connect_args=props)
        try:
            with engine.connect() as sa_conn:
                orm = sa_conn.execute(select(_USERS.c.name).filter_by(ssn=value)).all()
                by_name = sa_conn.execute(select(_USERS.c.ssn).filter_by(name=name)).all()
        finally:
            engine.dispose()
        # Through DB-API and SQLAlchemy filter_by the value is sent encrypted.
        assert dbapi == []
        assert orm == []
        # The row is there, and SQLAlchemy reads it decrypted.
        assert [tuple(row) for row in by_name] == [(value,)]

    @enable_on_engines([DatabaseEngine.MYSQL])
    @kms_builds()
    def test_django_equality_on_encrypted_column_matches_no_row(self, build: str, conn: AwsWrapperConnection,
                                                                django_users: Dict[str, Any]) -> None:
        name, value = f"equality-django-{build}", "pt-equality-django"
        with conn.cursor() as cur:
            cur.execute(_INSERT, (name, value))
        conn.commit()
        users = django_users["model"].objects.using(django_users["aliases"][build])
        assert users.filter(ssn=value).count() == 0
        assert users.filter(name=name).count() == 1

    @kms_builds()
    def test_annotated_placeholder_stored_encrypted(self, build: str, conn: AwsWrapperConnection,
                                                    key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        plain, mapped, overriding, updated = (f"annotated-{shape}-{build}"
                                              for shape in ("plain", "mapped", "overriding", "updated"))
        t = _TABLE
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {t} (name, ssn) VALUES (%s, /*@encrypt:{t}.ssn*/ %s)", (plain, "pt-annotated-plain"))
            cur.execute(f"INSERT INTO {t} (name, note, ssn) VALUES ('{mapped}', %s, /*@encrypt:{t}.ssn*/ %s)",
                        ("annotated-note", "pt-annotated-mapped"))
            # The column list maps this value to the plain column alt; the annotation names ssn.
            cur.execute(f"INSERT INTO {t} (name, alt) VALUES (%s, /*@encrypt:{t}.ssn*/ %s)",
                        (overriding, "pt-annotated-overriding"))
            cur.execute(_INSERT, (updated, "pt-annotated-before"))
            # A ? inside a literal before the annotated placeholder.
            cur.execute(f"UPDATE {t} SET note = 'really?', ssn = /*@encrypt:{t}.ssn*/ %s WHERE name = %s",
                        ("pt-annotated-updated", updated))
        conn.commit()
        ssn = key_rows[_TABLE]
        # alt has no metadata row of its own, so its payload is opened as ssn's.
        overriding_stored = [kms_verifier.decode(_TABLE, "ssn", payload) for payload in
                             kms_verifier.stored(f"SELECT alt FROM {_TABLE} WHERE name = %s ORDER BY id", (overriding,))]
        assert kms_verifier.opened(_TABLE, "ssn", plain) == [(ssn, "pt-annotated-plain")]
        assert kms_verifier.opened(_TABLE, "ssn", mapped) == [(ssn, "pt-annotated-mapped")]
        assert overriding_stored == [(ssn, "pt-annotated-overriding")]
        assert kms_verifier.opened(_TABLE, "ssn", updated) == [(ssn, "pt-annotated-updated")]

    @kms_builds()
    def test_prepared_and_numbered_placeholders(self, build: str, conn: AwsWrapperConnection,
                                                key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        prepared, numbered = f"placeholder-prepared-{build}", f"placeholder-numbered-{build}"
        if is_pg():
            with conn.cursor() as cur:
                cur.execute(_INSERT, (prepared, "pt-placeholder-prepared"), prepare=True)
            numbered_sql = f"INSERT INTO {_TABLE} (name, ssn) VALUES ($1, $2)"
            numbered_cursor: Dict[str, Any] = {}
        else:
            with conn.cursor(prepared=True) as cur:
                cur.execute(_INSERT, (prepared, "pt-placeholder-prepared"))
            numbered_sql = f"INSERT INTO {_TABLE} (name, ssn) VALUES (?, ?)"
            numbered_cursor = {"prepared": True}
        conn.commit()
        # A write naming an encrypted column with $1 or ? placeholders is refused before anything is sent.
        with pytest.raises(MetadataError), conn.cursor(**numbered_cursor) as cur:
            cur.execute(numbered_sql, (numbered, "pt-placeholder-numbered"))
        conn.rollback()
        assert kms_verifier.opened(_TABLE, "ssn", prepared) == [(key_rows[_TABLE], "pt-placeholder-prepared")]
        assert kms_verifier.stored(_SELECT, (numbered,)) == []

    @kms_builds()
    def test_cte_named_like_encrypted_table_raises_metadata_error(self, build: str, conn: AwsWrapperConnection,
                                                                  key_rows: Dict[str, int]) -> None:
        # Each statement has a CTE with the name of the first table, whose ssn is encrypted. On MySQL, a reads that
        # table. PostgreSQL reads the CTEs in these statements, but it writes the table when an INSERT or UPDATE
        # targets a CTE's name, so the plugin refuses every statement with such a CTE on both engines.
        statements = [f"WITH RECURSIVE a AS (SELECT ssn FROM {_FIRST}), {_FIRST} AS (SELECT ssn FROM {source}) "
                      f"SELECT ssn FROM a" for source in (_SECOND, _PLAIN)]
        statements.append(f"WITH {_FIRST} AS (SELECT ssn FROM {_SECOND}) SELECT z.ssn FROM (WITH RECURSIVE c AS "
                          f"(SELECT ssn FROM {_FIRST}), {_FIRST} AS (SELECT ssn FROM {_PLAIN}) SELECT ssn FROM c) z "
                          f"WHERE NOT EXISTS (SELECT 1 FROM {_TABLE} WHERE 1 = 0)")
        errors = []
        for sql in statements:
            with conn.cursor() as cur:
                errors.append(raised(conn, lambda: cur.execute(sql)))
        assert [type(err) for err in errors] == [MetadataError] * len(statements)

    @kms_builds()
    def test_target_cursor_is_driver_cursor(self, build: str, conn: AwsWrapperConnection) -> None:
        with conn.cursor() as cur, conn.target_connection.cursor() as own:
            assert type(cur.target_cursor) is type(own)

    @kms_builds()
    @pytest.mark.parametrize("how", ["close", "leave-with"])
    def test_cursor_close_closes_driver_cursor(self, build: str, how: str, conn: AwsWrapperConnection) -> None:
        if how == "close":
            cur = conn.cursor()
            target: Any = cur.target_cursor
            cur.close()
        else:
            with conn.cursor() as cur:
                target = cur.target_cursor
        assert isinstance(raised(conn, lambda: target.execute("SELECT 1")), DB_ERRORS)

    @kms_builds()
    def test_target_cursor_bypasses_plugin(self, build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                           kms_verifier: PayloadVerifier) -> None:
        name, unseen = f"target-{build}", f"target-unseen-{build}"
        write(conn, _INSERT, (name, "pt-target"))
        with conn.cursor() as cur:
            target: Any = cur.target_cursor
            target.execute(_SELECT, (name,))
            stored = target.fetchone()[0]
            target.fetchall()
            # A write on the driver cursor isn't encrypted, so the trigger rejects its plaintext with the driver's
            # error.
            with pytest.raises(DB_ERRORS, match=TRIGGER_REJECTION):
                target.execute(_INSERT, (unseen, "pt-target-unseen-" + "x" * 64))
        conn.rollback()
        assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows[_TABLE], "pt-target")]
        # A read on the driver cursor returns the stored payload, which the plugin doesn't decrypt.
        assert bytes(stored) == bytes(kms_verifier.stored(_SELECT, (name,))[0])
        assert kms_verifier.stored(_SELECT, (unseen,)) == []

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_composable_insert_and_select(self, build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                          kms_verifier: PayloadVerifier) -> None:
        name = f"composable-{build}"
        with conn.cursor() as cur:
            cur.execute(pgsql.SQL("INSERT INTO {} (name, ssn) VALUES ({}, {})").format(
                pgsql.Identifier(_TABLE), pgsql.Placeholder(), pgsql.Placeholder()), (name, "pt-composable"))
            conn.commit()
            query = pgsql.SQL("SELECT ssn FROM {} WHERE name = {}").format(pgsql.Identifier(_TABLE), pgsql.Placeholder())
            cur.execute(query, (name,))
            got = cur.fetchone()[0]
        assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows[_TABLE], "pt-composable")]
        assert got == "pt-composable"

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_composed_insert_with_percent_placeholders_encrypted(self, build: str, conn: AwsWrapperConnection,
                                                                 key_rows: Dict[str, int],
                                                                 kms_verifier: PayloadVerifier) -> None:
        name = f"composed-{build}"
        with conn.cursor() as cur:
            cur.execute(pgsql.SQL("INSERT INTO {} (name, ssn) VALUES (%s, %s)").format(pgsql.Identifier(_TABLE)),
                        (name, "pt-composed"))
        conn.commit()
        assert kms_verifier.opened(_TABLE, "ssn", name) == [(key_rows[_TABLE], "pt-composed")]

    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_bytes_sql_encrypted(self, build: str, conn: AwsWrapperConnection, key_rows: Dict[str, int],
                                 kms_verifier: PayloadVerifier) -> None:
        names = [f"bytes-{build}-{i}" for i in range(2)]
        for i, name in enumerate(names):
            with conn.cursor() as cur:
                cur.execute(_INSERT.encode(), (name, f"pt-bytes-{i}"))
            conn.commit()
        assert [kms_verifier.opened(_TABLE, "ssn", name) for name in names] == \
            [[(key_rows[_TABLE], f"pt-bytes-{i}")] for i in range(2)]

    @kms_builds()
    def test_insert_select_passed_to_database(self, build: str, kms_connect: Callable[..., AwsWrapperConnection],
                                              key_rows: Dict[str, int], kms_verifier: PayloadVerifier,
                                              kms_log_capture: io.StringIO) -> None:
        t, staging = _TABLE, f"kms_statements_staging_{build.replace('-', '_')}"
        seed, copy, constant, staged = (f"insert-select-{part}-{build}" for part in ("seed", "copy", "constant", "staged"))
        values = {"seed": "pt-insert-select-seed", "constant": "pt-insert-select-constant-" + "x" * 64,
                  "staged": "pt-insert-select-staged-" + "x" * 64}
        with kms_connect() as conn:
            write(conn, _INSERT, (seed, values["seed"]))
        create_table(staging, f"name VARCHAR(100), ssn {binary_type()}")
        try:
            with plain_connect() as plain:
                write(plain, f"INSERT INTO {staging} (name, ssn) VALUES (%s, %s)", (staged, values["staged"].encode()))
            start = len(kms_log_capture.getvalue())
            # Each statement on a connection of its own, so each logs its pass-through warning.
            with kms_connect() as conn:
                write(conn, f"INSERT INTO {t} (name, ssn) SELECT '{copy}', ssn FROM {t} WHERE name = '{seed}'")
                copied = read_column(conn, _SELECT, copy)
            outcomes = {}
            with kms_connect() as conn:
                outcomes["constant"] = outcome(raised(conn, lambda: write(
                    conn, f"INSERT INTO {t} (name, ssn) SELECT '{constant}', '{values['constant']}'")))
            with kms_connect() as conn:
                outcomes["staged"] = outcome(raised(conn, lambda: write(
                    conn, f"INSERT INTO {t} (name, ssn) SELECT name, ssn FROM {staging}")))
            with kms_connect() as conn:
                parameter_error = raised(conn, lambda: write(
                    conn, f"INSERT INTO {t} (name, ssn) SELECT name, ssn FROM {staging} WHERE name = %s", (staged,)))
            logged = kms_log_capture.getvalue()[start:]
        finally:
            drop_table(staging)
        warnings = [line for line in logged.splitlines()
                    if line.startswith("[KmsEncryptionPlugin]") and AnalysisGap.VALUES_FROM_QUERY.value in line]
        assert copied == [values["seed"]]
        # The copy holds the seed's payload, as the database copied it.
        assert kms_verifier.stored(_SELECT, (copy,)) == kms_verifier.stored(_SELECT, (seed,))
        # Without parameters the statements reach the database, and only the trigger stops the plaintext.
        assert outcomes == {"constant": "rejected", "staged": "rejected"}
        # With a parameter the plugin can't pair with its column, it refuses before anything is sent.
        assert type(parameter_error) is MetadataError
        assert AnalysisGap.VALUES_FROM_QUERY.value in str(parameter_error)
        assert len(warnings) == 3
        assert [label for label, value in values.items() if any(value in line for line in warnings)] == []
        assert kms_verifier.stored(f"SELECT name FROM {t} WHERE name IN (%s, %s)", (constant, staged)) == []

    @kms_builds()
    def test_non_bound_writes_raise_metadata_error(self, build: str, conn: AwsWrapperConnection,
                                                   key_rows: Dict[str, int], kms_verifier: PayloadVerifier) -> None:
        t, name = _TABLE, f"unbound-{build}"
        write(conn, _INSERT, (name, "pt-unbound"))
        before = _table_rows(build)
        statements: Dict[str, Tuple[str, Any]] = {
            "insert-default": (f"INSERT INTO {t} (name, ssn) VALUES (%s, DEFAULT)", (f"unbound-insert-{build}",)),
            "insert-default-inlined": (f"INSERT INTO {t} (name, ssn) VALUES ('unbound-insert-{build}', DEFAULT)", None)}
        expressions = {"default": "DEFAULT", "same-column": "ssn", "other-column": "note",
                       "constant-expression": "CONCAT('999', '-99-9999')"}
        for label, expression in expressions.items():
            statements[f"update-{label}"] = (f"UPDATE {t} SET ssn = {expression} WHERE name = %s", (name,))
            statements[f"update-{label}-inlined"] = (f"UPDATE {t} SET ssn = {expression} WHERE name = '{name}'", None)
        statements["update-function"] = (f"UPDATE {t} SET ssn = upper(%s) WHERE name = %s", ("pt-unbound-upper", name))
        statements["update-function-inlined"] = (f"UPDATE {t} SET ssn = upper('pt-unbound-upper') "
                                                 f"WHERE name = '{name}'", None)
        if is_pg():
            statements["cte-literal"] = (f"WITH w AS (INSERT INTO {t} (name, ssn) VALUES ('unbound-cte-{build}', "
                                         f"'pt-unbound-cte') RETURNING id) SELECT id FROM w", None)
        refusals = {}
        for label, (sql, params) in statements.items():
            with conn.cursor() as cur:
                err = raised(conn, lambda: cur.execute(sql, params))
            refusals[label] = _refusal(err)
        after = _table_rows(build)
        # A NULL in the SQL text is stored.
        write(conn, f"UPDATE {t} SET ssn = NULL WHERE name = %s", (name,))
        assert refusals == dict.fromkeys(statements, (MetadataError, t, "ssn"))
        assert after == before
        assert kms_verifier.stored(_SELECT, (name,)) == [None]

    @kms_builds()
    def test_upsert_copying_incoming_value_stores_payload(self, build: str, conn: AwsWrapperConnection,
                                                          key_rows: Dict[str, int],
                                                          kms_verifier: PayloadVerifier) -> None:
        t = _TABLE
        if is_pg():
            upserts = {"excluded": f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) "
                                   "ON CONFLICT (name) DO UPDATE SET ssn = EXCLUDED.ssn"}
            other_column = f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET ssn = EXCLUDED.note"
        else:
            upserts = {"values": f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) ON DUPLICATE KEY UPDATE ssn = VALUES(ssn)",
                       "row-alias": f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) AS new "
                                    "ON DUPLICATE KEY UPDATE ssn = new.ssn"}
            other_column = f"INSERT INTO {t} (name, ssn) VALUES (%s, %s) ON DUPLICATE KEY UPDATE ssn = VALUES(note)"
        want = {}
        with conn.cursor() as cur:
            for label, sql in upserts.items():
                for row in ("new", "existing"):
                    name, value = f"copy-upsert-{label}-{row}-{build}", f"pt-copy-upsert-{label}-{row}"
                    if row == "existing":
                        cur.execute(_INSERT, (name, "pt-copy-upsert-before"))
                    cur.execute(sql, (name, value))
                    want[name] = value
        conn.commit()
        refused_name = f"copy-upsert-other-column-{build}"
        with conn.cursor() as cur:
            refused = raised(conn, lambda: cur.execute(other_column, (refused_name, "pt-copy-upsert-other-column")))
        opened = {name: kms_verifier.opened(t, "ssn", name) for name in want}
        # Each row holds the bound value's payload; for an existing row the trigger accepted the UPDATE branch.
        assert opened == {name: [(key_rows[t], value)] for name, value in want.items()}
        assert _refusal(refused) == (MetadataError, t, "ssn")
        assert kms_verifier.stored(_SELECT, (refused_name,)) == []

    @kms_builds()
    def test_multi_statement_write_encrypted_or_refused(self, build: str, conn: AwsWrapperConnection,
                                                        key_rows: Dict[str, int], kms_verifier: PayloadVerifier,
                                                        kms_statement_log: str) -> None:
        t = _TABLE
        names = {case: f"multi-{case}-{build}" for case in ("bound", "literal", "no-column-list")}
        # Built by concatenation, so the test's own text never holds a value whole.
        values = {case: "pt-multi-" + case + "-" + secrets.token_hex(4) for case in names}
        no_list_id = "DEFAULT" if is_pg() else "NULL"
        statements = {"bound": (f"SELECT 1; INSERT INTO {t} (name, ssn) VALUES (%s, %s)",
                                (names["bound"], values["bound"])),
                      "literal": (f"SELECT 1; INSERT INTO {t} (name, ssn) VALUES (%s, '{values['literal']}')",
                                  (names["literal"],)),
                      "no-column-list": (f"SELECT 1; INSERT INTO {t} VALUES ({no_list_id}, %s, NULL, %s, NULL)",
                                         (names["no-column-list"], values["no-column-list"]))}
        if is_pg():
            # psycopg's default cursor refuses several statements with parameters; its ClientCursor sends them.
            conn.target_connection.cursor_factory = psycopg.ClientCursor
            try:
                cur = conn.cursor()
            finally:
                conn.target_connection.cursor_factory = psycopg.Cursor
        else:
            cur = conn.cursor()
        errors = {}
        with cur:
            for case, (sql, params) in statements.items():
                errors[case] = raised(conn, lambda: _run_every_statement(cur, sql, params))
                if errors[case] is None:
                    conn.commit()
        logged = statement_log_scan(values, kms_statement_log)
        assert {case: type(err) for case, err in errors.items()} == \
            {"bound": type(None), "literal": MetadataError, "no-column-list": MetadataError}
        assert _refusal(errors["literal"]) == (MetadataError, t, "ssn")
        assert AnalysisGap.NO_COLUMN_LIST.value in str(errors["no-column-list"])
        # The later statement's bound value is stored encrypted, and the refused strings store nothing.
        assert kms_verifier.opened(t, "ssn", names["bound"]) == [(key_rows[t], values["bound"])]
        assert [case for case in ("literal", "no-column-list") if kms_verifier.stored(_SELECT, (names[case],))] == []
        assert logged == dict.fromkeys(values, 0)
