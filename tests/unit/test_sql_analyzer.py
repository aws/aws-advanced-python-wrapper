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

import dataclasses
import logging
import sys
import threading

import pytest
import sqlglot
from sqlglot import exp

from aws_advanced_python_wrapper.encryption import sql_analyzer
from aws_advanced_python_wrapper.encryption.sql_analyzer import (AnalysisGap,
                                                                 StatementKind,
                                                                 TableRef,
                                                                 analyze)
from aws_advanced_python_wrapper.encryption.sql_dialect import MYSQL, PG
from aws_advanced_python_wrapper.encryption.sql_tokenizer import (ColumnRef,
                                                                  ParamStyle)


@pytest.fixture(autouse=True)
def _fresh_cache():
    # The analysis cache is process-wide: without this a test can pass on an
    # earlier test's cached result instead of the code path it names.
    sql_analyzer._cached_analysis.cache_clear()
    yield
    sql_analyzer._cached_analysis.cache_clear()


BOTH = pytest.mark.parametrize("dialect", [PG, MYSQL], ids=["pg", "mysql"])
P = (1,)  # positional parameters
N = {"x": 1}  # named parameters


def assigned(a):
    return [(u.ordinal, str(u.column)) for u in a.assignments]


def compared(a):
    return [(u.ordinal, str(u.column)) for u in a.comparisons]


def unbound(a):
    return [str(c) for c in a.unbound_writes]


def names(tables):
    return [str(t) for t in tables]


class TestInsert:
    @BOTH
    def test_columns_pair_with_their_placeholders(self, dialect):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s)", dialect, P)
        assert a.kind is StatementKind.INSERT and a.writes and a.complete
        assert assigned(a) == [(0, "users.name"), (1, "users.ssn")]
        assert names(a.written_tables) == ["users"]
        assert a.parameter_columns() == {0: ColumnRef("name", "users"), 1: ColumnRef("ssn", "users")}

    @BOTH
    def test_multi_row_values(self, dialect):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s), (%s, %s)", dialect, P)
        assert assigned(a) == [(0, "users.name"), (1, "users.ssn"), (2, "users.name"), (3, "users.ssn")]

    @BOTH
    def test_named_placeholders(self, dialect):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%(n)s, %(s)s)", dialect, N)
        pairs = [(u.placeholder.key, str(u.column)) for u in a.assignments]
        assert pairs == [("n", "users.name"), ("s", "users.ssn")]

    @BOTH
    def test_expression_literal_and_default_are_unbound(self, dialect):
        a = analyze("INSERT INTO users (a, b, c, d) VALUES (lower(%s), 'x', DEFAULT, %s)", dialect, P)
        assert assigned(a) == [(1, "users.d")]
        assert unbound(a) == ["users.a", "users.b", "users.c"]
        assert a.complete

    @BOTH
    def test_null_needs_no_encryption(self, dialect):
        a = analyze("INSERT INTO users (a, b) VALUES (%s, NULL)", dialect, P)
        assert assigned(a) == [(0, "users.a")]
        assert unbound(a) == []

    @BOTH
    def test_casts_are_reported(self, dialect):
        sql = ("INSERT INTO users (a, b) VALUES (%s::VARCHAR, CAST(%s AS BINARY))" if dialect is PG
               else "INSERT INTO users (a, b) VALUES (CAST(%s AS CHAR), CAST(%s AS BINARY))")
        a = analyze(sql, dialect, P)
        assert assigned(a) == [(0, "users.a"), (1, "users.b")]
        assert all(u.casts for u in a.assignments)

    def test_casts_are_outermost_first(self):
        a = analyze("INSERT INTO users (a) VALUES (CAST(%s::text AS bytea))", PG, P)
        assert a.assignments[0].casts == ("VARBINARY", "TEXT")

    @BOTH
    def test_no_column_list(self, dialect):
        a = analyze("INSERT INTO users VALUES (%s, %s)", dialect, P)
        assert a.writes and a.gap is AnalysisGap.NO_COLUMN_LIST
        assert assigned(a) == []

    def test_default_values(self):
        assert analyze("INSERT INTO users DEFAULT VALUES", PG).gap is AnalysisGap.NO_COLUMN_LIST

    @BOTH
    def test_insert_select(self, dialect):
        a = analyze("INSERT INTO users (name, ssn) SELECT name, ssn FROM staging WHERE id = %s", dialect, P)
        assert a.gap is AnalysisGap.VALUES_FROM_QUERY
        assert assigned(a) == []
        assert compared(a) == [(0, "staging.id")]
        assert names(a.tables) == ["users", "staging"] and names(a.written_tables) == ["users"]

    @BOTH
    def test_row_length_mismatch(self, dialect):
        assert analyze("INSERT INTO users (a, b) VALUES (%s)", dialect, P).gap is AnalysisGap.ROW_LENGTH_MISMATCH

    def test_schema_qualified_table(self):
        a = analyze("INSERT INTO app.users (ssn) VALUES (%s)", PG, P)
        assert a.written_tables == (TableRef("users", "app"),)
        assert assigned(a) == [(0, "users.ssn")]

    def test_alias_with_column_list(self):
        a = analyze("INSERT INTO users AS u (ssn) VALUES (%s)", PG, P)
        assert assigned(a) == [(0, "users.ssn")]

    def test_returning_columns(self):
        a = analyze("INSERT INTO users (ssn) VALUES (%s) RETURNING id, ssn AS s, 1", PG, P)
        assert a.result_columns == (ColumnRef("id", "users"), ColumnRef("ssn", "users"), None)


class TestUpsert:
    def test_on_conflict_assignments(self):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET ssn = %s, note = 'x' WHERE users.age > %s", PG, P)
        assert assigned(a) == [(0, "users.name"), (1, "users.ssn"), (2, "users.ssn")]
        assert unbound(a) == ["users.note"]
        assert compared(a) == [(3, "users.age")]

    def test_excluded_copy_of_the_same_column_is_not_a_write(self):
        a = analyze("INSERT INTO users (ssn) VALUES (%s) ON CONFLICT (id) DO UPDATE SET ssn = EXCLUDED.ssn",
                    PG, P)
        assert unbound(a) == []

    def test_excluded_copy_of_another_column_is(self):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET ssn = EXCLUDED.name", PG, P)
        assert unbound(a) == ["users.ssn"]

    def test_on_conflict_do_nothing(self):
        a = analyze("INSERT INTO users (ssn) VALUES (%s) ON CONFLICT DO NOTHING", PG, P)
        assert assigned(a) == [(0, "users.ssn")] and a.complete

    def test_on_duplicate_key_update(self):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE ssn = %s, name = VALUES(name), note = VALUES(ssn)", MYSQL, P)
        assert assigned(a) == [(0, "users.name"), (1, "users.ssn"), (2, "users.ssn")]
        assert unbound(a) == ["users.note"]

    def test_on_duplicate_key_update_with_row_alias(self):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s) AS new "
                    "ON DUPLICATE KEY UPDATE ssn = new.ssn, name = new.ssn", MYSQL, P)
        assert unbound(a) == ["users.name"]

    def test_replace(self):
        a = analyze("REPLACE INTO users (name, ssn) VALUES (%s, %s)", MYSQL, P)
        assert a.kind is StatementKind.INSERT
        assert assigned(a) == [(0, "users.name"), (1, "users.ssn")]

    def test_insert_set_form(self):
        a = analyze("INSERT IGNORE INTO users SET name = %s, ssn = %s, note = NULL", MYSQL, P)
        assert assigned(a) == [(0, "users.name"), (1, "users.ssn")]

    @pytest.mark.parametrize("sql", [
        "INSERT LOW_PRIORITY INTO users (ssn) VALUES (%s)",
        "INSERT HIGH_PRIORITY IGNORE INTO users (ssn) VALUES (%s)",
        "INSERT DELAYED INTO users (ssn) VALUES (%s)",
        "REPLACE LOW_PRIORITY INTO users (ssn) VALUES (%s)",
        "REPLACE users SET ssn = %s",
    ])
    def test_mysql_modifiers(self, sql):
        assert assigned(analyze(sql, MYSQL, P)) == [(0, "users.ssn")]

    def test_values_row(self):
        a = analyze("INSERT INTO users (a, b) VALUES ROW(%s, %s), ROW(%s, NULL)", MYSQL, P)
        assert assigned(a) == [(0, "users.a"), (1, "users.b"), (2, "users.a")]


class TestUpdate:
    @BOTH
    def test_set_and_where(self, dialect):
        a = analyze("UPDATE users SET ssn = %s, name = 'x' WHERE id = %s", dialect, P)
        assert a.kind is StatementKind.UPDATE and a.writes and a.complete
        assert assigned(a) == [(0, "users.ssn")]
        assert unbound(a) == ["users.name"]
        assert compared(a) == [(1, "users.id")]

    @BOTH
    def test_alias_and_qualified_assignment(self, dialect):
        a = analyze("UPDATE users u SET u.ssn = %s WHERE u.id = %s", dialect, P)
        assert assigned(a) == [(0, "users.ssn")]
        assert compared(a) == [(1, "users.id")]

    @pytest.mark.parametrize("sql", ["UPDATE LOW_PRIORITY users SET ssn = %s",
                                     "UPDATE IGNORE users SET ssn = %s",
                                     "UPDATE LOW_PRIORITY IGNORE users SET ssn = %s"])
    def test_mysql_modifiers(self, sql):
        # Unhandled, the parser reads LOW_PRIORITY as the table.
        a = analyze(sql, MYSQL, P)
        assert names(a.written_tables) == ["users"]
        assert assigned(a) == [(0, "users.ssn")]

    def test_mysql_multi_table_qualified(self):
        a = analyze("UPDATE users u JOIN orders o ON o.uid = u.id SET u.ssn = %s, o.total = %s WHERE o.id = %s",
                    MYSQL, P)
        assert a.complete
        assert assigned(a) == [(0, "users.ssn"), (1, "orders.total")]
        assert compared(a) == [(2, "orders.id")]
        assert names(a.written_tables) == ["users", "orders"]

    def test_mysql_multi_table_comma(self):
        a = analyze("UPDATE users, orders SET users.ssn = %s WHERE users.id = orders.uid", MYSQL, P)
        assert assigned(a) == [(0, "users.ssn")] and a.complete

    def test_mysql_multi_table_unqualified_is_ambiguous(self):
        a = analyze("UPDATE users u JOIN orders o ON o.uid = u.id SET ssn = %s", MYSQL, P)
        assert a.gap is AnalysisGap.AMBIGUOUS_TABLE
        assert assigned(a) == [(0, "ssn")]

    def test_postgresql_from_does_not_add_a_written_table(self):
        a = analyze("UPDATE users SET ssn = %s FROM other o WHERE o.id = users.id AND o.ssn = %s", PG, P)
        assert names(a.written_tables) == ["users"]
        assert assigned(a) == [(0, "users.ssn")]
        assert compared(a) == [(1, "other.ssn")]

    def test_multi_column_assignment(self):
        a = analyze("UPDATE users SET (a, b) = (%s, %s), (c, d) = ROW(%s, 'x'), (e, f) = (SELECT 1, 2)", PG, P)
        assert assigned(a) == [(0, "users.a"), (1, "users.b"), (2, "users.c")]
        assert unbound(a) == ["users.d", "users.e", "users.f"]

    def test_set_default(self):
        assert unbound(analyze("UPDATE users SET ssn = DEFAULT", PG)) == ["users.ssn"]

    def test_returning(self):
        a = analyze("UPDATE users SET ssn = %s RETURNING ssn", PG, P)
        assert a.result_columns == (ColumnRef("ssn", "users"),)


class TestOtherWrites:
    def test_merge(self):
        a = analyze("MERGE INTO users u USING src s ON u.id = s.id "
                    "WHEN MATCHED AND s.flag = %s THEN UPDATE SET ssn = %s "
                    "WHEN MATCHED THEN DELETE "
                    "WHEN NOT MATCHED THEN INSERT (id, ssn) VALUES (%s, s.ssn)", PG, P)
        assert a.kind is StatementKind.MERGE and a.writes and a.complete
        assert assigned(a) == [(1, "users.ssn"), (2, "users.id")]
        assert unbound(a) == ["users.ssn"]
        assert compared(a) == [(0, "src.flag")]
        assert names(a.written_tables) == ["users"]

    def test_merge_insert_without_columns(self):
        a = analyze("MERGE INTO users u USING src s ON u.id = s.id "
                    "WHEN NOT MATCHED THEN INSERT VALUES (%s, %s)", PG, P)
        assert a.gap is AnalysisGap.NO_COLUMN_LIST

    def test_merge_that_only_deletes(self):
        a = analyze("MERGE INTO users u USING src s ON u.id = s.id WHEN MATCHED THEN DELETE", PG)
        assert a.writes and a.complete and assigned(a) == [] and unbound(a) == []

    def test_data_modifying_cte(self):
        a = analyze("WITH w AS (INSERT INTO users (ssn) VALUES (%s) RETURNING id) SELECT * FROM w", PG, P)
        assert a.kind is StatementKind.SELECT and a.writes
        assert assigned(a) == [(0, "users.ssn")]
        assert names(a.tables) == ["users"]  # w is a CTE, not a table

    def test_update_cte(self):
        a = analyze("WITH w AS (UPDATE users SET ssn = %s WHERE id = %s RETURNING id) SELECT id FROM w", PG, P)
        assert assigned(a) == [(0, "users.ssn")] and compared(a) == [(1, "users.id")]

    def test_read_only_cte_is_a_read(self):
        a = analyze("WITH w AS (SELECT ssn FROM users) SELECT ssn FROM w WHERE ssn = %s", PG, P)
        assert not a.writes
        assert names(a.tables) == ["users"]
        assert compared(a) == [(0, "ssn")]  # a CTE column has no table
        assert a.result_columns == (ColumnRef("ssn", "users"),)

    def test_cte_insert_select(self):
        a = analyze("WITH a AS (SELECT %s AS v) INSERT INTO users (ssn) SELECT v FROM a", PG, P)
        assert a.gap is AnalysisGap.VALUES_FROM_QUERY

    def test_copy_from_names_unbound_columns(self):
        a = analyze("COPY users (name, ssn) FROM STDIN", PG)
        assert a.kind is StatementKind.COPY_FROM and a.writes and a.complete
        assert unbound(a) == ["users.name", "users.ssn"]

    def test_copy_from_without_columns(self):
        assert analyze("COPY users FROM STDIN WITH (FORMAT csv)", PG).gap is AnalysisGap.NO_COLUMN_LIST

    def test_copy_to_is_a_read(self):
        a = analyze("COPY users TO STDOUT", PG)
        assert a.kind is StatementKind.COPY_TO and not a.writes
        assert names(a.tables) == ["users"]

    def test_copy_query_to(self):
        a = analyze("COPY (SELECT ssn FROM users) TO STDOUT", PG)
        assert not a.writes and names(a.tables) == ["users"]

    def test_several_statements_are_all_read(self):
        # The ordinals run across the whole string, so each statement's
        # placeholders still map to the right parameters.
        a = analyze("UPDATE t SET a = %s; INSERT INTO users (name, ssn) VALUES (%s, %s)", MYSQL, P)
        assert a.kind is StatementKind.UPDATE and a.writes and a.complete
        assert assigned(a) == [(0, "t.a"), (1, "users.name"), (2, "users.ssn")]
        assert a.result_columns is None

    @BOTH
    def test_delete(self, dialect):
        a = analyze("DELETE FROM users WHERE ssn = %s", dialect, P)
        assert a.kind is StatementKind.DELETE and not a.writes
        assert compared(a) == [(0, "users.ssn")]

    def test_mysql_multi_table_delete(self):
        a = analyze("DELETE t FROM t JOIN u ON u.id = t.id WHERE u.a = %s", MYSQL, P)
        assert compared(a) == [(0, "u.a")]


class TestComparisons:
    def test_operators(self):
        a = analyze("SELECT * FROM t WHERE a = %s AND b <> %s AND c < %s AND d >= %s AND e LIKE %s "
                    "AND f ILIKE %s AND g IS NOT DISTINCT FROM %s AND %s = h", PG, P)
        assert compared(a) == [(i, f"t.{c}") for i, c in enumerate("abcdefgh")]

    def test_in_between_and_any(self):
        a = analyze("SELECT * FROM t WHERE a IN (%s, %s) AND b BETWEEN %s AND %s AND c = ANY(%s)", PG, P)
        assert compared(a) == [(0, "t.a"), (1, "t.a"), (2, "t.b"), (3, "t.b"), (4, "t.c")]
        assert [u.array for u in a.comparisons] == [False] * 4 + [True]

    def test_mysql_null_safe_equals(self):
        assert compared(analyze("SELECT * FROM t WHERE a <=> %s", MYSQL, P)) == [(0, "t.a")]

    def test_join_resolves_aliases_and_leaves_unqualified_columns_open(self):
        a = analyze("SELECT u.ssn FROM users u JOIN orders o ON o.uid = u.id "
                    "WHERE u.name = %s AND o.id = %s AND total = %s", PG, P)
        assert compared(a) == [(0, "users.name"), (1, "orders.id"), (2, "total")]

    def test_subquery_scope(self):
        a = analyze("SELECT * FROM users WHERE id IN (SELECT uid FROM orders WHERE total = %s) AND ssn = %s",
                    PG, P)
        assert compared(a) == [(0, "orders.total"), (1, "users.ssn")]

    def test_correlated_qualifier_from_an_outer_query(self):
        a = analyze("SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.uid = u.id AND u.ssn = %s)",
                    PG, P)
        assert compared(a) == [(0, "users.ssn")]

    def test_column_against_column_is_not_a_parameter(self):
        assert compared(analyze("SELECT * FROM t WHERE a = b", PG)) == []

    def test_jsonb_question_mark_operator(self):
        # "?" is an operator on PostgreSQL, not a placeholder.
        a = analyze("SELECT ssn FROM users WHERE data ? 'k' AND id = %s", PG, P)
        assert a.complete and compared(a) == [(0, "users.id")]

    def test_unknown_qualifier_is_taken_as_a_table(self):
        assert compared(analyze("SELECT * FROM users WHERE other.ssn = %s", PG, P)) == [(0, "other.ssn")]


class TestReads:
    @BOTH
    def test_tables_and_result_columns(self, dialect):
        a = analyze("SELECT u.ssn AS s, o.ssn, name, 1 FROM users u JOIN orders o ON o.uid = u.id", dialect)
        assert a.kind is StatementKind.SELECT and not a.writes
        assert names(a.tables) == ["users", "orders"]
        assert a.result_columns == (ColumnRef("ssn", "users"), ColumnRef("ssn", "orders"), None, None)

    def test_single_table_unqualified(self):
        a = analyze("SELECT ssn, name FROM users WHERE id = %s", PG, P)
        assert a.result_columns == (ColumnRef("ssn", "users"), ColumnRef("name", "users"))

    @pytest.mark.parametrize("sql", ["SELECT * FROM users", "SELECT u.* FROM users u",
                                     "SELECT ssn FROM users UNION SELECT ssn FROM archive"])
    def test_result_columns_unknown(self, sql):
        assert analyze(sql, PG).result_columns is None

    def test_subquery_column_is_traced_to_its_table(self):
        a = analyze("SELECT s.ssn FROM (SELECT ssn FROM users) s", PG)
        assert a.result_columns == (ColumnRef("ssn", "users"),)
        assert names(a.tables) == ["users"]

    @BOTH
    @pytest.mark.parametrize("sql, params, expected", [
        ("SELECT x.ssn FROM (SELECT ssn FROM users) AS x", None, (ColumnRef("ssn", "users"),)),
        ("SELECT ssn FROM (SELECT ssn FROM users) x", None, (ColumnRef("ssn", "users"),)),
        ("WITH c AS (SELECT ssn FROM users) SELECT c.ssn FROM c", None, (ColumnRef("ssn", "users"),)),
        ("SELECT y.ssn FROM (SELECT x.ssn FROM (SELECT ssn FROM users) x) y", None, (ColumnRef("ssn", "users"),)),
        ("SELECT x.s FROM (SELECT ssn AS s FROM users) x", None, (ColumnRef("ssn", "users"),)),
        ("WITH c AS (SELECT ssn FROM users) SELECT x.ssn FROM (SELECT ssn FROM c) x", None,
         (ColumnRef("ssn", "users"),)),
        ("SELECT u.name, d.ssn FROM users u JOIN (SELECT id, ssn FROM archive) d ON d.id = u.id", None,
         (ColumnRef("name", "users"), ColumnRef("ssn", "archive"))),
        ("WITH a AS (SELECT id FROM archive), c AS (SELECT ssn FROM users) SELECT c.ssn FROM c", None,
         (ColumnRef("ssn", "users"),)),
        ("SELECT y.ssn FROM (SELECT id FROM archive) x JOIN (SELECT id, ssn FROM users) y ON x.id = y.id", None,
         (ColumnRef("ssn", "users"),)),
        # SQLAlchemy's legacy Query.first() over a model with a joined eager load.
        ("SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_ssn AS anon_1_users_ssn, addresses_1.id AS addresses_1_id "
         "FROM (SELECT users.id AS users_id, users.ssn AS users_ssn FROM users LIMIT %s) AS anon_1 "
         "LEFT OUTER JOIN addresses AS addresses_1 ON anon_1.users_id = addresses_1.user_id", P,
         (ColumnRef("id", "users"), ColumnRef("ssn", "users"), ColumnRef("id", "addresses"))),
    ], ids=["derived", "only_source", "cte", "nested", "relabelled", "cte_behind_derived", "joined", "two_ctes",
            "second_derived", "orm_first"])
    def test_derived_and_cte_columns_trace_to_their_table(self, dialect, sql, params, expected):
        assert analyze(sql, dialect, params).result_columns == expected

    @BOTH
    @pytest.mark.parametrize("sql", [
        "SELECT x.ssn FROM (SELECT ssn FROM users UNION SELECT ssn FROM archive) x",
        "SELECT x.ssn FROM (SELECT * FROM users) x",
        "SELECT x.s FROM (SELECT upper(ssn) AS s FROM users) x",
        "SELECT ssn FROM (SELECT ssn FROM users) x, (SELECT ssn FROM archive) y",
        "SELECT g.ssn FROM (SELECT ssn FROM users) y CROSS JOIN generate_series(1, 2) AS g",
    ], ids=["set_operation", "star", "expression", "two_sources", "function_source"])
    def test_untraceable_derived_columns_stay_unknown(self, dialect, sql):
        assert analyze(sql, dialect).result_columns == (None,)

    @BOTH
    def test_self_shadowing_cte_stays_unknown(self, dialect):
        # The inner users is the table, but every unqualified users reads as
        # the CTE, and a CTE doesn't see itself, so the trace stops there.
        assert analyze("WITH users AS (SELECT ssn FROM users) SELECT ssn FROM users", dialect).result_columns == (None,)

    @BOTH
    @pytest.mark.parametrize("sql, expected", [
        # Inside c, users is the table: a later CTE of that name isn't seen.
        ("WITH c AS (SELECT ssn FROM users), users AS (SELECT name AS ssn FROM people) SELECT ssn FROM c", (None,)),
        ("WITH c AS (SELECT u.ssn FROM users u), users AS (SELECT name AS ssn FROM people) SELECT c.ssn FROM c", (None,)),
        ("WITH c AS (SELECT x.ssn FROM (SELECT ssn FROM users) x), users AS (SELECT name AS ssn FROM people) "
         "SELECT ssn FROM c", (None,)),
        ("WITH users AS (SELECT name AS ssn FROM people), c AS (SELECT ssn FROM users) SELECT ssn FROM c",
         (ColumnRef("name", "people"),)),
        # Past c, the trace takes the outer users; under RECURSIVE, PostgreSQL
        # reads the later inner one instead, so the column stays unknown.
        ("WITH users AS (SELECT addr AS ssn FROM homes) SELECT z.ssn FROM (WITH c AS (SELECT ssn FROM users), "
         "users AS (SELECT name AS ssn FROM people) SELECT ssn FROM c) z", (ColumnRef("addr", "homes"),)),
        ("WITH users AS (SELECT addr AS ssn FROM homes) SELECT z.ssn FROM (WITH RECURSIVE c AS (SELECT ssn FROM users), "
         "users AS (SELECT name AS ssn FROM people) SELECT ssn FROM c) z", (None,)),
    ], ids=["forward", "forward_qualified", "forward_in_derived", "backward", "nested_outer", "nested_outer_recursive"])
    def test_a_cte_sees_only_the_ctes_before_it(self, dialect, sql, expected):
        assert analyze(sql, dialect).result_columns == expected

    @BOTH
    @pytest.mark.parametrize("sql", [
        "WITH c(ssn, name) AS (SELECT name, ssn FROM users) SELECT c.ssn, c.name FROM c",
        "SELECT x.ssn, x.name FROM (SELECT name, ssn FROM users) AS x(ssn, name)",
    ], ids=["cte", "derived"])
    def test_a_source_with_a_column_list_stays_unknown(self, dialect, sql):
        # The list renames the columns by position (here c.ssn is users.name),
        # which the trace doesn't follow.
        assert analyze(sql, dialect).result_columns == (None, None)

    @pytest.mark.parametrize("sql, expected", [
        ("WITH c AS (SELECT name, ssn FROM users) SELECT x.ssn FROM c AS x(ssn, name)", (None,)),
        ("SELECT d.ssn FROM users AS u(a, b) JOIN (SELECT id, ssn FROM archive) d ON d.id = u.a",
         (ColumnRef("ssn", "archive"),)),
    ], ids=["cte_renamed_where_read", "other_source_renamed"])
    def test_only_a_column_list_on_the_traced_source_matters(self, sql, expected):
        assert analyze(sql, PG).result_columns == expected

    def test_returning_list_is_not_traced(self):
        a = analyze("UPDATE users SET name = %s FROM (SELECT id, ssn FROM archive) x WHERE users.id = x.id "
                    "RETURNING x.ssn", PG, P)
        assert a.result_columns == (None,)

    def test_trace_stops_after_eight_levels(self):
        def nested(levels):
            sql = "SELECT ssn FROM users"
            for i in range(levels):
                sql = f"SELECT t{i}.ssn FROM ({sql}) t{i}"
            return sql

        assert analyze(nested(8), PG).result_columns == (ColumnRef("ssn", "users"),)
        assert analyze(nested(9), PG).result_columns == (None,)

    @pytest.mark.parametrize("sql, expected", [
        ('SELECT x."S" FROM (SELECT ssn AS "S" FROM users) x', (ColumnRef("ssn", "users"),)),
        ('SELECT x.s FROM (SELECT ssn AS "S" FROM users) x', (None,)),
    ], ids=["quoted", "unquoted"])
    def test_quoted_alias_matches_as_quoted(self, sql, expected):
        assert analyze(sql, PG).result_columns == expected

    @pytest.mark.parametrize("sql", [
        "SELECT x.s FROM (SELECT ssn AS `S` FROM users) x",
        "SELECT X.SSN FROM (SELECT ssn FROM users) x",
    ], ids=["alias", "qualifier"])
    def test_mysql_traces_names_without_case(self, sql):
        assert analyze(sql, MYSQL).result_columns == (ColumnRef("ssn", "users"),)

    def test_union_tables(self):
        assert names(analyze("SELECT ssn FROM users UNION ALL SELECT ssn FROM archive", PG).tables) == [
            "users", "archive"]

    def test_for_update_is_not_a_write(self):
        assert not analyze("SELECT * FROM users WHERE id = %s FOR UPDATE", PG, P).writes


class TestIdentifiers:
    def test_postgresql_folds_ascii_letters_only(self):
        # As PostgreSQL does in a multibyte (UTF-8) database.
        assert PG.fold_identifier("ÄBc", quoted=False) == "Äbc"
        assert PG.fold_identifier("ÄBc", quoted=True) == "ÄBc"
        assert MYSQL.fold_identifier("ÄBc", quoted=False) == "ÄBc"
        assert (PG.metadata_key("Users"), MYSQL.metadata_key("Users")) == ("Users", "users")

    def test_postgresql_folds_unquoted_names(self):
        a = analyze('INSERT INTO Users (Name, "SSN") VALUES (%s, %s)', PG, P)
        assert assigned(a) == [(0, "users.name"), (1, "users.SSN")]

    def test_postgresql_quoted_table(self):
        a = analyze('UPDATE "Users" SET ssn = %s', PG, P)
        assert names(a.written_tables) == ["Users"]

    def test_mysql_keeps_case_and_strips_backticks(self):
        a = analyze("INSERT INTO `Users` (`Name`, SSN) VALUES (%s, %s)", MYSQL, P)
        assert assigned(a) == [(0, "Users.Name"), (1, "Users.SSN")]

    def test_mysql_alias_match_is_case_insensitive(self):
        a = analyze("UPDATE users U SET u.ssn = %s WHERE U.id = %s", MYSQL, P)
        assert assigned(a) == [(0, "users.ssn")] and compared(a) == [(1, "users.id")]

    def test_quoted_alias(self):
        a = analyze('SELECT "U".ssn FROM users "U" WHERE "U".id = %s', PG, P)
        assert compared(a) == [(0, "users.id")]


class TestAnnotations:
    def test_annotation_overrides_inference(self):
        a = analyze("INSERT INTO t (a, b) VALUES (/*@encrypt:users.ssn*/ %s, %s)", PG, P)
        assert [(u.ordinal, str(u.column)) for u in a.annotations] == [(0, "users.ssn")]
        assert a.parameter_columns() == {0: ColumnRef("ssn", "users"), 1: ColumnRef("b", "t")}

    def test_alias_qualifier_is_resolved(self):
        a = analyze("UPDATE users u SET note = /*@encrypt:u.ssn*/ %s", PG, P)
        assert [str(u.column) for u in a.annotations] == ["users.ssn"]

    def test_annotations_survive_an_unparseable_statement(self):
        a = analyze("INSERT INTO t OVERRIDING SYSTEM VALUE VALUES (/*@encrypt:t.ssn*/ %s)", PG, P)
        assert a.gap is AnalysisGap.UNPARSEABLE
        assert [str(u.column) for u in a.annotations] == ["t.ssn"]

    def test_annotations_survive_an_unsupported_statement(self):
        a = analyze("EXPLAIN ANALYZE INSERT INTO t (a) VALUES (/*@encrypt:t.a*/ %s)", PG, P)
        assert [str(u.column) for u in a.annotations] == ["t.a"]

    def test_stray_annotations_are_reported(self):
        a = analyze("INSERT INTO t (a) VALUES (%s) /*@encrypt:t.a*/", PG, P)
        assert a.stray_annotations == ("/*@encrypt:t.a*/",)


class TestNotEstablished:
    @pytest.mark.parametrize("dialect, sql, params, gap", [
        (PG, "INSERT INTO t (a) VALUES ('%s')", P, AnalysisGap.HIDDEN_PLACEHOLDER),
        (MYSQL, "INSERT INTO t (a) VALUES (%s) -- %s", P, AnalysisGap.HIDDEN_PLACEHOLDER),
        (PG, "INSERT INTO t (a) VALUES (%s, %(x)s)", P, AnalysisGap.DRIVER_ERROR),
        (PG, "INSERT INTO t (a) VALUES (%s) WHERE 1 % 2", P, AnalysisGap.DRIVER_ERROR),
        (PG, "INSERT INTO t (a) VALUES ('abc", P, AnalysisGap.UNPARSEABLE),
        (PG, "INSERT INTO t (a VALUES (%s)", P, AnalysisGap.UNPARSEABLE),
        (PG, "INSERT INTO t (a) OVERRIDING SYSTEM VALUE VALUES (%s)", P, AnalysisGap.UNPARSEABLE),
        (PG, "EXPLAIN ANALYZE INSERT INTO t (a) VALUES (%s)", P, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "EXECUTE p (%s)", P, AnalysisGap.UNSUPPORTED_STATEMENT),
        (MYSQL, "LOAD DATA INFILE 'x' INTO TABLE t", None, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "SET search_path TO app; INSERT INTO t (a) VALUES (%s)", P, AnalysisGap.UNSUPPORTED_STATEMENT),
    ])
    def test_writes_not_established(self, dialect, sql, params, gap):
        a = analyze(sql, dialect, params)
        assert a.writes and a.gap is gap and not a.complete
        assert a.assignments == () and a.unbound_writes == ()

    @pytest.mark.parametrize("dialect, sql, tables", [
        (PG, "INSERT INTO t (a, b) VALUES (%s, 'x%s')", (TableRef("t"),)),
        (PG, 'SELECT a AS "x%s" FROM t WHERE b = %s', (TableRef("t"),)),
        (PG, "UPDATE app.t SET a = %s /* %s */ WHERE id = %s", (TableRef("t", "app"),)),
        (PG, "SELECT t.a FROM t JOIN u ON u.id = t.id WHERE t.b = %s AND t.c = 'x%s'", (TableRef("t"), TableRef("u"))),
        (PG, "WITH c AS (SELECT a FROM t) SELECT a FROM c WHERE a = %s -- %s", (TableRef("t"),)),
        (PG, "PREPARE p AS INSERT INTO t (a, b) VALUES ($1, 'x%s')", (TableRef("t"),)),
        (MYSQL, "INSERT INTO t (a, b) VALUES (%s, 'x%s')", (TableRef("t"),)),
        (MYSQL, "SELECT a AS `x%s` FROM t WHERE b = %s", (TableRef("t"),)),
        (MYSQL, "UPDATE app.t SET a = %s WHERE id = %s -- %s", (TableRef("t", "app"),)),
        (MYSQL, "INSERT INTO t (a, `b%s`) VALUES (%s, %s)", (TableRef("t"),)),
        (MYSQL, "SELECT t.a FROM t JOIN u ON u.id = t.id WHERE t.b = %s # %s", (TableRef("t"), TableRef("u"))),
        (MYSQL, "INSERT INTO t (t.a, t.b) VALUES (%s, 'x%s')", (TableRef("t"),)),
        # The main path doesn't analyze these statements, but their tables
        # must still be listed, or a caller would let them reach a table with
        # encrypted columns.
        (PG, "SET search_path TO app; INSERT INTO t (a, b) VALUES (%s, 'x%s')", (TableRef("t"),)),
        (PG, "CREATE TABLE u AS SELECT a FROM t WHERE b = %s AND c = 'x%s'", (TableRef("u"), TableRef("t"))),
        (MYSQL, "EXPLAIN INSERT INTO t (a, b) VALUES (%s, 'x%s')", (TableRef("t"),)),
        (MYSQL, "DESCRIBE SELECT a FROM t WHERE b = %s AND c = 'x%s'", (TableRef("t"),)),
    ], ids=["pg_literal", "pg_identifier_select", "pg_comment", "pg_join", "pg_cte", "pg_prepare", "mysql_literal",
            "mysql_identifier_select", "mysql_comment", "mysql_identifier", "mysql_join", "mysql_qualified_columns",
            "pg_set_then_insert", "pg_create_table_as", "mysql_explain_insert", "mysql_describe_select"])
    def test_hidden_placeholder_gap_still_lists_the_tables(self, dialect, sql, tables):
        # The tables are what lets a caller refuse such a statement when one
        # of them has encrypted columns; the placeholders still can't be
        # paired with columns, so nothing else is reported.
        a = analyze(sql, dialect, P)
        assert a.gap is AnalysisGap.HIDDEN_PLACEHOLDER and a.kind is StatementKind.UNKNOWN
        assert a.tables == tables
        assert a.assignments == () and a.comparisons == () and a.annotations == () and a.result_columns is None

    def test_unparseable_read_is_not_a_write(self):
        a = analyze("SELECT FROM WHERE %s", PG, P)
        assert a.kind is StatementKind.UNKNOWN and not a.writes

    @pytest.mark.parametrize("sql", ["CREATE TABLE t (a int)", "BEGIN", "CALL p(1)", "", "  -- just a comment"])
    def test_statements_that_write_no_column(self, sql):
        a = analyze(sql, PG)
        assert not a.writes and a.complete

    def test_placeholder_text_in_the_sql_is_a_mismatch(self):
        # Something that looks like a normalized placeholder must not be
        # taken for one.
        a = analyze("INSERT INTO t (a, b) VALUES (%s, :_kms_p0)", PG, P)
        assert a.gap is AnalysisGap.PLACEHOLDER_MISMATCH

    def test_parser_failure_of_any_kind_is_unparseable(self, monkeypatch):
        def boom(*_args, **_kwargs):
            raise RecursionError("too deep")
        monkeypatch.setattr(sql_analyzer.sqlglot, "parse", boom)
        a = sql_analyzer._analysis("INSERT INTO t (a) VALUES (%s)", PG, sql_analyzer.ParamStyle.POSITIONAL)
        assert a.gap is AnalysisGap.UNPARSEABLE and a.detail == "RecursionError"

    def test_mysql_without_params_leaves_percent_s(self):
        # Connector/Python sends %s as written when there are no parameters.
        a = analyze("INSERT INTO t (a) VALUES (%s)", MYSQL, ())
        assert a.placeholders == () and a.gap is AnalysisGap.UNPARSEABLE


class TestApi:
    def test_cached_and_immutable(self):
        a = analyze("SELECT * FROM t WHERE a = %s", PG, P)
        assert analyze("SELECT * FROM t WHERE a = %s", PG, [2]) is a
        with pytest.raises(AttributeError):
            a.writes = True  # type: ignore[misc]

    def test_cache_is_per_dialect_and_style(self):
        sql = "SELECT * FROM t WHERE a = %s"
        assert analyze(sql, PG, P) is not analyze(sql, MYSQL, P)
        assert analyze(sql, PG, P) is not analyze(sql, PG, None)

    def test_long_sql_is_not_cached(self):
        rows = ", ".join(["(%s, %s)"] * 2000)
        sql = f"INSERT INTO users (name, ssn) VALUES {rows}"
        assert len(sql) > sql_analyzer.CACHEABLE_SQL_LENGTH
        a = analyze(sql, MYSQL, P)
        assert len(a.assignments) == 4000 and a.assignments[-1].ordinal == 3999
        assert analyze(sql, MYSQL, P) is not a

    def test_short_sql_with_many_placeholders_is_not_cached(self):
        # A bulk INSERT under the length limit still holds an object per
        # placeholder; caching every row count would grow without need.
        sql = "INSERT INTO t (a) VALUES " + ",".join(["(%s)"] * (sql_analyzer.CACHEABLE_PERCENT_SIGNS + 1))
        assert len(sql) <= sql_analyzer.CACHEABLE_SQL_LENGTH
        assert analyze(sql, PG, P) is not analyze(sql, PG, P)

    def test_nested_results_are_frozen(self):
        a = analyze("INSERT INTO users (ssn) VALUES (%s)", PG, P)
        with pytest.raises(dataclasses.FrozenInstanceError):
            a.assignments[0].column = ColumnRef("x")  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            a.assignments[0].column.table = "x"  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            a.placeholders[0].key = 5  # type: ignore[misc]

    def test_recursion_error_is_not_cached(self, monkeypatch):
        # How deep sqlglot can recurse depends on the caller's stack, so a
        # failure for one caller must not be served to the next.
        real = sqlglot.parse
        calls = []

        def flaky(sql, read):
            calls.append(sql)
            if len(calls) == 1:
                raise RecursionError("too deep")
            return real(sql, read=read)

        monkeypatch.setattr(sql_analyzer.sqlglot, "parse", flaky)
        sql = "UPDATE users SET ssn = %s"
        assert analyze(sql, PG, P).gap is AnalysisGap.UNPARSEABLE
        assert analyze(sql, PG, P).gap is None

    def test_deep_nesting_from_a_deep_stack_is_a_gap(self):
        sql = "UPDATE users SET ssn = " + "(" * 40 + "%s" + ")" * 40
        result = []

        def deep(n):
            if n:
                return deep(n - 1)
            result.append(analyze(sql, PG, P))

        thread = threading.Thread(target=deep, args=(sys.getrecursionlimit() - 200,))
        thread.start()
        thread.join()
        assert result and result[0].gap is AnalysisGap.UNPARSEABLE

    def test_type_error_for_non_str(self):
        with pytest.raises(TypeError):
            analyze(b"SELECT 1", PG)  # type: ignore[arg-type]

    @pytest.mark.parametrize("dialect, sql, params", [
        (MYSQL, "CALL p(%s)", P),
        (PG, "EXPLAIN ANALYZE INSERT INTO t (a) VALUES (%s)", P),
        # Parsed, but sqlglot falls back to a command (and would log the
        # statement, literal included).
        (PG, "COPY users (ssn) FROM STDIN WITH (FORMAT text) WHERE ssn <> '123-45-6789'", None),
        (MYSQL, "SELECT JSON_EXTRACT(doc, '$[last]') FROM t WHERE ssn = '123-45-6789'", None),
    ])
    def test_the_parser_never_logs_statement_text(self, caplog, dialect, sql, params):
        with caplog.at_level(logging.DEBUG, logger="sqlglot"):
            analyze(sql, dialect, params)
        assert caplog.records == []

    def test_the_applications_own_sqlglot_logging_is_untouched(self, caplog):
        with caplog.at_level(logging.WARNING, logger="sqlglot"):
            sqlglot.parse("CALL p(1)", read="mysql")
        assert caplog.records

    @pytest.mark.parametrize("dialect, sql, params", [
        (PG, "INSERT INTO t (c, d) VALUES ('100%(card 4111-1111-1111-1111)x', %s)", P),
        (PG, "INSERT INTO t (c, d) VALUES ('4111-1111-1111-1111 %d', %s)", P),
        (MYSQL, "INSERT INTO t (c) VALUES (%(ssn 123-45-6789)d)", {"a": 1}),
        (PG, "INSERT INTO t (secret_col) VALUES ('top secret', %s", P),
    ])
    def test_detail_never_quotes_the_sql(self, dialect, sql, params):
        a = analyze(sql, dialect, params)
        assert a.gap is not None
        assert a.detail is not None or a.gap is AnalysisGap.UNPARSEABLE
        for secret in ("4111", "123-45", "secret"):
            assert secret not in (a.detail or "")


class TestReviewRegressions:
    """Regression tests for statements the analysis once got wrong: annotation
    scopes, MySQL syntax, ORM-rendered SQL and statement kinds."""

    # ---- annotations resolve in the placeholder's own scope ----

    @pytest.mark.parametrize("dialect, sql, expected", [
        (MYSQL, "UPDATE accounts u SET x = %s WHERE id = 1; UPDATE users u SET ssn = /*@encrypt:u.ssn*/ %s",
         {0: "accounts.x", 1: "users.ssn"}),
        (PG, "WITH w AS (UPDATE users u SET ssn = /*@encrypt:u.ssn*/ %s RETURNING u.id) SELECT * FROM w",
         {0: "users.ssn"}),
        (PG, "WITH w AS (INSERT INTO users AS u (ssn) VALUES (/*@encrypt:u.ssn*/ %s) RETURNING id) SELECT * FROM w",
         {0: "users.ssn"}),
        (PG, "UPDATE users u SET name = %s WHERE id IN "
             "(SELECT uid FROM accounts u WHERE u.secret = /*@encrypt:u.secret*/ %s)",
         {0: "users.name", 1: "accounts.secret"}),
        (PG, "SELECT 1 FROM accounts u UNION SELECT 1 FROM users u WHERE u.ssn = /*@encrypt:u.ssn*/ %s",
         {0: "users.ssn"}),
    ])
    def test_annotation_alias_is_resolved_where_the_placeholder_is(self, dialect, sql, expected):
        a = analyze(sql, dialect, P)
        assert {k: str(v) for k, v in a.parameter_columns().items()} == expected

    def test_bare_annotation_keeps_the_table_the_analysis_found(self):
        a = analyze("UPDATE users SET ssn = /*@encrypt:ssn*/ %s", PG, P)
        assert a.parameter_columns() == {0: ColumnRef("ssn", "users")}

    def test_bare_annotation_naming_another_column_takes_the_only_table(self):
        a = analyze("INSERT INTO users (note) VALUES (/*@encrypt:ssn*/ %s)", PG, P)
        assert a.parameter_columns() == {0: ColumnRef("ssn", "users")}

    def test_mysql_annotation_alias_is_case_insensitive(self):
        a = analyze("UPDATE users u SET note = /*@encrypt:U.ssn*/ %s", MYSQL, P)
        assert [str(u.column) for u in a.annotations] == ["users.ssn"]

    @pytest.mark.parametrize("sql", [
        "INSERT INTO t (a, b) VALUES (/*@encrypt:t.a*/ %s, '%s')",       # hidden placeholder
        "INSERT INTO t (a, b) VALUES (/*@encrypt:t.a*/ %s, :_kms_p0)",   # placeholder mismatch
    ])
    def test_annotations_survive_every_gap(self, sql):
        a = analyze(sql, PG, P)
        assert a.gap is not None
        assert [str(u.column) for u in a.annotations] == ["t.a"]

    # ---- MySQL executable comments run on the server ----

    @pytest.mark.parametrize("sql", [
        "INSERT INTO users (/*!80000 ssn, */ name) VALUES (%s /*!80000 , DEFAULT*/)",
        "INSERT INTO users (name, ssn) VALUES (%s, %s) /*! ON DUPLICATE KEY UPDATE ssn = 'plain' */",
        "UPDATE users SET name = %s /*!, ssn = 'plain' */ WHERE id = %s",
        "SELECT 1 /*M! , 2 */",
    ])
    def test_executable_comment_is_not_established(self, sql):
        a = analyze(sql, MYSQL, P)
        assert a.gap is AnalysisGap.EXECUTABLE_COMMENT
        assert a.assignments == ()

    def test_executable_comment_write_keyword_counts(self):
        assert analyze("SELECT 1 /*! ; INSERT INTO t (a) VALUES (1) */", MYSQL).writes

    def test_optimizer_hint_is_an_ordinary_comment(self):
        a = analyze("UPDATE /*+ NO_INDEX_MERGE(users) */ users SET ssn = %s", MYSQL, P)
        assert a.complete and assigned(a) == [(0, "users.ssn")]

    # ---- MySQL charset introducers (Django renders binary values as _binary %s) ----

    def test_binary_introducer_in_insert(self):
        a = analyze("INSERT INTO `Person` (`name`, `ssn`, `note`) VALUES (%s, _binary %s, %s), (%s, _binary %s, %s)",
                    MYSQL, P)
        assert a.complete and unbound(a) == []
        assert assigned(a) == [(0, "Person.name"), (1, "Person.ssn"), (2, "Person.note"),
                               (3, "Person.name"), (4, "Person.ssn"), (5, "Person.note")]
        assert [p.introducer for p in a.placeholders] == [None, "_binary", None] * 2

    def test_binary_introducer_in_update_and_upsert(self):
        a = analyze("UPDATE `Person` SET `ssn` = _binary %s, `name` = %s WHERE `Person`.`id` = %s", MYSQL, P)
        assert a.complete and assigned(a) == [(0, "Person.ssn"), (1, "Person.name")]
        a = analyze("INSERT INTO `Person` (`ssn`) VALUES (_binary %s) AS new ON DUPLICATE KEY UPDATE `ssn` = new.`ssn`",
                    MYSQL, P)
        assert a.complete and unbound(a) == []

    def test_introducer_needs_a_known_charset(self):
        # "_x %s" is not an introducer, so the statement reads as written.
        a = analyze("SELECT _x %s", MYSQL, P)
        assert [p.introducer for p in a.placeholders] == [None]

    def test_introducer_only_on_mysql(self):
        assert analyze("SELECT _binary %s", PG, P).placeholders[0].introducer is None

    # ---- MySQL row alias with a column list renames by position ----

    def test_row_alias_columns_copy_by_position(self):
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s) AS new(n, s) ON DUPLICATE KEY UPDATE ssn = new.s",
                    MYSQL, P)
        assert unbound(a) == []

    def test_row_alias_column_naming_another_insert_column_is_unbound(self):
        # new.ssn is the incoming *name*: alias columns rename by position.
        a = analyze("INSERT INTO users (name, ssn) VALUES (%s, %s) AS new(ssn, name) "
                    "ON DUPLICATE KEY UPDATE ssn = new.ssn", MYSQL, P)
        assert unbound(a) == ["users.ssn"]

    # ---- copies from another table ----

    @pytest.mark.parametrize("dialect, sql", [
        (MYSQL, "UPDATE users u JOIN orders o ON o.uid = u.id SET u.ssn = o.ssn"),
        (PG, "UPDATE users SET ssn = o.ssn FROM other o WHERE o.id = users.id"),
    ])
    def test_copy_of_another_tables_column_of_the_same_name_is_unbound(self, dialect, sql):
        assert unbound(analyze(sql, dialect)) == ["users.ssn"]

    # ---- SQLAlchemy's multi-row ORM insert on PostgreSQL ----

    def test_insert_select_from_values_list(self):
        sql = ("INSERT INTO users (name, ssn) SELECT p0::VARCHAR, p1::BYTEA FROM "
               "(VALUES (%(name__0)s::VARCHAR, %(ssn__0)s, 0), (%(name__1)s::VARCHAR, %(ssn__1)s, 1)) "
               "AS imp_sen(p0, p1, sen_counter) ORDER BY sen_counter RETURNING users.id, users.id AS id__1")
        a = analyze(sql, PG, N)
        assert a.complete
        assert [(u.placeholder.key, str(u.column)) for u in a.assignments] == [
            ("name__0", "users.name"), ("ssn__0", "users.ssn"), ("name__1", "users.name"), ("ssn__1", "users.ssn")]
        assert a.assignments[1].casts == ("VARBINARY",)

    @pytest.mark.parametrize("sql", [
        "INSERT INTO users (ssn) SELECT v.p0 FROM (VALUES (%s)) AS v(p0) WHERE v.p0 IS NOT NULL",
        "INSERT INTO users (ssn) SELECT upper(p0) FROM (VALUES (%s)) AS v(p0)",
        "INSERT INTO users (ssn) SELECT p0 FROM (VALUES (%s)) AS v(p0) JOIN x ON true",
        "INSERT INTO users (ssn) SELECT q FROM (VALUES (%s)) AS v(p0)",
        "INSERT INTO users (name, ssn) SELECT %s, %s",
    ])
    def test_other_insert_selects_are_values_from_query(self, sql):
        assert analyze(sql, PG, P).gap is AnalysisGap.VALUES_FROM_QUERY

    # ---- statements that run a stored statement ----

    def test_execute_may_write(self):
        a = analyze("EXECUTE ins_user(%s, %s)", PG, P)
        assert a.writes and a.gap is AnalysisGap.UNSUPPORTED_STATEMENT

    def test_prepare_body_is_read(self):
        a = analyze("PREPARE ins (text, text) AS INSERT INTO users (name, ssn) VALUES ($1, '123-45-6789')", PG)
        assert a.kind is StatementKind.INSERT and a.writes
        assert a.gap is AnalysisGap.PREPARED_PARAMETERS
        # The literal is reported as an unbound write; $1 is bound by a later
        # EXECUTE.
        assert unbound(a) == ["users.ssn"]

    def test_prepare_of_a_read(self):
        a = analyze('PREPARE "q" AS SELECT ssn FROM users WHERE id = $1', PG)
        assert a.kind is StatementKind.SELECT and not a.writes and a.complete

    # ---- DELETE ... USING ----

    def test_delete_using_aliases_are_in_scope(self):
        a = analyze("DELETE FROM users USING accounts a WHERE a.uid = users.id AND a.secret = %s RETURNING a.secret",
                    PG, P)
        assert compared(a) == [(0, "accounts.secret")]
        assert a.result_columns == (ColumnRef("secret", "accounts"),)

    # ---- ALL / SOME ----

    @pytest.mark.parametrize("sql", ["SELECT * FROM t WHERE c <> ALL(%s)", "SELECT * FROM t WHERE c = SOME(%s)",
                                     "SELECT * FROM t WHERE c = ANY(%s::text[])"])
    def test_all_some_and_any_are_array_comparisons(self, sql):
        a = analyze(sql, PG, P)
        assert [(u.ordinal, str(u.column), u.array) for u in a.comparisons] == [(0, "t.c", True)]

    # ---- MySQL qualified INSERT column lists ----

    @pytest.mark.parametrize("sql, expected", [
        ("INSERT INTO users (users.name, users.ssn) VALUES (%s, %s)", [(0, "users.name"), (1, "users.ssn")]),
        ("INSERT INTO mydb.users (mydb.users.ssn) VALUES (%s)", [(0, "users.ssn")]),
        ("INSERT INTO `users` (`users`.`ssn`) VALUES (%s)", [(0, "users.ssn")]),
    ])
    def test_mysql_qualified_insert_columns(self, sql, expected):
        a = analyze(sql, MYSQL, P)
        assert a.complete and assigned(a) == expected

    # ---- names that are not plain identifiers ----

    def test_subscripted_column_is_not_a_column_name(self):
        a = analyze("MERGE INTO t USING s ON 1=1 WHEN NOT MATCHED THEN INSERT (ssn[1]) VALUES (%s)", PG, P)
        assert a.gap is AnalysisGap.UNSUPPORTED_STATEMENT
        assert assigned(a) == []

    # ---- keyword fallback ----

    @pytest.mark.parametrize("sql", [
        "SET search_path TO app; WITH w AS (INSERT INTO users (ssn) VALUES (%s) RETURNING id) SELECT * FROM w",
        "SET search_path TO app; COPY users (ssn) FROM STDIN",
    ])
    def test_unparsed_statement_before_a_write_is_not_complete(self, sql):
        a = analyze(sql, PG, P)
        assert a.writes and a.gap is AnalysisGap.UNSUPPORTED_STATEMENT

    # ---- parse-trust guards ----

    def test_placeholder_missing_from_the_parse_is_a_mismatch(self, monkeypatch):
        real = sqlglot.parse
        monkeypatch.setattr(sql_analyzer.sqlglot, "parse",
                            lambda _sql, read: real("INSERT INTO users (name, ssn) VALUES (:_kms_p0, 'x')", read=read))
        a = sql_analyzer._analysis("INSERT INTO users (name, ssn) VALUES (%s, %s)", PG, ParamStyle.POSITIONAL)
        assert a.gap is AnalysisGap.PLACEHOLDER_MISMATCH and a.assignments == ()

    def test_command_fallback_is_unparseable(self, monkeypatch):
        monkeypatch.setattr(sql_analyzer.sqlglot, "parse", lambda _sql, read: [exp.Command(this="INSERT")])
        a = sql_analyzer._analysis("INSERT INTO users (ssn) VALUES ('x')", PG, ParamStyle.NONE)
        assert a.writes and a.gap is AnalysisGap.UNPARSEABLE

    def test_unterminated_is_unparseable_even_if_the_parser_accepts_it(self, monkeypatch):
        real = sqlglot.parse
        monkeypatch.setattr(sql_analyzer.sqlglot, "parse",
                            lambda _sql, read: real("INSERT INTO users (ssn) VALUES (:_kms_p0)", read=read))
        a = sql_analyzer._analysis("INSERT INTO users (ssn) VALUES (%s) /* a /* b */", PG, ParamStyle.POSITIONAL)
        assert a.writes and a.gap is AnalysisGap.UNPARSEABLE and a.assignments == ()

    # ---- row lengths ----

    @pytest.mark.parametrize("sql", [
        "INSERT INTO users (a) VALUES (%s, %s)",
        "MERGE INTO users u USING src s ON u.id = s.id WHEN NOT MATCHED THEN INSERT (id, ssn) VALUES (%s)",
        "MERGE INTO users u USING src s ON u.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (%s, %s)",
    ])
    def test_row_length_mismatch_both_ways(self, sql):
        assert analyze(sql, PG, P).gap is AnalysisGap.ROW_LENGTH_MISMATCH

    def test_several_selects_have_no_result_columns(self):
        assert analyze("SELECT ssn FROM users; SELECT name FROM accounts", PG).result_columns is None

    # ---- performance ----

    def test_long_boolean_chain_is_linear(self):
        sql = "SELECT ssn FROM users WHERE " + " OR ".join(["(id = %s AND ssn = %s)"] * 3000)
        a = analyze(sql, PG, P)
        assert len(a.comparisons) == 6000 and a.comparisons[-1].column == ColumnRef("ssn", "users")


class TestReviewRegressionsRound2:
    """Regression tests: annotations across tables, SQLAlchemy's multi-row
    insert, regex cost, caching and the logging filter."""

    # ---- annotations ----

    @pytest.mark.parametrize("sql, expected", [
        ("UPDATE users u SET name = %s WHERE id IN (SELECT uid FROM accounts a WHERE a.x = /*@encrypt:u.ssn*/ %s)",
         {0: "users.name", 1: "users.ssn"}),
        ("SELECT * FROM users u WHERE EXISTS "
         "(SELECT 1 FROM accounts a WHERE a.uid = u.id AND a.x = /*@encrypt:u.ssn*/ %s)",
         {0: "users.ssn"}),
    ])
    def test_annotation_qualifier_resolves_through_outer_queries(self, sql, expected):
        assert {k: str(v) for k, v in analyze(sql, PG, P).parameter_columns().items()} == expected

    @pytest.mark.parametrize("dialect, sql, expected", [
        (PG, "UPDATE users SET ssn = /*@encrypt:ssn*/ %s FROM accounts a WHERE a.id = users.id",
         ColumnRef("ssn", "users")),
        (MYSQL, "UPDATE users u JOIN accounts a ON a.uid = u.id SET u.SSN = /*@encrypt:ssn*/ %s",
         ColumnRef("ssn", "users")),
        # A column other than the one assigned: the single written table.
        (PG, "UPDATE users SET note = /*@encrypt:ssn*/ %s FROM accounts a WHERE a.id = users.id",
         ColumnRef("ssn", "users")),
        # Several written tables and another column: the table can't be told.
        (MYSQL, "UPDATE users u JOIN accounts a ON a.uid = u.id SET u.note = /*@encrypt:ssn*/ %s, a.x = 1",
         ColumnRef("ssn")),
    ])
    def test_bare_annotation_with_several_tables(self, dialect, sql, expected):
        assert analyze(sql, dialect, P).parameter_columns() == {0: expected}

    @pytest.mark.parametrize("dialect, sql, expected", [
        (PG, "INSERT INTO orders (user_id, payment_info) SELECT id, /*@encrypt:payment_info*/ %s FROM temp",
         ColumnRef("payment_info", "orders")),
        (MYSQL, "INSERT INTO orders (payment_info) SELECT /*@encrypt:payment_info*/ %s FROM DUAL",
         ColumnRef("payment_info", "orders")),
        (PG, "INSERT INTO users (name, ssn) SELECT name, /*@encrypt:ssn*/ %s FROM accounts", ColumnRef("ssn", "users")),
        (PG, "UPDATE users SET ssn = (SELECT /*@encrypt:ssn*/ %s FROM accounts)", ColumnRef("ssn", "users")),
    ])
    def test_bare_annotation_in_a_query_feeding_a_write_names_the_written_table(self, dialect, sql, expected):
        # The value goes to the write's target, even though the subquery reads another table.
        assert analyze(sql, dialect, P).parameter_columns()[0] == expected

    # ---- SQLAlchemy's multi-row insert ----

    def test_insert_select_from_values_maps_select_items_by_name(self):
        a = analyze("INSERT INTO users (name, ssn) SELECT p1, p0 FROM (VALUES (%s, %s)) AS v(p0, p1)", PG, P)
        assert a.complete
        assert assigned(a) == [(0, "users.ssn"), (1, "users.name")]

    @pytest.mark.parametrize("sql", [
        "INSERT INTO users (ssn, note) SELECT p0, p0 FROM (VALUES (%s)) AS v(p0)",
        "INSERT INTO users (name, ssn) SELECT v.p0, p0::text FROM (VALUES (%s, %s)) AS v(p0, p1)",
    ])
    def test_insert_select_from_values_using_a_column_twice_is_not_read(self, sql):
        # One value would fill two columns, and could be encrypted for only one.
        a = analyze(sql, PG, P)
        assert a.gap is AnalysisGap.VALUES_FROM_QUERY and a.assignments == ()

    @pytest.mark.parametrize("sql, gap", [
        ("INSERT INTO users (name, ssn) SELECT p0 FROM (VALUES (%s, %s)) AS v(p0, p1)", AnalysisGap.VALUES_FROM_QUERY),
        ("INSERT INTO users (name, ssn) SELECT p0, p1 FROM (VALUES (%s, %s), (%s)) AS v(p0, p1)",
         AnalysisGap.ROW_LENGTH_MISMATCH),
        ("INSERT INTO users (ssn) SELECT p0 FROM (VALUES (%s)) AS v(p0) FETCH FIRST 1 ROWS ONLY",
         AnalysisGap.VALUES_FROM_QUERY),
    ])
    def test_insert_select_from_values_widths_and_extras(self, sql, gap):
        assert analyze(sql, PG, P).gap is gap

    # ---- regex cost on indented SQL ----

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("dialect, sql", [
        (MYSQL, "\n" + " " * 60 + "\n" + " " * 60 + "UPDATE users SET ssn = %s WHERE id = %s AND (name = 'x'"),
        (MYSQL, " " * 60 + "SELECT a FROM t WHERE b = %s INTO OUTFILE '/tmp/x'"),
        (MYSQL, " " * 60 + "INSERT INTO users (users.ssn) VALUE (%s"),
        (PG, "\n" + " " * 60 + "PREPARE p (int) /* c */ AS INSERT INTO users (ssn) VALUES ($1)"),
        (PG, "/* a */" * 60 + " PREPARE p (int) /* c */ AS INSERT INTO users (ssn) VALUES ($1)"),
    ])
    def test_leading_whitespace_and_comments_do_not_backtrack(self, dialect, sql):
        assert analyze(sql, dialect, P).gap is not None

    def test_indented_prepare_and_qualified_insert_are_still_read(self):
        a = analyze("\n    PREPARE p (int) /* c */ AS INSERT INTO users (ssn) VALUES ('x')", PG)
        assert a.gap is AnalysisGap.PREPARED_PARAMETERS and unbound(a) == ["users.ssn"]
        a = analyze("\n    INSERT INTO users (users.ssn) VALUES (%s)", MYSQL, P)
        assert a.complete and assigned(a) == [(0, "users.ssn")]

    # ---- MySQL qualified INSERT columns with dotted back-quoted names ----

    @pytest.mark.parametrize("sql, expected", [
        ("INSERT INTO users (users.name, `x.ssn`) VALUES (%s, %s)", [(0, "users.name"), (1, "users.x.ssn")]),
        ("INSERT INTO users (users.name, `ssn.v2`) VALUES (%s, %s)", [(0, "users.name"), (1, "users.ssn.v2")]),
        ("INSERT INTO users (`users`.`ssn`, name) VALUES (%s, %s)", [(0, "users.ssn"), (1, "users.name")]),
    ])
    def test_back_quoted_name_with_a_dot_is_one_name(self, sql, expected):
        a = analyze(sql, MYSQL, P)
        assert a.complete and assigned(a) == expected

    def test_qualifier_retry_only_for_inserts(self, monkeypatch):
        calls = []
        real = sql_analyzer._without_insert_column_qualifiers
        monkeypatch.setattr(sql_analyzer, "_without_insert_column_qualifiers",
                            lambda text: calls.append(text) or real(text))
        analyze("UPDATE users SET ssn = %s WHERE (", MYSQL, P)
        assert calls == []

    # ---- keyword fallback ----

    @pytest.mark.parametrize("dialect, sql", [
        (PG, "SET search_path TO app; PREPARE p AS INSERT INTO users (ssn) VALUES ('123-45-6789')"),
        (PG, "EXPLAIN ANALYZE EXECUTE p(%s)"),
        (MYSQL, "SELECT 1 /*!LOAD DATA INFILE 'x' INTO TABLE users */"),
    ])
    def test_more_statements_that_may_write(self, dialect, sql):
        a = analyze(sql, dialect, P)
        assert a.writes and a.gap is not None

    # ---- names that are not plain identifiers ----

    def test_subscripted_update_target_is_unsupported(self):
        a = analyze("UPDATE users SET ssn[1] = %s", PG, P)
        assert a.writes and a.gap is AnalysisGap.UNSUPPORTED_STATEMENT and a.assignments == ()

    # ---- TABLE is not parsed (sqlglot reads it as an alias) ----

    def test_table_statement_is_not_parsed(self):
        a = analyze("TABLE users", PG)
        assert a.kind is StatementKind.SELECT and not a.writes and a.tables == ()

    # ---- scopes ----

    def test_nearest_query_memo_does_not_cross_queries(self):
        a = analyze("SELECT * FROM users u WHERE EXISTS "
                    "(SELECT 1 FROM accounts WHERE u.name = %s GROUP BY secret HAVING secret = %s)", PG, P)
        assert compared(a) == [(0, "users.name"), (1, "accounts.secret")]

    @pytest.mark.timeout(10)
    def test_long_boolean_chain_is_time_bounded(self):
        sql = "SELECT ssn FROM users WHERE " + " OR ".join(["(id = %s AND ssn = %s)"] * 12000)
        assert len(analyze(sql, PG, P).comparisons) == 24000

    # ---- caching ----

    def test_long_sql_with_few_placeholders_is_not_cached(self):
        sql = "UPDATE users SET ssn = %s WHERE note = '" + "x" * sql_analyzer.CACHEABLE_SQL_LENGTH + "'"
        assert sql.count("%") <= sql_analyzer.CACHEABLE_PERCENT_SIGNS
        assert analyze(sql, PG, P) is not analyze(sql, PG, P)

    def test_recursion_error_wrapped_by_the_tokenizer_is_not_cached(self, monkeypatch):
        real = sqlglot.parse
        calls = []

        def flaky(sql, read):
            calls.append(sql)
            if len(calls) == 1:
                try:
                    raise RecursionError("too deep")
                except RecursionError as e:
                    raise sqlglot.errors.TokenError("wrapped") from e
            return real(sql, read=read)

        monkeypatch.setattr(sql_analyzer.sqlglot, "parse", flaky)
        sql = "SELECT a FROM users WHERE ssn = %s"
        first = analyze(sql, PG, P)
        assert first.gap is AnalysisGap.UNPARSEABLE and first.detail == "RecursionError"
        assert analyze(sql, PG, P).gap is None

    # ---- the logging filter is per thread ----

    def test_quiet_filter_does_not_silence_other_threads(self, caplog):
        entered, release = threading.Event(), threading.Event()

        def parse_elsewhere():
            with sql_analyzer._QUIET.active():
                entered.set()
                release.wait(5)

        thread = threading.Thread(target=parse_elsewhere)
        thread.start()
        try:
            assert entered.wait(5)
            with caplog.at_level(logging.WARNING, logger="sqlglot"):
                logging.getLogger("sqlglot").warning("application record")
            assert [r.getMessage() for r in caplog.records] == ["application record"]
        finally:
            release.set()
            thread.join()


class TestReviewRegressionsRound3:
    """Regression tests: regexes that fail after a long gap, and which table a
    bare annotation gets."""

    # ---- regexes that fail after a long run of whitespace or comments ----

    _BAD_PREPARE = "PREPARE p (int AS INSERT INTO users (ssn) VALUES ($1)"   # unbalanced header
    _PARTITION = "INSERT INTO users PARTITION (p0) (users.ssn) VALUES (%s)"  # valid MySQL neither read can parse

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("dialect, sql, gap", [
        (PG, " " * 60 + _BAD_PREPARE, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "/* a */ " * 60 + _BAD_PREPARE, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "-- " + "-" * 60 + "\n" + _BAD_PREPARE, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "-- " * 60 + "\n" + _BAD_PREPARE, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "-- header line\r\n" * 40 + _BAD_PREPARE, AnalysisGap.UNSUPPORTED_STATEMENT),
        (PG, "PREPARE p" + "-- c\r\n" * 40 + "INSERT INTO users (ssn) VALUES ($1)", AnalysisGap.UNSUPPORTED_STATEMENT),
        (MYSQL, " " * 60 + _PARTITION, AnalysisGap.UNPARSEABLE),
        (MYSQL, "/* a */ " * 60 + _PARTITION, AnalysisGap.UNPARSEABLE),
        (MYSQL, "-- " + "-" * 60 + "\n" + _PARTITION, AnalysisGap.UNPARSEABLE),
        (MYSQL, "-- " + " -- ".join("abcdefghijklmnopqrstuvwxyz") + "\n" + _PARTITION, AnalysisGap.UNPARSEABLE),
        (MYSQL, "-- c \n" * 40 + "INSERT INTO users SET ssn = %s, note = 'a' 'b' garbage(", AnalysisGap.UNPARSEABLE),
    ], ids=["pg-spaces", "pg-comments", "pg-dash-banner", "pg-dashes", "pg-crlf", "pg-crlf-header", "my-spaces",
            "my-comments", "my-dash-banner", "my-dashed-prose", "my-trailing-space"])
    def test_a_regex_that_fails_after_a_long_gap_fails_fast(self, dialect, sql, gap):
        assert analyze(sql, dialect, P).gap is gap

    def test_line_comments_in_a_prepare_header(self):
        a = analyze("-- c\r\n  PREPARE -- x\r\n p (int) -- y \r\n AS INSERT INTO users (ssn) VALUES ('x')", PG)
        assert a.gap is AnalysisGap.PREPARED_PARAMETERS and unbound(a) == ["users.ssn"]

    # ---- bare annotations: written vs compared values ----

    @pytest.mark.parametrize("dialect, sql, ordinal, expected", [
        # A filter value gets the table it is compared in, even in a statement that writes.
        (PG, "UPDATE users SET note = %s WHERE id IN "
             "(SELECT user_id FROM cards WHERE lower(pan) = /*@encrypt:pan*/ %s)", 1, ColumnRef("pan", "cards")),
        (PG, "INSERT INTO audit (note) SELECT note FROM users WHERE coalesce(ssn, '') = /*@encrypt:ssn*/ %s",
         0, ColumnRef("ssn", "users")),
        (PG, "WITH c AS (SELECT user_id FROM cards WHERE lower(pan) = /*@encrypt:pan*/ %s) "
             "UPDATE users SET flag = true WHERE id IN (SELECT user_id FROM c)", 0, ColumnRef("pan", "cards")),
        # Another statement's write doesn't count.
        (MYSQL, "UPDATE a SET x = %s; SELECT * FROM cards WHERE lower(pan) = /*@encrypt:pan*/ %s",
         1, ColumnRef("pan", "cards")),
        (MYSQL, "SELECT * FROM users u WHERE u.tax = /*@encrypt:ssn*/ %s; UPDATE audit SET n = n + 1",
         0, ColumnRef("ssn", "users")),
        (PG, "INSERT INTO logs (msg) VALUES (%s); SELECT id FROM users WHERE ssn || '' = /*@encrypt:ssn*/ %s",
         1, ColumnRef("ssn", "users")),
        # Values that flow into the write: the written table.
        (PG, "INSERT INTO users (ssn) SELECT /*@encrypt:ssn*/ %s FROM temp", 0, ColumnRef("ssn", "users")),
        (PG, "UPDATE users SET note = (SELECT /*@encrypt:ssn*/ %s FROM temp LIMIT 1)", 0, ColumnRef("ssn", "users")),
        # Values bound in a source of the write may feed it or only be compared
        # with another table's column: the table can't be told.
        (PG, "MERGE INTO users t USING (SELECT /*@encrypt:ssn*/ %s AS v) s ON t.id = 1 "
             "WHEN NOT MATCHED THEN INSERT (id, ssn) VALUES (1, s.v)", 0, ColumnRef("ssn")),
        (PG, "WITH s AS (SELECT /*@encrypt:ssn*/ %s AS v) INSERT INTO users (ssn) SELECT v FROM s",
         0, ColumnRef("ssn")),
        # Several written tables: the table can't be told.
        (MYSQL, "UPDATE users u JOIN accounts a ON a.uid = u.id "
                "SET u.note = (SELECT /*@encrypt:ssn*/ %s FROM temp), a.x = 1", 0, ColumnRef("ssn")),
    ])
    def test_bare_annotation_by_where_the_value_goes(self, dialect, sql, ordinal, expected):
        assert analyze(sql, dialect, P).parameter_columns()[ordinal] == expected

    @pytest.mark.parametrize("dialect, sql, expected", [
        # A value compared or tested inside a written expression is not written.
        (PG, "INSERT INTO audit (note) SELECT CASE WHEN lower(ssn) = /*@encrypt:ssn*/ %s THEN 'm' END FROM users",
         ColumnRef("ssn", "users")),
        (PG, "INSERT INTO audit (flag) SELECT lower(ssn) = /*@encrypt:ssn*/ %s FROM users", ColumnRef("ssn", "users")),
        (MYSQL, "INSERT INTO audit (note) SELECT IF(lower(ssn) = /*@encrypt:ssn*/ %s, 'm', 'n') FROM users",
         ColumnRef("ssn", "users")),
        (PG, "INSERT INTO audit (note) SELECT CASE lower(ssn) WHEN /*@encrypt:ssn*/ %s THEN 'm' END FROM users",
         ColumnRef("ssn", "users")),
        (PG, "UPDATE users SET flag = (SELECT lower(pan) = /*@encrypt:pan*/ %s FROM cards LIMIT 1)",
         ColumnRef("pan", "cards")),
        (PG, "UPDATE users SET note = CASE WHEN lower(a.tax) = /*@encrypt:tax*/ %s THEN 'x' END "
             "FROM accounts a WHERE a.uid = users.id", ColumnRef("tax")),
        # The THEN value is written.
        (PG, "UPDATE users SET note = CASE WHEN flag THEN /*@encrypt:ssn*/ %s END", ColumnRef("ssn", "users")),
        # A parameter CTE or derived table compared with another table's column.
        (PG, "WITH params AS (SELECT /*@encrypt:ssn*/ %s::text AS ssn) UPDATE orders o SET status = 'flagged' "
             "FROM customers c, params p WHERE o.customer_id = c.id AND c.ssn = p.ssn", ColumnRef("ssn")),
        (PG, "WITH params AS (SELECT /*@encrypt:ssn*/ %s AS ssn) INSERT INTO audit (customer_id) "
             "SELECT c.id FROM customers c JOIN params p ON c.ssn = p.ssn", ColumnRef("ssn")),
        (MYSQL, "INSERT INTO audit (customer_id) WITH params AS (SELECT /*@encrypt:ssn*/ %s AS ssn) "
                "SELECT c.id FROM customers c JOIN params p ON c.ssn = p.ssn", ColumnRef("ssn")),
        # A value fed to the write through a source: bare as well.
        (PG, "INSERT INTO t (a) SELECT j.v FROM src JOIN (SELECT /*@encrypt:ssn*/ %s AS v) j ON true",
         ColumnRef("ssn")),
        (PG, "INSERT INTO t (a) SELECT s.v FROM (SELECT /*@encrypt:ssn*/ %s AS v) s", ColumnRef("ssn")),
    ], ids=["case-when", "boolean-item", "mysql-if", "simple-case", "set-subquery-item", "set-case-two-sources",
            "case-then", "param-cte-update", "param-cte-insert", "mysql-param-cte", "join-source", "from-source"])
    def test_bare_annotation_on_a_compared_or_source_value(self, dialect, sql, expected):
        assert analyze(sql, dialect, P).parameter_columns()[0] == expected

    @pytest.mark.parametrize("dialect, sql, expected", [
        # THEN, ELSE and IF() results in an INSERT ... SELECT reading another table are written.
        (PG, "INSERT INTO audit (note) SELECT CASE WHEN flag THEN /*@encrypt:ssn*/ %s END FROM users",
         ColumnRef("ssn", "audit")),
        (PG, "INSERT INTO audit (note) SELECT CASE WHEN flag THEN 'a' ELSE /*@encrypt:ssn*/ %s END FROM users",
         ColumnRef("ssn", "audit")),
        (MYSQL, "INSERT INTO audit (note) SELECT IF(flag, /*@encrypt:ssn*/ %s, 'n') FROM users",
         ColumnRef("ssn", "audit")),
        # Tests other than the plain comparisons.
        (PG, "INSERT INTO audit (flag) SELECT lower(ssn) IN (/*@encrypt:ssn*/ %s) FROM users", ColumnRef("ssn", "users")),
        (PG, "INSERT INTO audit (flag) SELECT lower(ssn) BETWEEN /*@encrypt:ssn*/ %s AND 'z' FROM users",
         ColumnRef("ssn", "users")),
        (PG, "INSERT INTO audit (flag) SELECT ssn ~ /*@encrypt:ssn*/ %s FROM users", ColumnRef("ssn", "users")),
        (PG, "INSERT INTO audit (flag) SELECT doc @> /*@encrypt:ssn*/ %s::jsonb FROM users", ColumnRef("ssn", "users")),
        (MYSQL, "INSERT INTO audit (flag) SELECT ssn REGEXP /*@encrypt:ssn*/ %s FROM users", ColumnRef("ssn", "users")),
        (MYSQL, "INSERT INTO audit (flag) SELECT MATCH (ssn) AGAINST (/*@encrypt:ssn*/ %s) FROM users",
         ColumnRef("ssn", "users")),
        # The operand of a simple CASE.
        (PG, "INSERT INTO audit (note) SELECT CASE /*@encrypt:ssn*/ %s WHEN lower(ssn) THEN 'm' END FROM users",
         ColumnRef("ssn", "users")),
        # A window's PARTITION BY is not written.
        (PG, "INSERT INTO audit (n) SELECT row_number() OVER (PARTITION BY /*@encrypt:ssn*/ %s) FROM users",
         ColumnRef("ssn", "users")),
        # Sources that read a table: bare, not that table.
        (PG, "INSERT INTO t (a) SELECT j.v FROM src JOIN (SELECT /*@encrypt:ssn*/ %s AS v FROM cards) j ON true",
         ColumnRef("ssn")),
        (PG, "INSERT INTO t (a) SELECT s.v FROM (SELECT /*@encrypt:ssn*/ %s AS v FROM cards) s", ColumnRef("ssn")),
        (PG, "WITH s AS (SELECT /*@encrypt:ssn*/ %s AS v FROM cards) INSERT INTO users (note) SELECT v FROM s",
         ColumnRef("ssn")),
        (PG, "MERGE INTO users t USING (SELECT /*@encrypt:ssn*/ %s AS v FROM cards) s ON t.id = 1 "
             "WHEN NOT MATCHED THEN INSERT (id, note) VALUES (1, s.v)", ColumnRef("ssn")),
        (PG, "INSERT INTO t (a) SELECT l.v FROM src CROSS JOIN LATERAL (SELECT /*@encrypt:ssn*/ %s AS v FROM cards) l",
         ColumnRef("ssn")),
        (PG, "INSERT INTO t (a) SELECT l.v FROM src, LATERAL (SELECT /*@encrypt:ssn*/ %s AS v FROM cards) l",
         ColumnRef("ssn")),
        # A SET value in an UPDATE that also reads another table: the assignment is not a test.
        (PG, "UPDATE users SET note = /*@encrypt:ssn*/ %s FROM cards c WHERE c.uid = users.id",
         ColumnRef("ssn", "users")),
    ], ids=["then", "else", "mysql-if-true", "in", "between", "regex", "jsonb-contains", "mysql-regexp",
            "mysql-match", "simple-case-operand", "partition-by", "join-source-reads", "from-source-reads",
            "cte-reads", "merge-using-reads", "cross-join-lateral-reads", "comma-lateral-reads", "set-with-from"])
    def test_bare_annotation_written_tested_or_from_a_source(self, dialect, sql, expected):
        assert analyze(sql, dialect, P).parameter_columns()[0] == expected

    # A value tested by any of these operators resolves to the table it is tested against on
    # every supported sqlglot, though the operators are Predicates only from 30.16.
    @pytest.mark.parametrize("op", ["~*", "!~*", "<@", "&&", "?", "?|", "?&", "-|-", "&<", "&>"])
    def test_bare_annotation_tested_by_an_operator(self, op):
        sql = f"UPDATE audit SET flag = (SELECT ssn {op} /*@encrypt:ssn*/ %s FROM users LIMIT 1)"
        assert analyze(sql, PG, P).parameter_columns()[0] == ColumnRef("ssn", "users")

    def test_bare_annotation_tested_by_overlaps(self):
        sql = "UPDATE audit SET flag = (SELECT (d1, ssn) OVERLAPS (d2, /*@encrypt:ssn*/ %s) FROM users LIMIT 1)"
        assert analyze(sql, PG, P).parameter_columns()[0] == ColumnRef("ssn", "users")

    # A condition with no test operator around the value (a function sqlglot
    # does not model as one): only the slot keeps the value out of the write.
    @pytest.mark.parametrize("dialect, sql, expected", [
        (PG, "INSERT INTO audit (uid) SELECT id FROM users WHERE starts_with(ssn, /*@encrypt:ssn*/ %s)",
         ColumnRef("ssn", "users")),
        (MYSQL, "INSERT INTO audit (uid) SELECT id FROM users WHERE LOCATE(/*@encrypt:ssn*/ %s, ssn)",
         ColumnRef("ssn", "users")),
        (PG, "INSERT INTO audit (n) SELECT count(*) FROM users GROUP BY note "
             "HAVING bool_or(starts_with(ssn, /*@encrypt:ssn*/ %s))", ColumnRef("ssn", "users")),
        (PG, "UPDATE users SET flag = true FROM cards c "
             "WHERE c.uid = users.id AND starts_with(c.pan, /*@encrypt:pan*/ %s)", ColumnRef("pan")),
        (PG, "MERGE INTO users t USING cards c ON t.id = c.uid AND starts_with(c.pan, /*@encrypt:pan*/ %s) "
             "WHEN MATCHED THEN UPDATE SET flag = true", ColumnRef("pan")),
        (PG, "MERGE INTO users t USING cards c ON t.id = c.uid "
             "WHEN MATCHED AND starts_with(c.pan, /*@encrypt:pan*/ %s) THEN UPDATE SET flag = true", ColumnRef("pan")),
    ], ids=["where", "mysql-where", "having", "update-from-where", "merge-on", "merge-when-condition"])
    def test_bare_annotation_in_a_condition_slot(self, dialect, sql, expected):
        assert analyze(sql, dialect, P).parameter_columns()[0] == expected

    @pytest.mark.parametrize("sql, ordinal, expected", [
        ("INSERT INTO audit (n) SELECT count(*) FROM users GROUP BY note HAVING max(lower(ssn)) = /*@encrypt:ssn*/ %s",
         0, ColumnRef("ssn", "users")),
        ("MERGE INTO users t USING cards c ON t.id = c.uid AND lower(c.pan) = /*@encrypt:pan*/ %s "
         "WHEN MATCHED THEN UPDATE SET flag = true", 0, ColumnRef("pan")),
        ("MERGE INTO users t USING cards c ON t.id = c.uid "
         "WHEN MATCHED AND lower(c.pan) = /*@encrypt:pan*/ %s THEN UPDATE SET flag = true", 0, ColumnRef("pan")),
        ("UPDATE users SET flag = true FROM cards c WHERE c.uid = users.id AND lower(c.pan) = /*@encrypt:pan*/ %s",
         0, ColumnRef("pan")),
        ("MERGE INTO users t USING cards c ON t.id = c.uid "
         "WHEN NOT MATCHED THEN INSERT (id, note) VALUES (c.uid, /*@encrypt:ssn*/ %s)", 0, ColumnRef("ssn", "users")),
        ("MERGE INTO users t USING cards c ON t.id = c.uid "
         "WHEN MATCHED THEN UPDATE SET note = /*@encrypt:ssn*/ %s", 0, ColumnRef("ssn", "users")),
        ("INSERT INTO logs (msg) VALUES (%s); UPDATE users SET note = /*@encrypt:ssn*/ %s", 1, ColumnRef("ssn", "users")),
        ("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET note = /*@encrypt:ssn*/ %s",
         0, ColumnRef("ssn", "t")),
    ], ids=["having", "merge-on", "merge-when-condition", "update-from-where", "merge-when-insert", "merge-when-update",
            "two-writes", "on-conflict-set"])
    def test_bare_annotation_slots(self, sql, ordinal, expected):
        assert analyze(sql, PG, P).parameter_columns()[ordinal] == expected

    # ---- one-dialect gaps ----

    def test_replace_with_qualified_columns_is_read(self):
        a = analyze("REPLACE INTO users (users.ssn) VALUES (%s)", MYSQL, P)
        assert a.complete and assigned(a) == [(0, "users.ssn")]

    @BOTH
    def test_table_statement_is_not_parsed_on_either_dialect(self, dialect):
        a = analyze("TABLE users", dialect)
        assert a.kind is StatementKind.SELECT and not a.writes and a.tables == ()

    # ---- a cyclic exception chain ends ----

    @pytest.mark.timeout(10)
    def test_cyclic_exception_chain_does_not_hang(self, monkeypatch):
        def cyclic(sql, read):
            error = sqlglot.errors.ParseError("x")
            error.__cause__ = error
            raise error

        monkeypatch.setattr(sql_analyzer.sqlglot, "parse", cyclic)
        a = sql_analyzer._analysis("SELECT 1", PG, ParamStyle.NONE)
        assert a.gap is AnalysisGap.UNPARSEABLE and a.detail == "ParseError"
