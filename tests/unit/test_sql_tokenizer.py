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

import re
from types import MappingProxyType

import pytest

from aws_advanced_python_wrapper.encryption.sql_dialect import MYSQL, PG
from aws_advanced_python_wrapper.encryption.sql_tokenizer import (
    ColumnRef, ParamStyle, normalized_ordinal, tokenize)

POS, NAMED, NONE = ParamStyle.POSITIONAL, ParamStyle.NAMED, ParamStyle.NONE


def keys(tokens):
    return [p.key for p in tokens.placeholders]


def hidden(tokens):
    return [p.hidden for p in tokens.placeholders]


class TestParamStyle:
    @pytest.mark.parametrize("params, expected", [
        (None, NONE), ((), POS), ([], POS), ((1,), POS), ([1], POS),
        ({}, NAMED), ({"a": 1}, NAMED), (MappingProxyType({"a": 1}), NAMED),
    ])
    def test_psycopg(self, params, expected):
        # psycopg splits the query whenever params is not None, even if empty.
        assert ParamStyle.of(params, PG) is expected

    @pytest.mark.parametrize("params, expected", [
        (None, NONE), ((), NONE), ([], NONE), ({}, NONE), ((1,), POS), ([1], POS), ({"a": 1}, NAMED),
        # Connector/Python only accepts a dict, list or tuple.
        (MappingProxyType({"a": 1}), NONE), ("abc", NONE),
    ])
    def test_mysql_connector(self, params, expected):
        # Connector/Python substitutes only for a non-empty dict, list or tuple.
        assert ParamStyle.of(params, MYSQL) is expected


class TestPsycopgPlaceholders:
    def test_positional_in_order(self):
        t = tokenize("INSERT INTO t (a, b) VALUES (%s, %s)", PG, POS)
        assert keys(t) == [0, 1]
        assert [p.ordinal for p in t.placeholders] == [0, 1]

    def test_binary_and_text_formats_are_placeholders(self):
        assert keys(tokenize("VALUES (%s, %b, %t)", PG, POS)) == [0, 1, 2]

    def test_named_may_repeat(self):
        t = tokenize("UPDATE t SET a = %(v)s, b = %(v)s WHERE id = %(id)b", PG, NAMED)
        assert keys(t) == ["v", "v", "id"]
        assert [p.ordinal for p in t.placeholders] == [0, 1, 2]

    def test_double_percent_is_a_percent_sign(self):
        t = tokenize("SELECT 7 %% 2, a FROM t WHERE b LIKE 'x%%' AND c = %s", PG, POS)
        assert keys(t) == [0]
        assert "7 % 2" in t.normalized and "'x%'" in t.normalized

    def test_placeholders_inside_literals_and_comments_are_still_bound(self):
        # psycopg does not skip quotes or comments, so neither may we: the
        # second %s is the second parameter even though it's in a string.
        sql = "SELECT %s, '%s', \"%s\", $$%s$$, $tag$%s$tag$, E'\\'%s', /* %s */ -- %s\n %s"
        t = tokenize(sql, PG, POS)
        assert keys(t) == list(range(9))
        assert hidden(t) == [False, True, True, True, True, True, True, True, False]
        assert t.has_hidden_placeholders

    def test_nested_block_comments(self):
        t = tokenize("/* a /* b */ %s */ SELECT %s", PG, POS)
        assert hidden(t) == [True, False]

    @pytest.mark.timeout(10)
    def test_deeply_nested_comments_stay_linear(self):
        depth = 100_000
        t = tokenize("SELECT /*" + " /*" * depth + " */" * (depth + 1) + " %s", PG, POS)
        assert hidden(t) == [False]

    def test_dollar_number_is_not_a_dollar_quote(self):
        t = tokenize("SELECT $1, %s", PG, POS)
        assert hidden(t) == [False]

    def test_identifier_with_dollar_is_not_a_dollar_quote(self):
        t = tokenize("SELECT a$b$, %s", PG, POS)
        assert hidden(t) == [False]

    def test_standard_strings_do_not_escape_backslash(self):
        # standard_conforming_strings=on: 'a\' is a whole literal.
        t = tokenize("SELECT 'a\\', %s", PG, POS)
        assert hidden(t) == [False]

    def test_escape_strings_do(self):
        t = tokenize("SELECT E'a\\', %s'", PG, POS)
        assert hidden(t) == [True]

    def test_doubled_quotes(self):
        t = tokenize("SELECT 'it''s', \"a\"\"b\", %s", PG, POS)
        assert hidden(t) == [False]

    @pytest.mark.parametrize("sql, rejected", [
        ("SELECT 1 % 2", True), ("SELECT '100%'", True), ("SELECT %d", True), ("SELECT %()s", True),
        # A lone % at the very end is not matched by psycopg's pattern either.
        ("SELECT %", False),
    ])
    def test_driver_rejects_other_percent_sequences(self, sql, rejected):
        assert (tokenize(sql, PG, POS).driver_error is not None) is rejected

    @pytest.mark.parametrize("sql, style", [
        ("SELECT '%(card 4111-1111-1111-1111)x', %s", POS),
        ("SELECT '4111-1111-1111-1111 %d'", POS),
        ("VALUES (%s, %(4111-1111)s)", NAMED),
    ])
    def test_driver_error_never_quotes_the_sql(self, sql, style):
        error = tokenize(sql, PG, style).driver_error
        assert error is not None and "4111" not in error

    def test_percent_before_newline_is_left_alone(self):
        # psycopg's pattern does not match a newline after %.
        assert tokenize("SELECT 1 %\n2", PG, POS).driver_error is None

    @pytest.mark.parametrize("style", [POS, NAMED])
    def test_mixed_styles_are_a_driver_error(self, style):
        assert tokenize("VALUES (%s, %(a)s)", PG, style).driver_error is not None

    @pytest.mark.parametrize("sql, style", [("VALUES (%s)", NAMED), ("VALUES (%(a)s)", POS)])
    def test_style_mismatch_is_a_driver_error(self, sql, style):
        assert tokenize(sql, PG, style).driver_error is not None

    def test_no_params_means_no_placeholders(self):
        t = tokenize("SELECT '%s', %s, 1 %% 2", PG, NONE)
        assert t.placeholders == ()
        assert t.driver_error is None
        assert t.normalized == "SELECT '%s', %s, 1 %% 2"

    def test_spans(self):
        sql = "SELECT %(name)s"
        p = tokenize(sql, PG, NAMED).placeholders[0]
        assert sql[p.start:p.end] == "%(name)s"


class TestMySqlPlaceholders:
    def test_positional(self):
        assert keys(tokenize("INSERT INTO t (a, b) VALUES (%s, %s)", MYSQL, POS)) == [0, 1]

    def test_double_percent_is_not_an_escape(self):
        # Connector/Python replaces every "%s" substring, including in "%%s".
        t = tokenize("SELECT 5 %% 2, %%s", MYSQL, POS)
        assert keys(t) == [0]
        assert t.driver_error is None

    def test_any_other_percent_sequence_is_left_alone(self):
        t = tokenize("SELECT a FROM t WHERE b LIKE 'x%' AND c = %d AND d = %s", MYSQL, POS)
        assert keys(t) == [0]
        assert t.driver_error is None

    def test_named_only_with_a_dict(self):
        assert keys(tokenize("VALUES (%(a)s, %s)", MYSQL, NAMED)) == ["a"]
        assert keys(tokenize("VALUES (%(a)s, %s)", MYSQL, POS)) == [0]

    def test_named_percent_conversion_is_a_percent_sign(self):
        t = tokenize("SELECT %(a)s, 5 %(x)% 2", MYSQL, NAMED)
        assert keys(t) == ["a"]
        assert "5 % 2" in t.normalized

    def test_named_other_conversion_is_a_driver_error(self):
        error = tokenize("SELECT '%(123-45-6789)d'", MYSQL, NAMED).driver_error
        assert error is not None and "123-45" not in error

    def test_no_params_means_no_placeholders(self):
        assert tokenize("SELECT %s", MYSQL, NONE).placeholders == ()

    def test_placeholders_inside_literals_and_comments_are_still_bound(self):
        sql = "SELECT %s, '%s', \"%s\", `%s`, /* %s */, # %s\n -- %s\n %s"
        t = tokenize(sql, MYSQL, POS)
        assert hidden(t) == [False, True, True, True, True, True, True, False]

    def test_backslash_escapes_in_strings(self):
        t = tokenize("SELECT 'a\\'b', \"c\\\"d\", %s", MYSQL, POS)
        assert hidden(t) == [False]

    def test_double_dash_needs_whitespace_to_start_a_comment(self):
        t = tokenize("SELECT 1--%s", MYSQL, POS)
        assert hidden(t) == [False]
        assert hidden(tokenize("SELECT 1-- %s", MYSQL, POS)) == [True]

    def test_block_comments_do_not_nest(self):
        t = tokenize("/* a /* b */ %s */", MYSQL, POS)
        assert hidden(t) == [False]

    @pytest.mark.parametrize("sql, executable", [
        ("SELECT /*!40001 SQL_NO_CACHE */ 1", True),
        ("SELECT /*! 1 */", True),
        ("SELECT /*M!100000 1 */", True),
        ("SELECT /*+ hint */ 1", False),
        ("SELECT '/*! x */'", False),
    ])
    def test_executable_comments(self, sql, executable):
        assert tokenize(sql, MYSQL, NONE).executable_comment is executable

    def test_executable_comment_is_only_a_mysql_notion(self):
        assert not tokenize("SELECT /*! 1 */", PG, NONE).executable_comment

    def test_executable_comment_keywords(self):
        assert "INSERT" in tokenize("SELECT 1 /*! ; INSERT INTO t VALUES (1) */", MYSQL, NONE).keywords

    @pytest.mark.parametrize("sql, introducers", [
        ("VALUES (_binary %s, _utf8mb4'x', %s)", ["_binary", None]),
        ("VALUES (_BINARY   %s)", ["_binary"]),
        ("VALUES (_x %s, a_binary %s)", [None, None]),
        ("VALUES ('_binary' %s)", [None]),
        ("VALUES (_binary '%s')", [None]),
    ])
    def test_charset_introducers(self, sql, introducers):
        t = tokenize(sql, MYSQL, POS)
        assert [p.introducer for p in t.placeholders] == introducers
        if introducers[0]:
            assert "_binary" not in t.normalized.lower()

    def test_introducer_inside_a_comment_is_ignored(self):
        t = tokenize("VALUES (-- _binary\n%s)", MYSQL, POS)
        assert [p.introducer for p in t.placeholders] == [None]
        assert "_binary" in t.normalized


class TestLexing:
    @pytest.mark.parametrize("dialect, sql", [
        (PG, "SELECT 'abc"), (PG, "SELECT \"abc"), (PG, "SELECT $$abc"), (PG, "SELECT /* /* */"),
        (PG, "SELECT 'it''"), (MYSQL, "SELECT 'abc\\'"), (MYSQL, "SELECT `a"), (MYSQL, "SELECT /* a"),
    ])
    def test_unterminated(self, dialect, sql):
        assert tokenize(sql, dialect, POS).unterminated

    def test_leading_words_per_statement(self):
        t = tokenize("/* hint */ insert INTO t VALUES (1); -- c\n (SELECT 1); ;  UPDATE t SET a = 1;", PG, NONE)
        assert t.leading_words == ("INSERT", "SELECT", "UPDATE")

    def test_semicolon_in_literal_does_not_split(self):
        assert tokenize("SELECT ';' ; SELECT 2", PG, NONE).leading_words == ("SELECT", "SELECT")
        assert tokenize("SELECT ';'", MYSQL, NONE).leading_words == ("SELECT",)

    def test_keywords_ignore_literals_comments_and_placeholder_names(self):
        t = tokenize("SELECT 'INSERT' /* UPDATE */ FROM t WHERE a = %(merge)s", PG, NAMED)
        assert t.keywords == {"FROM"}

    def test_type_error_for_non_str(self):
        with pytest.raises(TypeError):
            tokenize(b"SELECT 1", PG, NONE)  # type: ignore[arg-type]


class TestNormalized:
    def test_placeholders_carry_their_ordinal(self):
        t = tokenize("VALUES (%s, %s)", PG, POS)
        names = re.findall(r":(\w+)", t.normalized)
        assert [normalized_ordinal(n) for n in names] == [0, 1]

    def test_cast_after_placeholder_stays_readable(self):
        assert ":_kms_p0 ::bytea" in tokenize("VALUES (%s::bytea)", PG, POS).normalized

    @pytest.mark.parametrize("name, expected", [
        ("_kms_p0", 0), ("_kms_p12", 12), ("_kms_p", None), ("_kms_px", None), ("other", None), (None, None),
        ("_kms_p٣", None),  # a non-ASCII digit
    ])
    def test_normalized_ordinal(self, name, expected):
        assert normalized_ordinal(name) == expected

    @pytest.mark.parametrize("sql, expected", [
        ("REPLACE INTO t (a) VALUES (%s)", "INSERT INTO t (a) VALUES"),
        ("REPLACE LOW_PRIORITY INTO t (a) VALUES (%s)", "INSERT INTO t"),
        ("INSERT HIGH_PRIORITY IGNORE INTO t (a) VALUES (%s)", "INSERT INTO t"),
        ("INSERT DELAYED INTO t (a) VALUES (%s)", "INSERT INTO t"),
        ("UPDATE LOW_PRIORITY IGNORE t SET a = %s", "UPDATE t SET"),
        ("DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE a = %s", "DELETE FROM t"),
        ("/* x */ replace t SET a = %s", "/* x */ INSERT t SET"),
    ])
    def test_mysql_statement_modifiers_and_replace(self, sql, expected):
        assert expected in " ".join(tokenize(sql, MYSQL, POS).normalized.split())

    def test_replace_function_is_left_alone(self):
        t = tokenize("SELECT REPLACE(a, 'x', 'y') FROM t; UPDATE t SET a = REPLACE(a, 'x', %s)", MYSQL, POS)
        assert t.normalized.count("REPLACE(") == 2

    def test_modifier_words_elsewhere_are_left_alone(self):
        # QUICK is not reserved and can name a table.
        t = tokenize("UPDATE quick SET a = %s", MYSQL, POS)
        assert "UPDATE quick" in t.normalized

    def test_postgresql_keeps_modifier_words(self):
        assert "UPDATE ignore" in tokenize("UPDATE ignore SET a = %s", PG, POS).normalized


class TestAnnotations:
    def test_annotation_before_a_placeholder(self):
        t = tokenize("INSERT INTO t (a, b) VALUES (%s, /*@encrypt:users.ssn*/ %s)", PG, POS)
        assert [p.annotation for p in t.placeholders] == [None, ColumnRef("ssn", "users")]
        assert t.stray_annotations == ()

    def test_named_placeholder(self):
        t = tokenize("UPDATE t SET b = /*@encrypt:ssn*/%(ssn)s", PG, NAMED)
        assert t.placeholders[0].annotation == ColumnRef("ssn")

    def test_schema_is_dropped(self):
        t = tokenize("VALUES (/*@encrypt:app.users.ssn*/ %s)", PG, POS)
        assert t.placeholders[0].annotation == ColumnRef("ssn", "users")

    @pytest.mark.parametrize("ref, expected", [
        # References the shared [\w.]+ grammar allows: empty and extra parts.
        (".ssn", ColumnRef("ssn")),
        ("t..ssn", ColumnRef("ssn", "t")),
        ("a.b.t.ssn", ColumnRef("ssn", "t")),
        ("9t.1a", ColumnRef("1a", "9t")),
    ])
    def test_empty_and_extra_parts(self, ref, expected):
        t = tokenize(f"VALUES (/*@encrypt:{ref}*/ %s)", PG, POS)
        assert t.placeholders[0].annotation == expected

    def test_doubled_quote_in_a_quoted_part(self):
        t = tokenize('VALUES (/*@encrypt:"a""b".ssn*/ %s)', PG, POS)
        assert t.placeholders[0].annotation == ColumnRef("ssn", 'a"b')

    def test_postgresql_folds_unquoted_parts_and_keeps_quoted_ones(self):
        t = tokenize('VALUES (/*@encrypt:Users."SSN"*/ %s)', PG, POS)
        assert t.placeholders[0].annotation == ColumnRef("SSN", "users")

    def test_mysql_backtick_parts(self):
        t = tokenize("VALUES (/*@encrypt:`My Table`.SSN*/ %s)", MYSQL, POS)
        assert t.placeholders[0].annotation == ColumnRef("SSN", "My Table")

    @pytest.mark.parametrize("sql", [
        "VALUES (%s /*@encrypt:users.ssn*/)",        # after, not before
        "VALUES (/*@encrypt:users.ssn*/ 'x', %s)",   # not followed by a placeholder
        "VALUES (/*@encrypt:users.*/ %s)",           # no column
        "VALUES (/*@encrypt:users ssn*/ %s)",
        "VALUES (/*@encrypt:users.ssn*/ '%s')",      # a literal, not a placeholder, follows
    ])
    def test_stray_annotations(self, sql):
        t = tokenize(sql, PG, POS)
        assert all(p.annotation is None for p in t.placeholders)
        assert len(t.stray_annotations) == 1

    def test_annotation_before_a_hidden_placeholder_is_stray(self):
        # psycopg reads %("x")s as one placeholder that starts outside a
        # quoted identifier and ends inside it.
        t = tokenize('VALUES (/*@encrypt:t.a*/ %("x")s)', PG, NAMED)
        assert hidden(t) == [True]
        assert t.placeholders[0].annotation is None and len(t.stray_annotations) == 1

    def test_space_after_comment_opener_is_not_an_annotation(self):
        t = tokenize("VALUES (/* @encrypt:users.ssn*/ %s)", PG, POS)
        assert t.placeholders[0].annotation is None
        assert t.stray_annotations == ()

    def test_annotation_inside_a_literal_is_text(self):
        t = tokenize("VALUES ('/*@encrypt:users.ssn*/', %s)", PG, POS)
        assert t.placeholders[0].annotation is None


# ---- the drivers themselves as the oracle ----
#
# These call the drivers' own placeholder scanning, so a driver upgrade that
# changes what it binds fails here rather than silently shifting which value
# the plugin encrypts.

_ORACLE_SQL = [
    "INSERT INTO t (a, b) VALUES (%s, %s)",
    "SELECT %s, '%s', \"%s\" -- %s\n, %s",
    "SELECT 7 %% 2, %s, '%%s', /* %s */ %s",
    "UPDATE t SET a = %s WHERE b LIKE 'x%%' AND c IN (%s, %s)",
    "SELECT $$ %s $$, E'\\'%s', %s",
    "SELECT %%s, %s%s",
]

_NAMED_ORACLE_SQL = [
    "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)",
    "UPDATE t SET a = %(v)s, b = %(v)s WHERE c = '%(w)s' -- %(x)s",
    "SELECT %(a)s, 5 %% 2, %(b)s",
]


@pytest.mark.parametrize("sql", _ORACLE_SQL + _NAMED_ORACLE_SQL)
def test_matches_psycopg(sql):
    queries = pytest.importorskip("psycopg._queries")
    named = "%(" in sql
    parts = queries._split_query(sql.encode())
    expected = [part.item for part in parts[:-1]]
    t = tokenize(sql, PG, NAMED if named else POS)
    assert keys(t) == expected
    assert t.driver_error is None  # psycopg accepted it


@pytest.mark.parametrize("sql", _ORACLE_SQL)
def test_matches_mysql_connector_positional(sql):
    cursor = pytest.importorskip("mysql.connector.cursor")
    expected = list(range(len(cursor.RE_PY_PARAM.findall(sql.encode()))))
    assert keys(tokenize(sql, MYSQL, POS)) == expected


@pytest.mark.parametrize("sql", _NAMED_ORACLE_SQL)
def test_matches_mysql_connector_named(sql):
    cursor = pytest.importorskip("mysql.connector.cursor")
    expected = [m.group("mapping_key").decode() for m in cursor.RE_PY_MAPPING_PARAM.finditer(sql.encode())
                if m.group("conversion_type") == b"s"]
    assert keys(tokenize(sql, MYSQL, NAMED)) == expected
