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

"""Finds a statement's bind parameters the way its driver does.

Both supported drivers use the ``pyformat`` paramstyle, but they do not find
placeholders the same way, and neither skips quoted literals or comments:

* psycopg, when given parameters (even an empty sequence), treats every ``%``
  as the start of a placeholder: ``%s``/``%b``/``%t``, ``%(name)s`` (or ``b``
  / ``t``), or ``%%`` for a literal percent sign. Anything else after a ``%``
  is a ``ProgrammingError``, and positional and named placeholders cannot be
  mixed. Given ``None`` it substitutes nothing.
* MySQL Connector/Python, given a non-empty list or tuple, replaces every
  ``%s`` substring and nothing else (``%%`` is left alone). Given a non-empty
  dict, it replaces ``%(name)s`` and turns ``%(name)%`` into ``%``. Given an
  empty value it substitutes nothing, and given any other type it raises
  ``ProgrammingError``.

So the n-th placeholder found here is the n-th value the driver binds, even
when it sits inside a quoted literal or a comment. Such a placeholder is
reported as hidden: the statement's structure says nothing about which column
it fills, and the statement is almost certainly broken anyway.

The SQL is also lexed (quotes, comments, statement separators) for what the
driver doesn't need: spotting hidden placeholders, reading
``/*@encrypt:table.column*/`` annotations, and producing a copy of the SQL a
structural parser can read, with each placeholder replaced by a named
placeholder that carries its position.

The lexer assumes server defaults: ``standard_conforming_strings=on`` on
PostgreSQL, and backslash escapes on and ``ANSI_QUOTES`` off on MySQL. Under
other settings a literal may end somewhere else on the server. MySQL runs the
body of an executable comment (``/*! ... */``), so a statement with one is
flagged (:attr:`TokenizedSql.executable_comment`) rather than read as if the
comment were not there. The database-side HMAC trigger, not this module,
guarantees an encrypted column never holds plaintext.

The placeholder rules are the drivers' (psycopg, MySQL Connector/Python), not
the engines': another driver for the same engine needs its own.

This module has no third-party dependencies.
"""

from __future__ import annotations

import re
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, FrozenSet, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.encryption.sql_dialect import SqlDialect

#: Prefix of the named placeholders written into :attr:`TokenizedSql.normalized`:
#: ``:_kms_p3`` stands for the placeholder whose ordinal is 3.
NORMALIZED_PLACEHOLDER_PREFIX = "_kms_p"


class ParamStyle(Enum):
    """How a statement's parameters were passed, which decides what its driver
    treats as a placeholder."""

    #: No parameters, or (MySQL) a type the driver refuses: nothing is
    #: substituted.
    NONE = "none"
    #: A sequence: ``%s`` placeholders, bound by position.
    POSITIONAL = "positional"
    #: A mapping: ``%(name)s`` placeholders, bound by name.
    NAMED = "named"

    @classmethod
    def of(cls, params: Any, dialect: SqlDialect) -> ParamStyle:
        """The style the driver for ``dialect`` applies to ``params``, the
        second argument of ``execute``."""
        if dialect.name == "mysql":
            # Connector/Python substitutes only for a non-empty dict, list or
            # tuple.
            if not params:
                return cls.NONE
            if isinstance(params, dict):
                return cls.NAMED
            return cls.POSITIONAL if isinstance(params, (list, tuple)) else cls.NONE
        if params is None:
            return cls.NONE
        return cls.NAMED if isinstance(params, Mapping) else cls.POSITIONAL


@dataclass(frozen=True, slots=True)
class ColumnRef:
    """A column a statement names, with names folded the way the engine folds
    them (see :meth:`SqlDialect.fold_identifier`)."""

    column: str
    #: The table, without any schema; ``None`` when the statement does not say
    #: which of its tables the column belongs to.
    table: Optional[str] = None

    def __str__(self) -> str:
        return self.column if self.table is None else f"{self.table}.{self.column}"


@dataclass(frozen=True, slots=True)
class Placeholder:
    """One bind parameter, in the order the driver binds them."""

    #: Position among the statement's placeholders, from 0.
    ordinal: int
    #: What the driver binds here: the index into the parameter sequence, or
    #: the key in the parameter mapping. A key may be used more than once.
    key: Union[int, str]
    #: Span of the placeholder in the original SQL.
    start: int
    end: int
    #: Overlaps a quoted literal (including a dollar-quoted one), a quoted
    #: identifier, or a comment.
    hidden: bool = False
    #: The column a ``/*@encrypt:...*/`` annotation directly before it names,
    #: folded like any identifier. Its qualifier is not resolved yet: see
    #: :attr:`StatementAnalysis.annotations` for what the analyzer makes of it.
    annotation: Optional[ColumnRef] = None
    #: A MySQL charset introducer written directly before it (``_binary`` in
    #: ``_binary %s``, as Django renders binary values), lower-cased. Anything
    #: in between, an annotation included, hides the introducer.
    introducer: Optional[str] = None


@dataclass(frozen=True)
class TokenizedSql:
    """What :func:`tokenize` found."""

    style: ParamStyle
    placeholders: Tuple[Placeholder, ...]
    #: The SQL with every placeholder replaced by ``:_kms_p<ordinal>``, the
    #: driver's ``%`` escapes resolved, and on MySQL ``REPLACE`` read as
    #: ``INSERT``, statement modifiers such as ``LOW_PRIORITY`` dropped, and
    #: charset introducers before a placeholder removed (see
    #: :attr:`Placeholder.introducer`). Only for a structural parser; never
    #: sent to the database.
    normalized: str
    #: The first word of each statement, upper-cased.
    leading_words: Tuple[str, ...]
    #: Which of the keywords that matter for spotting a write (``INSERT``,
    #: ``UPDATE``, ``REPLACE``, ``MERGE``, ``UPSERT``, ``COPY``, ``FROM``,
    #: ``EXECUTE``, ``LOAD``) appear outside literals, comments and
    #: placeholders, or inside a MySQL executable comment.
    keywords: FrozenSet[str]
    #: A quoted literal, quoted identifier, or comment runs to the end of the
    #: text.
    unterminated: bool = False
    #: MySQL only: a ``/*! ... */`` (or MariaDB ``/*M! ... */``) comment, whose
    #: body the server runs.
    executable_comment: bool = False
    #: Why the driver will reject the placeholders (an unknown ``%`` sequence,
    #: positional and named placeholders mixed, or a style the parameters
    #: don't have); ``None`` when it won't.
    driver_error: Optional[str] = None
    #: ``/*@encrypt:...*/`` comments that annotate no placeholder or can't be
    #: read, as written. A caller may want to warn about them.
    stray_annotations: Tuple[str, ...] = ()

    @property
    def has_hidden_placeholders(self) -> bool:
        return any(p.hidden for p in self.placeholders)


_KEYWORDS = frozenset({"INSERT", "UPDATE", "REPLACE", "MERGE", "UPSERT", "COPY", "FROM", "EXECUTE", "LOAD"})

# MySQL statement modifiers between the keyword and the table. They change how
# a statement runs and leave what it writes alone. The structural parser reads
# "UPDATE LOW_PRIORITY users" as a table named LOW_PRIORITY aliased users, so
# they are dropped from the normalized copy. All are reserved words except
# QUICK, which can only follow DELETE.
_MYSQL_MODIFIERS = {
    "INSERT": frozenset({"LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED", "IGNORE"}),
    "REPLACE": frozenset({"LOW_PRIORITY", "DELAYED"}),
    "UPDATE": frozenset({"LOW_PRIORITY", "IGNORE"}),
    "DELETE": frozenset({"LOW_PRIORITY", "QUICK", "IGNORE"}),
}

# ---- lexers ----
#
# Alternatives are tried in order at each token boundary. The ``*_end`` groups
# capture a closing quote, empty when the literal runs to the end of the text.

_PG_TOKEN = re.compile(r"""
      (?P<line_comment>--[^\n]*)
    | (?P<block_comment>/\*)
    | (?P<estring>[Ee]'(?:[^'\\]|\\.|'')*(?P<estring_end>'?))
    | (?P<string>'(?:[^']|'')*(?P<string_end>'?))
    | (?P<ident>"(?:[^"]|"")*(?P<ident_end>"?))
    | (?P<param>\$\d+)
    | (?P<dollar>\$(?:[^\W\d]\w*)?\$)
    | (?P<word>[^\W\d][\w$]*)
    | (?P<number>\d[\w.]*)
    | (?P<semicolon>;)
    | (?P<space>\s+)
    | (?P<other>[^\w\s'"$;/\-]+|.)
""", re.VERBOSE | re.DOTALL)

_MYSQL_TOKEN = re.compile(r"""
      (?P<line_comment>(?:\#|--(?=\s|\Z))[^\n]*)
    | (?P<block_comment>/\*.*?(?P<block_comment_end>\*/|\Z))
    | (?P<string>'(?:[^'\\]|\\.|'')*(?P<string_end>'?))
    | (?P<dqstring>"(?:[^"\\]|\\.|"")*(?P<dqstring_end>"?))
    | (?P<ident>`(?:[^`]|``)*(?P<ident_end>`?))
    | (?P<word>[^\W\d][\w$]*)
    | (?P<number>\d[\w.]*)
    | (?P<semicolon>;)
    | (?P<space>\s+)
    | (?P<other>[^\w\s'"`;/\-\#]+|.)
""", re.VERBOSE | re.DOTALL)

_QUOTED_KINDS = frozenset({"estring", "string", "dqstring", "ident"})

# A block comment that is an annotation: /*@encrypt:<ref>*/, with no space
# after the opening delimiter. <ref> is dot-separated parts, table.column or a
# bare column; leading parts (a schema) are ignored. A part is word characters,
# or a name quoted with the engine's identifier quote. Every [\w.]+ reference
# that ends in a column is accepted, empty parts included: ".c" is the bare
# column c and "t..c" is t.c. A reference that ends in "." names no column and
# is reported as stray.
_ANNOTATION = re.compile(r"/\*@encrypt:(?P<ref>.*?)\*/\Z", re.DOTALL)
_PG_REF_PART = re.compile(r'(?P<word>\w+)|"(?P<quoted>(?:[^"]|"")+)"')
_MYSQL_REF_PART = re.compile(r"(?P<word>\w+)|`(?P<quoted>(?:[^`]|``)+)`")

# A MySQL charset introducer (_binary, _utf8mb4, ...) and the space after it,
# directly before a placeholder.
_INTRODUCER = re.compile(r"(?<![\w$])_(?P<charset>[A-Za-z0-9]+)\s*\Z")
_MYSQL_CHARSETS = frozenset({
    "armscii8", "ascii", "big5", "binary", "cp1250", "cp1251", "cp1256", "cp1257", "cp850", "cp852", "cp866",
    "cp932", "dec8", "eucjpms", "euckr", "gb18030", "gb2312", "gbk", "geostd8", "greek", "hebrew", "hp8",
    "keybcs2", "koi8r", "koi8u", "latin1", "latin2", "latin5", "latin7", "macce", "macroman", "sjis", "swe7",
    "tis620", "ucs2", "ujis", "utf16", "utf16le", "utf32", "utf8", "utf8mb3", "utf8mb4",
})
_WORD = re.compile(r"[^\W\d][\w$]*")
_COMMENT_DELIMITER = re.compile(r"/\*|\*/")

# The drivers' own placeholder patterns.
_PSYCOPG_PLACEHOLDER = re.compile(r"%(?:\(([^)]+)\).|.)")  # no DOTALL, as in psycopg
_MYSQL_POSITIONAL = re.compile(r"%s")
_MYSQL_NAMED = re.compile(r"%\((?P<key>[^)]+)\)(?P<conv>[diouxXeEfFgGcrs%])")

_Edit = Tuple[int, int, str]
_FAR = 1 << 62  # past any string offset


@dataclass
class _Lexed:
    hidden_spans: List[Tuple[int, int]]  # literals, quoted identifiers, comments; sorted
    block_comments: List[Tuple[int, int]]
    # (UPPER word, start, end); "" stands for any other significant token, so
    # that two words with something between them are not taken as adjacent.
    words: List[Tuple[str, int, int]]
    separators: List[int]                # offsets of the ";" between statements
    unterminated: bool = False
    executable_comment: bool = False
    # Words inside MySQL executable comments, which the server runs.
    executable_words: List[str] = field(default_factory=list)


def tokenize(sql: str, dialect: SqlDialect, style: ParamStyle) -> TokenizedSql:
    """Find the placeholders in ``sql`` as the driver for ``dialect`` does when
    given parameters of ``style``.

    :raises TypeError: if ``sql`` is not a ``str``.
    """
    if not isinstance(sql, str):
        raise TypeError(f"sql must be a str, not {type(sql).__name__}")
    mysql = dialect.name == "mysql"
    lexed = _lex(sql, _MYSQL_TOKEN if mysql else _PG_TOKEN, mysql=mysql)

    edits: List[_Edit] = []
    if mysql:
        placeholders, driver_error = _mysql_placeholders(sql, style, lexed, edits)
        placeholders = _attach_introducers(sql, lexed, placeholders, edits)
    else:
        placeholders, driver_error = _psycopg_placeholders(sql, style, lexed, edits)

    stray: List[str] = []
    if "@encrypt:" in sql:
        placeholders = _attach_annotations(sql, dialect, lexed, placeholders, stray)

    # A word inside a placeholder (the s of %s, a %(name)s key) is not SQL.
    words = _outside(lexed.words, placeholders)
    bounds = _statement_bounds(words, lexed.separators)
    if mysql:
        edits.extend(_mysql_rewrites(words, bounds))

    keywords = {w for w, _, _ in words if w in _KEYWORDS}
    keywords.update(w for w in lexed.executable_words if w in _KEYWORDS)
    return TokenizedSql(
        style=style,
        placeholders=tuple(placeholders),
        normalized=_apply(sql, edits),
        leading_words=_leading_words(words, bounds),
        keywords=frozenset(keywords),
        unterminated=lexed.unterminated,
        executable_comment=lexed.executable_comment,
        driver_error=driver_error,
        stray_annotations=tuple(stray),
    )


def normalized_ordinal(name: Optional[str]) -> Optional[int]:
    """The ordinal of a placeholder in the normalized SQL, from the name a
    parser reports for it (``_kms_p3`` -> 3); ``None`` for any other name."""
    if not name or not name.startswith(NORMALIZED_PLACEHOLDER_PREFIX):
        return None
    digits = name[len(NORMALIZED_PLACEHOLDER_PREFIX):]
    return int(digits) if digits.isascii() and digits.isdigit() else None


# ---- driver placeholders ----
#
# The error texts never quote the SQL: a rejected "%(...)x" sequence can sit
# inside a literal holding a value.

def _psycopg_placeholders(sql: str, style: ParamStyle, lexed: _Lexed,
                          edits: List[_Edit]) -> Tuple[List[Placeholder], Optional[str]]:
    if style is ParamStyle.NONE:
        return [], None
    found: List[Placeholder] = []
    error: Optional[str] = None
    named = positional = False
    for m in _PSYCOPG_PLACEHOLDER.finditer(sql):
        text = m.group(0)
        if text == "%%":
            edits.append((m.start(), m.end(), "%"))
            continue
        if text == "%(" or text[-1] not in "sbt":
            error = error or f"psycopg rejects the % sequence at offset {m.start()}"
            continue
        name = m.group(1)
        named, positional = named or name is not None, positional or name is None
        ordinal = len(found)
        found.append(_placeholder(ordinal, ordinal if name is None else name, m, lexed))
        edits.append(_placeholder_edit(ordinal, m))
    if named and positional:
        error = error or "positional and named placeholders are mixed"
    elif (named or positional) and named != (style is ParamStyle.NAMED):
        error = error or f"the placeholders don't match {style.value} parameters"
    return found, error


def _mysql_placeholders(sql: str, style: ParamStyle, lexed: _Lexed,
                        edits: List[_Edit]) -> Tuple[List[Placeholder], Optional[str]]:
    found: List[Placeholder] = []
    error: Optional[str] = None
    if style is ParamStyle.POSITIONAL:
        for m in _MYSQL_POSITIONAL.finditer(sql):
            ordinal = len(found)
            found.append(_placeholder(ordinal, ordinal, m, lexed))
            edits.append(_placeholder_edit(ordinal, m))
    elif style is ParamStyle.NAMED:
        for m in _MYSQL_NAMED.finditer(sql):
            conv = m.group("conv")
            if conv == "%":
                edits.append((m.start(), m.end(), "%"))
            elif conv != "s":
                error = error or f"MySQL Connector/Python rejects the %(...) conversion at offset {m.start()}"
            else:
                ordinal = len(found)
                found.append(_placeholder(ordinal, m.group("key"), m, lexed))
                edits.append(_placeholder_edit(ordinal, m))
    return found, error


def _placeholder(ordinal: int, key: Union[int, str], m: re.Match, lexed: _Lexed) -> Placeholder:
    return Placeholder(ordinal=ordinal, key=key, start=m.start(), end=m.end(),
                       hidden=_overlaps(lexed.hidden_spans, m.start(), m.end()))


def _placeholder_edit(ordinal: int, m: re.Match) -> _Edit:
    # Padded so the placeholder cannot fuse with a neighboring token.
    return m.start(), m.end(), f" :{NORMALIZED_PLACEHOLDER_PREFIX}{ordinal} "


def _overlaps(spans: List[Tuple[int, int]], start: int, end: int) -> bool:
    i = bisect_right(spans, (start, _FAR)) - 1
    if i >= 0 and spans[i][1] > start:
        return True
    return i + 1 < len(spans) and spans[i + 1][0] < end


def _attach_introducers(sql: str, lexed: _Lexed, placeholders: List[Placeholder],
                        edits: List[_Edit]) -> List[Placeholder]:
    # "_binary %s" binds the value as a binary string, which is what a
    # ciphertext is. The parser misreads the pair (as an alias, or not at all),
    # so the introducer is recorded on the placeholder and removed from the
    # normalized copy. Only whitespace may separate the two.
    if "_" not in sql:
        return placeholders
    result = list(placeholders)
    for i, p in enumerate(placeholders):
        if p.hidden:
            continue
        m = _INTRODUCER.search(sql, max(0, p.start - 64), p.start)
        if m is None or m.group("charset").lower() not in _MYSQL_CHARSETS \
                or _overlaps(lexed.hidden_spans, m.start(), m.end()):
            continue
        result[i] = replace(p, introducer=f"_{m.group('charset').lower()}")
        edits.append((m.start(), m.end(), " "))
    return result


# ---- annotations ----

def _attach_annotations(sql: str, dialect: SqlDialect, lexed: _Lexed,
                        placeholders: List[Placeholder], stray: List[str]) -> List[Placeholder]:
    by_start = {p.start: i for i, p in enumerate(placeholders)}
    result = list(placeholders)
    for start, end in lexed.block_comments:
        if not sql.startswith("/*@encrypt:", start):
            continue
        m = _ANNOTATION.match(sql, start, end)
        ref = None if m is None else _annotation_ref(m.group("ref"), dialect)
        nxt = end
        while nxt < len(sql) and sql[nxt].isspace():
            nxt += 1
        index = by_start.get(nxt)
        if ref is None or index is None or result[index].hidden:
            stray.append(sql[start:end])
        else:
            result[index] = replace(result[index], annotation=ref)
    return result


def _annotation_ref(text: str, dialect: SqlDialect) -> Optional[ColumnRef]:
    mysql = dialect.name == "mysql"
    part_re, quote = (_MYSQL_REF_PART, "`") if mysql else (_PG_REF_PART, '"')
    parts: List[str] = []
    pos = 0
    while pos <= len(text):
        if text.startswith(".", pos) or pos == len(text):
            parts.append("")  # an empty part, as in ".c" or "t..c"
        else:
            m = part_re.match(text, pos)
            if m is None:
                return None
            if m.group("word") is not None:
                parts.append(dialect.fold_identifier(m.group("word"), quoted=False))
            else:
                parts.append(dialect.fold_identifier(m.group("quoted").replace(quote * 2, quote), quoted=True))
            pos = m.end()
        if pos < len(text) and text[pos] != ".":
            return None
        pos += 1
    if not parts[-1]:
        return None  # no column ("t.")
    # The metadata has no schema: the table is the nearest named part before
    # the column, and anything before that is ignored.
    table = next((part for part in reversed(parts[:-1]) if part), None)
    return ColumnRef(column=parts[-1], table=table)


# ---- lexing ----

def _lex(sql: str, token_re: re.Pattern, mysql: bool) -> _Lexed:
    lexed = _Lexed([], [], [], [])
    pos, n = 0, len(sql)
    while pos < n:
        m = token_re.match(sql, pos)
        assert m is not None  # "other" matches any character
        kind, end = m.lastgroup, m.end()
        if kind == "block_comment":
            if not mysql:
                end = _nested_comment_end(sql, pos)
            elif not m.group("block_comment_end"):
                lexed.unterminated = True
            if end < 0:
                end, lexed.unterminated = n, True
            if mysql and (sql.startswith("/*!", pos) or sql.startswith("/*M!", pos)):
                lexed.executable_comment = True
                lexed.executable_words.extend(w.upper() for w in _WORD.findall(sql, pos, end))
            lexed.block_comments.append((pos, end))
            lexed.hidden_spans.append((pos, end))
        elif kind == "dollar":
            close = sql.find(m.group(0), end)
            if close < 0:
                end, lexed.unterminated = n, True
            else:
                end = close + len(m.group(0))
            lexed.hidden_spans.append((pos, end))
        elif kind in _QUOTED_KINDS:
            if not m.group(f"{kind}_end"):
                lexed.unterminated = True
            lexed.hidden_spans.append((pos, end))
        elif kind == "line_comment":
            lexed.hidden_spans.append((pos, end))
        elif kind == "word":
            lexed.words.append((m.group(0).upper(), pos, end))
        elif kind == "semicolon":
            lexed.separators.append(pos)
        elif kind != "space":
            lexed.words.append(("", pos, end))
        pos = end
    return lexed


def _nested_comment_end(sql: str, start: int) -> int:
    # PostgreSQL block comments nest. Returns -1 when unterminated. One pass
    # over the delimiters, so deep nesting stays linear.
    depth = 0
    for m in _COMMENT_DELIMITER.finditer(sql, start):
        depth += 1 if m.group(0) == "/*" else -1
        if depth == 0:
            return m.end()
    return -1


def _outside(words: List[Tuple[str, int, int]], placeholders: List[Placeholder]) -> List[Tuple[str, int, int]]:
    if not placeholders:
        return words
    spans = [(p.start, p.end) for p in placeholders]
    return [w for w in words if not _overlaps(spans, w[1], w[2])]


def _statement_bounds(words: List[Tuple[str, int, int]], separators: List[int]) -> List[Tuple[int, int]]:
    """(begin, end) indexes into ``words`` of each non-empty statement."""
    bounds: List[Tuple[int, int]] = []
    begin = 0
    for separator in separators + [_FAR]:
        end = begin
        while end < len(words) and words[end][1] < separator:
            end += 1
        if end > begin:
            bounds.append((begin, end))
        begin = end
    return bounds


def _leading_words(words: List[Tuple[str, int, int]], bounds: List[Tuple[int, int]]) -> Tuple[str, ...]:
    # A statement's first word, skipping leading punctuation such as "("; ""
    # when it has no word at all.
    return tuple(next((w for w, _, _ in words[begin:end] if w), "") for begin, end in bounds)


def _mysql_rewrites(words: List[Tuple[str, int, int]], bounds: List[Tuple[int, int]]) -> List[_Edit]:
    leading = {begin for begin, _ in bounds}
    edits: List[_Edit] = []
    for i, (word, start, end) in enumerate(words):
        modifiers = _MYSQL_MODIFIERS.get(word)
        if modifiers is None:
            continue
        if word == "REPLACE":
            # Only the statement keyword, not the REPLACE() string function.
            if i not in leading:
                continue
            edits.append((start, end, "INSERT"))
        j = i + 1
        while j < len(words) and words[j][0] in modifiers:
            edits.append((words[j][1], words[j][2], " "))
            j += 1
    return edits


def _apply(sql: str, edits: List[_Edit]) -> str:
    if not edits:
        return sql
    out: List[str] = []
    pos = 0
    for start, end, text in sorted(edits):
        out.append(sql[pos:start])
        out.append(text)
        pos = end
    out.append(sql[pos:])
    return "".join(out)
