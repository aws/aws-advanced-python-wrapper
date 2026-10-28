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

"""Works out which bind parameter of a statement fills or is compared with
which column, so values bound for an encrypted column can be encrypted.

:func:`analyze` finds the placeholders the way the driver does
(:mod:`.sql_tokenizer`), parses the statement with ``sqlglot``, and pairs each
placeholder with a column by its position, which the normalized SQL carries in
the placeholder's name. It knows nothing about encryption metadata: the
plugin looks the reported columns up.

The analysis is best effort, and reports where it fell short:
:attr:`StatementAnalysis.gap` is ``None`` only when every column the statement
itself writes (not one written by a routine, ``CALL`` or ``DO`` it runs) was
established, either as filled by a placeholder
(:attr:`~StatementAnalysis.assignments`) or by something else
(:attr:`~StatementAnalysis.unbound_writes`). A statement that could not be
parsed is never mistaken for one that writes nothing.
"""

from __future__ import annotations

import logging
import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List,
                    Optional, Sequence, Set, Tuple)

try:
    import sqlglot
    from sqlglot import exp
except ModuleNotFoundError as e:  # pragma: no cover - exercised by the import-guard test
    # Only a missing package gets the install hint; a broken install raises
    # its own error.
    if (e.name or "").partition(".")[0] != "sqlglot":
        raise
    raise ImportError(
        "The KMS encryption plugin needs the 'sqlglot' package. "
        "Install it with: pip install 'aws-advanced-python-wrapper[encryption]'") from e

from aws_advanced_python_wrapper.encryption.sql_tokenizer import (
    ColumnRef, ParamStyle, Placeholder, TokenizedSql, normalized_ordinal,
    tokenize)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.encryption.sql_dialect import SqlDialect


class StatementKind(Enum):
    """What the (first) statement is."""

    SELECT = "SELECT"
    INSERT = "INSERT"  # also MySQL REPLACE
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"
    COPY_FROM = "COPY_FROM"
    COPY_TO = "COPY_TO"
    #: Recognized but not one the analysis reads (DDL, SET, CALL...).
    OTHER = "OTHER"
    #: Not established: the statement could not be parsed, was not parsed
    #: (see :attr:`StatementAnalysis.gap`), or its parse was not trusted.
    UNKNOWN = "UNKNOWN"


class AnalysisGap(Enum):
    """Why a statement's parameters or written columns could not all be
    established. The values are fixed text, safe to log."""

    #: The SQL could not be parsed (or ran out inside a quote or comment).
    UNPARSEABLE = "the statement could not be parsed"
    #: A placeholder sits inside a quoted literal, quoted identifier or
    #: comment, where the driver still binds it. The tables the statement
    #: names are still listed, but no placeholder's column is inferred.
    HIDDEN_PLACEHOLDER = "a placeholder is inside a quoted literal, quoted identifier or comment"
    #: The driver will reject the statement's placeholders.
    DRIVER_ERROR = "the driver will reject the placeholders"
    #: The parsed statement does not have exactly the placeholders the driver
    #: binds, so the parse can't be trusted.
    PLACEHOLDER_MISMATCH = "the parsed statement does not match its placeholders"
    #: MySQL runs the body of a ``/*! ... */`` comment, which the analysis
    #: can't read as SQL.
    EXECUTABLE_COMMENT = "the statement has a MySQL executable comment"
    #: The statement may write but is not a form the analysis reads
    #: (``EXPLAIN ANALYZE INSERT``, ``EXECUTE``, ``LOAD DATA``, ...).
    UNSUPPORTED_STATEMENT = "the statement is not a form the analysis reads"
    #: A ``PREPARE``: its ``$n`` parameters are bound by a later ``EXECUTE``,
    #: which the analysis does not see.
    PREPARED_PARAMETERS = "the prepared statement's parameters arrive with a later EXECUTE"
    #: An ``INSERT`` (or ``MERGE ... INSERT``, ``COPY ... FROM``) with no column
    #: list, or ``DEFAULT VALUES``.
    NO_COLUMN_LIST = "the statement does not name the columns it writes"
    #: ``INSERT ... SELECT`` and similar: the rows come from a query, so which
    #: value fills which column is not read.
    VALUES_FROM_QUERY = "the values come from a query"
    #: A ``VALUES`` row with more or fewer values than columns.
    ROW_LENGTH_MISMATCH = "a VALUES row does not match the column list"
    #: An ``UPDATE`` of several tables assigns a column whose table can't be
    #: told.
    AMBIGUOUS_TABLE = "an assignment's table can't be established"


@dataclass(frozen=True, slots=True)
class TableRef:
    """A table a statement names."""

    name: str
    schema: Optional[str] = None

    def __str__(self) -> str:
        return self.name if self.schema is None else f"{self.schema}.{self.name}"


@dataclass(frozen=True, slots=True)
class ParameterUse:
    """A placeholder whose value fills, or is compared with, a column."""

    placeholder: Placeholder
    column: ColumnRef
    #: Types the value is cast to, outermost first (``%s::text::bytea`` ->
    #: ``("VARBINARY", "TEXT")``), as ``sqlglot`` names them.
    casts: Tuple[str, ...] = ()
    #: Compared with ``= ANY(...)``, ``ALL(...)`` or ``SOME(...)``: the value
    #: is a list.
    array: bool = False

    @property
    def ordinal(self) -> int:
        return self.placeholder.ordinal


@dataclass(frozen=True, slots=True)
class StatementAnalysis:
    """What :func:`analyze` established about one SQL string."""

    kind: StatementKind
    #: The string stores values: an INSERT, UPDATE, MERGE, COPY ... FROM, a
    #: data-modifying CTE, a PREPARE of one of those, or (when it could not
    #: be read) a statement whose keywords say it may write.
    writes: bool
    placeholders: Tuple[Placeholder, ...]
    #: Every table the string names, without duplicates and excluding CTE
    #: names (outer queries first, not in text order).
    tables: Tuple[TableRef, ...] = ()
    #: The tables it writes.
    written_tables: Tuple[TableRef, ...] = ()
    #: Placeholders written into a column.
    assignments: Tuple[ParameterUse, ...] = ()
    #: Placeholders compared with a column (``WHERE``, ``ON``, ``HAVING``...).
    comparisons: Tuple[ParameterUse, ...] = ()
    #: Placeholders annotated with ``/*@encrypt:...*/``. These take precedence
    #: over the rest. Once the statement is parsed and its placeholders
    #: matched, a qualifier is resolved through the aliases in scope at the
    #: placeholder, and a bare column gets a table only when the statement
    #: says which: the table the value is written to, or else the only table
    #: the query around it reads. Otherwise the annotation is kept as
    #: written, only folded.
    annotations: Tuple[ParameterUse, ...] = ()
    #: Columns written with something other than a placeholder (a literal, an
    #: expression, ``DEFAULT``, another column, a ``COPY`` stream). ``NULL``
    #: and copying a conflicting row's own value for the same column
    #: (``EXCLUDED.c`` / ``VALUES(c)`` / a row alias) are not listed.
    unbound_writes: Tuple[ColumnRef, ...] = ()
    #: The source column of each result column, by position, when the result
    #: is a plain select list or ``RETURNING`` list: an entry is ``None`` for an
    #: expression, a column of a subquery or CTE, or one whose table can't be
    #: told. ``None`` altogether when unknown (``*``, set operations,
    #: ``VALUES``, ``PREPARE``, several statements, no result).
    result_columns: Optional[Tuple[Optional[ColumnRef], ...]] = None
    #: Why the analysis is incomplete: for a statement that writes, why its
    #: written columns could not all be established; for any statement, a
    #: placeholder or parse problem that makes its parameters untrustworthy.
    #: ``None`` when complete.
    gap: Optional[AnalysisGap] = None
    #: ``/*@encrypt:...*/`` comments that annotate nothing or can't be read.
    stray_annotations: Tuple[str, ...] = ()
    #: Diagnostic detail for :attr:`gap`, safe to log: it never quotes the SQL.
    detail: Optional[str] = field(default=None, compare=False)

    @property
    def complete(self) -> bool:
        return self.gap is None

    def parameter_columns(self) -> Dict[int, ColumnRef]:
        """The column each placeholder belongs to, by ordinal: comparisons,
        then assignments, then annotations, each overriding the last."""
        result: Dict[int, ColumnRef] = {}
        for use in (*self.comparisons, *self.assignments, *self.annotations):
            result[use.ordinal] = use.column
        return result


#: Only short statements with few ``%`` signs (an upper bound on their
#: placeholders) are cached, as psycopg does with its own query cache. Bulk
#: INSERTs differ in their row count and rarely repeat, so caching them would
#: only make the cache large.
CACHEABLE_SQL_LENGTH = 4096
CACHEABLE_PERCENT_SIGNS = 64


def analyze(sql: str, dialect: SqlDialect, params: Any = None) -> StatementAnalysis:
    """Analyze ``sql`` as the driver for ``dialect`` runs it with ``params``
    (the second argument of ``execute``; for ``executemany``, one of its
    parameter sets). Only the kind of ``params`` matters (none, a sequence, a
    mapping, and on MySQL whether it is empty), not its values. The result is
    immutable and may be shared.

    :raises TypeError: if ``sql`` is not a ``str``.
    """
    style = ParamStyle.of(params, dialect)
    if isinstance(sql, str) and len(sql) <= CACHEABLE_SQL_LENGTH and sql.count("%") <= CACHEABLE_PERCENT_SIGNS:
        try:
            return _cached_analysis(sql, dialect, style)
        except _Uncacheable as e:
            return e.analysis
    return _analysis(sql, dialect, style)


class _Uncacheable(Exception):
    """Carries an analysis that must not be cached (lru_cache does not cache
    exceptions)."""

    def __init__(self, analysis: StatementAnalysis):
        super().__init__()
        self.analysis = analysis


@lru_cache(maxsize=1024)
def _cached_analysis(sql: str, dialect: SqlDialect, style: ParamStyle) -> StatementAnalysis:
    result = _analysis(sql, dialect, style)
    if result.detail == RecursionError.__name__:
        # Depends on how deep the calling thread's stack already is; another
        # caller may parse the same SQL.
        raise _Uncacheable(result)
    return result


# Leading words of the statements handed to the parser. Anything else is
# reported without parsing it: sqlglot falls back to an opaque "command" for
# what it doesn't support, and logs it.
_PARSED = {
    "postgresql": frozenset({"SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "COPY", "VALUES", "PREPARE"}),
    "mysql": frozenset({"SELECT", "WITH", "INSERT", "REPLACE", "UPDATE", "DELETE", "VALUES"}),
}
_SQLGLOT_DIALECT = {"postgresql": "postgres", "mysql": "mysql"}
_WRITE_LEADING = frozenset({"INSERT", "UPDATE", "REPLACE", "MERGE", "UPSERT", "LOAD", "EXECUTE"})
_WRITE_WORDS = frozenset({"INSERT", "UPDATE", "REPLACE", "MERGE", "UPSERT"})
_KINDS = {"SELECT": StatementKind.SELECT, "WITH": StatementKind.SELECT, "VALUES": StatementKind.SELECT,
          "TABLE": StatementKind.SELECT, "INSERT": StatementKind.INSERT, "REPLACE": StatementKind.INSERT,
          "UPDATE": StatementKind.UPDATE, "DELETE": StatementKind.DELETE, "MERGE": StatementKind.MERGE}

# Whitespace and comments, one character or one whole comment per repetition,
# with a line comment running through its newline. If the pattern could split
# the same text two ways, a failed match would backtrack exponentially.
_GAP = r"(?:\s|--[^\n]*(?:\n|\Z)|/\*[^*]*\*+(?:[^/*][^*]*\*+)*/)*"

# PREPARE name [(type, ...)] AS <statement>, on the normalized SQL.
_PREPARE = re.compile(rf"""
    \A{_GAP}
    PREPARE{_GAP}(?:"(?:[^"]|"")+"|[^\W\d][\w$]*){_GAP}
    (?:\((?:[^()]|\([^()]*\))*\){_GAP})?
    AS\b
""", re.IGNORECASE | re.VERBOSE)

# MySQL: sqlglot rejects a qualified INSERT column list ("INSERT INTO t (t.a)");
# a qualifier there can only name the target (the server rejects any other).
_MYSQL_NAME = r"(?:`(?:[^`]|``)+`|[\w$]+)"
_MYSQL_INSERT_COLUMNS = re.compile(rf"""
    \A(?P<head>{_GAP}INSERT{_GAP}(?:INTO{_GAP})?{_MYSQL_NAME}(?:\s*\.\s*{_MYSQL_NAME})*\s*\()
    (?P<columns>[^()]*)\)
""", re.IGNORECASE | re.VERBOSE)
# One name of the column list, and the dot after it when it is a qualifier.
# A back-quoted name is one token, dots and all.
_MYSQL_COLUMN_PART = re.compile(rf"(?P<name>{_MYSQL_NAME})(?P<dot>\s*\.\s*)?")


class _QuietWhileAnalyzing(logging.Filter):
    """Drops sqlglot's records while this module parses on the current thread.
    sqlglot logs the statement text (literals included) when it falls back to
    an opaque command or reports an unsupported construct; the application's
    own sqlglot logging is untouched."""

    def __init__(self) -> None:
        super().__init__()
        self._local = threading.local()

    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(self._local, "active", False)

    @contextmanager
    def active(self) -> Iterator[None]:
        previous = getattr(self._local, "active", False)
        self._local.active = True
        try:
            yield
        finally:
            self._local.active = previous


_QUIET = _QuietWhileAnalyzing()
logging.getLogger("sqlglot").addFilter(_QUIET)


def _analysis(sql: str, dialect: SqlDialect, style: ParamStyle) -> StatementAnalysis:
    tokens = tokenize(sql, dialect, style)
    annotations = tuple(ParameterUse(p, p.annotation) for p in tokens.placeholders if p.annotation is not None)

    def result(**kwargs: Any) -> StatementAnalysis:
        kwargs.setdefault("annotations", annotations)
        return StatementAnalysis(placeholders=tokens.placeholders, stray_annotations=tokens.stray_annotations,
                                 **kwargs)

    early: Optional[AnalysisGap] = None
    if tokens.unterminated:
        early = AnalysisGap.UNPARSEABLE
    elif tokens.driver_error:
        early = AnalysisGap.DRIVER_ERROR
    elif tokens.has_hidden_placeholders:
        early = AnalysisGap.HIDDEN_PLACEHOLDER
    elif tokens.executable_comment:
        early = AnalysisGap.EXECUTABLE_COMMENT
    if early is not None:
        # The parser reads a hidden placeholder as literal or comment text, so
        # the tables are still known, and a caller can refuse the statement
        # when one of them has encrypted columns.
        tables = _named_tables(tokens, dialect) if early is AnalysisGap.HIDDEN_PLACEHOLDER else ()
        return result(kind=StatementKind.UNKNOWN, writes=_may_write(tokens), gap=early, detail=tokens.driver_error,
                      tables=tables)

    if not tokens.leading_words:
        return result(kind=StatementKind.OTHER, writes=False)
    if any(w not in _PARSED[dialect.name] for w in tokens.leading_words):
        writes = _may_write(tokens)
        kind = _KINDS.get(tokens.leading_words[0], StatementKind.OTHER) if len(tokens.leading_words) == 1 \
            else StatementKind.OTHER
        return result(kind=kind, writes=writes, gap=AnalysisGap.UNSUPPORTED_STATEMENT if writes else None)

    parsed = _parse_statement(tokens, dialect)
    if parsed is None:
        writes = _may_write(tokens)
        return result(kind=StatementKind.OTHER, writes=writes,
                      gap=AnalysisGap.UNSUPPORTED_STATEMENT if writes else None)
    trees, failure = parsed
    if trees is None:
        return result(kind=StatementKind.UNKNOWN, writes=_may_write(tokens), gap=AnalysisGap.UNPARSEABLE,
                      detail=failure)

    mismatch = _placeholder_mismatch(trees, tokens)
    if mismatch is not None or not trees or any(isinstance(t, exp.Command) for t in trees):
        return result(kind=StatementKind.UNKNOWN, writes=_may_write(tokens),
                      gap=AnalysisGap.PLACEHOLDER_MISMATCH if mismatch else AnalysisGap.UNPARSEABLE, detail=mismatch)

    walker = _Walker(dialect, tokens, prepared=tokens.leading_words == ("PREPARE",))
    for tree in trees:
        walker.statement(tree)
    return walker.result(trees, annotations, result)


def _parse(text: str, dialect: SqlDialect) -> Tuple[Optional[List[exp.Expression]], Optional[str]]:
    try:
        with _QUIET.active():
            parsed = sqlglot.parse(text, read=_SQLGLOT_DIALECT[dialect.name])
    except Exception as e:  # sqlglot raises ParseError/TokenError, but must never break a statement
        # sqlglot's tokenizer re-raises a RecursionError as a TokenError.
        cause: Optional[BaseException] = e
        seen: Set[int] = set()
        while cause is not None and id(cause) not in seen:
            if isinstance(cause, RecursionError):
                return None, RecursionError.__name__
            seen.add(id(cause))
            cause = cause.__cause__ or cause.__context__
        return None, type(e).__name__
    return [t for t in parsed if isinstance(t, exp.Expression)], None


def _parse_statement(tokens: TokenizedSql, dialect: SqlDialect) -> Optional[Tuple[Optional[List[exp.Expression]], Optional[str]]]:
    """Parses the normalized SQL the way the analysis reads it. A PostgreSQL
    ``PREPARE name AS`` prefix is left out, and a MySQL INSERT or REPLACE
    that doesn't parse is tried again without its column qualifiers. ``None``
    when a PREPARE has no such prefix."""
    text = tokens.normalized
    if tokens.leading_words == ("PREPARE",):
        m = _PREPARE.match(text)
        if m is None:
            return None
        text = text[m.end():]

    trees, failure = _parse(text, dialect)
    if trees is None and tokens.leading_words in (("INSERT",), ("REPLACE",)) and dialect.name == "mysql":
        unqualified = _without_insert_column_qualifiers(text)
        if unqualified is not None:
            trees, failure = _parse(unqualified, dialect)
    return trees, failure


def _named_tables(tokens: TokenizedSql, dialect: SqlDialect) -> Tuple[TableRef, ...]:
    """The tables the statement names, read from the text the analysis
    parses; none when it doesn't parse."""
    parsed = _parse_statement(tokens, dialect)
    trees = parsed[0] if parsed is not None else None
    if not trees:
        return ()
    walker = _Walker(dialect, tokens)
    for tree in trees:
        walker.collect_tables(tree)
    return walker.tables


def _without_insert_column_qualifiers(text: str) -> Optional[str]:
    m = _MYSQL_INSERT_COLUMNS.match(text)
    if m is None or "." not in m.group("columns"):
        return None
    columns = _MYSQL_COLUMN_PART.sub(lambda part: "" if part.group("dot") else part.group(0), m.group("columns"))
    return f"{m.group('head')}{columns}){text[m.end():]}"


def _may_write(tokens: TokenizedSql) -> bool:
    # Keywords only: what a statement that could not be read may be doing.
    words = tokens.keywords
    for leading in tokens.leading_words:
        if leading in _WRITE_LEADING or (leading == "COPY" and "FROM" in words):
            return True
        if leading in ("WITH", "EXPLAIN", "PREPARE") and words & _WRITE_WORDS:
            return True
        if leading == "EXPLAIN" and "EXECUTE" in words:
            return True  # EXPLAIN ANALYZE EXECUTE runs the prepared statement
    # A MySQL executable comment can turn any statement into a write.
    return tokens.executable_comment and bool(words & (_WRITE_WORDS | _WRITE_LEADING))


def _placeholder_mismatch(trees: List[exp.Expression], tokens: TokenizedSql) -> Optional[str]:
    seen: Set[int] = set()
    for tree in trees:
        for node in tree.find_all(exp.Placeholder):
            ordinal = normalized_ordinal(node.name)
            if ordinal is None:
                continue  # a "?" or ":name" the driver does not bind
            if ordinal in seen or ordinal >= len(tokens.placeholders):
                return "a placeholder appears more than once"
            seen.add(ordinal)
    if len(seen) != len(tokens.placeholders):
        return f"{len(tokens.placeholders) - len(seen)} placeholder(s) missing from the parse"
    return None


# ---- the walk ----

def _by_ordinal(use: ParameterUse) -> int:
    return use.ordinal


#: What an alias or table name in scope names: a table, or ``None`` for a
#: subquery, CTE or function.
_Source = Optional[TableRef]
#: Maps an assignment's qualifier (if any) to the table it writes.
_TableOf = Callable[[Optional[str]], Optional[str]]
#: For a same-row copy in an upsert: qualifier key -> {source column key ->
#: the insert column key it carries}; ``None`` maps a column to itself.
_SameRow = Dict[str, Optional[Dict[str, str]]]

_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ, exp.NullSafeNEQ,
                exp.Like, exp.ILike)
_QUERIES = (exp.Select, exp.Update, exp.Delete, exp.Merge, exp.Insert)
_ARRAY_FUNCTIONS = frozenset({"ALL", "SOME", "ANY"})
_WRITES = (exp.Insert, exp.Update, exp.Merge)
# Operators that test a value rather than produce one. Most of them are
# Predicates only from sqlglot 30.16, and MATCH ... AGAINST is not one in any
# version, so they are listed by name as well.
_TESTS = (exp.Predicate, exp.RegexpLike, exp.RegexpILike, exp.RegexpFullMatch, exp.ArrayOverlaps, exp.ArrayContains,
          exp.ArrayContainsAll, exp.ArrayContainedBy, exp.JSONBContains, exp.JSONBContainsAnyTopKeys,
          exp.JSONBContainsAllTopKeys, exp.Overlaps, exp.Adjacent, exp.ExtendsLeft, exp.ExtendsRight,
          exp.MatchAgainst)
# Slots of a query whose values are not written: conditions, grouping
# (including a window's PARTITION BY), sorting, limits and RETURNING. A value
# bound there feeds no column of the write above.
_NOT_WRITTEN_ARGS = frozenset({"where", "having", "on", "condition", "group", "partition_by", "order", "limit",
                               "offset", "qualify", "returning"})
# Slots of a query that hold its sources (CTEs, FROM, JOIN, LATERAL, USING). A
# value bound in a source may feed the write, or may only be compared with
# another table's column, so the table it is written to can't be told.
_SOURCE_ARGS = frozenset({"with_", "from_", "joins", "laterals", "using"})
# The Select args a "SELECT p0, p1 FROM (VALUES ...) AS v(p0, p1)" may set and
# still be read as plain rows (ORDER BY changes no row's values).
_PLAIN_SELECT_ARGS = frozenset({"expressions", "from_", "order"})

# How many derived tables or CTEs a result column is followed through.
_MAX_DEPTH = 8


class _Walker:
    """Collects what one or more parsed statements write, compare and read."""

    def __init__(self, dialect: SqlDialect, tokens: TokenizedSql, prepared: bool = False):
        self._dialect = dialect
        self._tokens = tokens
        # The body of a PREPARE: its $n parameters are bound later.
        self._prepared = prepared
        self._kind: Optional[StatementKind] = None
        self._writes = False
        self._tables: Dict[TableRef, None] = {}
        self._written: Dict[TableRef, None] = {}
        self._assignments: List[ParameterUse] = []
        self._comparisons: List[ParameterUse] = []
        self._unbound: List[ColumnRef] = []
        self._gap: Optional[AnalysisGap] = None
        self._cte_names: Set[str] = set()
        # Ids of the SET and ON CONFLICT assignments (EQ nodes), which are
        # neither comparisons nor tests.
        self._assignment_nodes: Set[int] = set()
        self._scopes: Dict[int, Dict[str, _Source]] = {}
        # Nearest enclosing query of a node, memoized so a long AND/OR chain
        # is resolved in linear time.
        self._nearest: Dict[int, Optional[exp.Expression]] = {}
        self._placeholder_nodes: Dict[int, exp.Expression] = {}
        # Tables each write node (INSERT, UPDATE, MERGE) writes.
        self._written_by: Dict[int, List[TableRef]] = {}

    # ---- entry ----

    def statement(self, tree: exp.Expression) -> None:
        self.collect_tables(tree)
        for found in tree.find_all(exp.Placeholder):
            ordinal = normalized_ordinal(found.name)
            if ordinal is not None:
                self._placeholder_nodes[ordinal] = found

        kind = self._statement_kind(tree)
        if self._kind is None:
            self._kind = kind

        # Every write, wherever it sits: the statement itself or a
        # data-modifying CTE. MERGE's own WHEN ... INSERT/UPDATE are read by
        # the MERGE.
        for node in tree.walk():
            if isinstance(node, exp.Insert) and not isinstance(node.parent, exp.When):
                self._insert(node)
            elif isinstance(node, exp.Update) and not isinstance(node.parent, exp.When):
                self._update(node)
            elif isinstance(node, exp.Merge):
                self._merge(node)
            elif isinstance(node, exp.Copy):
                self._copy(node)
        self._collect_comparisons(tree)

    def collect_tables(self, tree: exp.Expression) -> None:
        """Adds the tables ``tree`` names, leaving out its CTE names."""
        for cte in tree.find_all(exp.CTE):
            alias = cte.args.get("alias")
            if isinstance(alias, exp.TableAlias):
                self._cte_names.add(self._key(alias.this))
        for table in tree.find_all(exp.Table):
            ref = self._table_ref(table)
            if ref is not None:
                self._tables.setdefault(ref, None)

    @property
    def tables(self) -> Tuple[TableRef, ...]:
        return tuple(self._tables)

    def result(self, trees: List[exp.Expression], annotations: Tuple[ParameterUse, ...],
               build: Callable[..., StatementAnalysis]) -> StatementAnalysis:
        assignments = tuple(sorted(self._assignments, key=_by_ordinal))
        comparisons = tuple(sorted(self._comparisons, key=_by_ordinal))
        inferred = {use.ordinal: use.column for use in (*comparisons, *assignments)}
        resolved = tuple(ParameterUse(a.placeholder, self._resolve_annotation(a.placeholder, a.column, inferred))
                         for a in annotations)
        if self._prepared:
            self._note(AnalysisGap.PREPARED_PARAMETERS)
        return build(
            kind=self._kind or StatementKind.OTHER,
            writes=self._writes,
            tables=tuple(self._tables),
            written_tables=tuple(self._written),
            assignments=assignments,
            comparisons=comparisons,
            annotations=resolved,
            unbound_writes=tuple(self._unbound),
            result_columns=self._result_columns(trees[0]) if len(trees) == 1 and not self._prepared else None,
            gap=self._gap if self._writes else None)

    # ---- statements ----

    def _statement_kind(self, tree: exp.Expression) -> StatementKind:
        if isinstance(tree, exp.Copy):
            return StatementKind.COPY_FROM if tree.args.get("kind") else StatementKind.COPY_TO
        for cls, kind in ((exp.Insert, StatementKind.INSERT), (exp.Update, StatementKind.UPDATE),
                          (exp.Delete, StatementKind.DELETE), (exp.Merge, StatementKind.MERGE),
                          (exp.Query, StatementKind.SELECT), (exp.Values, StatementKind.SELECT)):
            if isinstance(tree, cls):
                return kind
        return StatementKind.OTHER

    def _insert(self, node: exp.Insert) -> None:
        self._writes = True
        target = node.this
        columns: Optional[List[Optional[str]]] = None
        if isinstance(target, exp.Schema):
            table = target.this
            columns = [self._identifier(i) for i in target.expressions]
        else:
            table = target
            # sqlglot attaches the column list of "INSERT INTO t AS a (c)" to
            # the alias.
            alias = table.args.get("alias") if isinstance(table, exp.Table) else None
            if alias is not None and alias.columns:
                columns = [self._identifier(i) for i in alias.columns]
        ref = self._written_table(table, node)
        name = None if ref is None else ref.name
        if ref is None:
            self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
        if columns is not None and any(c is None for c in columns):
            self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
            columns = None

        source = node.expression
        if columns is None:
            self._note(AnalysisGap.NO_COLUMN_LIST)
        elif isinstance(source, exp.Values):
            self._insert_rows(columns, name, source.expressions)
        elif source is None:
            self._note(AnalysisGap.NO_COLUMN_LIST)  # DEFAULT VALUES
        elif not self._insert_from_values(columns, name, source):
            self._note(AnalysisGap.VALUES_FROM_QUERY)

        conflict = node.args.get("conflict")
        if isinstance(conflict, exp.OnConflict):
            self._upsert(conflict, name, columns, source)

    def _insert_rows(self, columns: List[Optional[str]], table: Optional[str], rows: List[exp.Expression],
                     positions: Optional[List[Tuple[int, Tuple[str, ...]]]] = None) -> None:
        # positions: for each insert column, the position in the row that
        # fills it and the casts on the way (default: the same position).
        for row in rows:
            values = self._row_values(row)
            width = len(columns) if positions is None else max(p for p, _ in positions) + 1
            if values is None or (len(values) != width if positions is None else len(values) < width):
                self._note(AnalysisGap.ROW_LENGTH_MISMATCH)
            if values is None:
                continue
            for i, column in enumerate(columns):
                position, casts = (i, ()) if positions is None else positions[i]
                if position < len(values) and column is not None:
                    self._write(ColumnRef(column, table), values[position], casts)

    def _insert_from_values(self, columns: List[Optional[str]], table: Optional[str],
                            source: exp.Expression) -> bool:
        """``INSERT INTO t (a, b) SELECT x, y FROM (VALUES (...), ...) AS v(x, y)``,
        the form SQLAlchemy uses for a multi-row ORM insert. When each insert
        column is filled by a different column of the VALUES list (possibly
        cast), it is read like an ordinary multi-row VALUES. An ORDER BY is
        allowed. A join, a filter, an expression other than a cast, or one
        VALUES column filling two insert columns is not."""
        if not isinstance(source, exp.Select) or len(source.expressions) != len(columns):
            return False
        if any(value for key, value in source.args.items() if key not in _PLAIN_SELECT_ARGS):
            return False  # a join, a filter, a limit, ... (or an arg this code does not know)
        from_ = source.args.get("from_")
        values = from_.this if isinstance(from_, exp.From) else None
        alias = values.args.get("alias") if isinstance(values, exp.Values) else None
        if not isinstance(alias, exp.TableAlias) or not alias.columns:
            return False
        names = {self._key(c): i for i, c in enumerate(alias.columns)}
        alias_key = self._key(alias.this)
        positions: List[Tuple[int, Tuple[str, ...]]] = []
        for item in source.expressions:
            inner, casts = self._unwrap_casts(item)
            qualifier = inner.args.get("table") if isinstance(inner, exp.Column) else None
            if not isinstance(inner, exp.Column) or not isinstance(inner.this, exp.Identifier) \
                    or (qualifier is not None and self._key(qualifier) != alias_key) \
                    or self._key(inner.this) not in names:
                return False
            positions.append((names[self._key(inner.this)], casts))
        if len({position for position, _ in positions}) != len(positions):
            return False  # one value would fill two columns
        assert isinstance(values, exp.Values)
        self._insert_rows(columns, table, values.expressions, positions)
        return True

    def _upsert(self, conflict: exp.OnConflict, table: Optional[str], columns: Optional[List[Optional[str]]],
                source: Optional[exp.Expression]) -> None:
        same_row: _SameRow = {}
        if self._dialect.name != "mysql":
            same_row[self._key("excluded")] = None
        row_alias = source.args.get("alias") if isinstance(source, exp.Values) else None
        if isinstance(row_alias, exp.TableAlias):
            renamed: Optional[Dict[str, str]] = None
            if row_alias.columns:
                # AS new(c1, c2) renames the inserted columns by position.
                renamed = {}
                for alias_column, column in zip(row_alias.columns, columns or ()):
                    if column is not None:
                        renamed[self._key(alias_column)] = self._dialect.metadata_key(column)
            same_row[self._key(row_alias.this)] = renamed
        for assignment in conflict.expressions:
            self._assign(assignment, lambda _q: table, same_row=same_row)

    def _update(self, node: exp.Update) -> None:
        self._writes = True
        target = node.this
        joins = (target.args.get("joins") or []) if isinstance(target, exp.Table) else []
        targets = [target] + [j.this for j in joins]
        scope = self._scope(node)
        ref = self._written_table(target, node)
        if ref is None:
            self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
        for extra in targets[1:]:
            self._written_table(extra, node)

        def table_of(qualifier: Optional[str]) -> Optional[str]:
            if len(targets) == 1:
                # One table to write: a qualifier can only mean it.
                # PostgreSQL's FROM adds tables to read, not to write.
                return None if ref is None else ref.name
            source = scope.get(self._dialect.metadata_key(qualifier)) if qualifier else None
            if source is None:
                self._note(AnalysisGap.AMBIGUOUS_TABLE)
                return None
            return source.name

        for assignment in node.expressions:
            self._assign(assignment, table_of)

    def _merge(self, node: exp.Merge) -> None:
        self._writes = True
        ref = self._written_table(node.this, node)
        name = None if ref is None else ref.name
        whens = node.args.get("whens")
        for when in (whens.expressions if whens is not None else ()):
            then = when.args.get("then")
            if isinstance(then, exp.Update):
                for assignment in then.expressions:
                    self._assign(assignment, lambda _q: name)
            elif isinstance(then, exp.Insert):
                columns = [self._identifier(c) for c in then.this.expressions] \
                    if isinstance(then.this, exp.Tuple) else None
                if columns is None or any(c is None for c in columns):
                    self._note(AnalysisGap.NO_COLUMN_LIST if columns is None else AnalysisGap.UNSUPPORTED_STATEMENT)
                    continue
                self._insert_rows(columns, name, [then.expression])

    def _copy(self, node: exp.Copy) -> None:
        if not node.args.get("kind"):
            return  # COPY ... TO reads
        self._writes = True
        target = node.this
        table = target.this if isinstance(target, exp.Schema) else target
        ref = self._written_table(table)
        name = None if ref is None else ref.name
        if not isinstance(target, exp.Schema) or not target.expressions:
            self._note(AnalysisGap.NO_COLUMN_LIST)
            return
        # The rows arrive as a stream, never as bind parameters.
        for identifier in target.expressions:
            column = self._identifier(identifier)
            if column is None:
                self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
            else:
                self._unbound.append(ColumnRef(column, name))

    # ---- writes ----

    def _assign(self, assignment: exp.Expression, table_of: _TableOf, same_row: Optional[_SameRow] = None) -> None:
        self._assignment_nodes.add(id(assignment))
        if not isinstance(assignment, exp.EQ):
            self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
            return
        target, value = assignment.this, assignment.expression
        if isinstance(target, exp.Tuple):
            # SET (a, b) = (v1, v2) / ROW(v1, v2); a subquery fills them all.
            values = self._row_values(value)
            if values is None or len(values) != len(target.expressions):
                for column in target.expressions:
                    name = self._identifier(column) if isinstance(column, exp.Column) else None
                    if name is None:
                        self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
                        continue
                    self._unbound.append(ColumnRef(name, table_of(self._fold(column.args.get("table")))))
                return
            pairs = list(zip(target.expressions, values))
        else:
            pairs = [(target, value)]
        for column, v in pairs:
            name = self._identifier(column) if isinstance(column, exp.Column) else None
            if name is None:
                self._note(AnalysisGap.UNSUPPORTED_STATEMENT)
                continue
            if same_row is not None and self._copies_same_column(v, name, same_row):
                continue
            self._write(ColumnRef(name, table_of(self._fold(column.args.get("table")))), v)

    def _write(self, column: ColumnRef, value: exp.Expression, outer_casts: Tuple[str, ...] = ()) -> None:
        param = self._param(value)
        if param is not None:
            ordinal, casts, _ = param
            self._assignments.append(ParameterUse(self._tokens.placeholders[ordinal], column, outer_casts + casts))
        elif self._prepared and self._server_parameter(value):
            pass  # bound by a later EXECUTE; the statement's gap says so
        elif not isinstance(value, exp.Null):
            self._unbound.append(column)

    @staticmethod
    def _server_parameter(value: exp.Expression) -> bool:
        while isinstance(value, (exp.Paren, exp.Cast, exp.TryCast)):
            value = value.this
        return isinstance(value, exp.Parameter)

    def _copies_same_column(self, value: exp.Expression, column: str, same_row: _SameRow) -> bool:
        # The incoming row's own value for this column, already encrypted (or
        # refused) where the row was written: EXCLUDED.c, VALUES(c) on MySQL,
        # or alias.c with a MySQL row alias (by position when the alias names
        # its columns).
        key = self._dialect.metadata_key(column)
        qualifier = value.args.get("table") if isinstance(value, exp.Column) else None
        if qualifier is not None:
            qualifier_key = self._key(qualifier)
            if qualifier_key not in same_row:
                return False
            renamed = same_row[qualifier_key]
            source = self._key(value.this)
            return (renamed.get(source) if renamed is not None else source) == key
        if self._dialect.name == "mysql" and isinstance(value, exp.Anonymous) \
                and str(value.this).upper() == "VALUES" and len(value.expressions) == 1:
            inner = value.expressions[0]
            if isinstance(inner, exp.Column) and inner.args.get("table") is None:
                inner = inner.this
            return isinstance(inner, exp.Identifier) and self._key(inner) == key
        return False

    def _row_values(self, row: Optional[exp.Expression]) -> Optional[List[exp.Expression]]:
        if isinstance(row, exp.Paren):
            return [row.this]
        if isinstance(row, exp.Tuple):
            items = list(row.expressions)
            # MySQL's VALUES ROW(...) comes back as a one-item tuple.
            if len(items) == 1 and self._is_row(items[0]):
                return list(items[0].expressions)
            return items
        if row is not None and self._is_row(row):
            return list(row.expressions)
        return None

    @staticmethod
    def _is_row(node: exp.Expression) -> bool:
        return isinstance(node, exp.Anonymous) and str(node.this).upper() == "ROW"

    # ---- comparisons ----

    def _collect_comparisons(self, tree: exp.Expression) -> None:
        for node in tree.walk():
            if id(node) in self._assignment_nodes:
                continue
            if isinstance(node, _COMPARISONS):
                self._compare(node, node.this, node.expression)
                self._compare(node, node.expression, node.this)
            elif isinstance(node, exp.In):
                for item in node.expressions:
                    self._compare(node, node.this, item)
            elif isinstance(node, exp.Between):
                self._compare(node, node.this, node.args.get("low"))
                self._compare(node, node.this, node.args.get("high"))

    def _compare(self, node: exp.Expression, column: Optional[exp.Expression],
                 value: Optional[exp.Expression]) -> None:
        if not isinstance(column, exp.Column) or not isinstance(column.this, exp.Identifier) or value is None:
            return
        param = self._param(value)
        if param is None:
            return
        ordinal, casts, array = param
        ref = self._column_ref(column, node)
        if ref is not None:
            self._comparisons.append(ParameterUse(self._tokens.placeholders[ordinal], ref, casts, array))

    # ---- values ----

    @staticmethod
    def _unwrap_casts(value: exp.Expression) -> Tuple[exp.Expression, Tuple[str, ...]]:
        casts: List[str] = []
        while True:
            if isinstance(value, exp.Paren):
                value = value.this
            elif isinstance(value, (exp.Cast, exp.TryCast)):
                to = value.args.get("to")
                casts.append(to.this.name if isinstance(to, exp.DataType) else str(to))
                value = value.this
            else:
                return value, tuple(casts)

    def _param(self, value: exp.Expression) -> Optional[Tuple[int, Tuple[str, ...], bool]]:
        """(ordinal, casts, array) when ``value`` is a placeholder, possibly in
        parentheses, casts, or ANY/ALL/SOME."""
        value, casts = self._unwrap_casts(value)
        array = False
        if isinstance(value, (exp.Any, exp.All)):
            array, (value, more) = True, self._unwrap_casts(value.this)
            casts += more
        elif isinstance(value, exp.Anonymous) and str(value.this).upper() in _ARRAY_FUNCTIONS \
                and len(value.expressions) == 1:
            # sqlglot reads PostgreSQL's ALL(...) and SOME(...) as functions.
            array, (value, more) = True, self._unwrap_casts(value.expressions[0])
            casts += more
        if isinstance(value, exp.Placeholder):
            ordinal = normalized_ordinal(value.name)
            if ordinal is not None:
                return ordinal, casts, array
        return None

    # ---- names and scopes ----

    def _fold(self, name: Any) -> Optional[str]:
        """The name an identifier resolves to, folded as the engine folds it;
        ``None`` when ``name`` is not an identifier."""
        if isinstance(name, exp.Identifier):
            return self._dialect.fold_identifier(name.this, quoted=bool(name.args.get("quoted")))
        if isinstance(name, str) and name:
            return self._dialect.fold_identifier(name, quoted=False)
        return None

    def _identifier(self, node: Any) -> Optional[str]:
        """The column a node names, or ``None`` when it is not a plain name
        (a subscript, a field of a composite, ...)."""
        if isinstance(node, exp.Column):
            node = node.this
        return self._fold(node) if isinstance(node, exp.Identifier) else None

    def _key(self, name: Any) -> str:
        """The form names are compared in (case-insensitive on MySQL)."""
        return self._dialect.metadata_key(self._fold(name) or "")

    def _table_ref(self, table: exp.Table) -> Optional[TableRef]:
        if not isinstance(table.this, exp.Identifier):
            return None  # a function or a subquery
        name = self._fold(table.this)
        assert name is not None
        schema = self._fold(table.args.get("db"))
        if schema is None and self._dialect.metadata_key(name) in self._cte_names:
            return None
        return TableRef(name, schema)

    def _written_table(self, table: Optional[exp.Expression],
                       write: Optional[exp.Expression] = None) -> Optional[TableRef]:
        ref = self._table_ref(table) if isinstance(table, exp.Table) else None
        if ref is not None:
            self._written.setdefault(ref, None)
            if write is not None:
                self._written_by.setdefault(id(write), []).append(ref)
        return ref

    def _write_targets(self, node: exp.Expression) -> Optional[Sequence[TableRef]]:
        """The tables written by the INSERT, UPDATE or MERGE that the value at
        ``node`` feeds.

        Returns ``None`` when the value is not written. That is the case when,
        on the way up, it is compared or otherwise tested (:data:`_TESTS`), is
        the condition or operand of a ``CASE`` or ``IF``, or sits in one of
        :data:`_NOT_WRITTEN_ARGS`, and when it reaches no write at all.

        Returns an empty sequence when the table can't be told: the value
        reaches the write through one of :data:`_SOURCE_ARGS` (a CTE, or a
        ``FROM``, ``JOIN``, ``LATERAL`` or ``USING`` source), or the write's
        target is not a plain table."""
        child: Any = node
        parent: Any = node.parent
        through_source = False
        while parent is not None:
            key = child.arg_key
            if key in _NOT_WRITTEN_ARGS:
                return None
            # A SET or ON CONFLICT "c = value" is an assignment, not a test.
            if isinstance(parent, _TESTS) and id(parent) not in self._assignment_nodes:
                return None
            if isinstance(parent, (exp.If, exp.Case)) and key == "this":
                return None
            through_source = through_source or key in _SOURCE_ARGS
            # A MERGE's WHEN ... UPDATE / INSERT belongs to the MERGE.
            if isinstance(parent, _WRITES) and not isinstance(parent.parent, exp.When):
                return () if through_source else self._written_by.get(id(parent), ())
            child, parent = parent, parent.parent
        return None

    def _scope(self, query: exp.Expression) -> Dict[str, _Source]:
        """Alias or table name -> what it names, for the sources one query
        reads directly."""
        cached = self._scopes.get(id(query))
        if cached is not None:
            return cached
        sources: Dict[str, _Source] = {}

        def add(node: Any) -> None:
            if isinstance(node, list):
                for item in node:
                    add(item)
                return
            if not isinstance(node, exp.Expression):
                return  # an absent arg can be None or False
            if isinstance(node, (exp.From, exp.Join)):
                add(node.this)
                return
            alias = node.args.get("alias")
            alias_name = alias.this if isinstance(alias, exp.TableAlias) else None
            ref = self._table_ref(node) if isinstance(node, exp.Table) else None
            if alias_name is not None:
                sources[self._key(alias_name)] = ref
            if ref is not None:
                sources.setdefault(self._dialect.metadata_key(ref.name), ref)
            elif isinstance(node, exp.Table) and alias_name is None:
                sources[self._key(node.this)] = None  # a CTE named directly
            if isinstance(node, exp.Table):
                add(node.args.get("joins"))

        if isinstance(query, exp.Insert):
            add(query.this.this if isinstance(query.this, exp.Schema) else query.this)
        else:
            if isinstance(query, (exp.Update, exp.Delete, exp.Merge)):
                add(query.args.get("this"))
            add(query.args.get("from_"))
            add(query.args.get("joins"))
            add(query.args.get("using"))  # a list for DELETE ... USING
            add(query.args.get("tables"))
        self._scopes[id(query)] = sources
        return sources

    def _nearest_query(self, node: exp.Expression) -> Optional[exp.Expression]:
        """The innermost query around ``node`` (excluding ``node`` itself)."""
        path: List[int] = []
        parent = node.parent
        found: Optional[exp.Expression] = None
        while parent is not None:
            if id(parent) in self._nearest:
                found = self._nearest[id(parent)]
                break
            if isinstance(parent, _QUERIES):
                found = parent
                break
            path.append(id(parent))
            parent = parent.parent
        for key in path:
            self._nearest[key] = found
        return found

    def _enclosing_queries(self, node: exp.Expression) -> Iterator[exp.Expression]:
        query = self._nearest_query(node)
        while query is not None:
            yield query
            query = self._nearest_query(query)

    def _column_ref(self, column: exp.Column, at: exp.Expression) -> Optional[ColumnRef]:
        name = self._identifier(column)
        if name is None:
            return None
        qualifier = self._fold(column.args.get("table"))
        if qualifier is not None:
            key = self._dialect.metadata_key(qualifier)
            for query in self._enclosing_queries(at):
                scope = self._scope(query)
                if key in scope:
                    source = scope[key]
                    return ColumnRef(name, None if source is None else source.name)
            # Not an alias in sight: taken as a table name.
            return ColumnRef(name, qualifier)
        nearest = self._nearest_query(at)
        if nearest is not None:
            sources = set(self._scope(nearest).values())
            if len(sources) == 1:
                only = next(iter(sources))
                return ColumnRef(name, None if only is None else only.name)
        return ColumnRef(name)

    def _resolve_annotation(self, placeholder: Placeholder, ref: ColumnRef,
                            inferred: Dict[int, ColumnRef]) -> ColumnRef:
        """Resolve an annotation's qualifier through the aliases in scope at
        its placeholder, nearest query first. A qualifier that names nothing
        in scope, or names a subquery or CTE, is kept as a table name.

        A bare column gets a table only when the statement says which:

        1. the table the analysis found for that placeholder, if it is the
           same column;
        2. otherwise, for a written value (a VALUES row, a SET value, or the
           select list of a query feeding an INSERT, UPDATE or MERGE; see
           :meth:`_write_targets`), the table that write writes. With several
           written tables, or when the value reaches the write through a CTE
           or a FROM, JOIN, LATERAL or USING source, it stays bare;
        3. otherwise, for a compared or read value, the only table its
           nearest query reads.

        In every other case it stays bare."""
        node = self._placeholder_nodes.get(placeholder.ordinal)
        if ref.table is not None:
            key = self._dialect.metadata_key(ref.table)
            for query in (self._enclosing_queries(node) if node is not None else ()):
                scope = self._scope(query)
                if key in scope:
                    source = scope[key]
                    return ColumnRef(ref.column, ref.table if source is None else source.name)
            return ref
        found = inferred.get(placeholder.ordinal)
        if found is not None and found.table is not None \
                and self._dialect.metadata_key(found.column) == self._dialect.metadata_key(ref.column):
            return ColumnRef(ref.column, found.table)
        if node is None:
            return ref
        targets = self._write_targets(node)
        if targets is not None:
            tables = {t.name: None for t in targets}
            return ColumnRef(ref.column, next(iter(tables))) if len(tables) == 1 else ref
        nearest = self._nearest_query(node)
        if nearest is not None:
            sources = set(self._scope(nearest).values())
            if len(sources) == 1:
                only = next(iter(sources))
                if only is not None:
                    return ColumnRef(ref.column, only.name)
        return ref

    # ---- results ----

    def _result_columns(self, tree: exp.Expression) -> Optional[Tuple[Optional[ColumnRef], ...]]:
        if isinstance(tree, exp.Select):
            items = tree.expressions
        else:
            returning = tree.args.get("returning") if isinstance(tree, (exp.Insert, exp.Update, exp.Delete)) else None
            if not isinstance(returning, exp.Returning):
                return None
            items = returning.expressions
        result: List[Optional[ColumnRef]] = []
        for item in items:
            inner = item.this if isinstance(item, exp.Alias) else item
            if isinstance(inner, exp.Star) or (isinstance(inner, exp.Column) and isinstance(inner.this, exp.Star)):
                return None
            ref = self._column_ref(inner, inner) if isinstance(inner, exp.Column) else None
            if (ref is None or ref.table is None) and isinstance(tree, exp.Select):
                # Read through a derived table or CTE: follow it to its table.
                ref = self._trace(inner, tree, 0)
            result.append(ref if ref is not None and ref.table is not None else None)
        return tuple(result)

    def _trace(self, node: exp.Expression, query: exp.Expression, depth: int) -> Optional[ColumnRef]:
        """The table column a result column of ``query`` comes from, followed
        through derived tables and CTEs; ``None`` past a set operation, ``*``,
        an expression or :data:`_MAX_DEPTH` levels."""
        if depth > _MAX_DEPTH or not isinstance(node, exp.Column):
            return None
        ref = self._column_ref(node, node)
        if ref is not None and ref.table is not None:
            return ref
        name = self._identifier(node)
        if name is None:
            return None
        qualifier = self._fold(node.args.get("table"))
        inner = self._derived_query(None if qualifier is None else self._dialect.metadata_key(qualifier), query)
        if not isinstance(inner, exp.Select):
            return None
        key = self._dialect.metadata_key(name)
        for item in inner.expressions:
            # The alias is folded from its Identifier, so quoting counts.
            out = self._fold(item.args.get("alias")) if isinstance(item, exp.Alias) else self._identifier(item)
            if out is not None and self._dialect.metadata_key(out) == key:
                return self._trace(item.this if isinstance(item, exp.Alias) else item, inner, depth + 1)
        return None

    def _derived_query(self, qualifier: Optional[str], query: exp.Expression) -> Optional[exp.Expression]:
        """The query behind the derived table or CTE that ``query`` reads as
        ``qualifier``, or behind its only source when there is no qualifier.
        A source whose alias lists column names is not followed: the list
        renames the columns by position. A CTE sees only the CTEs written
        before it, as the engines resolve a non-recursive ``WITH``. In a
        ``RECURSIVE`` list, a source name no earlier CTE defines leaves the
        column unknown."""
        nodes: List[exp.Expression] = []
        from_ = query.args.get("from_")
        if from_ is not None:
            nodes.append(from_.this)
        nodes.extend(j.this for j in query.args.get("joins") or [])
        if qualifier is None and len(nodes) != 1:
            return None
        for node in nodes:
            alias = node.args.get("alias")
            alias_name = self._fold(alias.this) if isinstance(alias, exp.TableAlias) else None
            renamed = isinstance(alias, exp.TableAlias) and bool(alias.args.get("columns"))
            if isinstance(node, exp.Subquery):
                if qualifier is None or (alias_name is not None and self._dialect.metadata_key(alias_name) == qualifier):
                    return None if renamed else node.this
            elif isinstance(node, exp.Table) and isinstance(node.this, exp.Identifier):
                key = self._key(node.this)
                seen = key if alias_name is None else self._dialect.metadata_key(alias_name)
                if qualifier is not None and seen != qualifier:
                    continue
                if renamed:
                    return None
                outer: Optional[exp.Expression] = query
                inside: Any = None
                while outer is not None:
                    with_ = outer.args.get("with_")
                    recursive = with_ is not None and bool(with_.args.get("recursive"))
                    for cte in (with_.expressions if with_ is not None else ()):
                        if cte is inside:
                            if recursive:
                                return None  # PostgreSQL lets a RECURSIVE list read later CTEs
                            break  # a CTE sees only the CTEs before it
                        cte_alias = cte.args.get("alias")
                        if isinstance(cte_alias, exp.TableAlias) and self._key(cte_alias.this) == key:
                            return None if cte_alias.args.get("columns") else cte.this
                    inside = outer.parent
                    outer = self._nearest_query(outer)
                return None
        return None

    def _note(self, gap: AnalysisGap) -> None:
        if self._gap is None:
            self._gap = gap
