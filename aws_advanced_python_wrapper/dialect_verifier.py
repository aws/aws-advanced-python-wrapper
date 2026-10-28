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

"""Database-dialect identification rules, as data plus two verifiers.

A dialect says *what* identifies it -- a few queries and a predicate over
the rows each returns -- as :class:`DialectVerificationRules`. A verifier
says *how* to run those against a connection. There are two verifiers
because there are two connection kinds, and only the execution differs
between them: :class:`SyncDialectVerifier` for DB-API connections and
:class:`AsyncDialectVerifier` for the async drivers.

Why composition rather than a sync method and an async method on every
dialect: the rules are the same either way (the queries are already
shared), so writing them twice means the two can silently drift -- which
is how the async layer ended up identifying only one of the six candidate
dialects. Here the rules are written once and both engines can run them,
so a dialect added later works under both without further async work.

Dialects may still override ``is_dialect`` / ``is_dialect_async`` directly
when their identification does not fit this vocabulary; the rules are the
default, not a requirement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection

RowPredicate = Callable[[Tuple], bool]
"""Takes one result row, answers whether it satisfies the rule."""


# -- Row predicates ----------------------------------------------------------
#
# Each guards the row length itself: a server returning fewer columns than
# expected means "not this dialect", never an IndexError.

def col_truthy(index: int) -> RowPredicate:
    """Column ``index`` is truthy."""
    def predicate(row: Tuple) -> bool:
        return len(row) > index and bool(row[index])
    return predicate


def col_falsy(index: int) -> RowPredicate:
    """Column ``index`` is present but falsy."""
    def predicate(row: Tuple) -> bool:
        return len(row) > index and not bool(row[index])
    return predicate


def col_non_empty_str(index: int) -> RowPredicate:
    """Column ``index`` is a non-empty string.

    An empty value is how MySQL reports an unset ``SHOW VARIABLES`` entry,
    so blank has to be distinguished from present.
    """
    def predicate(row: Tuple) -> bool:
        if len(row) <= index:
            return False
        value = row[index]
        return value is not None and str(value) != ""
    return predicate


def col_equals_ignore_case(index: int, expected: str) -> RowPredicate:
    """Column ``index`` equals ``expected``, case-insensitively."""
    lowered = expected.lower()

    def predicate(row: Tuple) -> bool:
        if len(row) <= index or row[index] is None:
            return False
        return str(row[index]).lower() == lowered
    return predicate


def col_contains_ignore_case(index: int, needle: str) -> RowPredicate:
    """Column ``index`` contains ``needle``, case-insensitively."""
    lowered = needle.lower()

    def predicate(row: Tuple) -> bool:
        if len(row) <= index or row[index] is None:
            return False
        return lowered in str(row[index]).lower()
    return predicate


def col_greater_than(index: int, threshold: float) -> RowPredicate:
    """Column ``index`` is a number greater than ``threshold``."""
    def predicate(row: Tuple) -> bool:
        if len(row) <= index:
            return False
        value = row[index]
        return value is not None and value > threshold
    return predicate


def all_of(*predicates: RowPredicate) -> RowPredicate:
    """Every predicate holds for the same row."""
    def predicate(row: Tuple) -> bool:
        return all(p(row) for p in predicates)
    return predicate


# -- Spec --------------------------------------------------------------------

@dataclass(frozen=True)
class Rule:
    """One query plus the condition its rows must satisfy.

    ``predicate`` of None means the query returning any row at all is the
    condition -- the existence tests several dialects rely on.

    ``match_any_row`` scans the whole result set and passes if any row
    satisfies the predicate; the default inspects only the first row. The
    distinction is load bearing: the MySQL and RDS PG rules read a
    multi-row result where the identifying row is not necessarily first.
    """

    query: str
    predicate: Optional[RowPredicate] = None
    match_any_row: bool = False


@dataclass(frozen=True)
class DialectVerificationRules:
    """The complete identification test for one dialect.

    All rules must pass. ``inherits`` chains a parent dialect's rules that
    must pass first, mirroring the ``super().is_dialect(...)`` calls the
    hand-written probes used -- an Aurora PG host is a PG host first.
    """

    rules: Tuple[Rule, ...] = field(default_factory=tuple)
    inherits: Optional[DialectVerificationRules] = None


# -- Runners -----------------------------------------------------------------

class SyncDialectVerifier:
    """Runs a :class:`DialectVerificationRules` against a DB-API connection.

    Restores the connection's transaction state on the way out and then
    lets the error propagate. Callers decide what a failure means:
    ``DatabaseDialect.is_dialect`` treats it as "not this dialect", while
    the manager's async candidate walk distinguishes "answered no" from
    "could not ask" so it does not cache a guess it never verified.

    The timeout is the caller's too --
    ``DatabaseDialectManager.query_for_dialect`` wraps this in
    ``preserve_transaction_status_with_timeout``.
    """

    @staticmethod
    def run(
            rules: Optional[DialectVerificationRules],
            conn: Connection,
            driver_dialect: DriverDialect) -> bool:
        if rules is None:
            return False

        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            return SyncDialectVerifier._evaluate(rules, conn)
        except Exception:
            # Roll back only a transaction verification itself opened: a SELECT
            # on an autocommit=False connection leaves it mid-transaction,
            # which would then block the caller's session-state changes.
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()
            raise

    @staticmethod
    def _evaluate(rules: DialectVerificationRules, conn: Connection) -> bool:
        if rules.inherits is not None and not SyncDialectVerifier._evaluate(rules.inherits, conn):
            return False

        for rule in rules.rules:
            if not SyncDialectVerifier._evaluate_rule(rule, conn):
                return False
        return True

    @staticmethod
    def _evaluate_rule(rule: Rule, conn: Connection) -> bool:
        from contextlib import closing
        with closing(conn.cursor()) as cursor:
            cursor.execute(rule.query)
            if rule.match_any_row:
                rows = cursor.fetchall()
                if rule.predicate is None:
                    return bool(rows)
                return any(rule.predicate(row) for row in rows)

            row = cursor.fetchone()
            if row is None:
                return False
            return True if rule.predicate is None else rule.predicate(row)


class AsyncDialectVerifier:
    """Runs a :class:`DialectVerificationRules` against an async connection.

    Same rules, same semantics as :class:`SyncDialectVerifier` --
    including letting errors propagate; only the cursor handling is
    awaited. The caller supplies the timeout via ``asyncio.wait_for``.
    """

    @staticmethod
    async def run(
            rules: Optional[DialectVerificationRules],
            conn: Any,
            driver_dialect: Any) -> bool:
        if rules is None:
            return False

        from aws_advanced_python_wrapper.aio.dialect_utils import \
            AsyncDialectUtils

        was_in_transaction = await AsyncDialectUtils._safe_in_transaction(
            conn, driver_dialect)
        try:
            return await AsyncDialectVerifier._evaluate(rules, conn)
        finally:
            # Unconditional, unlike the sync runner's rollback-on-error: a
            # successful SELECT also leaves psycopg INTRANS, which
            # then makes the caller's set_read_only/set_autocommit raise.
            await AsyncDialectUtils._restore_idle(conn, was_in_transaction)

    @staticmethod
    async def _evaluate(rules: DialectVerificationRules, conn: Any) -> bool:
        if rules.inherits is not None and not await AsyncDialectVerifier._evaluate(rules.inherits, conn):
            return False

        for rule in rules.rules:
            if not await AsyncDialectVerifier._evaluate_rule(rule, conn):
                return False
        return True

    @staticmethod
    async def _evaluate_rule(rule: Rule, conn: Any) -> bool:
        from aws_advanced_python_wrapper.aio.dialect_utils import \
            AsyncDialectUtils

        if rule.match_any_row:
            rows = await AsyncDialectUtils.fetch_all(conn, rule.query)
            if rule.predicate is None:
                return bool(rows)
            return any(rule.predicate(row) for row in rows)

        row = await AsyncDialectUtils.fetch_one(conn, rule.query)
        if row is None:
            return False
        return True if rule.predicate is None else rule.predicate(row)
