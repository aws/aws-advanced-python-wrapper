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

"""Per-driver classification of transient connect errors.

Used by code paths that open a new DBAPI connection outside the wrapper's
plugin chain (SQLAlchemy pool creators, Django backends) and need their
own retry-with-backoff because the plugin-level retry doesn't fire there.

The per-driver split mirrors :mod:`pg_exception_handler` /
:mod:`mysql_exception_handler`. Each driver block defines:

  * the SQLSTATEs / errnos / message prefixes that classify as transient
  * which SQLSTATE *class prefixes* (e.g. ``"08"``) cover entire families
    of network errors

A single :func:`is_transient_connect_error` walks the union of all of
these so retry consumers don't need to know which driver they're talking
to at the retry site.
"""

from __future__ import annotations

import random
from typing import FrozenSet, Tuple

# ───── PostgreSQL (psycopg) ────────────────────────────────────────────
# Exact SQLSTATEs that are unambiguously transient. Notably we list
# 57P01/02/03 individually rather than a "57P" prefix because 57P04 is
# ``database_dropped`` (permanent) and would be wrong to retry on.
PG_TRANSIENT_CONNECT_SQLSTATES: FrozenSet[str] = frozenset({
    "57P01",  # admin_shutdown
    "57P02",  # crash_shutdown
    "57P03",  # cannot_connect_now ("the database system is starting up")
    "08006",  # connection_failure (SQL-standard)
})
# Class-08 (``connection_exception``) is uniformly transient per the SQL
# standard; psycopg emits the various sub-codes (08003, 08006, 08001…)
# during Aurora rotation.
PG_TRANSIENT_CONNECT_SQLSTATE_PREFIXES: Tuple[str, ...] = ("08",)
# libpq wire-level errors carry no SQLSTATE from the server. Mirrors
# :data:`pg_exception_handler.PgExceptionHandler._NETWORK_ERROR_MESSAGES`,
# plus the client-side connect-timeout message. psycopg raises
# ``ConnectionTimeout("connection timeout expired")`` when ``connect_timeout``
# elapses before the server answers -- which is exactly what happens while
# Aurora is promoting a new writer (the endpoint accepts the TCP connection
# but the backend hasn't finished booting, so the libpq handshake stalls).
# That is transient: the next attempt, once promotion completes, succeeds.
# It carries no SQLSTATE (the server never replied), so only the message
# prefix can classify it.
PG_TRANSIENT_CONNECT_MESSAGE_PREFIXES: Tuple[str, ...] = (
    "connection failed",
    "consuming input failed",
    "connection socket closed",
    "the connection is closed",
    "connection timeout expired",
)

# ───── MySQL Connector (sync) ──────────────────────────────────────────
# mysql-connector-python primarily classifies connection errors by
# ``errno`` (integer). SQLSTATEs are mostly ``HY000`` (general error)
# with the actual signal in the errno. Mirrors
# :data:`mysql_exception_handler.MysqlExceptionHandler._NETWORK_ERRORS`.
MYSQL_TRANSIENT_CONNECT_ERRNOS: FrozenSet[int] = frozenset({
    2001,  # Can't create UNIX socket
    2002,  # Can't connect to local MySQL server through socket
    2003,  # Can't connect to MySQL server
    2004,  # Can't create TCP/IP socket
    2006,  # MySQL server has gone away
    2012,  # Error in server handshake
    2013,  # Lost connection to MySQL server during query
    2026,  # SSL connection error
    2055,  # Lost connection to MySQL server (read failure)
})
# ``08`` (standard connection class) and ``HY`` (driver-level "general
# error" used by mysql-connector when it doesn't have a specific SQLSTATE)
# are both treated as network. Matches mysql_exception_handler.py:71.
MYSQL_TRANSIENT_CONNECT_SQLSTATE_PREFIXES: Tuple[str, ...] = ("08", "HY")
MYSQL_TRANSIENT_CONNECT_MESSAGE_PREFIXES: Tuple[str, ...] = (
    "MySQL Connection not available",
)

# ───── Combined views (driver-agnostic retry consumers use these) ─────
_ALL_SQLSTATES: FrozenSet[str] = PG_TRANSIENT_CONNECT_SQLSTATES
_ALL_ERRNOS: FrozenSet[int] = MYSQL_TRANSIENT_CONNECT_ERRNOS
_ALL_SQLSTATE_PREFIXES: Tuple[str, ...] = (
    PG_TRANSIENT_CONNECT_SQLSTATE_PREFIXES
    + MYSQL_TRANSIENT_CONNECT_SQLSTATE_PREFIXES
)
_ALL_MESSAGE_PREFIXES: Tuple[str, ...] = (
    PG_TRANSIENT_CONNECT_MESSAGE_PREFIXES
    + MYSQL_TRANSIENT_CONNECT_MESSAGE_PREFIXES
)

# ───── Retry budget defaults ───────────────────────────────────────────
# Exponential backoff with cap, plus equal-jitter to spread retries
# from concurrent callers (multiple pool connections reconnecting after
# the same Aurora failover would otherwise stampede on identical
# wall-clock intervals).
#
# With these defaults the per-attempt schedule (max-jitter values) is:
#   1, 1.5, 2.25, 3.4, 5.1, 7.6, 11.4, 17.1, 25.6, 30 (cap)
# Mean total budget across all 10 attempts is ~79s; worst-case is ~105s.
# This comfortably covers Aurora's documented 30-60s post-failover
# window plus outliers (multi-region promotions, slow boots) without
# growing the cap unnecessarily.
DEFAULT_MAX_ATTEMPTS: int = 10
DEFAULT_INITIAL_BACKOFF_S: float = 1.0
DEFAULT_BACKOFF_MULTIPLIER: float = 1.5
DEFAULT_MAX_BACKOFF_S: float = 30.0


def compute_backoff(
        attempt: int,
        initial: float = DEFAULT_INITIAL_BACKOFF_S,
        multiplier: float = DEFAULT_BACKOFF_MULTIPLIER,
        max_backoff: float = DEFAULT_MAX_BACKOFF_S,
        jitter: bool = True,
) -> float:
    """Exponential backoff with cap and equal-jitter. ``attempt`` is 0-indexed.

    With defaults and jitter enabled, the per-attempt sleep falls in the
    range ``[base/2, base]`` where ``base = min(initial * multiplier**attempt,
    max_backoff)``::

        attempt 0 →  0.5s …  1.0s
        attempt 1 →  1.0s …  2.0s
        attempt 2 →  2.0s …  4.0s
        attempt 3 →  4.0s …  8.0s
        attempt 4 →  8.0s … 16.0s
        attempt 5 → 15.0s … 30.0s    (capped at max_backoff)

    Equal-jitter (``base/2 + random.uniform(0, base/2)``) per AWS
    Architecture Blog "Exponential Backoff And Jitter" — guarantees a
    minimum wait equal to half of the deterministic backoff while
    spreading concurrent retries over the upper half of each window.

    Pass ``jitter=False`` for tests or for callers that need deterministic
    sleep durations.
    """
    if attempt < 0:
        attempt = 0
    base = min(initial * (multiplier ** attempt), max_backoff)
    if not jitter:
        return base
    return base / 2 + random.uniform(0, base / 2)


def is_transient_connect_error(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` looks like a transient connect failure
    that should trigger retry-with-backoff at the call site.

    Checks in this order (cheapest first):

      1. ``errno`` (MySQL-style numeric error code)
      2. exact-match SQLSTATE (PG-specific codes)
      3. SQLSTATE class prefix (covers both PG ``"08"`` and MySQL ``"HY"``)
      4. message-prefix matching for libpq/connector wire errors that
         arrive without a SQLSTATE

    Note on psycopg: ``psycopg.Error`` exceptions sometimes carry
    ``args == ()`` and place the useful info on ``.diag`` / ``.sqlstate`` /
    ``.pgcode``. The SQLSTATE branch (#2/#3) handles that case — do NOT
    add an ``args[0]``-only check above it, that would skip the SQLSTATE
    match for psycopg and miscount the transient-error class.
    """
    # 1) errno (mysql-connector)
    errno = getattr(exc, "errno", None)
    if isinstance(errno, int) and errno in _ALL_ERRNOS:
        return True

    # 2-3) SQLSTATE (psycopg uses ``sqlstate``; some libraries set ``pgcode``)
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if sqlstate is not None:
        if sqlstate in _ALL_SQLSTATES:
            return True
        if any(sqlstate.startswith(p) for p in _ALL_SQLSTATE_PREFIXES):
            return True

    # 4) message-prefix match
    msg = str(exc.args[0]) if exc.args else str(exc)
    return any(msg.startswith(p) for p in _ALL_MESSAGE_PREFIXES)
