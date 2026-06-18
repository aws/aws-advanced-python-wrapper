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

"""Unit tests for transient connect-error classification."""

from __future__ import annotations

import pytest

from aws_advanced_python_wrapper.utils.transient_connect import \
    is_transient_connect_error


class _PgError(Exception):
    """Mimics psycopg.Error's attribute shape (sqlstate may be None)."""

    def __init__(self, message: str, sqlstate=None):
        super().__init__(message)
        self.sqlstate = sqlstate


class _MysqlError(Exception):
    """Mimics mysql-connector / pymysql error shape (errno + sqlstate)."""

    def __init__(self, message: str, errno=None, sqlstate=None):
        super().__init__(message)
        self.errno = errno
        self.sqlstate = sqlstate


# ───── PostgreSQL ─────────────────────────────────────────────────────
@pytest.mark.parametrize("sqlstate", ["08006", "08001", "08003", "08P01"])
def test_pg_class_08_is_transient(sqlstate):
    assert is_transient_connect_error(_PgError("boom", sqlstate=sqlstate))


@pytest.mark.parametrize("sqlstate", ["57P01", "57P02", "57P03"])
def test_pg_admin_shutdown_codes_transient(sqlstate):
    assert is_transient_connect_error(_PgError("starting up", sqlstate=sqlstate))


def test_pg_database_dropped_not_transient():
    # 57P04 is database_dropped -- permanent, must NOT retry.
    assert not is_transient_connect_error(_PgError("db dropped", sqlstate="57P04"))


@pytest.mark.parametrize("msg", [
    "connection failed: connection to server at \"10.0.0.1\" failed: "
    "server closed the connection unexpectedly",
    "consuming input failed: SSL error: unexpected eof while reading",
    "the connection is closed",
    "connection socket closed",
])
def test_pg_wire_messages_transient(msg):
    # libpq wire errors arrive with no SQLSTATE -- classified by message.
    assert is_transient_connect_error(_PgError(msg, sqlstate=None))


def test_pg_connect_timeout_is_transient():
    # Regression: psycopg raises ConnectionTimeout("connection timeout
    # expired") with NO sqlstate while Aurora promotes a new writer. Must be
    # transient so retry/poll loops keep waiting instead of bailing early.
    assert is_transient_connect_error(
        _PgError("connection timeout expired", sqlstate=None))


def test_pg_auth_failure_not_transient():
    # Credential misconfiguration must fail fast (28P01 = invalid_password).
    assert not is_transient_connect_error(
        _PgError("password authentication failed for user", sqlstate="28P01"))


# ───── MySQL ──────────────────────────────────────────────────────────
@pytest.mark.parametrize("errno", [2002, 2003, 2006, 2013, 2055])
def test_mysql_network_errnos_transient(errno):
    assert is_transient_connect_error(_MysqlError("net", errno=errno))


def test_mysql_access_denied_not_transient():
    # 1045 = ER_ACCESS_DENIED_ERROR -- a credential error, not transient.
    assert not is_transient_connect_error(
        _MysqlError("Access denied for user", errno=1045, sqlstate="28000"))
