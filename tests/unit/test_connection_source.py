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

import threading
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.encryption.connection_source import (
    CallerConnectionSource, PluginConnectionSource, fetch_rows, run_statement)
from aws_advanced_python_wrapper.errors import QueryTimeoutError
from aws_advanced_python_wrapper.pg_driver_dialect import PgDriverDialect
from aws_advanced_python_wrapper.utils.decorators import \
    mark_connection_abandoned
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def conn():
    # The caller's connection: a source that commits, rolls back or closes it fails the test.
    raw = MagicMock()
    for name in ("commit", "rollback", "close"):
        getattr(raw, name).side_effect = AssertionError(f"the caller source must not {name}")
    return raw


class TestConnectionSources:
    def test_caller_source_yields_same_connection(self, conn):
        with CallerConnectionSource(conn).connection() as c:
            assert c is conn

    def test_plugin_source_opens_autocommit_connection_and_closes_it(self):
        service = MagicMock()
        service.props = {"plugins": "kms_encryption", "user": "u"}
        raw = MagicMock()
        service.force_connect.return_value = raw
        plugin = object()
        with PluginConnectionSource(service, plugin).connection() as c:
            assert c is raw
            host, props, skip = service.force_connect.call_args.args
            assert host is service.current_host_info
            assert skip is plugin
            assert props == service.props
            assert props is not service.props
            service.driver_dialect.set_autocommit.assert_called_once_with(raw, True)
        # The close runs through the dialect.
        service.driver_dialect.execute.assert_called_once()

    def test_plugin_source_closes_on_error(self):
        service = MagicMock()
        service.props = {}
        with pytest.raises(RuntimeError):
            with PluginConnectionSource(service).connection():
                raise RuntimeError("query failed")
        service.driver_dialect.execute.assert_called_once()


@pytest.fixture
def raws():
    return [MagicMock(name="first"), MagicMock(name="second")]


@pytest.fixture
def service(raws):
    service = MagicMock()
    service.props = {}
    service.force_connect.side_effect = raws
    # Run the close the source routes through the dialect, so each connection records its own.
    service.driver_dialect.execute.side_effect = lambda method_name, func, **kwargs: func()
    return service


@pytest.fixture
def source(service):
    return PluginConnectionSource(service)


class TestStatementScope:
    def test_statement_shares_one_connection_and_closes_it(self, service, raws, source):
        with source.statement():
            for _ in range(3):
                with source.connection() as c:
                    assert c is raws[0]
            raws[0].close.assert_not_called()
        raws[0].close.assert_called_once()
        assert service.force_connect.call_count == 1
        service.driver_dialect.set_autocommit.assert_called_once_with(raws[0], True)

    def test_statement_lookup_failure_closes_connection(self, raws, source):
        with pytest.raises(RuntimeError, match="query failed"):
            with source.statement():
                with source.connection():
                    raise RuntimeError("query failed")
        raws[0].close.assert_called_once()
        # The failed scope has ended, so a lookup gets a connection of its own again.
        with source.connection() as c:
            assert c is raws[1]
        raws[1].close.assert_called_once()

    def test_statement_without_lookup_opens_nothing(self, service, source):
        with source.statement():
            pass
        service.force_connect.assert_not_called()
        service.driver_dialect.execute.assert_not_called()

    def test_statement_is_per_thread(self, raws, source):
        seen: List[Any] = []

        def lookup():
            with source.connection() as c:
                seen.append(c)

        with source.statement():
            with source.connection():
                pass
            other = threading.Thread(target=lookup, daemon=True)
            other.start()
            other.join(5)
            assert not other.is_alive()
            assert seen == [raws[1]]
            raws[1].close.assert_called_once()
            raws[0].close.assert_not_called()
        raws[0].close.assert_called_once()

    def test_nested_statement_keeps_outer_connection(self, service, raws, source):
        with source.statement():
            with source.connection() as outer:
                pass
            with source.statement():
                with source.connection() as inner:
                    assert inner is outer
            raws[0].close.assert_not_called()
            with source.connection() as after:
                assert after is outer
        raws[0].close.assert_called_once()
        assert service.force_connect.call_count == 1

    def test_statement_autocommit_failure_closes_connection(self, service, raws, source):
        service.driver_dialect.set_autocommit.side_effect = [RuntimeError("autocommit failed"), None]
        with source.statement():
            with pytest.raises(RuntimeError, match="autocommit failed"):
                with source.connection():
                    pass
            raws[0].close.assert_called_once()
            with source.connection() as c:
                assert c is raws[1]
            raws[1].close.assert_not_called()
        raws[1].close.assert_called_once()


def test_connection_abandoned_connection_left_open(raws, source):
    with source.connection() as c:
        mark_connection_abandoned(c)
    raws[0].close.assert_not_called()


def test_statement_abandoned_connection_left_open(raws, source):
    with source.statement():
        with source.connection() as c:
            mark_connection_abandoned(c)
    raws[0].close.assert_not_called()


@pytest.fixture
def dialect():
    return PgDriverDialect(Properties())


def test_fetch_rows_lower_cases_keys_and_handles_no_result(dialect, mocker):
    conn = mocker.MagicMock()
    cursor = conn.cursor.return_value
    cursor.description = [("TABLE_NAME",), ("column_name",)]
    cursor.fetchall.return_value = [("t", "c")]
    assert fetch_rows(dialect, conn, "SELECT") == [{"table_name": "t", "column_name": "c"}]

    cursor.description = None
    assert fetch_rows(dialect, conn, "UPDATE") == []


def test_fetch_rows_accepts_dict_rows(dialect, mocker):
    # A connection opened with a dict-like row factory (psycopg dict_row)
    # returns mappings; their keys are lower-cased like tuple rows' names.
    conn = mocker.MagicMock()
    cursor = conn.cursor.return_value
    cursor.description = [("TABLE_NAME",), ("column_name",)]
    cursor.fetchall.return_value = [{"TABLE_NAME": "t", "column_name": "c"}]
    assert fetch_rows(dialect, conn, "SELECT") == [{"table_name": "t", "column_name": "c"}]


def test_run_statement_returns_affected_rows(dialect, mocker):
    conn = mocker.MagicMock()
    conn.cursor.return_value.rowcount = -1
    assert run_statement(dialect, conn, "DELETE FROM t WHERE id = %s", (1,)) == 0

    conn.cursor.return_value.rowcount = 2
    assert run_statement(dialect, conn, "DELETE FROM t WHERE id = %s", (1,)) == 2


def test_fetch_rows_execute_times_out_with_socket_timeout(mocker, timeout_dialect, wait_then_return):
    dialect, event = timeout_dialect
    conn = mocker.MagicMock()
    conn.cursor.return_value.execute.side_effect = wait_then_return(event, None)
    with pytest.raises(QueryTimeoutError):
        fetch_rows(dialect, conn, "SELECT 1")


def test_fetch_rows_fetch_times_out_with_socket_timeout(mocker, timeout_dialect, wait_then_return):
    dialect, event = timeout_dialect
    conn = mocker.MagicMock()
    cursor = conn.cursor.return_value
    cursor.description = [("id",)]
    cursor.fetchall.side_effect = wait_then_return(event, [])
    with pytest.raises(QueryTimeoutError):
        fetch_rows(dialect, conn, "SELECT 1")


def test_run_statement_times_out_with_socket_timeout(mocker, timeout_dialect, wait_then_return):
    dialect, event = timeout_dialect
    conn = mocker.MagicMock()
    conn.cursor.return_value.execute.side_effect = wait_then_return(event, None)
    conn.cursor.return_value.rowcount = 1
    with pytest.raises(QueryTimeoutError):
        run_statement(dialect, conn, "DELETE FROM t")
