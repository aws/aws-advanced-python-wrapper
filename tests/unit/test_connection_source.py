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
    CallerConnectionSource, PluginConnectionSource)


@pytest.fixture
def conn():
    # The caller's connection: a source that commits, rolls back or closes it fails the test.
    raw = MagicMock()
    for name in ("commit", "rollback", "close"):
        getattr(raw, name).side_effect = AssertionError(f"the caller source must not {name}")
    return raw


class TestConnectionSources:
    def test_caller_source_yields_the_same_connection(self, conn):
        with CallerConnectionSource(conn).connection() as c:
            assert c is conn

    def test_plugin_source_opens_an_independent_autocommit_connection_and_closes_it(self):
        service = MagicMock()
        service.props = {"plugins": "kms_encryption", "user": "u"}
        raw = MagicMock()
        service.force_connect.return_value = raw
        plugin = object()
        with PluginConnectionSource(service, plugin).connection() as c:
            assert c is raw
            host, props, skip = service.force_connect.call_args.args
            assert host is service.current_host_info and skip is plugin
            assert props == service.props and props is not service.props
            service.driver_dialect.set_autocommit.assert_called_once_with(raw, True)
        service.driver_dialect.execute.assert_called_once()  # close routed through the dialect

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
    def test_statement_shares_one_connection_and_closes_it_at_the_end(self, service, raws, source):
        with source.statement():
            for _ in range(3):
                with source.connection() as c:
                    assert c is raws[0]
            raws[0].close.assert_not_called()
        raws[0].close.assert_called_once()
        assert service.force_connect.call_count == 1
        service.driver_dialect.set_autocommit.assert_called_once_with(raws[0], True)

    def test_statement_closes_its_connection_when_a_lookup_fails(self, raws, source):
        with pytest.raises(RuntimeError, match="query failed"):
            with source.statement():
                with source.connection():
                    raise RuntimeError("query failed")
        raws[0].close.assert_called_once()
        # The failed scope has ended, so a lookup gets a connection of its own again.
        with source.connection() as c:
            assert c is raws[1]
        raws[1].close.assert_called_once()

    def test_statement_without_a_lookup_opens_nothing(self, service, source):
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

    def test_nested_statement_keeps_the_outer_connection(self, service, raws, source):
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

    def test_autocommit_failure_in_a_statement_closes_the_half_opened_connection(self, service, raws, source):
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
