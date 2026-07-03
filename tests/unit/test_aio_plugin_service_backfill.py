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

"""Tests for AsyncPluginService method backfill (K.4)."""

from __future__ import annotations

import asyncio
from typing import Any, List, Optional

import pytest

from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
    AllowedAndBlockedHosts
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


class _FakeDriverDialect:
    def __init__(self, in_txn: bool = False) -> None:
        self._in_txn = in_txn
        self.network_bound_methods = {"*"}

    async def is_in_transaction(self, conn: Any) -> bool:
        return self._in_txn

    async def transfer_session_state(self, from_conn, to_conn):
        pass


class _FakeDatabaseDialect:
    host_id_query = "SELECT aurora_db_instance_identifier()"
    default_port = 5432
    is_reader_query = "SELECT pg_is_in_recovery()"


class _FakeCursor:
    def __init__(self, row) -> None:
        self._row = row

    async def execute(self, query: str) -> None:
        pass

    async def fetchone(self):
        return self._row

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, row=("instance-1",)) -> None:
        self._row = row

    def cursor(self):
        return _FakeCursor(self._row)


class _FakeHostListProvider:
    def __init__(self, topology: List[HostInfo]) -> None:
        self._topology = topology
        self.refresh_calls = 0
        self.force_refresh_calls = 0

    async def refresh(self, conn: Any):
        self.refresh_calls += 1
        return tuple(self._topology)

    async def force_refresh(self, conn: Any):
        self.force_refresh_calls += 1
        return tuple(self._topology)


def _make_service(
        in_txn: bool = False,
        conn: Optional[Any] = None) -> AsyncPluginServiceImpl:
    props = Properties()
    svc = AsyncPluginServiceImpl(props, _FakeDriverDialect(in_txn))  # type: ignore[arg-type]
    svc._current_connection = conn
    svc._database_dialect = _FakeDatabaseDialect()  # type: ignore[assignment]
    return svc


# ----- allowed_and_blocked_hosts ---------------------------------------


def test_allowed_and_blocked_hosts_default_is_none() -> None:
    svc = _make_service()
    assert svc.allowed_and_blocked_hosts is None


def test_allowed_and_blocked_hosts_setter_round_trip() -> None:
    svc = _make_service()
    filter_ = AllowedAndBlockedHosts(
        allowed_host_ids={"a", "b"},
        blocked_host_ids={"c"},
    )
    svc.allowed_and_blocked_hosts = filter_
    assert svc.allowed_and_blocked_hosts is filter_


def test_allowed_and_blocked_hosts_can_be_cleared() -> None:
    svc = _make_service()
    svc.allowed_and_blocked_hosts = AllowedAndBlockedHosts(
        allowed_host_ids=set(), blocked_host_ids=set())
    svc.allowed_and_blocked_hosts = None
    assert svc.allowed_and_blocked_hosts is None


# ----- is_in_transaction / update_in_transaction ---------------


def test_is_in_transaction_default_false() -> None:
    svc = _make_service()
    assert svc.is_in_transaction is False


def test_update_in_transaction_explicit_value_sets_flag() -> None:
    svc = _make_service()
    asyncio.run(svc.update_in_transaction(True))
    assert svc.is_in_transaction is True
    asyncio.run(svc.update_in_transaction(False))
    assert svc.is_in_transaction is False


def test_update_in_transaction_queries_driver_when_no_explicit_value() -> None:
    svc = _make_service(in_txn=True, conn=_FakeConn())
    asyncio.run(svc.update_in_transaction())
    assert svc.is_in_transaction is True


def test_update_in_transaction_raises_without_connection() -> None:
    svc = _make_service(conn=None)
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.update_in_transaction())


# ----- update_driver_dialect (sync-parity no-op) --------------------


def test_update_driver_dialect_is_noop() -> None:
    svc = _make_service()
    # Must not raise and must leave driver_dialect unchanged.
    prev = svc.driver_dialect
    svc.update_driver_dialect(object())
    assert svc.driver_dialect is prev


# ----- identify_connection ----------------------------------------


def test_identify_connection_returns_topology_match() -> None:
    conn = _FakeConn(("instance-1",))
    svc = _make_service(conn=conn)
    hosts = [
        HostInfo("instance-1.cluster.rds", host_id="instance-1",
                 role=HostRole.WRITER),
        HostInfo("instance-2.cluster.rds", host_id="instance-2",
                 role=HostRole.READER),
    ]
    for h in hosts:
        h.set_availability(HostAvailability.AVAILABLE)
    svc.host_list_provider = _FakeHostListProvider(hosts)  # type: ignore[assignment]

    result = asyncio.run(svc.identify_connection())
    assert result is not None
    assert result.host_id == "instance-1"


def test_identify_connection_falls_back_to_force_refresh() -> None:
    conn = _FakeConn(("instance-2",))
    svc = _make_service(conn=conn)
    hosts = [
        HostInfo("instance-2.cluster.rds", host_id="instance-2",
                 role=HostRole.READER),
    ]
    provider = _FakeHostListProvider(hosts)
    svc.host_list_provider = provider  # type: ignore[assignment]

    asyncio.run(svc.identify_connection())
    # refresh hit first; no fallback needed when match is found.
    assert provider.refresh_calls == 1
    assert provider.force_refresh_calls == 0


def test_identify_connection_forces_refresh_on_miss() -> None:
    """When refresh() returns a topology that doesn't include the
    connection's instance, we force_refresh as a last try."""
    conn = _FakeConn(("instance-unknown",))
    svc = _make_service(conn=conn)
    hosts = [
        HostInfo("instance-other.cluster.rds", host_id="instance-other",
                 role=HostRole.READER),
    ]
    provider = _FakeHostListProvider(hosts)
    svc.host_list_provider = provider  # type: ignore[assignment]

    result = asyncio.run(svc.identify_connection())
    assert result is None
    assert provider.refresh_calls == 1
    assert provider.force_refresh_calls == 1


def test_identify_connection_requires_connection() -> None:
    svc = _make_service(conn=None)
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.identify_connection())


def test_identify_connection_returns_none_without_host_list_provider() -> None:
    conn = _FakeConn()
    svc = _make_service(conn=conn)
    svc.host_list_provider = None  # type: ignore[assignment]
    result = asyncio.run(svc.identify_connection())
    assert result is None


def test_identify_connection_returns_none_without_database_dialect() -> None:
    conn = _FakeConn()
    svc = _make_service(conn=conn)
    svc._database_dialect = None
    result = asyncio.run(svc.identify_connection())
    assert result is None
