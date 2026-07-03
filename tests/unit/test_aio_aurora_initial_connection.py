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

"""Unit tests for :class:`AsyncAuroraInitialConnectionStrategyPlugin`.

Covers the load-bearing branches ported from sync:

1. Non-RDS-cluster URL passes through with no verification.
2. Writer cluster URL + already connected to writer -> returns original conn.
3. Writer cluster URL + connected to reader -> retries via writer instance.
4. Reader cluster URL + connected to reader -> returns conn.
5. Reader cluster URL + no readers in topology -> returns writer conn
   (simulated Aurora reader fallback).
6. Timeout exhausted -> returns None -> falls back to plain connect_func.
7. READER_INITIAL_HOST_SELECTOR_STRATEGY not supported -> raises AwsWrapperError.
8. Reader probe failure on non-login exception -> marks host UNAVAILABLE.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_initial_connection_strategy_plugin import \
    AsyncAuroraInitialConnectionStrategyPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Helpers -----------------------------------------------------------


_WRITER_CLUSTER = "my-cluster.cluster-XYZ.us-east-1.rds.amazonaws.com"
_READER_CLUSTER = "my-cluster.cluster-ro-XYZ.us-east-1.rds.amazonaws.com"
_WRITER_INSTANCE = "my-cluster-inst-1.XYZ.us-east-1.rds.amazonaws.com"
_READER_INSTANCE = "my-cluster-inst-2.XYZ.us-east-1.rds.amazonaws.com"


def _writer_host() -> HostInfo:
    return HostInfo(host=_WRITER_INSTANCE, port=5432, role=HostRole.WRITER)


def _reader_host() -> HostInfo:
    return HostInfo(host=_READER_INSTANCE, port=5432, role=HostRole.READER)


def _cluster_host_info(host: str = _WRITER_CLUSTER) -> HostInfo:
    return HostInfo(host=host, port=5432, role=HostRole.WRITER)


def _build(
        all_hosts: Tuple[HostInfo, ...] = (),
        role: HostRole = HostRole.WRITER,
        accepts_strategy_result: bool = True,
        strategy_pick: Optional[HostInfo] = None,
        props_overrides: Optional[dict] = None):
    props = Properties({
        "host": _WRITER_CLUSTER,
        "port": "5432",
        # Keep retry bounds short so "exhaustion" tests don't burn
        # real wall-clock time.
        "open_connection_retry_timeout_ms": "100",
        "open_connection_retry_interval_ms": "10",
    })
    if props_overrides:
        for k, v in props_overrides.items():
            props[k] = v
    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(name="direct_conn")
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(props, driver_dialect)
    svc._all_hosts = all_hosts

    # Patch async plugin-service surface used by the plugin.
    svc.get_host_role = AsyncMock(return_value=role)  # type: ignore[method-assign]
    svc.force_refresh_host_list = AsyncMock()  # type: ignore[method-assign]
    svc.accepts_strategy = MagicMock(  # type: ignore[method-assign]
        return_value=accepts_strategy_result)
    svc.get_host_info_by_strategy = MagicMock(  # type: ignore[method-assign]
        return_value=strategy_pick)
    svc.is_login_exception = MagicMock(return_value=False)  # type: ignore[method-assign]
    svc.set_availability = MagicMock()  # type: ignore[method-assign]
    # _open_direct routes fresh instance connects through the plugin
    # pipeline via ``plugin_service.connect``. Replace it with the same
    # AsyncMock the old pipeline-bypass tests used for
    # ``driver_dialect.connect``, so existing test configuration
    # (return_value / side_effect on driver_dialect.connect) carries
    # over unchanged.
    svc.connect = driver_dialect.connect  # type: ignore[method-assign]

    plugin = AsyncAuroraInitialConnectionStrategyPlugin(svc)
    return plugin, svc, driver_dialect


async def _noop_connect_func_return(conn: Any):
    return conn


# ---- 1. Non-RDS-cluster URL passes through -----------------------------


def test_non_rds_cluster_host_passes_through():
    plugin, svc, driver_dialect = _build()
    host = HostInfo(host="some-random.example.com", port=5432)
    expected = MagicMock(name="conn")

    async def _connect_func():
        return expected

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is expected
    # No verification path taken.
    driver_dialect.connect.assert_not_awaited()
    svc.get_host_role.assert_not_awaited()
    # initial_connection_host_info untouched (plugin didn't set it).
    assert svc.initial_connection_host_info is None


# ---- 2. Writer cluster URL + already writer -> direct writer conn ------


def test_writer_cluster_already_writer_returns_direct_conn():
    writer = _writer_host()
    # Plugin picks writer directly from topology (non-cluster DNS), opens
    # driver_dialect.connect, verifies role -> WRITER -> returns that conn.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
        role=HostRole.WRITER,
    )
    writer_conn = MagicMock(name="writer_direct_conn")
    driver_dialect.connect.return_value = writer_conn

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock(name="cluster_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is writer_conn
    driver_dialect.connect.assert_awaited_once()
    assert svc.initial_connection_host_info is writer


# ---- 3. Writer cluster URL + connected to reader -> retry --------------


def test_writer_cluster_connected_to_reader_retries_and_swaps():
    writer = _writer_host()
    # First direct-connect lands on a reader, second lands on a writer.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
    )
    first_conn = MagicMock(name="reader_conn_first")
    second_conn = MagicMock(name="writer_conn_second")
    driver_dialect.connect.side_effect = [first_conn, second_conn]
    # get_host_role returns READER then WRITER.
    svc.get_host_role.side_effect = [HostRole.READER, HostRole.WRITER]

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock()

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is second_conn
    # First (stale) conn was aborted.
    driver_dialect.abort_connection.assert_any_await(first_conn)
    # Topology force-refreshed on the bad conn.
    svc.force_refresh_host_list.assert_awaited()
    # initial_connection_host_info -> writer from topology.
    assert svc.initial_connection_host_info is writer


# ---- 4. Reader cluster URL + connected to reader -> returns conn -------


def test_reader_cluster_connected_to_reader_returns_conn():
    reader = _reader_host()
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    reader_conn = MagicMock(name="reader_conn")
    driver_dialect.connect.return_value = reader_conn

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock(name="cluster_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is reader_conn
    assert svc.initial_connection_host_info is reader


# ---- 5. Reader cluster URL + no readers -> writer fallback -------------


def test_reader_cluster_no_readers_returns_writer_fallback():
    writer = _writer_host()
    # Topology has only a writer. _pick_reader returns None
    # (strategy_pick=None), so the plugin falls through the "topology
    # stale" branch, opens via connect_func, probes, and since no
    # readers exist it returns that connection unmodified.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
        role=HostRole.WRITER,  # connect_func-opened conn reports WRITER
        strategy_pick=None,
    )
    cluster_conn = MagicMock(name="cluster_conn")

    async def _connect_func():
        return cluster_conn

    host = _cluster_host_info(_READER_CLUSTER)

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is cluster_conn
    # No-readers fallback sets initial_connection_host_info to the
    # writer (via _pick_writer).
    assert svc.initial_connection_host_info is writer


# ---- 6. Timeout exhausted -> falls back to plain connect_func ----------


def test_timeout_exhausted_falls_back_to_plain_connect_func():
    writer = _writer_host()
    # Plugin always sees role=READER on a writer-cluster URL, so every
    # retry fails and timeout kicks in. After the verified_writer path
    # returns None, the plugin falls back to connect_func().
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
        role=HostRole.READER,  # never a WRITER -> loop exhausts
    )
    driver_dialect.connect.return_value = MagicMock(name="reader_direct")
    fallback_conn = MagicMock(name="fallback_conn")

    async def _connect_func():
        return fallback_conn

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    # Verified path exhausted -> plain connect_func call returned.
    assert result is fallback_conn


# ---- 7. Unsupported strategy -> AwsWrapperError ------------------------


def test_unsupported_reader_strategy_raises():
    reader = _reader_host()
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, reader),
        role=HostRole.READER,
        accepts_strategy_result=False,  # reject the strategy
    )

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock()

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_run())


# ---- 8. Reader non-login exception -> host marked UNAVAILABLE ----------


def test_reader_non_login_exception_marks_unavailable():
    reader = _reader_host()
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    # driver_dialect.connect raises on every direct attempt -> reader
    # candidate gets marked UNAVAILABLE each iteration until the loop
    # exits. is_login_exception is already mocked to return False.
    driver_dialect.connect.side_effect = RuntimeError("network-down")

    host = _cluster_host_info(_READER_CLUSTER)
    fallback_conn = MagicMock(name="fallback_conn")

    async def _connect_func():
        return fallback_conn

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())

    # Timeout expired -> plain connect_func fallback.
    assert result is fallback_conn
    # At least one UNAVAILABLE mark was written for the picked reader.
    assert svc.set_availability.call_count >= 1
    called_aliases = svc.set_availability.call_args_list[0][0][0]
    assert reader.host in "".join(called_aliases)
    assert svc.set_availability.call_args_list[0][0][1] == \
        HostAvailability.UNAVAILABLE
