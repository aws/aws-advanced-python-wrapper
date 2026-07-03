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

"""Unit tests for :class:`AsyncStaleDnsPlugin`.

Covers the six load-bearing branches ported from sync:

1. Non-writer-cluster hostname passes through with no DNS lookup.
2. Writer-cluster host + writer role + matching DNS returns original conn.
3. Writer-cluster host + reader role -> force-refresh + swap.
4. Writer-cluster host + writer role + mismatched DNS -> swap.
5. Writer not in allowed topology -> raises AwsWrapperError.
6. ``notify_host_list_changed`` with ``CONVERTED_TO_READER`` resets cache.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.aio.stale_dns_plugin import \
    AsyncStaleDnsPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Helpers -----------------------------------------------------------


def _build(
        all_hosts: tuple = (),
        role: HostRole = HostRole.WRITER,
        host_list_provider: Optional[Any] = None):
    props = Properties({
        "host": "my-cluster.cluster-XYZ.us-east-1.rds.amazonaws.com",
        "port": "5432",
    })
    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(return_value=MagicMock(name="writer_conn"))
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(props, driver_dialect)
    svc._all_hosts = all_hosts

    # Always install a non-static host list provider so the dynamic
    # provider guard passes. Tests that want to exercise the guard
    # install a real AsyncStaticHostListProvider themselves.
    if host_list_provider is None:
        host_list_provider = MagicMock()
    svc._host_list_provider = host_list_provider

    # Patch get_host_role to return the desired role without hitting a
    # real DatabaseDialect (AsyncPluginServiceImpl.get_host_role
    # requires one set). Use type: ignore[method-assign] for mypy --
    # we're intentionally overriding bound methods for the test.
    svc.get_host_role = AsyncMock(return_value=role)  # type: ignore[method-assign]
    svc.refresh_host_list = AsyncMock()  # type: ignore[method-assign]
    svc.force_refresh_host_list = AsyncMock()  # type: ignore[method-assign]
    # The fresh-writer connection is opened via ``plugin_service.connect``
    # which routes through the full plugin pipeline. Patch the bound
    # method so tests can assert without wiring a real plugin manager.
    svc.connect = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(name="writer_conn"))

    plugin = AsyncStaleDnsPlugin(svc)
    return plugin, svc, driver_dialect


def _cluster_host() -> HostInfo:
    # RdsUtils.is_writer_cluster_dns matches the "cluster-" group.
    return HostInfo(
        host="my-cluster.cluster-XYZ.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )


def _writer_instance() -> HostInfo:
    return HostInfo(
        host="my-cluster-inst-1.XYZ.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )


def _reader_instance() -> HostInfo:
    return HostInfo(
        host="my-cluster-inst-2.XYZ.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.READER,
    )


# ---- Subscription ------------------------------------------------------


def test_subscribed_methods_covers_connect_and_notify():
    plugin, *_ = _build()
    subs = plugin.subscribed_methods
    assert "connect" in subs
    assert "notify_host_list_changed" in subs


# ---- 1: non-writer-cluster host passes through -------------------------


def test_non_writer_cluster_host_passes_through_without_dns_lookup():
    plugin, svc, driver_dialect = _build()
    host = HostInfo(host="some-random.example.com", port=5432)
    conn = MagicMock(name="conn")

    async def _connect_func():
        return conn

    with patch("socket.gethostbyname") as mock_dns:
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

    assert result is conn
    mock_dns.assert_not_called()
    driver_dialect.connect.assert_not_awaited()
    svc.get_host_role.assert_not_awaited()


# ---- 2: writer role + matching DNS -> no swap --------------------------


def test_writer_role_matching_dns_returns_original_conn():
    writer = _writer_instance()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,), role=HostRole.WRITER,
    )
    host = _cluster_host()
    conn = MagicMock(name="original_conn")

    async def _connect_func():
        return conn

    # Both resolve to the same IP: no stale DNS, no swap.
    with patch("socket.gethostbyname", return_value="10.0.0.1"):
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

    assert result is conn
    # No fresh writer connection opened through the pipeline.
    svc.connect.assert_not_awaited()
    # Writer role -> plain refresh (not force).
    svc.refresh_host_list.assert_awaited_once()
    svc.force_refresh_host_list.assert_not_awaited()


# ---- 3: reader role -> force-refresh + swap ----------------------------


def test_reader_role_force_refreshes_and_swaps_to_writer():
    writer = _writer_instance()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,), role=HostRole.READER,
    )
    host = _cluster_host()
    stale_conn = MagicMock(name="stale_conn")

    async def _connect_func():
        return stale_conn

    # IPs can match; reader role alone forces the swap.
    with patch("socket.gethostbyname", return_value="10.0.0.5"):
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

    # New connection came from plugin_service.connect (pipeline), not
    # connect_func.
    svc.connect.assert_awaited_once()
    assert result is svc.connect.return_value
    # Reader role -> force-refresh, not plain refresh.
    svc.force_refresh_host_list.assert_awaited_once()
    svc.refresh_host_list.assert_not_awaited()
    # Stale conn aborted.
    driver_dialect.abort_connection.assert_awaited_once_with(stale_conn)
    # initial_connection_host_info updated.
    assert svc.initial_connection_host_info is writer


# ---- 4: mismatched DNS -> swap -----------------------------------------


def test_mismatched_dns_triggers_swap_even_when_role_is_writer():
    writer = _writer_instance()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,), role=HostRole.WRITER,
    )
    host = _cluster_host()
    stale_conn = MagicMock(name="stale_conn")

    async def _connect_func():
        return stale_conn

    # Cluster endpoint resolves to a different IP than the writer
    # instance -> stale DNS.
    def _fake_dns(h):
        return "10.0.0.1" if h == host.host else "10.0.0.99"

    with patch("socket.gethostbyname", side_effect=_fake_dns):
        async def _run():
            return await plugin.connect(
                target_driver_func=MagicMock(),
                driver_dialect=driver_dialect,
                host_info=host,
                props=svc.props,
                is_initial_connection=False,
                connect_func=_connect_func,
            )

        result = asyncio.run(_run())

    svc.connect.assert_awaited_once()
    assert result is svc.connect.return_value
    driver_dialect.abort_connection.assert_awaited_once_with(stale_conn)
    # is_initial_connection=False -> initial_connection_host_info stays None.
    assert svc.initial_connection_host_info is None


# ---- 5: writer not in topology -> raises -------------------------------


def test_writer_not_in_topology_raises():
    # all_hosts contains only the reader; writer candidate is the
    # separately-constructed writer instance absent from allowed.
    writer = _writer_instance()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,), role=HostRole.READER,
    )

    # Clear allowed hosts AFTER _pick_writer runs by replacing
    # _pick_writer's view: prime writer_host_info but empty the allowed
    # topology used in _contains_host_port.
    plugin._writer_host_info = writer
    plugin._writer_host_address = "10.0.0.99"
    svc._all_hosts = ()  # writer not in allowed topology

    host = _cluster_host()
    stale_conn = MagicMock(name="stale_conn")

    async def _connect_func():
        return stale_conn

    def _fake_dns(h):
        return "10.0.0.1" if h == host.host else "10.0.0.99"

    with patch("socket.gethostbyname", side_effect=_fake_dns):
        async def _run():
            return await plugin.connect(
                target_driver_func=MagicMock(),
                driver_dialect=driver_dialect,
                host_info=host,
                props=svc.props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )

        with pytest.raises(AwsWrapperError):
            asyncio.run(_run())


# ---- 6: notify_host_list_changed resets cache --------------------------


def test_notify_host_list_changed_resets_on_converted_to_reader():
    plugin, svc, _ = _build()
    writer = _writer_instance()
    plugin._writer_host_info = writer
    plugin._writer_host_address = "10.0.0.99"

    # Sync keys by host_info.url (host:port/). Match that.
    changes = {
        writer.url: {HostEvent.CONVERTED_TO_READER},
    }
    plugin.notify_host_list_changed(changes)

    assert plugin._writer_host_info is None
    assert plugin._writer_host_address is None


def test_notify_host_list_changed_ignores_unrelated_events():
    plugin, _, _ = _build()
    writer = _writer_instance()
    plugin._writer_host_info = writer
    plugin._writer_host_address = "10.0.0.99"

    changes = {
        writer.url: {HostEvent.HOST_ADDED},
    }
    plugin.notify_host_list_changed(changes)

    # Cache still primed.
    assert plugin._writer_host_info is writer
    assert plugin._writer_host_address == "10.0.0.99"


def test_notify_host_list_changed_noop_when_writer_not_tracked():
    plugin, _, _ = _build()
    # writer_host_info is None; should not raise.
    changes = {"anything": {HostEvent.CONVERTED_TO_READER}}
    plugin.notify_host_list_changed(changes)
    assert plugin._writer_host_info is None


# ---- Guard: static host list provider rejected -------------------------


def test_static_host_list_provider_raises_on_connect():
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncStaticHostListProvider
    props = Properties({"host": "h", "port": "5432"})
    static_hlp = AsyncStaticHostListProvider(props)
    plugin, svc, driver_dialect = _build(host_list_provider=static_hlp)

    host = _cluster_host()

    async def _connect_func():
        return MagicMock(name="conn")

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
