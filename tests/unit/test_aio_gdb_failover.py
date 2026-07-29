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

"""Unit tests for the async GDB (Aurora Global Database) failover plugin.

Async counterpart of the sync ``GdbFailoverPlugin`` behavior: region-aware
failover that, on a writer change, picks a target according to the
``GdbFailoverMode`` configured for the writer's region (home vs. inactive).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.gdb_failover_plugin import \
    AsyncGdbFailoverPlugin
from aws_advanced_python_wrapper.aio.plugin_factory import (
    PLUGIN_FACTORIES, PLUGIN_FACTORY_WEIGHTS)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverFailedError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.gdb_failover_mode import GdbFailoverMode
from aws_advanced_python_wrapper.utils.properties import Properties

_W_EAST = "w.cvqiy8w244sz.us-east-1.rds.amazonaws.com"
_R_EAST = "r1.cvqiy8w244sz.us-east-1.rds.amazonaws.com"
_W_WEST = "w.cvqiy8w244sz.us-west-2.rds.amazonaws.com"
_R_WEST = "r2.cvqiy8w244sz.us-west-2.rds.amazonaws.com"
_WRITER_CLUSTER = "mycluster.cluster-cvqiy8w244sz.us-east-1.rds.amazonaws.com"


def _make_plugin(*, home_region="us-east-1", host=_W_EAST, overrides=None,
                 all_hosts=(), role=HostRole.WRITER):
    props_dict = {
        "host": host, "port": "5432",
        "enable_failover": "true", "failover_timeout_sec": "1.0",
    }
    if home_region is not None:
        props_dict["failover_home_region"] = home_region
    if overrides:
        props_dict.update(overrides)
    props = Properties(props_dict)

    svc = MagicMock()
    svc.props = props
    svc.initial_connection_host_info = HostInfo(host=host, port=5432)
    svc.current_host_info = HostInfo(host=host, port=5432)
    svc.all_hosts = tuple(all_hosts)
    svc.filter_hosts = MagicMock(side_effect=lambda hosts: list(hosts))
    svc.force_refresh_host_list = AsyncMock()
    svc.refresh_host_list = AsyncMock()
    svc.get_host_info_by_strategy = MagicMock(
        side_effect=lambda r, s, hosts: hosts[0] if hosts else None)
    svc.get_host_role = AsyncMock(return_value=role)
    conn = MagicMock(name="new_conn")
    svc.force_connect = AsyncMock(return_value=conn)
    svc.set_current_connection = AsyncMock()
    svc.current_connection = MagicMock(name="old_conn")
    dd = MagicMock()
    dd.abort_connection = AsyncMock()
    svc.driver_dialect = dd

    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=tuple(all_hosts))

    plugin = AsyncGdbFailoverPlugin(svc, hlp, props)
    return plugin, svc, hlp, conn


# ---- _init_failover_mode -----------------------------------------------


def test_init_failover_mode_resolves_home_region_from_prop():
    plugin, *_ = _make_plugin(home_region="us-east-1")
    plugin._init_failover_mode()
    assert plugin._home_region == "us-east-1"


def test_init_failover_mode_resolves_home_region_from_host_when_prop_absent():
    plugin, svc, *_ = _make_plugin(home_region=None, host=_W_WEST)
    svc.initial_connection_host_info = HostInfo(host=_W_WEST, port=5432)
    plugin._init_failover_mode()
    assert plugin._home_region == "us-west-2"


def test_init_failover_mode_raises_when_no_initial_host():
    plugin, svc, *_ = _make_plugin()
    svc.initial_connection_host_info = None
    with pytest.raises(AwsWrapperError):
        plugin._init_failover_mode()


def test_init_failover_mode_raises_when_no_resolvable_home_region():
    plugin, svc, *_ = _make_plugin(home_region=None, host="localhost")
    svc.initial_connection_host_info = HostInfo(host="localhost", port=5432)
    with pytest.raises(AwsWrapperError):
        plugin._init_failover_mode()


def test_init_failover_mode_default_strict_writer_for_writer_cluster_url():
    plugin, svc, *_ = _make_plugin(home_region=None, host=_WRITER_CLUSTER)
    svc.initial_connection_host_info = HostInfo(host=_WRITER_CLUSTER, port=5432)
    plugin._init_failover_mode()
    assert plugin._active_home_failover_mode == GdbFailoverMode.STRICT_WRITER
    assert plugin._inactive_home_failover_mode == GdbFailoverMode.STRICT_WRITER


def test_init_failover_mode_default_home_reader_or_writer_for_instance_url():
    plugin, *_ = _make_plugin(home_region="us-east-1", host=_W_EAST)
    plugin._init_failover_mode()
    assert plugin._active_home_failover_mode == GdbFailoverMode.HOME_READER_OR_WRITER


def test_init_failover_mode_honors_explicit_mode_overrides():
    plugin, *_ = _make_plugin(
        home_region="us-east-1",
        overrides={"active_home_failover_mode": "strict_writer",
                   "inactive_home_failover_mode": "strict_any_reader"})
    plugin._init_failover_mode()
    assert plugin._active_home_failover_mode == GdbFailoverMode.STRICT_WRITER
    assert plugin._inactive_home_failover_mode == GdbFailoverMode.STRICT_ANY_READER


# ---- _is_home_region ---------------------------------------------------


def test_is_home_region_casefold_match():
    plugin, *_ = _make_plugin(home_region="us-east-1")
    plugin._init_failover_mode()
    assert plugin._is_home_region("US-EAST-1") is True
    assert plugin._is_home_region("us-west-2") is False
    assert plugin._is_home_region(None) is False


# ---- _is_strict_writer_failover_mode -----------------------------------


def test_is_strict_writer_failover_mode_region_aware():
    plugin, svc, *_ = _make_plugin(
        home_region="us-east-1",
        overrides={"active_home_failover_mode": "strict_writer",
                   "inactive_home_failover_mode": "strict_any_reader"})
    # Current host in home region -> uses the active mode (STRICT_WRITER).
    svc.current_host_info = HostInfo(host=_W_EAST, port=5432)
    assert plugin._is_strict_writer_failover_mode() is True
    # Current host outside home region -> uses the inactive mode (not strict).
    svc.current_host_info = HostInfo(host=_W_WEST, port=5432)
    assert plugin._is_strict_writer_failover_mode() is False


# ---- _do_failover ------------------------------------------------------


def test_do_failover_strict_writer_sets_writer_connection():
    writer = HostInfo(host=_W_EAST, port=5432, role=HostRole.WRITER)
    reader = HostInfo(host=_R_EAST, port=5432, role=HostRole.READER)
    plugin, svc, _hlp, conn = _make_plugin(
        home_region="us-east-1",
        overrides={"active_home_failover_mode": "strict_writer"},
        all_hosts=(writer, reader), role=HostRole.WRITER)

    asyncio.run(plugin._do_failover(driver_dialect=svc.driver_dialect))

    svc.set_current_connection.assert_awaited()
    bound_conn, bound_host = svc.set_current_connection.call_args.args
    assert bound_conn is conn
    assert bound_host.host == _W_EAST


def test_do_failover_reader_mode_sets_reader_connection():
    writer = HostInfo(host=_W_EAST, port=5432, role=HostRole.WRITER)
    reader = HostInfo(host=_R_EAST, port=5432, role=HostRole.READER)
    plugin, svc, _hlp, conn = _make_plugin(
        home_region="us-east-1",
        overrides={"active_home_failover_mode": "strict_any_reader"},
        all_hosts=(writer, reader), role=HostRole.READER)

    asyncio.run(plugin._do_failover(driver_dialect=svc.driver_dialect))

    svc.set_current_connection.assert_awaited()
    _bound_conn, bound_host = svc.set_current_connection.call_args.args
    assert bound_host.role == HostRole.READER
    assert bound_host.host == _R_EAST


def test_do_failover_raises_when_no_writer_in_topology():
    # No writer ever appears -> _discover_writer loops until the (short) deadline
    # then raises FailoverFailedError without connecting anywhere.
    reader = HostInfo(host=_R_EAST, port=5432, role=HostRole.READER)
    plugin, svc, *_ = _make_plugin(
        home_region="us-east-1",
        overrides={"failover_timeout_sec": "0.3"},
        all_hosts=(reader,))

    with pytest.raises(FailoverFailedError):
        asyncio.run(plugin._do_failover(driver_dialect=svc.driver_dialect))
    svc.set_current_connection.assert_not_called()


def test_do_failover_discovers_writer_that_appears_on_a_later_probe():
    # Right after a failover the new writer often isn't visible on the first
    # probe; the monitor surfaces it a beat later. _do_failover must keep
    # force-probing rather than failing immediately.
    reader = HostInfo(host=_R_EAST, port=5432, role=HostRole.READER)
    writer = HostInfo(host=_W_EAST, port=5432, role=HostRole.WRITER)
    plugin, svc, _hlp, conn = _make_plugin(
        home_region="us-east-1",
        overrides={"active_home_failover_mode": "strict_writer"},
        all_hosts=(reader,), role=HostRole.WRITER)
    plugin._WRITER_DISCOVERY_DELAY_SEC = 0.05  # keep the test fast
    calls = {"n": 0}

    async def _force_refresh(*_a, **_k):
        calls["n"] += 1
        if calls["n"] >= 2:
            svc.all_hosts = (reader, writer)  # writer appears on a later probe

    svc.force_refresh_host_list = AsyncMock(side_effect=_force_refresh)

    asyncio.run(plugin._do_failover(driver_dialect=svc.driver_dialect))

    svc.set_current_connection.assert_awaited()
    _c, bound_host = svc.set_current_connection.call_args.args
    assert bound_host.host == _W_EAST
    assert calls["n"] >= 2


def test_do_failover_unreachable_writer_raises_failover_failed():
    writer = HostInfo(host=_W_EAST, port=5432, role=HostRole.WRITER)
    plugin, svc, *_ = _make_plugin(
        home_region="us-east-1",
        overrides={"active_home_failover_mode": "strict_writer",
                   "failover_timeout_sec": "0.3"},
        all_hosts=(writer,), role=HostRole.WRITER)
    svc.force_connect = AsyncMock(side_effect=OSError("connection refused"))

    with pytest.raises(FailoverFailedError):
        asyncio.run(plugin._do_failover(driver_dialect=svc.driver_dialect))
    svc.set_current_connection.assert_not_called()


# ---- factory registration ----------------------------------------------


def test_gdb_failover_registered_in_factory():
    assert "gdb_failover" in PLUGIN_FACTORIES
    factory = PLUGIN_FACTORIES["gdb_failover"]
    assert type(factory) in PLUGIN_FACTORY_WEIGHTS


def test_gdb_failover_factory_builds_plugin():
    props = Properties({"host": _W_EAST, "port": "5432"})
    factory = PLUGIN_FACTORIES["gdb_failover"]
    instance = factory.get_instance(
        MagicMock(), props, host_list_provider=MagicMock())
    assert isinstance(instance, AsyncGdbFailoverPlugin)


def test_gdb_failover_factory_requires_host_list_provider():
    props = Properties({"host": _W_EAST, "port": "5432"})
    factory = PLUGIN_FACTORIES["gdb_failover"]
    with pytest.raises(AwsWrapperError):
        factory.get_instance(MagicMock(), props, host_list_provider=None)
