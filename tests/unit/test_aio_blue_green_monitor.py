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

"""Tests for the async Blue/Green status monitor, status provider, and the
concrete connect/execute routings.

Covers the ported provider machinery: role/topology-aware corresponding-host
mapping (including mismatched reader counts), per-phase interval ramping,
rollback detection, the switchover timer, and POST-phase reject +
suspend-until-corresponding-host routing emission. Also covers the routing
semantics: BG_CONNECT_TIMEOUT_MS on suspend, TimeoutError on expiry,
IAM host substitution, and non-swallowed substitute failures.
"""

from __future__ import annotations

import asyncio
from time import perf_counter_ns
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.blue_green_plugin import (
    AsyncBlueGreenPlugin, AsyncBlueGreenStatusMonitor,
    AsyncBlueGreenStatusProvider, BlueGreenInterimStatus,
    BlueGreenIntervalRate, BlueGreenPhase, BlueGreenRole, BlueGreenStatus,
    RejectConnectRouting, SubstituteConnectRouting, SuspendConnectRouting,
    SuspendExecuteRouting, SuspendUntilCorrespondingHostFoundConnectRouting)
from aws_advanced_python_wrapper.database_dialect import BlueGreenDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.value_container import ValueContainer

# Realistic RDS endpoints so RdsUtils DNS heuristics fire as they would in prod.
BLUE_WRITER = "bg-blue.cluster-abc123.us-east-2.rds.amazonaws.com"
BLUE_READER_CLUSTER = "bg-blue.cluster-ro-abc123.us-east-2.rds.amazonaws.com"
GREEN_WRITER = "bg-green.cluster-abc123.us-east-2.rds.amazonaws.com"


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _reset_bg_singletons():
    AsyncBlueGreenPlugin._reset_for_tests()
    yield
    AsyncBlueGreenPlugin._reset_for_tests()


def _mock_plugin_service(probe_conn: Any = None, current_host_info: Optional[HostInfo] = None) -> Any:
    svc = MagicMock()
    svc.database_dialect = MagicMock(spec=BlueGreenDialect)
    svc.database_dialect.blue_green_status_query = "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.is_closed = AsyncMock(return_value=False)
    svc.driver_dialect.abort_connection = AsyncMock()
    svc.connect = AsyncMock(return_value=probe_conn)
    svc.force_connect = AsyncMock(return_value=probe_conn)
    svc.set_status = MagicMock()
    svc.get_status = MagicMock(return_value=None)
    svc.is_login_exception = MagicMock(return_value=True)
    svc.get_telemetry_factory = MagicMock()
    svc.network_bound_methods = {"Cursor.execute"}
    svc.current_host_info = current_host_info
    svc.host_list_provider = None
    return svc


def _mock_cursor(rows: List[tuple]) -> MagicMock:
    cur = MagicMock(name="cursor")
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=None)
    cur.execute = AsyncMock(return_value=None)
    cur.fetchall = AsyncMock(return_value=rows)
    return cur


def _mock_conn(rows: List[tuple]) -> MagicMock:
    conn = MagicMock(name="probe_conn")
    conn.cursor = MagicMock(return_value=_mock_cursor(rows))
    return conn


def _make_plugin(svc: Any, bg_id: str = "bg") -> AsyncBlueGreenPlugin:
    return AsyncBlueGreenPlugin(svc, Properties({"bg_id": bg_id}))


def _make_provider(
        svc: Any = None,
        switchover_ms: int = 180_000,
        suspend_blue: bool = False,
        bg_id: str = "bg") -> AsyncBlueGreenStatusProvider:
    if svc is None:
        svc = _mock_plugin_service(current_host_info=HostInfo("h", 5432))
    props = Properties({"bg_switchover_timeout_ms": str(switchover_ms)})
    if suspend_blue:
        props["bg_suspend_new_blue_connections"] = "True"
    return AsyncBlueGreenStatusProvider(svc, props, bg_id, HostInfo("h", 5432))


def _interim(
        phase: BlueGreenPhase,
        port: int = 5432,
        start_topology=(),
        host_names=None,
        start_ips=None,
        all_ip_changed: bool = False,
        all_removed: bool = False,
        all_topology_changed: bool = False) -> BlueGreenInterimStatus:
    hn = set(host_names) if host_names is not None else {h.host for h in start_topology}
    sip: ConcurrentDict = ConcurrentDict()
    if start_ips:
        for key, value in start_ips.items():
            sip.put(key, ValueContainer.of(value))
    return BlueGreenInterimStatus(
        phase, "1.0", port, tuple(start_topology), sip, tuple(start_topology), ConcurrentDict(),
        hn, all_ip_changed, all_removed, all_topology_changed)


# ----- Phase / role parsing (sync parity) --------------------------


def test_phase_parse_known_strings() -> None:
    assert BlueGreenPhase.parse_phase("AVAILABLE") == BlueGreenPhase.CREATED
    assert BlueGreenPhase.parse_phase("SWITCHOVER_INITIATED") == BlueGreenPhase.PREPARATION
    assert BlueGreenPhase.parse_phase("SWITCHOVER_IN_PROGRESS") == BlueGreenPhase.IN_PROGRESS
    assert BlueGreenPhase.parse_phase("SWITCHOVER_IN_POST_PROCESSING") == BlueGreenPhase.POST
    assert BlueGreenPhase.parse_phase("SWITCHOVER_COMPLETED") == BlueGreenPhase.COMPLETED


def test_phase_parse_none_is_not_created() -> None:
    assert BlueGreenPhase.parse_phase(None) == BlueGreenPhase.NOT_CREATED
    assert BlueGreenPhase.parse_phase("") == BlueGreenPhase.NOT_CREATED


def test_phase_parse_unknown_raises() -> None:
    with pytest.raises(ValueError):
        BlueGreenPhase.parse_phase("GARBAGE")


def test_phase_switchover_flag() -> None:
    assert not BlueGreenPhase.NOT_CREATED.is_switchover_active_or_completed
    assert not BlueGreenPhase.CREATED.is_switchover_active_or_completed
    assert BlueGreenPhase.PREPARATION.is_switchover_active_or_completed
    assert BlueGreenPhase.COMPLETED.is_switchover_active_or_completed


def test_role_parse_known_strings() -> None:
    assert BlueGreenRole.parse_role("BLUE_GREEN_DEPLOYMENT_SOURCE", "1.0") == BlueGreenRole.SOURCE
    assert BlueGreenRole.parse_role("BLUE_GREEN_DEPLOYMENT_TARGET", "1.0") == BlueGreenRole.TARGET


def test_role_parse_unknown_role_raises() -> None:
    with pytest.raises(ValueError):
        BlueGreenRole.parse_role("OTHER", "1.0")


def test_role_parse_unknown_version_raises() -> None:
    with pytest.raises(ValueError):
        BlueGreenRole.parse_role("BLUE_GREEN_DEPLOYMENT_SOURCE", "9.9")


def test_role_value_ordering_for_list_indexing() -> None:
    assert BlueGreenRole.SOURCE.value == 0
    assert BlueGreenRole.TARGET.value == 1


# ----- SubstituteConnectRouting -----------------------------------


def test_substitute_routing_non_ip_connects_via_plugin_service() -> None:
    new_conn = object()
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(return_value=new_conn)
    plugin = _make_plugin(svc)
    substitute = HostInfo(GREEN_WRITER, 5432)
    routing = SubstituteConnectRouting(substitute, f"{BLUE_WRITER}:5432/", BlueGreenRole.SOURCE, (substitute,))

    result = _run(routing.apply(plugin, HostInfo(BLUE_WRITER, 5432), Properties(), False, AsyncMock()))

    assert result is new_conn
    svc.connect.assert_awaited_once()
    assert svc.connect.call_args.kwargs.get("plugin_to_skip") is plugin


def test_substitute_routing_ip_without_iam_connects_directly() -> None:
    new_conn = object()
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(return_value=new_conn)
    plugin = _make_plugin(svc)
    substitute = HostInfo("10.0.0.5", 5432)
    routing = SubstituteConnectRouting(substitute, f"{BLUE_WRITER}/", BlueGreenRole.SOURCE, (HostInfo("iam", 5432),))

    result = _run(routing.apply(plugin, HostInfo(BLUE_WRITER), Properties(), False, AsyncMock()))

    assert result is new_conn
    assert svc.connect.call_args.kwargs.get("plugin_to_skip") is plugin


def test_substitute_routing_ip_with_iam_reroutes_and_calls_handler() -> None:
    new_conn = object()
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(return_value=new_conn)
    plugin = _make_plugin(svc)
    substitute = HostInfo("10.0.0.5", 5432)
    iam_host = HostInfo("iam-host.example.com", 5432)
    handled: List[str] = []
    routing = SubstituteConnectRouting(
        substitute, f"{BLUE_WRITER}/", BlueGreenRole.SOURCE, (iam_host,), lambda h: handled.append(h))

    result = _run(routing.apply(plugin, HostInfo(BLUE_WRITER), Properties({"plugins": "iam"}), False, AsyncMock()))

    assert result is new_conn
    assert handled == ["iam-host.example.com"]
    # The reroute connect is NOT given a plugin_to_skip (auth plugins must re-run).
    assert svc.connect.call_args.kwargs.get("plugin_to_skip") is None


def test_substitute_routing_ip_with_iam_requires_iam_host() -> None:
    svc = _mock_plugin_service()
    plugin = _make_plugin(svc)
    routing = SubstituteConnectRouting(HostInfo("10.0.0.5", 5432), f"{BLUE_WRITER}/", BlueGreenRole.SOURCE, None)

    with pytest.raises(AwsWrapperError, match="iamHost"):
        _run(routing.apply(plugin, HostInfo(BLUE_WRITER), Properties({"plugins": "iam"}), False, AsyncMock()))


def test_substitute_routing_does_not_swallow_non_login_failure() -> None:
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(side_effect=AwsWrapperError("network boom"))
    svc.is_login_exception = MagicMock(return_value=False)
    plugin = _make_plugin(svc)
    routing = SubstituteConnectRouting(
        HostInfo("10.0.0.5", 5432), f"{BLUE_WRITER}/", BlueGreenRole.SOURCE, (HostInfo("iam", 5432),))
    connect_func = AsyncMock()

    with pytest.raises(AwsWrapperError, match="network boom"):
        _run(routing.apply(plugin, HostInfo(BLUE_WRITER), Properties({"plugins": "iam"}), False, connect_func))
    connect_func.assert_not_awaited()


def test_substitute_routing_all_iam_login_failures_raises_cant_open() -> None:
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(side_effect=AwsWrapperError("login denied"))
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _make_plugin(svc)
    routing = SubstituteConnectRouting(
        HostInfo("10.0.0.5", 5432), f"{BLUE_WRITER}/", BlueGreenRole.SOURCE,
        (HostInfo("iam1", 5432), HostInfo("iam2", 5432)))

    with pytest.raises(AwsWrapperError, match="Can't establish connection"):
        _run(routing.apply(plugin, HostInfo(BLUE_WRITER), Properties({"plugins": "iam"}), False, AsyncMock()))
    assert svc.connect.await_count == 2


# ----- SuspendConnectRouting --------------------------------------


def test_suspend_connect_routing_releases_when_phase_advances() -> None:
    svc = _mock_plugin_service()
    in_progress = BlueGreenStatus("bg", BlueGreenPhase.IN_PROGRESS)
    completed = BlueGreenStatus("bg", BlueGreenPhase.COMPLETED)
    calls = {"n": 0}

    def _gs(*_a, **_k):
        calls["n"] += 1
        return in_progress if calls["n"] == 1 else completed

    svc.get_status = MagicMock(side_effect=_gs)
    plugin = _make_plugin(svc)
    routing = SuspendConnectRouting(None, BlueGreenRole.SOURCE, "bg")

    result = _run(routing.apply(
        plugin, HostInfo("h", 5432), Properties({"bg_connect_timeout_ms": "5000"}), False, AsyncMock()))
    assert result is None


def test_suspend_connect_routing_times_out_with_timeouterror() -> None:
    svc = _mock_plugin_service()
    svc.get_status = MagicMock(return_value=BlueGreenStatus("bg", BlueGreenPhase.IN_PROGRESS))
    plugin = _make_plugin(svc)
    routing = SuspendConnectRouting(None, BlueGreenRole.SOURCE, "bg")

    with pytest.raises(TimeoutError):
        _run(routing.apply(
            plugin, HostInfo("h", 5432), Properties({"bg_connect_timeout_ms": "150"}), False, AsyncMock()))


# ----- SuspendExecuteRouting --------------------------------------


def test_suspend_execute_routing_releases_with_empty_container() -> None:
    svc = _mock_plugin_service()
    in_progress = BlueGreenStatus("bg", BlueGreenPhase.IN_PROGRESS)
    completed = BlueGreenStatus("bg", BlueGreenPhase.COMPLETED)
    calls = {"n": 0}

    def _gs(*_a, **_k):
        calls["n"] += 1
        return in_progress if calls["n"] == 1 else completed

    svc.get_status = MagicMock(side_effect=_gs)
    plugin = _make_plugin(svc)
    routing = SuspendExecuteRouting(None, BlueGreenRole.SOURCE, "bg")

    result = _run(routing.apply(
        plugin, Properties({"bg_connect_timeout_ms": "5000"}), "Cursor.execute", AsyncMock()))
    assert not result.is_present()


def test_suspend_execute_routing_uses_bg_connect_timeout_not_switchover() -> None:
    """SuspendExecuteRouting must honour BG_CONNECT_TIMEOUT_MS (small here), NOT
    BG_SWITCHOVER_TIMEOUT_MS (large). A large switchover timeout would hang the
    test; a fast TimeoutError proves the connect timeout is used."""
    svc = _mock_plugin_service()
    svc.get_status = MagicMock(return_value=BlueGreenStatus("bg", BlueGreenPhase.IN_PROGRESS))
    plugin = _make_plugin(svc)
    routing = SuspendExecuteRouting(None, BlueGreenRole.SOURCE, "bg")
    props = Properties({"bg_connect_timeout_ms": "150", "bg_switchover_timeout_ms": "600000"})

    with pytest.raises(TimeoutError):
        _run(routing.apply(plugin, props, "Cursor.execute", AsyncMock()))


# ----- SuspendUntilCorrespondingHostFoundConnectRouting -----------


def test_suspend_until_found_releases_on_completed() -> None:
    svc = _mock_plugin_service()
    svc.get_status = MagicMock(return_value=BlueGreenStatus("bg", BlueGreenPhase.COMPLETED))
    plugin = _make_plugin(svc)
    routing = SuspendUntilCorrespondingHostFoundConnectRouting(None, BlueGreenRole.SOURCE, "bg")

    result = _run(routing.apply(
        plugin, HostInfo("h", 5432), Properties({"bg_connect_timeout_ms": "5000"}), False, AsyncMock()))
    assert result is None


def test_suspend_until_found_releases_when_pair_appears() -> None:
    svc = _mock_plugin_service()
    empty = BlueGreenStatus("bg", BlueGreenPhase.PREPARATION)
    ready = BlueGreenStatus(
        "bg", BlueGreenPhase.PREPARATION,
        corresponding_hosts={"h": (HostInfo("h"), HostInfo("green"))})
    calls = {"n": 0}

    def _gs(*_a, **_k):
        calls["n"] += 1
        return empty if calls["n"] == 1 else ready

    svc.get_status = MagicMock(side_effect=_gs)
    plugin = _make_plugin(svc)
    routing = SuspendUntilCorrespondingHostFoundConnectRouting(None, BlueGreenRole.SOURCE, "bg")

    result = _run(routing.apply(
        plugin, HostInfo("h", 5432), Properties({"bg_connect_timeout_ms": "5000"}), False, AsyncMock()))
    assert result is None


def test_suspend_until_found_times_out_with_timeouterror() -> None:
    svc = _mock_plugin_service()
    svc.get_status = MagicMock(return_value=BlueGreenStatus("bg", BlueGreenPhase.PREPARATION))
    plugin = _make_plugin(svc)
    routing = SuspendUntilCorrespondingHostFoundConnectRouting(None, BlueGreenRole.SOURCE, "bg")

    with pytest.raises(TimeoutError):
        _run(routing.apply(
            plugin, HostInfo("h", 5432), Properties({"bg_connect_timeout_ms": "150"}), False, AsyncMock()))


# ----- Provider: construction + dialect --------------------------


def test_provider_requires_bluegreen_dialect() -> None:
    svc = _mock_plugin_service()
    svc.database_dialect = MagicMock()  # not a BlueGreenDialect
    with pytest.raises(AwsWrapperError):
        AsyncBlueGreenStatusProvider(svc, Properties(), "bg", HostInfo("h", 5432))


def test_monitor_requires_bluegreen_dialect() -> None:
    svc = _mock_plugin_service()
    svc.database_dialect = MagicMock()  # not a BlueGreenDialect
    with pytest.raises(AwsWrapperError):
        AsyncBlueGreenStatusMonitor(
            BlueGreenRole.SOURCE, "bg", HostInfo("h", 5432), svc, Properties(), {}, None)


# ----- Provider: corresponding-host mapping (role/topology aware) --


def test_corresponding_hosts_writer_and_readers_map_by_role() -> None:
    provider = _make_provider()
    bw = HostInfo(BLUE_WRITER, 5432, HostRole.WRITER)
    br1 = HostInfo("blue-r1.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)
    br2 = HostInfo("blue-r2.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)
    gw = HostInfo(GREEN_WRITER, 5432, HostRole.WRITER)
    gr1 = HostInfo("green-r1.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)
    gr2 = HostInfo("green-r2.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)

    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.CREATED, start_topology=(bw, br1, br2)))
    provider._process_interim_status(BlueGreenRole.TARGET, _interim(BlueGreenPhase.CREATED, start_topology=(gw, gr1, gr2)))

    assert provider._corresponding_hosts.get(bw.host) == (bw, gw)
    # Blue readers sorted by host -> green readers sorted by host, index-matched.
    assert provider._corresponding_hosts.get(br1.host)[1] is gr1
    assert provider._corresponding_hosts.get(br2.host)[1] is gr2


def test_corresponding_hosts_more_blue_readers_than_green_wraps_modulo() -> None:
    provider = _make_provider()
    bw = HostInfo(BLUE_WRITER, 5432, HostRole.WRITER)
    br1 = HostInfo("blue-r1.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)
    br2 = HostInfo("blue-r2.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)
    gw = HostInfo(GREEN_WRITER, 5432, HostRole.WRITER)
    gr1 = HostInfo("green-r1.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)

    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.CREATED, start_topology=(bw, br1, br2)))
    provider._process_interim_status(BlueGreenRole.TARGET, _interim(BlueGreenPhase.CREATED, start_topology=(gw, gr1)))

    # Two blue readers, one green reader -> both blue readers map to the single green reader.
    assert provider._corresponding_hosts.get(br1.host)[1] is gr1
    assert provider._corresponding_hosts.get(br2.host)[1] is gr1


def test_corresponding_hosts_no_green_readers_map_to_green_writer() -> None:
    provider = _make_provider()
    bw = HostInfo(BLUE_WRITER, 5432, HostRole.WRITER)
    br1 = HostInfo("blue-r1.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)
    gw = HostInfo(GREEN_WRITER, 5432, HostRole.WRITER)

    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.CREATED, start_topology=(bw, br1)))
    provider._process_interim_status(BlueGreenRole.TARGET, _interim(BlueGreenPhase.CREATED, start_topology=(gw,)))

    assert provider._corresponding_hosts.get(br1.host)[1] is gw


def test_corresponding_hosts_cluster_dns_mapping() -> None:
    provider = _make_provider()
    source = _interim(
        BlueGreenPhase.CREATED,
        host_names={BLUE_WRITER, BLUE_READER_CLUSTER})
    target = _interim(
        BlueGreenPhase.CREATED,
        host_names={GREEN_WRITER, "bg-green.cluster-ro-abc123.us-east-2.rds.amazonaws.com"})

    provider._process_interim_status(BlueGreenRole.SOURCE, source)
    provider._process_interim_status(BlueGreenRole.TARGET, target)

    writer_pair = provider._corresponding_hosts.get(BLUE_WRITER)
    assert writer_pair is not None and writer_pair[1].host == GREEN_WRITER
    reader_pair = provider._corresponding_hosts.get(BLUE_READER_CLUSTER)
    assert reader_pair is not None
    assert reader_pair[1].host == "bg-green.cluster-ro-abc123.us-east-2.rds.amazonaws.com"


# ----- Provider: phase routing tables -----------------------------


def test_created_phase_has_no_routings() -> None:
    provider = _make_provider()
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.CREATED, host_names={BLUE_WRITER}))
    assert provider._summary_status.phase == BlueGreenPhase.CREATED
    assert provider._summary_status.connect_routings == []
    assert provider._summary_status.execute_routings == []


def test_in_progress_phase_suspends_connect_and_execute() -> None:
    provider = _make_provider()
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.IN_PROGRESS, host_names={BLUE_WRITER}))
    provider._process_interim_status(BlueGreenRole.TARGET, _interim(BlueGreenPhase.IN_PROGRESS, host_names={GREEN_WRITER}))

    status = provider._summary_status
    assert status.phase == BlueGreenPhase.IN_PROGRESS
    assert any(isinstance(r, SuspendConnectRouting) for r in status.connect_routings)
    assert len(status.execute_routings) >= 2
    assert all(isinstance(r, SuspendExecuteRouting) for r in status.execute_routings)


def test_post_phase_emits_reject_and_suspend_until_found() -> None:
    provider = _make_provider()
    bw = HostInfo(BLUE_WRITER, 5432, HostRole.WRITER)
    # Green side has a reader but no writer -> blue writer maps to (bw, None),
    # forcing a SuspendUntilCorrespondingHostFound routing.
    gr = HostInfo("green-r1.abc.us-east-2.rds.amazonaws.com", 5432, HostRole.READER)

    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.POST, start_topology=(bw,)))
    provider._process_interim_status(BlueGreenRole.TARGET, _interim(BlueGreenPhase.POST, start_topology=(gr,)))

    status = provider._summary_status
    assert status.phase == BlueGreenPhase.POST
    assert provider._corresponding_hosts.get(bw.host) == (bw, None)
    assert any(isinstance(r, SuspendUntilCorrespondingHostFoundConnectRouting) for r in status.connect_routings)
    assert any(isinstance(r, RejectConnectRouting) for r in status.connect_routings)


def test_post_routings_reject_only_when_all_green_changed_and_dns_kept() -> None:
    provider = _make_provider()
    provider._blue_dns_update_completed = True
    provider._all_green_hosts_changed_name = True
    provider._green_dns_removed = False
    routings = provider._get_post_status_connect_routings()
    assert len(routings) == 1
    assert isinstance(routings[0], RejectConnectRouting)

    provider._green_dns_removed = True
    assert provider._get_post_status_connect_routings() == []


# ----- Provider: rollback detection -------------------------------


def test_rollback_detected_when_phase_moves_backwards() -> None:
    provider = _make_provider()
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.IN_PROGRESS, host_names={BLUE_WRITER}))
    assert provider._rollback is False
    assert provider._latest_phase == BlueGreenPhase.IN_PROGRESS

    # A backwards move to a still-active phase (PREPARATION) marks rollback and
    # holds it -- rolling all the way back to CREATED would instead count as a
    # completed rollback and reset the context.
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.PREPARATION, host_names={BLUE_WRITER}))
    assert provider._rollback is True
    assert provider._latest_phase == BlueGreenPhase.PREPARATION


def test_rollback_to_created_resets_context() -> None:
    provider = _make_provider()
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.IN_PROGRESS, host_names={BLUE_WRITER}))
    # Rolling back all the way to CREATED completes the (rolled-back) switchover
    # and clears the accumulated context.
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.CREATED, host_names={BLUE_WRITER}))
    assert provider._rollback is False
    assert provider._latest_phase == BlueGreenPhase.NOT_CREATED
    assert len(provider._phase_times_ns) == 0


# ----- Provider: switchover timer ---------------------------------


def test_switchover_timer_expiry_forces_completed() -> None:
    provider = _make_provider(switchover_ms=180_000)
    # Force the timer into the past.
    provider._post_status_end_time_ns = perf_counter_ns() - 1
    assert provider._is_switchover_timer_expired() is True

    status = provider._get_status_of_in_progress()
    assert status.phase == BlueGreenPhase.COMPLETED


def test_switchover_timer_expiry_during_rollback_returns_created() -> None:
    provider = _make_provider()
    provider._rollback = True
    provider._post_status_end_time_ns = perf_counter_ns() - 1
    status = provider._get_status_of_in_progress()
    assert status.phase == BlueGreenPhase.CREATED


def test_preparation_phase_starts_switchover_timer() -> None:
    provider = _make_provider()
    assert provider._post_status_end_time_ns == 0
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.PREPARATION, host_names={BLUE_WRITER}))
    assert provider._post_status_end_time_ns > 0


# ----- Provider: interval ramping per phase -----------------------


def test_interval_ramping_per_phase() -> None:
    provider = _make_provider()
    expectations = [
        (BlueGreenPhase.NOT_CREATED, BlueGreenIntervalRate.BASELINE),
        (BlueGreenPhase.CREATED, BlueGreenIntervalRate.INCREASED),
        (BlueGreenPhase.PREPARATION, BlueGreenIntervalRate.HIGH),
        (BlueGreenPhase.IN_PROGRESS, BlueGreenIntervalRate.HIGH),
        (BlueGreenPhase.POST, BlueGreenIntervalRate.HIGH),
        (BlueGreenPhase.COMPLETED, BlueGreenIntervalRate.BASELINE),
    ]
    for phase, expected_rate in expectations:
        provider._summary_status = BlueGreenStatus("bg", phase)
        provider._update_monitors()
        for monitor in provider._monitors:
            assert monitor.interval_rate == expected_rate, f"phase {phase} -> {expected_rate}"


def test_completed_phase_stops_source_monitor() -> None:
    provider = _make_provider()
    provider._summary_status = BlueGreenStatus("bg", BlueGreenPhase.COMPLETED)
    provider._update_monitors()
    assert provider._monitors[BlueGreenRole.SOURCE.value].stop is True


def test_in_progress_phase_ramps_monitors_to_high_end_to_end() -> None:
    provider = _make_provider()
    provider._process_interim_status(BlueGreenRole.SOURCE, _interim(BlueGreenPhase.IN_PROGRESS, host_names={BLUE_WRITER}))
    for monitor in provider._monitors:
        assert monitor.interval_rate == BlueGreenIntervalRate.HIGH


# ----- Provider: singleton via plugin -----------------------------


def test_provider_deduped_per_bg_id_via_plugin(monkeypatch) -> None:
    monkeypatch.setattr(AsyncBlueGreenStatusProvider, "schedule_start", lambda self: None)
    svc = _mock_plugin_service(current_host_info=HostInfo("h", 5432))
    plugin1 = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))
    plugin2 = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))

    plugin1._init_status_provider(HostInfo("h", 5432))
    plugin2._init_status_provider(HostInfo("h", 5432))

    assert len(AsyncBlueGreenPlugin._status_providers) == 1


# ----- Provider: monitoring props ---------------------------------


def test_monitoring_props_strip_prefix_and_set_timeout_defaults() -> None:
    svc = _mock_plugin_service(current_host_info=HostInfo("h", 5432))
    props = Properties({"blue-green-monitoring-foo": "bar", "some_other": "keep"})
    provider = AsyncBlueGreenStatusProvider(svc, props, "bg", HostInfo("h", 5432))
    monitoring = provider._get_monitoring_props()

    # Prefix stripped: the monitoring override becomes the bare key.
    assert monitoring.get("foo") == "bar"
    assert "blue-green-monitoring-foo" not in monitoring
    # Non-prefixed props are preserved.
    assert monitoring.get("some_other") == "keep"
    # Connect/socket timeout defaults injected (10_000 ms -> 10 s).
    assert monitoring.get("connect_timeout") == 10
    assert monitoring.get("socket_timeout") == 10


# ----- Monitor ----------------------------------------------------


def test_monitor_not_running_before_start() -> None:
    svc = _mock_plugin_service()

    def processor(_role, _interim) -> None:
        pass

    monitor = AsyncBlueGreenStatusMonitor(
        BlueGreenRole.SOURCE, "bg", HostInfo("h", 5432), svc, Properties(), {}, processor)
    assert monitor.is_running() is False


def test_monitor_collect_status_filters_rows_by_role(monkeypatch) -> None:
    svc = _mock_plugin_service()
    monitor = AsyncBlueGreenStatusMonitor(
        BlueGreenRole.SOURCE, "bg", HostInfo("h", 5432), svc,
        Properties(), {BlueGreenIntervalRate.HIGH: 100}, None)
    # Avoid real DNS during the reconnect check / IP collection.
    monkeypatch.setattr(monitor, "_get_ip_address", AsyncMock(return_value=ValueContainer.empty()))
    monitor._connection = _mock_conn([
        ("1.0", BLUE_WRITER, 5432, "BLUE_GREEN_DEPLOYMENT_SOURCE", "SWITCHOVER_IN_PROGRESS"),
        ("1.0", GREEN_WRITER, 5433, "BLUE_GREEN_DEPLOYMENT_TARGET", "SWITCHOVER_IN_PROGRESS"),
    ])

    _run(monitor._collect_status())

    assert monitor._current_phase == BlueGreenPhase.IN_PROGRESS
    assert monitor._port == 5432
    assert any("bg-blue" in host for host in monitor._host_names)
    assert not any("bg-green" in host for host in monitor._host_names)


def test_monitor_build_interim_status_reflects_collected_state(monkeypatch) -> None:
    svc = _mock_plugin_service()
    monitor = AsyncBlueGreenStatusMonitor(
        BlueGreenRole.SOURCE, "bg", HostInfo("h", 5432), svc,
        Properties(), {BlueGreenIntervalRate.HIGH: 100}, None)
    monkeypatch.setattr(monitor, "_get_ip_address", AsyncMock(return_value=ValueContainer.empty()))
    monitor._connection = _mock_conn([
        ("1.0", BLUE_WRITER, 5432, "BLUE_GREEN_DEPLOYMENT_SOURCE", "SWITCHOVER_IN_PROGRESS"),
    ])

    _run(monitor._collect_status())
    interim = monitor._build_interim_status()

    assert interim.phase == BlueGreenPhase.IN_PROGRESS
    assert interim.port == 5432
    assert any("bg-blue" in host for host in interim.host_names)


def test_monitor_stop_is_idempotent() -> None:
    svc = _mock_plugin_service()

    def processor(_role, _interim) -> None:
        pass

    monitor = AsyncBlueGreenStatusMonitor(
        BlueGreenRole.SOURCE, "bg", HostInfo("h", 5432), svc, Properties(), {}, processor)

    async def _body():
        await monitor.stop_monitor()
        await monitor.stop_monitor()

    _run(_body())
    assert monitor.is_running() is False


def test_monitor_poll_hands_interim_to_processor(monkeypatch) -> None:
    published: List[BlueGreenInterimStatus] = []

    def processor(_role, interim) -> None:
        published.append(interim)

    svc = _mock_plugin_service()
    monitor = AsyncBlueGreenStatusMonitor(
        BlueGreenRole.SOURCE, "bg", HostInfo("h", 5432), svc,
        Properties(), {BlueGreenIntervalRate.HIGH: 100}, processor)
    monkeypatch.setattr(monitor, "_get_ip_address", AsyncMock(return_value=ValueContainer.empty()))
    monitor._connection = _mock_conn([
        ("1.0", BLUE_WRITER, 5432, "BLUE_GREEN_DEPLOYMENT_SOURCE", "SWITCHOVER_IN_PROGRESS"),
    ])

    async def _one_iteration():
        await monitor._collect_status()
        await monitor.collect_topology()
        await monitor._collect_ip_addresses()
        monitor._update_ip_address_flags()
        if monitor._interim_status_processor is not None:
            monitor._interim_status_processor(monitor._bg_role, monitor._build_interim_status())

    _run(_one_iteration())

    assert len(published) == 1
    assert published[0].phase == BlueGreenPhase.IN_PROGRESS
