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

"""Tests for BlueGreen status monitor/provider + new routings (L.3 + M)."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.blue_green_plugin import (
    AsyncBlueGreenMonitorService, AsyncBlueGreenPlugin,
    AsyncBlueGreenStatusMonitor, AsyncBlueGreenStatusProvider,
    BlueGreenInterimStatus, BlueGreenPhase, BlueGreenRole, BlueGreenStatus,
    SubstituteConnectRouting, SuspendConnectRouting, SuspendExecuteRouting,
    SuspendUntilCorrespondingHostFoundConnectRouting, _routings_for_phase)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _reset_bg_singletons():
    AsyncBlueGreenStatusProvider._reset_for_tests()
    AsyncBlueGreenMonitorService._reset_for_tests()
    yield
    try:
        asyncio.run(AsyncBlueGreenMonitorService.stop_all())
    except RuntimeError:
        pass
    AsyncBlueGreenStatusProvider._reset_for_tests()
    AsyncBlueGreenMonitorService._reset_for_tests()


def _mock_plugin_service(probe_conn: Any = None) -> Any:
    svc = MagicMock()
    svc.database_dialect = MagicMock()
    svc.database_dialect.blue_green_status_query = "SELECT * FROM rds_tools.show_topology()"
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.abort_connection = AsyncMock()
    svc.connect = AsyncMock(return_value=probe_conn)
    svc.set_status = MagicMock()
    svc.get_status = MagicMock(return_value=None)
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


# ----- Phase parsing -----------------------------------------------


def test_phase_parse_known_strings() -> None:
    assert BlueGreenPhase.parse_phase("AVAILABLE") == BlueGreenPhase.CREATED
    assert BlueGreenPhase.parse_phase(
        "SWITCHOVER_INITIATED") == BlueGreenPhase.PREPARATION
    assert BlueGreenPhase.parse_phase(
        "SWITCHOVER_IN_PROGRESS") == BlueGreenPhase.IN_PROGRESS
    assert BlueGreenPhase.parse_phase(
        "SWITCHOVER_IN_POST_PROCESSING") == BlueGreenPhase.POST
    assert BlueGreenPhase.parse_phase(
        "SWITCHOVER_COMPLETED") == BlueGreenPhase.COMPLETED


def test_phase_parse_unknown_defaults_to_not_created() -> None:
    assert BlueGreenPhase.parse_phase("GARBAGE") == BlueGreenPhase.NOT_CREATED
    assert BlueGreenPhase.parse_phase(None) == BlueGreenPhase.NOT_CREATED


def test_phase_switchover_flag() -> None:
    assert not BlueGreenPhase.NOT_CREATED.is_switchover_active_or_completed
    assert not BlueGreenPhase.CREATED.is_switchover_active_or_completed
    assert BlueGreenPhase.PREPARATION.is_switchover_active_or_completed
    assert BlueGreenPhase.IN_PROGRESS.is_switchover_active_or_completed
    assert BlueGreenPhase.POST.is_switchover_active_or_completed
    assert BlueGreenPhase.COMPLETED.is_switchover_active_or_completed


def test_role_parse_known_strings() -> None:
    assert BlueGreenRole.parse_role(
        "BLUE_GREEN_DEPLOYMENT_SOURCE") == BlueGreenRole.SOURCE
    assert BlueGreenRole.parse_role(
        "BLUE_GREEN_DEPLOYMENT_TARGET") == BlueGreenRole.TARGET


def test_role_parse_unknown_returns_none() -> None:
    assert BlueGreenRole.parse_role("OTHER") is None
    assert BlueGreenRole.parse_role(None) is None


# ----- _routings_for_phase ----------------------------------------


def test_routings_for_not_created_empty() -> None:
    c, e = _routings_for_phase(BlueGreenPhase.NOT_CREATED, {})
    assert c == [] and e == []


def test_routings_for_preparation_substitutes() -> None:
    corresponding: Dict[str, Tuple[str, Optional[str]]] = {
        "src.example.com": ("src.example.com", "tgt.example.com"),
        "tgt.example.com": ("tgt.example.com", "src.example.com"),
    }
    c, e = _routings_for_phase(BlueGreenPhase.PREPARATION, corresponding)
    assert len(c) == 2
    assert all(isinstance(r, SubstituteConnectRouting) for r in c)
    assert e == []


def test_routings_for_in_progress_suspends() -> None:
    c, e = _routings_for_phase(BlueGreenPhase.IN_PROGRESS, {})
    assert len(c) == 1
    assert isinstance(c[0], SuspendConnectRouting)
    assert len(e) == 1
    assert isinstance(e[0], SuspendExecuteRouting)


def test_routings_for_completed_empty() -> None:
    c, e = _routings_for_phase(BlueGreenPhase.COMPLETED, {})
    assert c == [] and e == []


# ----- SubstituteConnectRouting -----------------------------------


def test_substitute_connect_routing_redirects_via_plugin_service():
    """When corresponding host maps source -> target, SubstituteConnectRouting
    drives plugin_service.connect with the target."""
    new_conn = object()
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(return_value=new_conn)
    # Publish a status with a corresponding-host pair.
    status = BlueGreenStatus(
        bg_id="bg",
        phase=BlueGreenPhase.PREPARATION,
        corresponding_hosts={
            "src": ("src", "tgt"),
        },
        role_by_host={"src": BlueGreenRole.SOURCE},
    )
    svc.get_status = MagicMock(return_value=status)

    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))
    routing = SubstituteConnectRouting(host_matcher="src")

    async def _body():
        return await routing.apply(
            plugin=plugin,
            host_info=HostInfo("src", 5432),
            props=Properties(),
            is_initial_connection=False,
            connect_func=AsyncMock(),
        )

    result = asyncio.run(_body())
    assert result is new_conn
    svc.connect.assert_awaited_once()


def test_substitute_connect_routing_falls_through_when_no_pair():
    """No corresponding-host entry -> falls through to original connect_func."""
    svc = _mock_plugin_service()
    status = BlueGreenStatus(
        bg_id="bg",
        phase=BlueGreenPhase.PREPARATION,
        corresponding_hosts={},
    )
    svc.get_status = MagicMock(return_value=status)

    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))
    routing = SubstituteConnectRouting(host_matcher="src")
    fallback_result = object()
    connect_func = AsyncMock(return_value=fallback_result)

    async def _body():
        return await routing.apply(
            plugin=plugin,
            host_info=HostInfo("src", 5432),
            props=Properties(),
            is_initial_connection=False,
            connect_func=connect_func,
        )

    result = asyncio.run(_body())
    assert result is fallback_result
    connect_func.assert_awaited_once()


# ----- SuspendConnectRouting --------------------------------------


def test_suspend_connect_routing_waits_then_proceeds():
    """When phase transitions away from active during the wait, the routing
    proceeds with the original connect_func."""
    svc = _mock_plugin_service()
    status_progress = BlueGreenStatus(
        bg_id="bg",
        phase=BlueGreenPhase.IN_PROGRESS,
    )
    status_done = BlueGreenStatus(
        bg_id="bg",
        phase=BlueGreenPhase.COMPLETED,
    )
    # Each call returns the next value; once exhausted, stays on COMPLETED.
    call_count = [0]

    def _get_status(*_a, **_k):
        i = call_count[0]
        call_count[0] += 1
        return status_progress if i == 0 else status_done

    svc.get_status = MagicMock(side_effect=_get_status)
    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))

    routing = SuspendConnectRouting()
    connect_result = object()
    connect_func = AsyncMock(return_value=connect_result)

    async def _body():
        return await routing.apply(
            plugin=plugin,
            host_info=HostInfo("h", 5432),
            props=Properties({
                "bg_connect_timeout_ms": "5000",
            }),
            is_initial_connection=False,
            connect_func=connect_func,
        )

    result = asyncio.run(_body())
    assert result is connect_result


def test_suspend_connect_routing_times_out():
    """If the phase never transitions, suspend raises after timeout."""
    svc = _mock_plugin_service()
    status = BlueGreenStatus(bg_id="bg", phase=BlueGreenPhase.IN_PROGRESS)
    svc.get_status = MagicMock(return_value=status)
    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))

    routing = SuspendConnectRouting()

    async def _body():
        await routing.apply(
            plugin=plugin,
            host_info=HostInfo("h", 5432),
            props=Properties({
                # Tiny timeout so the test doesn't hang.
                "bg_connect_timeout_ms": "150",
            }),
            is_initial_connection=False,
            connect_func=AsyncMock(),
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_body())


# ----- SuspendExecuteRouting --------------------------------------


def test_suspend_execute_routing_waits_then_executes():
    svc = _mock_plugin_service()
    status_progress = BlueGreenStatus(
        bg_id="bg", phase=BlueGreenPhase.IN_PROGRESS)
    status_done = BlueGreenStatus(
        bg_id="bg", phase=BlueGreenPhase.COMPLETED)
    call_count = [0]

    def _get_status(*_a, **_k):
        i = call_count[0]
        call_count[0] += 1
        return status_progress if i == 0 else status_done

    svc.get_status = MagicMock(side_effect=_get_status)
    plugin = AsyncBlueGreenPlugin(svc, Properties({
        "bg_id": "bg",
        "bg_switchover_timeout_ms": "5000",
    }))

    routing = SuspendExecuteRouting()
    exec_result = object()
    execute_func = AsyncMock(return_value=exec_result)

    async def _body():
        return await routing.apply(
            plugin=plugin,
            execute_func=execute_func,
            method_name="Cursor.execute",
        )

    result = asyncio.run(_body())
    assert result is exec_result


def test_suspend_execute_routing_times_out():
    svc = _mock_plugin_service()
    status = BlueGreenStatus(bg_id="bg", phase=BlueGreenPhase.IN_PROGRESS)
    svc.get_status = MagicMock(return_value=status)
    plugin = AsyncBlueGreenPlugin(svc, Properties({
        "bg_id": "bg",
        "bg_switchover_timeout_ms": "150",
    }))

    routing = SuspendExecuteRouting()

    async def _body():
        await routing.apply(
            plugin=plugin,
            execute_func=AsyncMock(),
            method_name="Cursor.execute",
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_body())


# ----- SuspendUntilCorrespondingHostFoundConnectRouting -----------


def test_suspend_until_corresponding_host_found_proceeds_when_pair_appears():
    svc = _mock_plugin_service()
    svc.connect = AsyncMock(return_value=object())

    # Start with no corresponding host, then it appears.
    empty_status = BlueGreenStatus(
        bg_id="bg", phase=BlueGreenPhase.PREPARATION)
    ready_status = BlueGreenStatus(
        bg_id="bg",
        phase=BlueGreenPhase.PREPARATION,
        corresponding_hosts={"h": ("h", "target")},
    )
    call_count = [0]

    def _get_status(*_a, **_k):
        i = call_count[0]
        call_count[0] += 1
        return empty_status if i == 0 else ready_status

    svc.get_status = MagicMock(side_effect=_get_status)
    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "bg"}))

    routing = SuspendUntilCorrespondingHostFoundConnectRouting()

    async def _body():
        return await routing.apply(
            plugin=plugin,
            host_info=HostInfo("h", 5432),
            props=Properties({"bg_connect_timeout_ms": "5000"}),
            is_initial_connection=False,
            connect_func=AsyncMock(),
        )

    asyncio.run(_body())
    svc.connect.assert_awaited_once()


# ----- Monitor ---------------------------------------------------


def test_monitor_not_running_before_start() -> None:
    svc = _mock_plugin_service()

    async def processor(i: BlueGreenInterimStatus) -> None:
        pass

    m = AsyncBlueGreenStatusMonitor(
        svc, HostInfo("h", 5432), Properties(), "bg", 5_000, processor)
    assert m.is_running() is False


def test_monitor_poll_once_publishes_interim() -> None:
    # Rows match sync column layout: version, endpoint, port, role, status
    rows = [
        ("1.0", "src.example.com", 5432,
         "BLUE_GREEN_DEPLOYMENT_SOURCE", "SWITCHOVER_IN_PROGRESS"),
        ("1.0", "tgt.example.com", 5433,
         "BLUE_GREEN_DEPLOYMENT_TARGET", "SWITCHOVER_IN_PROGRESS"),
    ]
    probe = _mock_conn(rows)
    svc = _mock_plugin_service(probe_conn=probe)

    published: List[BlueGreenInterimStatus] = []

    async def processor(interim: BlueGreenInterimStatus) -> None:
        published.append(interim)

    m = AsyncBlueGreenStatusMonitor(
        svc, HostInfo("h", 5432), Properties(), "bg", 5_000, processor)

    async def _body():
        await m._poll_once()

    asyncio.run(_body())

    assert len(published) == 1
    interim = published[0]
    assert interim.bg_id == "bg"
    assert interim.phase == BlueGreenPhase.IN_PROGRESS
    assert interim.source_hosts == ("src.example.com",)
    assert interim.target_hosts == ("tgt.example.com",)


def test_monitor_stop_is_idempotent() -> None:
    svc = _mock_plugin_service()

    async def noop(_i: BlueGreenInterimStatus) -> None:
        pass

    m = AsyncBlueGreenStatusMonitor(
        svc, HostInfo("h", 5432), Properties(), "bg", 100, noop)

    async def _body():
        await m.stop()
        await m.stop()

    asyncio.run(_body())


# ----- Provider ----------------------------------------------------


def test_provider_is_singleton_per_cluster() -> None:
    svc = _mock_plugin_service()
    p1 = AsyncBlueGreenStatusProvider.get_or_create(svc, "bg1")
    p2 = AsyncBlueGreenStatusProvider.get_or_create(svc, "bg1")
    p3 = AsyncBlueGreenStatusProvider.get_or_create(svc, "bg2")
    assert p1 is p2
    assert p1 is not p3


def test_provider_publishes_status_with_correct_phase_routings() -> None:
    svc = _mock_plugin_service()
    provider = AsyncBlueGreenStatusProvider(svc, "bg")

    interim = BlueGreenInterimStatus(
        bg_id="bg",
        phase=BlueGreenPhase.IN_PROGRESS,
        source_hosts=("src",),
        target_hosts=("tgt",),
    )

    asyncio.run(provider.process_interim(interim))

    svc.set_status.assert_called_once()
    args, _ = svc.set_status.call_args
    assert args[0] is BlueGreenStatus
    assert args[1] == "bg"
    status = args[2]
    assert status.phase == BlueGreenPhase.IN_PROGRESS
    # IN_PROGRESS -> Suspend routings.
    assert len(status.connect_routings) == 1
    assert isinstance(status.connect_routings[0], SuspendConnectRouting)
    assert len(status.execute_routings) == 1
    assert isinstance(status.execute_routings[0], SuspendExecuteRouting)
    assert status.role_by_host["src"] == BlueGreenRole.SOURCE
    assert status.role_by_host["tgt"] == BlueGreenRole.TARGET


def test_provider_preparation_phase_builds_substitute_routings() -> None:
    svc = _mock_plugin_service()
    provider = AsyncBlueGreenStatusProvider(svc, "bg")

    interim = BlueGreenInterimStatus(
        bg_id="bg",
        phase=BlueGreenPhase.PREPARATION,
        source_hosts=("src",),
        target_hosts=("tgt",),
    )

    asyncio.run(provider.process_interim(interim))

    args, _ = svc.set_status.call_args
    status = args[2]
    # 2 SubstituteConnectRoutings: src->tgt, tgt->src.
    assert len(status.connect_routings) == 2
    for r in status.connect_routings:
        assert isinstance(r, SubstituteConnectRouting)


# ----- Monitor service --------------------------------------------


def test_monitor_service_dedupes_by_bg_id() -> None:
    svc = _mock_plugin_service()

    async def processor(_i: BlueGreenInterimStatus) -> None:
        pass

    async def _body():
        m1 = AsyncBlueGreenMonitorService.ensure_monitor(
            svc, HostInfo("h", 5432), Properties(), "bg",
            60_000, processor)
        m2 = AsyncBlueGreenMonitorService.ensure_monitor(
            svc, HostInfo("h", 5432), Properties(), "bg",
            60_000, processor)
        assert m1 is m2
        await AsyncBlueGreenMonitorService.stop_all()

    asyncio.run(_body())


def test_monitor_service_stop_all_terminates_monitors() -> None:
    svc = _mock_plugin_service()

    async def processor(_i: BlueGreenInterimStatus) -> None:
        pass

    async def _body():
        m = AsyncBlueGreenMonitorService.ensure_monitor(
            svc, HostInfo("h", 5432), Properties(), "bg-stop",
            60_000, processor)
        await AsyncBlueGreenMonitorService.stop_all()
        assert m.is_running() is False

    asyncio.run(_body())
