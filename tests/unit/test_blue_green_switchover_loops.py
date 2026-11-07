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

"""Calls suspended by the Blue/Green plugin while a switchover is IN_PROGRESS
(issue #1352).

Each test suspends a call on IN_PROGRESS, publishes the next phase, and
checks that the call comes back within a bound with what the new phase's
routings give.
"""

from __future__ import annotations

import asyncio
import logging
from itertools import count
from threading import Event, Thread
from typing import Any, Callable, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio import blue_green_plugin as aio_bg
from aws_advanced_python_wrapper.blue_green_plugin import (
    BlueGreenPhase, BlueGreenPlugin, BlueGreenRole, BlueGreenStatus,
    RejectConnectRouting, SubstituteConnectRouting, SuspendConnectRouting,
    SuspendExecuteRouting)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.telemetry.null_telemetry import \
    NullTelemetryFactory

BG_ID = "1"
PORT = 3306
BLUE_HOST = "database-1.cluster-c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
GREEN_HOST = "database-1-green-abc123.cluster-c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
GREEN_IP = "10.0.1.20"
# Bounds for the test setup, not for the plugin: how long the call may take to reach the suspend routing, and how
# long the stop switch may take to end a call that spins.
SUSPENDED_WITHIN_SEC = 5
STOP_WITHIN_SEC = 5


@pytest.fixture(autouse=True)
def no_status_monitors(monkeypatch):
    # A real status provider starts monitor threads or tasks that query the database; these tests publish the
    # status themselves.
    monkeypatch.setattr(BlueGreenPlugin, "_init_status_provider", lambda self: None)
    monkeypatch.setattr(aio_bg.AsyncBlueGreenPlugin, "_init_status_provider", lambda self, initial_host_info: None)
    yield
    BlueGreenPlugin._status_providers.clear()
    aio_bg.AsyncBlueGreenPlugin._reset_for_tests()


@pytest.fixture(autouse=True)
def no_debug_records(caplog):
    # The unit suite logs at DEBUG, and a call that spins logs a record on every pass; pytest would keep them all
    # in memory and print them with the failure.
    caplog.set_level(logging.INFO, logger="aws_advanced_python_wrapper.blue_green_plugin")
    caplog.set_level(logging.INFO, logger="aws_advanced_python_wrapper.aio.blue_green_plugin")


def _plugin_service(published: List[Any], suspended: Event, current_host_info: Optional[HostInfo] = None) \
        -> MagicMock:
    plugin_service = MagicMock()
    plugin_service.network_bound_methods = set()
    plugin_service.current_host_info = current_host_info
    plugin_service.is_plugin_in_use.return_value = False
    # Plain functions, not mocks, on the calls a suspended call repeats: a call that spins would otherwise record
    # a mock call on every pass and grow without bound. The last status in `published` is the current one.
    telemetry_factory = NullTelemetryFactory()
    plugin_service.get_telemetry_factory = lambda: telemetry_factory
    reads = count(1)

    def get_status(*args, **kwargs):
        # The plugin reads the status when the call starts, and the suspend routing reads it on entry and again as
        # it starts waiting for the phase to change, so a third read means the call is suspended.
        if next(reads) >= 3:
            suspended.set()
        return published[-1]

    plugin_service.get_status = get_status
    return plugin_service


def _call_across_move(
        call: Callable[[], Any], published: List[Any], suspended: Event, next_status: Any, bound_sec: float) \
        -> List[Any]:
    """Runs `call` in a daemon thread, publishes `next_status` once the call is suspended on IN_PROGRESS, and
    returns the call's result or exception in a list, or an empty list if it didn't return or raise within
    `bound_sec` of the move."""
    outcome: List[Any] = []

    def run():
        try:
            outcome.append(call())
        except Exception as e:  # a caller sees the exception, so the test gets it as the outcome
            outcome.append(e)
        finally:
            # A call that ends without being suspended mustn't leave the wait below to run to its bound.
            suspended.set()

    # A call that spins never returns, so it runs in a daemon thread and is only waited for with a bound.
    worker = Thread(target=run, daemon=True)
    worker.start()
    assert suspended.wait(SUSPENDED_WITHIN_SEC), \
        f"setup: the call didn't reach the suspend routing within {SUSPENDED_WITHIN_SEC} s"
    assert not outcome, f"setup: the call ended while the phase was still IN_PROGRESS: {outcome!r}"

    published.append(next_status)
    worker.join(bound_sec)
    returned = list(outcome)
    if not returned:
        # Stop switch: with no status published the plugin calls straight through, so a call that spins ends
        # here instead of burning a CPU for the rest of the session.
        published.append(None)
        worker.join(STOP_WITHIN_SEC)
        assert not worker.is_alive(), \
            f"setup: the stop switch didn't end the call within {STOP_WITHIN_SEC} s; it still runs in the background"

    return returned


def _role_by_host() -> ConcurrentDict[str, BlueGreenRole]:
    role_by_host: ConcurrentDict[str, BlueGreenRole] = ConcurrentDict()
    role_by_host.put_all({BLUE_HOST: BlueGreenRole.SOURCE, GREEN_HOST: BlueGreenRole.TARGET})
    return role_by_host


def _in_progress() -> BlueGreenStatus:
    return BlueGreenStatus(
        BG_ID,
        BlueGreenPhase.IN_PROGRESS,
        connect_routings=[SuspendConnectRouting(None, BlueGreenRole.TARGET, BG_ID)],
        execute_routings=[
            SuspendExecuteRouting(None, BlueGreenRole.SOURCE, BG_ID),
            SuspendExecuteRouting(None, BlueGreenRole.TARGET, BG_ID),
        ],
        role_by_host=_role_by_host())


def _post() -> BlueGreenStatus:
    # POST suspends no calls: new blue connections go to the green IP and new green connections are rejected.
    green_ip = HostInfo(GREEN_IP, PORT)
    iam_hosts = (HostInfo(GREEN_HOST, PORT), HostInfo(BLUE_HOST, PORT))
    return BlueGreenStatus(
        BG_ID,
        BlueGreenPhase.POST,
        connect_routings=[
            SubstituteConnectRouting(green_ip, BLUE_HOST, BlueGreenRole.SOURCE, iam_hosts),
            SubstituteConnectRouting(green_ip, f"{BLUE_HOST}:{PORT}", BlueGreenRole.SOURCE, iam_hosts),
            RejectConnectRouting(None, BlueGreenRole.TARGET),
        ],
        role_by_host=_role_by_host())


@pytest.mark.timeout(20)
@pytest.mark.xfail(strict=True, reason="#1352: C1 sync execute keeps spinning on the IN_PROGRESS routing after the phase moves on")
def test_suspended_execute_returns_after_switchover_moves_past_in_progress():
    """An execute suspended during IN_PROGRESS runs and returns its result soon after the status moves to POST."""
    published: List[Any] = [_in_progress()]
    suspended = Event()
    plugin = BlueGreenPlugin(_plugin_service(published, suspended, HostInfo(BLUE_HOST, PORT)), Properties())

    outcome = _call_across_move(
        lambda: plugin.execute(MagicMock(), "Cursor.execute", lambda: "result"), published, suspended, _post(), 2)

    assert outcome, "C1: execute() did not return within 2 s after the phase left IN_PROGRESS"
    assert outcome == ["result"], f"C1: execute() ended with {outcome[0]!r} instead of the target's result"


@pytest.mark.timeout(20)
@pytest.mark.xfail(strict=True, reason="#1352: C2 sync connect keeps spinning on the IN_PROGRESS routing after the phase moves on")
def test_suspended_connect_ends_after_switchover_moves_past_in_progress():
    """A connect to a green host suspended during IN_PROGRESS fails with the reject routing's error soon after the
    status moves to POST, which rejects new connections to green hosts."""
    published: List[Any] = [_in_progress()]
    suspended = Event()
    plugin = BlueGreenPlugin(_plugin_service(published, suspended), Properties())
    host_info = HostInfo(GREEN_HOST, PORT)

    outcome = _call_across_move(
        lambda: plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=host_info,
            props=Properties(),
            is_initial_connection=True,
            connect_func=MagicMock()),
        published, suspended, _post(), 2)

    assert outcome, "C2: connect() did not return or raise within 2 s after the phase left IN_PROGRESS"
    # A connect that skips the POST routings opens the green host, or ends with an error of its own, instead of
    # raising the error of POST's RejectConnectRouting.
    rejected = Messages.get("RejectConnectRouting.InProgressCantConnect")
    assert isinstance(outcome[0], AwsWrapperError) and str(outcome[0]) == rejected, \
        f"C2: connect() to a green host after the move to POST ended with {outcome[0]!r} instead of the POST reject error"


@pytest.mark.timeout(20)
@pytest.mark.xfail(strict=True, reason="#1352: C3 bg_connect_timeout_ms doesn't bound the sync execute loop")
def test_suspended_execute_with_short_connect_timeout_ends_after_switchover():
    """With bg_connect_timeout_ms=500, an execute suspended during IN_PROGRESS returns its result or raises a
    timeout error within 3 s of the status moving to POST, long after that timeout has passed."""
    published: List[Any] = [_in_progress()]
    suspended = Event()
    props = Properties({"bg_connect_timeout_ms": "500"})
    plugin = BlueGreenPlugin(_plugin_service(published, suspended, HostInfo(BLUE_HOST, PORT)), props)

    outcome = _call_across_move(
        lambda: plugin.execute(MagicMock(), "Cursor.execute", lambda: "result"), published, suspended, _post(), 3)

    assert outcome, \
        "C3: execute() with bg_connect_timeout_ms=500 did not return or raise within 3 s after the phase left IN_PROGRESS"
    # Either the loop takes the new status and runs the target, or the timeout ends it; any other error is a fault
    # in the call, not the timeout applying.
    assert outcome == ["result"] or isinstance(outcome[0], (TimeoutError, AwsWrapperError)), \
        f"C3: execute() with bg_connect_timeout_ms=500 ended with {outcome[0]!r}, neither the target's result nor a timeout"


@pytest.mark.timeout(20)
def test_async_suspended_execute_returns_after_switchover_moves_past_in_progress():
    """The async plugin's execute suspended during IN_PROGRESS returns its result soon after the status moves to
    POST. A guard: the async loops already take the latest status, so this passes on main."""
    in_progress = aio_bg.BlueGreenStatus(
        BG_ID,
        aio_bg.BlueGreenPhase.IN_PROGRESS,
        connect_routings=[aio_bg.SuspendConnectRouting(None, aio_bg.BlueGreenRole.TARGET, BG_ID)],
        execute_routings=[
            aio_bg.SuspendExecuteRouting(None, aio_bg.BlueGreenRole.SOURCE, BG_ID),
            aio_bg.SuspendExecuteRouting(None, aio_bg.BlueGreenRole.TARGET, BG_ID),
        ],
        role_by_host={BLUE_HOST: aio_bg.BlueGreenRole.SOURCE, GREEN_HOST: aio_bg.BlueGreenRole.TARGET})
    post = aio_bg.BlueGreenStatus(
        BG_ID,
        aio_bg.BlueGreenPhase.POST,
        connect_routings=[aio_bg.RejectConnectRouting(None, aio_bg.BlueGreenRole.TARGET)],
        role_by_host={BLUE_HOST: aio_bg.BlueGreenRole.SOURCE, GREEN_HOST: aio_bg.BlueGreenRole.TARGET})
    published: List[Any] = [in_progress]
    suspended = Event()
    # The async plugin service's get_status is synchronous too, so the same fake serves the async plugin.
    plugin = aio_bg.AsyncBlueGreenPlugin(
        _plugin_service(published, suspended, HostInfo(BLUE_HOST, PORT)), Properties())
    execute_func = AsyncMock(return_value="result")

    outcome = _call_across_move(
        lambda: asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", execute_func)),
        published, suspended, post, 2)

    assert outcome, "C4: async execute() did not return within 2 s after the phase left IN_PROGRESS"
    assert outcome == ["result"], f"C4: async execute() ended with {outcome[0]!r} instead of the target's result"
