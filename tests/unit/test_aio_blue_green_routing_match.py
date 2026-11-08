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

"""Endpoint matching of the async Blue/Green routings (issue #1352).

The status provider gives a routing an endpoint of the form ``host``,
``host:port``, ``ip`` or ``ip:port``. These tests build the routings with
those endpoints and check that they match the host they name, and that a
connection to that host is then routed by them.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.blue_green_plugin import (
    AsyncBlueGreenPlugin, BlueGreenPhase, BlueGreenRole, BlueGreenStatus,
    RejectConnectRouting, SubstituteConnectRouting, SuspendConnectRouting,
    SuspendExecuteRouting, SuspendUntilCorrespondingHostFoundConnectRouting)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties

BG_ID = "1"
PORT = 3306
BLUE_HOST = "database-1.cluster-c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
GREEN_HOST = "database-1-green-abc123.cluster-c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
BLUE_IP = "10.0.1.10"
GREEN_IP = "10.0.1.20"


@pytest.fixture(autouse=True)
def no_status_monitors(monkeypatch):
    # A real status provider starts monitor tasks that query the database; these tests publish the status
    # themselves.
    monkeypatch.setattr(AsyncBlueGreenPlugin, "_init_status_provider", lambda self, initial_host_info: None)
    yield
    AsyncBlueGreenPlugin._reset_for_tests()


def test_async_routings_match_the_endpoints_the_status_provider_builds():
    """Each async routing built with an endpoint of the form the status provider uses matches a host with that
    name and port, so the routing applies to connections and calls on that host."""
    blue = HostInfo(BLUE_HOST, PORT)
    blue_ip = HostInfo(BLUE_IP, PORT)
    cases = [
        (SubstituteConnectRouting(blue_ip, BLUE_HOST, BlueGreenRole.SOURCE, (blue,)), HostInfo(BLUE_HOST)),
        (SubstituteConnectRouting(blue_ip, f"{BLUE_HOST}:{PORT}", BlueGreenRole.SOURCE, (blue,)), blue),
        # A host name given in upper case still has a role, since the role lookup ignores case.
        (SubstituteConnectRouting(blue_ip, f"{BLUE_HOST}:{PORT}", BlueGreenRole.SOURCE, (blue,)),
         HostInfo(BLUE_HOST.upper(), PORT)),
        (SuspendUntilCorrespondingHostFoundConnectRouting(BLUE_HOST, BlueGreenRole.SOURCE, BG_ID), HostInfo(BLUE_HOST)),
        (SuspendUntilCorrespondingHostFoundConnectRouting(f"{BLUE_HOST}:{PORT}", None, BG_ID), blue),
        (SuspendConnectRouting(BLUE_IP, None, BG_ID), HostInfo(BLUE_IP)),
        (SuspendConnectRouting(f"{BLUE_IP}:{PORT}", None, BG_ID), blue_ip),
        (SuspendExecuteRouting(BLUE_IP, None, BG_ID), HostInfo(BLUE_IP)),
        (SuspendExecuteRouting(f"{BLUE_IP}:{PORT}", None, BG_ID), blue_ip),
    ]

    misses = [f"{type(routing).__name__} for {host_info.get_host_and_port()}"
              for routing, host_info in cases if not routing.is_match(host_info, BlueGreenRole.SOURCE)]
    assert misses == [], f"C6: {len(misses)} of {len(cases)} routings did not match the host they name: {misses}"


def test_async_routings_do_not_match_another_host_port_or_role():
    """An async routing with an endpoint doesn't match another host, the same host on another port, a host in the
    other role, or no host; a routing without an endpoint doesn't match the other role."""
    routing = SubstituteConnectRouting(
        HostInfo(GREEN_IP, PORT), f"{BLUE_HOST}:{PORT}", BlueGreenRole.SOURCE, (HostInfo(BLUE_HOST, PORT),))

    assert routing.is_match(HostInfo(GREEN_HOST, PORT), BlueGreenRole.SOURCE) is False
    assert routing.is_match(HostInfo(BLUE_HOST, PORT + 1), BlueGreenRole.SOURCE) is False
    assert routing.is_match(HostInfo(BLUE_HOST, PORT), BlueGreenRole.TARGET) is False
    assert routing.is_match(None, BlueGreenRole.SOURCE) is False
    assert SuspendConnectRouting(BLUE_IP, None, BG_ID).is_match(HostInfo(GREEN_IP), BlueGreenRole.SOURCE) is False
    assert SuspendExecuteRouting(None, BlueGreenRole.SOURCE, BG_ID).is_match(
        HostInfo(GREEN_HOST, PORT), BlueGreenRole.TARGET) is False


# If a routing returns no connection, the connect loop tries again for as long as the status stays the same,
# which here is for ever; the timeout ends such a call.
@pytest.mark.timeout(20)
def test_async_connect_during_post_goes_to_the_green_ip():
    """During POST, a new async connection to the blue host and port is opened to the green host's IP address that
    the substitute routing names, instead of to the blue name, which DNS may still resolve to blue."""
    green_ip = HostInfo(GREEN_IP, PORT)
    iam_hosts = (HostInfo(GREEN_HOST, PORT), HostInfo(BLUE_HOST, PORT))
    status = BlueGreenStatus(
        BG_ID,
        BlueGreenPhase.POST,
        connect_routings=[
            SubstituteConnectRouting(green_ip, BLUE_HOST, BlueGreenRole.SOURCE, iam_hosts),
            SubstituteConnectRouting(green_ip, f"{BLUE_HOST}:{PORT}", BlueGreenRole.SOURCE, iam_hosts),
            RejectConnectRouting(None, BlueGreenRole.TARGET),
        ],
        role_by_host={BLUE_HOST: BlueGreenRole.SOURCE, GREEN_HOST: BlueGreenRole.TARGET})
    plugin_service = MagicMock()
    plugin_service.network_bound_methods = set()
    plugin_service.get_status.return_value = status
    # Each fake connection remembers the host it was opened to: a routing opens the host it is given through the
    # plugin service, and connect_func opens the host the caller asked for.
    plugin_service.connect = AsyncMock(
        side_effect=lambda target_host, props, plugin_to_skip=None: SimpleNamespace(host=target_host.host))
    plugin = AsyncBlueGreenPlugin(plugin_service, Properties())

    conn = asyncio.run(plugin.connect(
        target_driver_func=MagicMock(),
        driver_dialect=MagicMock(),
        host_info=HostInfo(BLUE_HOST, PORT),
        props=Properties(),
        is_initial_connection=True,
        connect_func=AsyncMock(return_value=SimpleNamespace(host=BLUE_HOST))))

    assert conn.host == GREEN_IP, \
        f"C6: async connect() during POST opened '{conn.host}' instead of the green IP '{GREEN_IP}'"
