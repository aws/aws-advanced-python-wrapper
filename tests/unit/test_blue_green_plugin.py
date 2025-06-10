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
import time
from copy import deepcopy
from datetime import datetime
from types import MappingProxyType

from aws_advanced_python_wrapper.blue_green_plugin import (
    BlueGreenInterimStatus, BlueGreenPhase, BlueGreenRole, BlueGreenStatus,
    PassThroughConnectRouting, PassThroughExecuteRouting,
    SubstituteConnectRouting)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.value_container import ValueContainer


# TODO: remove unnecessary tests
def test_status_str():
    connect_routing1 = PassThroughConnectRouting(None, BlueGreenRole.SOURCE)
    connect_routing2 = PassThroughConnectRouting("localhost:5432", BlueGreenRole.SOURCE)
    connect_routings = (connect_routing1, connect_routing2)
    execute_routing1 = PassThroughExecuteRouting(None, BlueGreenRole.SOURCE)
    execute_routing2 = PassThroughExecuteRouting("localhost:5432", BlueGreenRole.SOURCE)
    execute_routings = (execute_routing1, execute_routing2)

    role_by_endpoint = MappingProxyType({"localhost-1": BlueGreenRole.SOURCE, "localhost-2": BlueGreenRole.TARGET})
    status = (
        BlueGreenStatus("asdf", BlueGreenPhase.PREPARATION, connect_routings, execute_routings, role_by_endpoint))
    print(f"\n{status}")


def test_interim_status_str():
    start_ips = ConcurrentDict()
    start_ips.put_if_absent("instance-1", ValueContainer.of("1.1.1.1"))
    start_ips.put_if_absent("instance-2", ValueContainer.empty())
    status = BlueGreenInterimStatus(
        BlueGreenPhase.CREATED,
        "1.0",
        5432,
        (HostInfo("instance-1"), HostInfo("instance-2")),
        start_ips,
        (HostInfo("instance-1"), HostInfo("instance-2")),
        start_ips,
        {"instance-1", "instance-2"},
        True,
        True,
        False
    )

    print(f"\n{status}")


def test_substitute_connect_routing():
    example_host = HostInfo("instance-1sdfsaklfdjsaklfdjsaklfjslkdfjslkdfjsa", 5432, HostRole.WRITER, HostAvailability.AVAILABLE)
    iam_hosts = (example_host, example_host, example_host)
    routing = SubstituteConnectRouting(
        "instance-1:5432",
        BlueGreenRole.SOURCE,
        example_host,
        iam_hosts,
        lambda host: None
    )

    print(f"\n{routing}")


def test_host_copy():
    h1 = HostInfo("localhost", 5432, HostRole.READER, HostAvailability.UNAVAILABLE, weight=5, host_id="localhost", last_update_time=datetime.now())
    h2 = deepcopy(h1)
    assert h1 == h2


def test_time():
    print(time.time())
