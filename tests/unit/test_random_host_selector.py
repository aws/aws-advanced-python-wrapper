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

import pytest

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.host_selector import RandomHostSelector
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

HOST_ROLE = HostRole.READER


@pytest.mark.parametrize("execution_number", range(50))
def test_get_host_given_unavailable_host(execution_number):
    unavailable_host: HostInfo = HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE)
    available_host: HostInfo = HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE)

    host_selector = RandomHostSelector()
    actual_host = host_selector.get_host((unavailable_host, available_host), HOST_ROLE, Properties())

    assert available_host == actual_host


@pytest.mark.parametrize("execution_number", range(50))
def test_get_host_given_multiple_unavailable_hosts(execution_number):
    hosts = (
        HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE),
        HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE),
        HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE),
        HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE)
    )

    host_selector = RandomHostSelector()
    actual_host = host_selector.get_host(hosts, HOST_ROLE, Properties())

    assert HostAvailability.AVAILABLE == actual_host.get_availability()
