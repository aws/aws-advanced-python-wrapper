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

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.host_selector import HighestWeightHostSelector
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

HOST_ROLE = HostRole.READER


def test_get_host_given_unavailable_host():
    unavailable_host: HostInfo = HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE)
    available_host: HostInfo = HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE)

    host_selector = HighestWeightHostSelector()
    actual_host = host_selector.get_host((unavailable_host, available_host), HOST_ROLE, Properties())

    assert available_host == actual_host


def test_get_host_given_multiple_unavailable_hosts():
    hosts = (
        HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE),
        HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE),
        HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE)
    )

    host_selector = HighestWeightHostSelector()
    actual_host = host_selector.get_host(hosts, HOST_ROLE, Properties())

    assert HostAvailability.AVAILABLE == actual_host.get_availability()


def test_get_host_given_different_weights():

    highest_weight_host = HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE, weight=3)

    hosts = (
        HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE),
        HostInfo(host="some_unavailable_host", role=HOST_ROLE, availability=HostAvailability.UNAVAILABLE),
        HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE, weight=1),
        HostInfo(host="some_available_host", role=HOST_ROLE, availability=HostAvailability.AVAILABLE, weight=2),
        highest_weight_host
    )

    host_selector = HighestWeightHostSelector()
    actual_host = host_selector.get_host(hosts, HOST_ROLE, Properties())

    assert actual_host == highest_weight_host
