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

from typing import Hashable

import pytest

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole


def test_host_info_defaults():
    host_info = HostInfo("testhost")
    assert host_info.host == "testhost"
    assert host_info.is_port_specified() is False
    assert host_info.get_host_and_port() == "testhost"
    assert host_info.role == HostRole.WRITER
    assert host_info.availability == HostAvailability.AVAILABLE


def test_host_and_port_includes_the_port_when_specified():
    assert HostInfo("testhost", 1234).get_host_and_port() == "testhost:1234"


def test_url_keeps_its_trailing_slash():
    """The availability cache, the notify_host_list_changed change map and the
    host monitoring monitor keys are all keyed on this exact string."""
    assert HostInfo("testhost", 1234).url == "testhost:1234/"


def test_each_host_gets_its_own_availability_strategy():
    """A shared default would count one host's failures against all of them."""
    first = HostInfo("a")
    second = HostInfo("b")
    assert first.host_availability_strategy is not second.host_availability_strategy


def test_copy_carries_the_availability_strategy():
    from copy import copy

    from aws_advanced_python_wrapper.host_availability import \
        ExponentialBackoffHostAvailabilityStrategy
    from aws_advanced_python_wrapper.utils.properties import Properties

    strategy = ExponentialBackoffHostAvailabilityStrategy(Properties())
    original = HostInfo("testhost", 1234, host_availability_strategy=strategy)
    assert copy(original).host_availability_strategy is strategy


def test_host_info_eq():
    a = HostInfo("testhost", 1234, HostRole.WRITER, HostAvailability.AVAILABLE)
    b = HostInfo("testhost", 1234, HostRole.WRITER, HostAvailability.AVAILABLE)

    assert a == a
    assert a == b
    b.role = HostRole.READER
    assert a != b
    assert 3 != a


def test_host_info_hash():
    host_info = HostInfo("testhost")

    assert not isinstance(host_info, Hashable)
    with pytest.raises(TypeError):
        hash(host_info)
