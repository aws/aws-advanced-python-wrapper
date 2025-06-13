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
    assert len(host_info.aliases) == 0
    assert len(host_info._all_aliases) == 1
    assert host_info.role == HostRole.WRITER
    assert host_info._availability == HostAvailability.AVAILABLE
    assert list(host_info._all_aliases)[0] == "testhost"


def test_host_as_alias():
    host_info = HostInfo("testhost", 1234)
    assert len(host_info._all_aliases) == 1
    assert list(host_info._all_aliases)[0] == "testhost:1234"


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
