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

from aws_advanced_python_wrapper import pep249
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.host_selector import RandomHostSelector


def test_random_host_selection_strategy():
    random_host_selector: RandomHostSelector = RandomHostSelector()
    host_role = HostRole.WRITER
    host_info_list = [HostInfo("testhost1"), HostInfo("testhost2"), HostInfo("testhost3")]

    host: HostInfo = random_host_selector.get_host(host_info_list, host_role)

    assert host_info_list.__contains__(host)


def test_random_host_selector_selects_roles():
    random_host_selector: RandomHostSelector = RandomHostSelector()
    host_role = HostRole.READER
    host_info_list = [HostInfo("testhost1", role=HostRole.WRITER), HostInfo("testhost2", role=HostRole.READER)]

    host: HostInfo = random_host_selector.get_host(host_info_list, host_role)

    assert host.host == "testhost2"


def test_random_host_no_eligible_hosts():
    random_host_selector: RandomHostSelector = RandomHostSelector()
    host_role = HostRole.READER
    host_info_list = [HostInfo("testhost1"), HostInfo("testhost2"), HostInfo("testhost3")]

    with pytest.raises(Exception) as e_info:
        random_host_selector.get_host(host_info_list, host_role)

    assert isinstance(e_info.value, pep249.Error)
