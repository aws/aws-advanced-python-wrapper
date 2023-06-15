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

from unittest import TestCase

from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole


class TestHostInfo(TestCase):
    def test_host_info_defaults(self):
        host_info = HostInfo("testhost")
        assert host_info.host == "testhost"
        assert host_info.is_port_specified() is False
        assert host_info.aliases.__len__() == 0
        assert host_info.all_aliases.__len__() == 1
        assert host_info.role == HostRole.WRITER
        assert host_info.availability == HostAvailability.AVAILABLE
        assert list(host_info.all_aliases)[0] == "testhost"

    def test_host_as_alias(self):
        host_info = HostInfo("testhost", 1234)
        assert host_info.all_aliases.__len__() == 1
        assert list(host_info.all_aliases)[0] == "testhost:1234"
