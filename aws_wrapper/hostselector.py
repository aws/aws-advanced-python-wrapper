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

import random
from typing import List, Protocol

from .hostinfo import HostRole, HostInfo
from .pep249 import Error


# Interface for a strategy defining how to pick a host from a list of hosts
class HostSelector(Protocol):

    def get_host(self, hosts: List[HostInfo], role: HostRole) -> HostInfo:
        ...


class RandomHostSelector(HostSelector):

    def get_host(self, hosts: List[HostInfo], role: HostRole) -> HostInfo:

        eligible_hosts = [host for host in hosts if host.role == role]

        if eligible_hosts.__len__() == 0:
            raise Error("No Eligible Hosts Found")

        return random.choice(eligible_hosts)
