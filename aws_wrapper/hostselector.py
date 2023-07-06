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

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .hostinfo import HostInfo, HostRole

import random
from typing import List, Protocol

from .pep249 import Error
from .utils.messages import Messages


# Interface for a strategy defining how to pick a host from a list of hosts
class HostSelector(Protocol):

    def get_host(self, hosts: List[HostInfo], role: HostRole) -> HostInfo:
        ...


class RandomHostSelector(HostSelector):

    def get_host(self, hosts: List[HostInfo], role: HostRole) -> HostInfo:

        eligible_hosts = [host for host in hosts if host.role == role]

        if len(eligible_hosts) == 0:
            raise Error(Messages.get("HostSelector.NoEligibleHost"))

        return random.choice(eligible_hosts)
