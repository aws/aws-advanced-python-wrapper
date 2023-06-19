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

from typing import List, Optional, Protocol

from aws_wrapper.hostinfo import HostInfo, HostRole
from aws_wrapper.pep249 import Connection


class HostListProvider(Protocol):
    def refresh(self, connection: Optional[Connection] = None) -> List[HostInfo]:
        ...

    def force_refresh(self, connection: Optional[Connection] = None) -> List[HostInfo]:
        ...

    def get_host_role(self, connection: Connection) -> HostRole:
        ...

    def identify_connection(self, connection: Connection) -> HostInfo:
        ...


class DynamicHostListProvider(HostListProvider):
    ...


class StaticHostListProvider(HostListProvider):
    ...


class HostListProviderService:
    ...
