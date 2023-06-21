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

from abc import abstractmethod
from typing import Optional, Protocol, runtime_checkable

from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.dialect import Dialect


class HostListProvider(Protocol):
    def refresh(self, connection: Optional[Connection] = None):
        ...

    def force_refresh(self, connection: Optional[Connection] = None):
        ...

    def get_host_role(self, connection: Optional[Connection] = None):
        ...

    def identify_connection(self, connection: Optional[Connection] = None):
        ...


@runtime_checkable
class DynamicHostListProvider(HostListProvider, Protocol):
    ...


@runtime_checkable
class StaticHostListProvider(HostListProvider, Protocol):
    ...


class ConnectionStringHostListProvider(StaticHostListProvider):
    def refresh(self, connection: Optional[Connection] = None):
        ...

    def force_refresh(self, connection: Optional[Connection] = None):
        ...

    def get_host_role(self, connection: Optional[Connection] = None):
        ...

    def identify_connection(self, connection: Optional[Connection] = None):
        ...


class HostListProviderService(Protocol):
    @property
    @abstractmethod
    def current_connection(self) -> Optional[Connection]:
        ...

    @property
    @abstractmethod
    def current_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    @abstractmethod
    def dialect(self) -> Dialect:
        ...

    @property
    @abstractmethod
    def host_list_provider(self) -> HostListProvider:
        ...

    @host_list_provider.setter
    def host_list_provider(self, value: HostListProvider):
        ...

    @property
    @abstractmethod
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        ...

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: HostInfo):
        ...

    def is_static_host_list_provider(self) -> bool:
        ...
