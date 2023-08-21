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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Set

from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, PropertiesUtils

if TYPE_CHECKING:
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection


class TargetDriverDialect(ABC):
    _dialect_code: str = TargetDriverDialectCodes.GENERIC
    _network_bound_methods: Set[str] = {"*"}
    _read_only: bool = False

    @property
    def dialect_code(self) -> str:
        return self._dialect_code

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._network_bound_methods

    def is_read_only(self, conn: Connection) -> bool:
        return self._read_only

    def set_read_only(self, conn: Connection, read_only: bool):
        self._read_only = read_only

    @abstractmethod
    def is_dialect(self, conn: Callable) -> bool:
        pass

    @abstractmethod
    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        pass

    @abstractmethod
    def is_closed(self, conn: Connection) -> bool:
        pass

    @abstractmethod
    def abort_connection(self, conn: Connection):
        pass

    @abstractmethod
    def is_in_transaction(self, conn: Connection) -> bool:
        pass


class GenericTargetDriverDialect(TargetDriverDialect):

    def is_dialect(self, conn: Callable) -> bool:
        return True

    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        prop_copy: Properties = Properties(props.copy())

        prop_copy["host"] = host_info.host

        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)

        PropertiesUtils.remove_wrapper_props(prop_copy)
        return prop_copy

    def is_closed(self, conn: Connection) -> bool:
        raise NotImplementedError(Messages.get_formatted("TargetDriverDialect.UnImplementedError", "is_closed"))

    def abort_connection(self, conn: Connection):
        raise NotImplementedError(Messages.get_formatted("TargetDriverDialect.UnImplementedError", "abort_connection"))

    def is_in_transaction(self, conn: Connection) -> bool:
        raise NotImplementedError(Messages.get_formatted("TargetDriverDialect.UnImplementedError", "is_in_transaction"))
