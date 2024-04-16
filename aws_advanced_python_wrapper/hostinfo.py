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

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, ClassVar, FrozenSet, Optional, Set

from aws_advanced_python_wrapper.host_availability import (
    HostAvailability, HostAvailabilityStrategy)

if TYPE_CHECKING:
    from datetime import datetime


class HostRole(Enum):
    UNKNOWN = auto()
    READER = auto()
    WRITER = auto()


@dataclass(eq=False)
class HostInfo:
    NO_PORT: ClassVar[int] = -1
    DEFAULT_WEIGHT = 100

    def __init__(
            self,
            host: str,
            port: int = NO_PORT,
            role: HostRole = HostRole.WRITER,
            availability: HostAvailability = HostAvailability.AVAILABLE,
            host_availability_strategy=HostAvailabilityStrategy(),
            weight: int = DEFAULT_WEIGHT,
            host_id: Optional[str] = None,
            last_update_time: Optional[datetime] = None):
        self.host = host
        self.port = port
        self.role = role
        self._availability = availability
        self.host_availability_strategy = host_availability_strategy
        self.weight = weight,
        self.host_id = host_id
        self.last_update_time = last_update_time

        self._aliases: Set[str] = set()
        self._all_aliases: Set[str] = {self.as_alias()}

    def __eq__(self, other: object):
        if self is object:
            return True
        if not isinstance(other, HostInfo):
            return False

        return self.host == other.host \
            and self.port == other.port \
            and self._availability == other._availability \
            and self.role == other.role

    def __str__(self):
        return f"HostInfo({self.host}, {self.port}, {self.role}, {self._availability})"

    @property
    def url(self):
        if self.is_port_specified():
            return f"{self.host}:{self.port}"
        else:
            return self.host

    @property
    def aliases(self) -> FrozenSet[str]:
        return frozenset(self._aliases)

    @property
    def all_aliases(self) -> FrozenSet[str]:
        return frozenset(self._all_aliases)

    def as_alias(self) -> str:
        return f"{self.host}:{self.port}" if self.is_port_specified() else self.host

    def add_alias(self, *aliases: str):
        if not aliases:
            return

        for alias in aliases:
            self._aliases.add(alias)
            self._all_aliases.add(alias)

    def as_aliases(self) -> FrozenSet[str]:
        return frozenset(self.all_aliases)

    def remove_alias(self, *kwargs):
        if not kwargs or len(kwargs) == 0:
            return

        for x in kwargs:
            self._aliases.discard(x)
            self._all_aliases.discard(x)

    def reset_aliases(self):
        self._aliases.clear()
        self._all_aliases.clear()
        self._all_aliases.add(self.as_alias())

    def is_port_specified(self) -> bool:
        return self.port != HostInfo.NO_PORT

    def get_availability(self) -> HostAvailability:
        if self.host_availability_strategy is not None:
            return self.host_availability_strategy.get_host_availability(self._availability)

        return self._availability

    def get_raw_availability(self) -> HostAvailability:
        return self._availability

    def set_availability(self, availability: HostAvailability):
        self._availability = availability
        if self.host_availability_strategy is not None:
            self.host_availability_strategy.set_host_availability(availability)

    def get_host_availability_strategy(self):
        return self.host_availability_strategy

    def set_host_availability_strategy(self, host_availability_strategy):
        self.host_availability_strategy = host_availability_strategy
