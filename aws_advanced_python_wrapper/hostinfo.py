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
from typing import TYPE_CHECKING, ClassVar, Optional, Tuple, TypeAlias

from aws_advanced_python_wrapper.host_availability import (
    HostAvailability, HostAvailabilityStrategy)

Topology: TypeAlias = Tuple["HostInfo", ...]

if TYPE_CHECKING:
    from datetime import datetime


class HostRole(Enum):
    UNKNOWN = auto()
    READER = auto()
    WRITER = auto()


@dataclass(eq=False)
class HostInfo:
    NO_PORT: ClassVar[int] = -1
    DEFAULT_WEIGHT: ClassVar[int] = 100

    def __init__(
            self,
            host: str,
            port: int = NO_PORT,
            role: HostRole = HostRole.WRITER,
            availability: HostAvailability = HostAvailability.AVAILABLE,
            host_availability_strategy: Optional[HostAvailabilityStrategy] = None,
            weight: int = DEFAULT_WEIGHT,
            host_id: Optional[str] = None,
            last_update_time: Optional[datetime] = None):
        self.host = host
        self.port = port
        self.role = role
        self.availability = availability
        # A default argument is evaluated once, so every host that omitted it
        # would share one strategy object.
        self.host_availability_strategy = (
            host_availability_strategy if host_availability_strategy is not None
            else HostAvailabilityStrategy())
        self.weight = weight
        self.host_id = host_id
        self.last_update_time = last_update_time

    def __eq__(self, other: object):
        if self is object:
            return True
        if not isinstance(other, HostInfo):
            return False

        return self.host == other.host \
            and self.port == other.port \
            and self.availability == other.availability \
            and self.role == other.role

    def __str__(self):
        return f"HostInfo({self.host}, {self.port}, {self.role}, {self.availability})"

    def __repr__(self):
        return f"HostInfo({self.host}, {self.port}, {self.role}, {self.availability})"

    def __copy__(self):
        return HostInfo(
            host=self.host,
            port=self.port,
            role=self.role,
            availability=self.availability,
            # Shared by reference, so the copy keeps the original's backoff
            # count and retry window rather than starting a fresh one.
            host_availability_strategy=self.host_availability_strategy,
            weight=self.weight,
            host_id=self.host_id,
            last_update_time=self.last_update_time
        )

    @property
    def url(self):
        # The trailing slash is part of the format: the availability cache, the
        # notify_host_list_changed change map and the monitor keys all use it.
        return f"{self.get_host_and_port()}/"

    def get_host_and_port(self):
        return f"{self.host}:{self.port}" if self.is_port_specified() else self.host

    def is_port_specified(self) -> bool:
        return self.port != HostInfo.NO_PORT

    def get_availability(self) -> HostAvailability:
        if self.host_availability_strategy is not None:
            return self.host_availability_strategy.get_host_availability(self.availability)

        return self.availability

    def get_raw_availability(self) -> HostAvailability:
        return self.availability

    def set_availability(self, availability: HostAvailability):
        self.availability = availability
        if self.host_availability_strategy is not None:
            self.host_availability_strategy.set_host_availability(availability)

    def get_host_availability_strategy(self):
        return self.host_availability_strategy

    def set_host_availability_strategy(self, host_availability_strategy):
        self.host_availability_strategy = host_availability_strategy
