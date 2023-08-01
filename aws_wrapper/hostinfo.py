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

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import ClassVar, FrozenSet, Optional, Set


class HostAvailability(Enum):
    UNCERTAIN = auto()
    AVAILABLE = auto()
    NOT_AVAILABLE = auto()


class HostRole(Enum):
    UNKNOWN = auto()
    READER = auto()
    WRITER = auto()


@dataclass(eq=False)
class HostInfo:
    NO_PORT: ClassVar[int] = -1

    host: str
    port: int = NO_PORT
    availability: HostAvailability = HostAvailability.AVAILABLE
    role: HostRole = HostRole.WRITER
    _aliases: Set[str] = field(default_factory=set)
    _all_aliases: Set[str] = field(default_factory=set)
    last_update_time: datetime = datetime.now()
    host_id: Optional[str] = None

    def __post_init__(self):
        self._all_aliases.add(self.as_alias())

    def __eq__(self, other: object):
        if self is object:
            return True
        if not isinstance(other, HostInfo):
            return False

        return self.host == other.host \
            and self.port == other.port \
            and self.availability == other.availability \
            and self.role == other.role

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

    def add_alias(self, *aliases):
        if not aliases:
            return

        for alias in aliases:
            self._aliases.add(alias)
            self._all_aliases.add(alias)

    def as_aliases(self):
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
