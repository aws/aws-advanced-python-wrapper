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
from enum import Enum
from typing import Set

NO_PORT: int = -1
DEFAULT_WEIGHT: int = 100


class HostAvailability(Enum):
    UNCERTAIN = 1,
    AVAILABLE = 2,
    NOT_AVAILABLE = 3


class HostRole(Enum):
    UNKNOWN = 1,
    READER = 2,
    WRITER = 3


@dataclass
class HostSpec:
    host: str
    port: int = NO_PORT
    availability: HostAvailability = HostAvailability.AVAILABLE
    role: HostRole = HostRole.WRITER
    aliases: Set[str] = field(default_factory=set)
    all_aliases: Set[str] = field(default_factory=set)
    last_update_time: datetime = datetime.now()

    def __post_init__(self):
        self.all_aliases.add(self.as_alias())

    def is_port_specified(self) -> bool:
        return self.port != NO_PORT

    def as_alias(self) -> str:
        return (self.host + ":" + str(self.port)) if self.is_port_specified() else self.host
