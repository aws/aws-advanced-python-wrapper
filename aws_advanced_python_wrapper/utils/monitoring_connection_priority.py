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

from enum import Enum
from typing import List, Optional


class MonitoringConnectionPriority(Enum):
    """Priority of the topology monitor's background connection.

    The topology monitor accepts whatever connection it obtains first, then
    asynchronously upgrades to a higher-priority connection.
    """

    STRICT_WRITER = "strict-writer"
    STRICT_READER = "strict-reader"
    WRITER_OR_READER = "writer-or-reader"

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional[MonitoringConnectionPriority]:
        if value is None:
            return None
        return _NAME_TO_VALUE.get(value.strip().lower())

    @classmethod
    def parse_list(cls, value: Optional[str]) -> List[MonitoringConnectionPriority]:
        """Parse a comma-separated priority list.

        Defaults to ``[STRICT_WRITER]`` when the value is unset/empty or when no
        item parses to a known priority. Duplicates are dropped, order preserved.
        """
        result: List[MonitoringConnectionPriority] = []
        if value is None or not value.strip():
            result.append(cls.STRICT_WRITER)
            return result

        for item in value.split(","):
            priority = cls.from_value(item.strip())
            if priority is not None and priority not in result:
                result.append(priority)

        if not result:
            result.append(cls.STRICT_WRITER)
        return result

    def is_satisfied_by(self, is_writer: bool) -> bool:
        if self is MonitoringConnectionPriority.STRICT_WRITER:
            return is_writer
        if self is MonitoringConnectionPriority.STRICT_READER:
            return not is_writer
        if self is MonitoringConnectionPriority.WRITER_OR_READER:
            return True
        return False


_NAME_TO_VALUE = {
    "strict-writer": MonitoringConnectionPriority.STRICT_WRITER,
    "strict-reader": MonitoringConnectionPriority.STRICT_READER,
    "writer-or-reader": MonitoringConnectionPriority.WRITER_OR_READER,
}
