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

from enum import Enum, auto
from typing import Optional

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages


class GdbFailoverMode(Enum):
    STRICT_WRITER = auto()
    STRICT_HOME_READER = auto()
    STRICT_OUT_OF_HOME_READER = auto()
    STRICT_ANY_READER = auto()
    HOME_READER_OR_WRITER = auto()
    OUT_OF_HOME_READER_OR_WRITER = auto()
    ANY_READER_OR_WRITER = auto()

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional["GdbFailoverMode"]:
        if value is None or not value.strip():
            return None

        normalized = value.strip().lower().replace("_", "-")
        mode = _NAME_TO_VALUE.get(normalized)
        if mode is None:
            raise AwsWrapperError(
                Messages.get_formatted("GlobalDbFailoverMode.InvalidValue", value))
        return mode


_NAME_TO_VALUE = {
    "strict-writer": GdbFailoverMode.STRICT_WRITER,
    "strict-home-reader": GdbFailoverMode.STRICT_HOME_READER,
    "strict-out-of-home-reader": GdbFailoverMode.STRICT_OUT_OF_HOME_READER,
    "strict-any-reader": GdbFailoverMode.STRICT_ANY_READER,
    "home-reader-or-writer": GdbFailoverMode.HOME_READER_OR_WRITER,
    "out-of-home-reader-or-writer": GdbFailoverMode.OUT_OF_HOME_READER_OR_WRITER,
    "any-reader-or-writer": GdbFailoverMode.ANY_READER_OR_WRITER,
}
