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

from aws_wrapper.utils.properties import Properties, WrapperProperties


class FailoverMode(Enum):
    STRICT_WRITER = auto()
    STRICT_READER = auto()
    READER_OR_WRITER = auto()


def get_failover_mode(properties: Properties) -> Optional[FailoverMode]:
    mode = WrapperProperties.FAILOVER_MODE.get(properties)
    if mode is None:
        return None
    else:
        mode = mode.lower()
        # TODO: reconsider the exact format we expect from the user here
        if mode == "strict_writer":
            return FailoverMode.STRICT_WRITER
        elif mode == "strict_reader":
            return FailoverMode.STRICT_READER
        else:
            return FailoverMode.READER_OR_WRITER
