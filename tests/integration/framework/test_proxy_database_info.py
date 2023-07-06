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

import typing
from typing import Any, Dict

from .test_database_info import TestDatabaseInfo


class TestProxyDatabaseInfo(TestDatabaseInfo):
    __test__ = False

    _control_port: int

    def __init__(self, database_info: Dict[str, Any]) -> None:
        super().__init__(database_info)
        if database_info is None:
            return

        self._control_port = typing.cast('int', database_info.get("controlPort"))

    def get_control_port(self) -> int:
        return self._control_port
