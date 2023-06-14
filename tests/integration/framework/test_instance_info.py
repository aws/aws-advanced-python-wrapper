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


class TestInstanceInfo:
    __test__ = False

    _instance_id: str  # "instance-1"
    _host: str  # "instance-1.ABC.cluster-XYZ.us-west-2.rds.amazonaws.com"
    _port: int

    def __init__(self, dict: Dict[str, Any]) -> None:
        if dict is None:
            return

        self._instance_id = typing.cast(str, dict.get("instanceId"))
        self._host = typing.cast(str, dict.get("host"))
        self._port = typing.cast(int, dict.get("port"))

    def get_instance_id(self) -> str:
        return self._instance_id

    def get_host(self) -> str:
        return self._host

    def get_port(self) -> int:
        return self._port
