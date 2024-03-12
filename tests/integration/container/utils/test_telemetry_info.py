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


class TestTelemetryInfo:
    __test__ = False

    _endpoint: str
    _endpoint_port: int

    def __init__(self, telemetry_info: Dict[str, Any]) -> None:
        if telemetry_info is None:
            return

        self._endpoint = typing.cast('str', telemetry_info.get("endpoint"))
        self._endpoint_port = typing.cast('int', telemetry_info.get("endpointPort"))

    def get_endpoint(self) -> str:
        return self._endpoint

    def get_endpoint_port(self) -> int:
        return self._endpoint_port
