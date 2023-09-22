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

    def __init__(self, instance_info: Dict[str, Any]) -> None:
        instance_id = instance_info.get("instanceId")
        host = instance_info.get("host")
        port = instance_info.get("port")

        if instance_id is None:
            instance_id = instance_info.get("DBInstanceIdentifier")

        endpoint = instance_info.get("Endpoint")
        if endpoint is not None:
            if host is None:
                host = endpoint.get("Address")
            if port is None:
                port = endpoint.get("Port")

        self._instance_id = typing.cast('str', instance_id)
        self._host = typing.cast('str', host)
        self._port = typing.cast('int', port)

    def get_instance_id(self) -> str:
        return self._instance_id

    def get_host(self) -> str:
        return self._host

    def get_port(self) -> int:
        return self._port

    def get_url(self) -> str:
        return self._host + ':' + str(self._port)
