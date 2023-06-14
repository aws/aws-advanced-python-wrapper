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
from typing import Any, Dict, Iterable, List

from .test_instance_info import TestInstanceInfo


class TestDatabaseInfo:
    __test__ = False

    _username: str
    _password: str
    _default_db_name: str

    _cluster_endpoint: str  # "ABC.cluster-XYZ.us-west-2.rds.amazonaws.com"
    _cluster_endpoint_port: int

    _cluster_read_only_endpoint: str  # "ABC.cluster-ro-XYZ.us-west-2.rds.amazonaws.com"
    _cluster_read_only_endpoint_port: int

    _instance_endpoint_suffix: str  # "XYZ.us-west-2.rds.amazonaws.com"
    _instance_endpoint_port: int

    _instances: List[TestInstanceInfo]

    def __init__(self, dict: Dict[str, Any]) -> None:
        if dict is None:
            return

        self._username = typing.cast(str, dict.get("username"))
        self._password = typing.cast(str, dict.get("password"))
        self._default_db_name = typing.cast(str, dict.get("defaultDbName"))
        self._cluster_endpoint = typing.cast(str, dict.get("clusterEndpoint"))
        self._cluster_endpoint_port = typing.cast(int, dict.get("clusterEndpointPort"))
        self._cluster_read_only_endpoint = typing.cast(str, dict.get("clusterReadOnlyEndpoint"))
        self._cluster_read_only_endpoint_port = typing.cast(int, dict.get("clusterReadOnlyEndpointPort"))
        self._instance_endpoint_suffix = typing.cast(str, dict.get("instanceEndpointSuffix"))
        self._instance_endpoint_port = typing.cast(int, dict.get("instanceEndpointPort"))

        self._instances = list()
        instances: Iterable[Any] = typing.cast(Iterable[Any],
                                               dict.get("instances"))
        if instances is not None:
            for f in instances:
                if f is not None:
                    self._instances.append(TestInstanceInfo(f))

    def get_instances(self) -> List[TestInstanceInfo]:
        return self._instances

    def get_username(self) -> str:
        return self._username

    def get_password(self) -> str:
        return self._password

    def get_default_db_name(self) -> str:
        return self._default_db_name

    def get_cluster_endpoint(self) -> str:
        return self._cluster_endpoint

    def get_cluster_endpoint_port(self) -> int:
        return self._cluster_endpoint_port

    def get_cluster_read_only_endpoint(self) -> str:
        return self._cluster_read_only_endpoint

    def get_cluster_read_only_endpoint_port(self) -> int:
        return self._cluster_read_only_endpoint_port

    def get_instance_endpoint_suffix(self) -> str:
        return self._instance_endpoint_suffix

    def get_instance_endpoint_port(self) -> int:
        return self._instance_endpoint_port

    def get_instance(self, instance_name: str) -> TestInstanceInfo:
        for i in self._instances:
            if i.get_instance_id() == instance_name:
                return i
        raise Exception("Instance {0} not found.".format(instance_name))

    def move_instance_first(self, instance_name: str):
        if instance_name is None:
            return

        for i in range(len(self._instances)):
            instance: TestInstanceInfo = self._instances[i]
            if instance.get_instance_id() == instance_name:
                self._instances.remove(instance)
                self._instances.insert(0, instance)
                return
        raise Exception("Instance {0} not found.".format(instance_name))
