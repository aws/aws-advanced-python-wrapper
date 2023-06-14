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
from typing import Any, Dict, Iterable, Set

from .database_engine import DatabaseEngine
from .database_engine_deployment import DatabaseEngineDeployment
from .database_instances import DatabaseInstances
from .target_python_version import TargetPythonVersion
from .test_environment_features import TestEnvironmentFeatures


class TestEnvironmentRequest:
    __test__ = False

    _engine: DatabaseEngine
    _instances: DatabaseInstances
    _deployment: DatabaseEngineDeployment
    _target_python_version: TargetPythonVersion
    _features: Set[TestEnvironmentFeatures]
    _num_of_instances: int = 1

    def __init__(self, dict: Dict[str, Any]) -> None:
        if dict is None:
            return

        self._engine = getattr(DatabaseEngine,
                               typing.cast(str, dict.get("engine")))
        self._instances = getattr(DatabaseInstances,
                                  typing.cast(str, dict.get("instances")))
        self._deployment = getattr(DatabaseEngineDeployment,
                                   typing.cast(str, dict.get("deployment")))
        self._target_python_version = getattr(
            TargetPythonVersion,
            typing.cast(str, dict.get("targetPythonVersion")))
        self._num_of_instances = typing.cast(int, dict.get("numOfInstances"))
        self._features = set()
        features: Iterable[str] = typing.cast(Iterable[str],
                                              dict.get("features"))
        if features is not None:
            for f in features:
                self._features.add(getattr(TestEnvironmentFeatures, f))

    def get_database_engine(self) -> DatabaseEngine:
        return self._engine

    def get_database_instances(self) -> DatabaseInstances:
        return self._instances

    def get_database_engine_deployment(self) -> DatabaseEngineDeployment:
        return self._deployment

    def get_target_python_version(self) -> TargetPythonVersion:
        return self._target_python_version

    def get_features(self) -> Set[TestEnvironmentFeatures]:
        return self._features

    def get_num_of_instances(self) -> int:
        return self._num_of_instances

    def get_display_name(self) -> str:
        return "Test environment [{0}, {1}, {2}, {3}, {4}, {5}]".format(
            self._deployment, self._engine, self._target_python_version,
            self._instances, self._num_of_instances, self._features)
