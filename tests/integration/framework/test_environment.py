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

import json
import os
import typing
from typing import Any, Dict, List, Optional

from toxiproxy import Toxiproxy  # type: ignore

from aws_wrapper.utils.messages import Messages
from .database_engine import DatabaseEngine
from .proxy_info import ProxyInfo
from .test_driver import TestDriver
from .test_environment_features import TestEnvironmentFeatures
from .test_environment_info import TestEnvironmentInfo


class TestEnvironment():
    __test__ = False

    _env: "TestEnvironment"  # class variable

    _info: TestEnvironmentInfo
    _proxies: Optional[Dict[str, ProxyInfo]]

    _current_driver: Optional[TestDriver]

    def __init__(self, dict: Dict[str, Any]) -> None:
        self._info = TestEnvironmentInfo(dict)
        self._proxies = None
        self._current_driver = None

    @staticmethod
    def get_current() -> "TestEnvironment":
        if not hasattr(TestEnvironment, "_env") or TestEnvironment._env is None:
            TestEnvironment._env = TestEnvironment._create()
        return TestEnvironment._env

    @staticmethod
    def _create() -> "TestEnvironment":
        info_json: Optional[str] = os.getenv("TEST_ENV_INFO_JSON")
        if info_json is None:
            raise Exception(
                Messages.get_formatted("Testing.EnvVarRequired", "TEST_ENV_INFO_JSON"))

        dict: Dict[str, Any] = typing.cast(Dict[str, Any],
                                           json.loads(str(info_json)))
        if not bool(dict):
            raise Exception(Messages.get_formatted("Testing.CantParse", "TEST_ENV_INFO_JSON"))
        else:
            env = TestEnvironment(dict)

            if TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED in env.get_info().get_request().get_features():
                TestEnvironment._init_proxies(env)

            return env

    @staticmethod
    def _init_proxies(environment: "TestEnvironment"):
        environment._proxies: Dict[str, ProxyInfo] = dict()  # type: ignore
        proxy_control_port: int = environment.get_info().get_proxy_database_info().get_control_port()

        for instance in environment.get_info().get_proxy_database_info().get_instances():
            client = Toxiproxy()
            client.update_api_consumer(instance.get_host(), proxy_control_port)
            proxies = client.proxies()
            if not bool(proxies):
                raise Exception(Messages.get_formatted("Testing.ProxyNotFound", instance.get_instance_id()))

            proxy_info = ProxyInfo(
                list(proxies.values())[0], instance.get_host(), proxy_control_port)
            environment._proxies[instance.get_instance_id()] = proxy_info

        if environment.get_info().get_proxy_database_info().get_cluster_endpoint() is not None:
            client = Toxiproxy()
            client.update_api_consumer(
                environment.get_info().get_proxy_database_info().get_cluster_endpoint(),
                proxy_control_port)

            proxy = client.get_proxy("{0}:{1}".format(
                environment.get_info().get_database_info().get_cluster_endpoint(),
                environment.get_info().get_database_info().get_cluster_endpoint_port()))

            if proxy is not None:
                proxy_info = ProxyInfo(
                    proxy,
                    environment.get_info().get_proxy_database_info().
                    get_cluster_endpoint(), proxy_control_port)
                environment._proxies[
                    environment.get_info().get_proxy_database_info().get_cluster_endpoint()] = proxy_info

        if environment.get_info().get_proxy_database_info().get_cluster_read_only_endpoint() is not None:
            client = Toxiproxy()
            client.update_api_consumer(
                environment.get_info().get_proxy_database_info().get_cluster_read_only_endpoint(),
                proxy_control_port)

            proxy = client.get_proxy("{0}:{1}".format(
                environment.get_info().get_database_info().get_cluster_read_only_endpoint(),
                environment.get_info().get_database_info().get_cluster_read_only_endpoint_port()))

            if proxy is not None:
                proxy_info = ProxyInfo(
                    proxy,
                    environment.get_info().get_proxy_database_info().
                    get_cluster_read_only_endpoint(), proxy_control_port)
                environment._proxies[
                    environment.get_info().get_proxy_database_info().get_cluster_read_only_endpoint()] = proxy_info

    def get_proxy_info(self, instance_name: str) -> ProxyInfo:
        if self._proxies is None:
            raise Exception(Messages.get_formatted("Testing.ProxyNotFound", instance_name))
        p: ProxyInfo = self._proxies.get(instance_name)  # type: ignore
        if p is None:
            raise Exception(Messages.get_formatted("Testing.ProxyNotFound", instance_name))
        return p

    def get_proxy_infos(self) -> List[ProxyInfo]:
        return list(self._proxies.values())  # type: ignore

    def get_info(self) -> TestEnvironmentInfo:
        return self._info

    def set_current_driver(self, test_driver: TestDriver):
        self._current_driver = test_driver

    def get_current_driver(self) -> Optional[TestDriver]:
        return self._current_driver

    def get_allowed_test_drivers(self) -> List[TestDriver]:
        allowed_test_drivers: List[TestDriver] = list()
        for d in list(TestDriver):
            if self.is_test_driver_allowed(d):
                allowed_test_drivers.append(d)

        return allowed_test_drivers

    def is_test_driver_allowed(self, test_driver: TestDriver) -> bool:
        features = self._info.get_request().get_features()
        database_engine = self._info.get_request().get_database_engine()

        driver_compatible_to_database_engine: bool = False
        disabled_by_feature: bool = False

        if test_driver == TestDriver.MYSQL:
            driver_compatible_to_database_engine = (database_engine == DatabaseEngine.MYSQL) \
             or (database_engine == DatabaseEngine.MARIADB)
            disabled_by_feature = TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS in features
        elif test_driver == TestDriver.PG:
            driver_compatible_to_database_engine = (
                database_engine == DatabaseEngine.PG)
            disabled_by_feature = TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS in features

        elif test_driver == TestDriver.MARIADB:
            driver_compatible_to_database_engine = (database_engine == DatabaseEngine.MYSQL) \
             or (database_engine == DatabaseEngine.MARIADB)
            disabled_by_feature = TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS in features

        else:
            raise NotImplementedError(test_driver)

        if disabled_by_feature or (not driver_compatible_to_database_engine):
            return False

        return True
