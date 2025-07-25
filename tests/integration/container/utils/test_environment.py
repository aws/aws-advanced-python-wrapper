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

from typing import TYPE_CHECKING

from aws_xray_sdk import global_sdk_config
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.sampling.local.sampler import LocalSampler
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import \
    OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

if TYPE_CHECKING:
    from .database_engine_deployment import DatabaseEngineDeployment
    from .test_database_info import TestDatabaseInfo
    from .test_instance_info import TestInstanceInfo
    from .test_proxy_database_info import TestProxyDatabaseInfo

import json
import os
import typing
from typing import Any, Dict, List, Optional

from toxiproxy import Toxiproxy  # type: ignore

from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.messages import Messages
from .database_engine import DatabaseEngine
from .proxy_info import ProxyInfo
from .test_driver import TestDriver
from .test_environment_features import TestEnvironmentFeatures
from .test_environment_info import TestEnvironmentInfo


class TestEnvironment:
    __test__ = False

    _env: TestEnvironment  # class variable

    _info: TestEnvironmentInfo
    _proxies: Optional[Dict[str, ProxyInfo]]

    _current_driver: Optional[TestDriver]

    def __init__(self, test_info: Dict[str, Any]) -> None:
        self._info = TestEnvironmentInfo(test_info)
        self._proxies = None
        self._current_driver = None

    def __repr__(self):
        deployment = self.get_deployment()
        engine = self.get_engine()
        num_instances = self.get_info().get_request().get_num_of_instances()
        python_version = self.get_info().get_request().get_target_python_version()
        return (f"TestEnvironment[deployment={deployment},engine={engine},instances={num_instances},"
                f"python_version={python_version},driver={self._current_driver}]")

    @staticmethod
    def get_current() -> TestEnvironment:
        if not hasattr(TestEnvironment, "_env") or TestEnvironment._env is None:
            TestEnvironment._env = TestEnvironment._create()
        return TestEnvironment._env

    @staticmethod
    def _create() -> TestEnvironment:
        info_json: Optional[str] = os.getenv("TEST_ENV_INFO_JSON")
        if info_json is None:
            raise Exception(
                Messages.get_formatted("Testing.EnvVarRequired", "TEST_ENV_INFO_JSON"))

        test_info: Dict[str, Any] = typing.cast('Dict[str, Any]', json.loads(str(info_json)))
        if not bool(dict):
            raise Exception(Messages.get_formatted("Testing.CantParse", "TEST_ENV_INFO_JSON"))
        else:
            env = TestEnvironment(test_info)

            if TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED in env.get_features():
                TestEnvironment._init_proxies(env)

            if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in env.get_features():
                xray_daemon_endpoint: str = ('{0}:{1}'
                                             .format(env.get_info().get_traces_telemetry_info()
                                                     .get_endpoint(),
                                                     str(env.get_info().get_traces_telemetry_info()
                                                         .get_endpoint_port())))
                xray_recorder.configure(daemon_address=xray_daemon_endpoint,
                                        context_missing="IGNORE_ERROR",
                                        sampler=LocalSampler(
                                            {"version": 1, "default": {"fixed_target": 1, "rate": 1.0}}))
                global_sdk_config.set_sdk_enabled(True)

            if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in env.get_features():
                otlp_daemon_endpoint: str = ('http://{0}:{1}'
                                             .format(env.get_info().get_metrics_telemetry_info().get_endpoint(),
                                                     str(env.get_info().get_metrics_telemetry_info()
                                                         .get_endpoint_port())))
                resource = Resource.create(attributes={SERVICE_NAME: "integration_tests"})
                reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otlp_daemon_endpoint),
                                                       export_interval_millis=10000)
                provider = MeterProvider(resource=resource, metric_readers=[reader])
                metrics.set_meter_provider(provider)

            return env

    @staticmethod
    def _init_proxies(environment: TestEnvironment):
        environment._proxies: Dict[str, ProxyInfo] = dict()  # type: ignore
        proxy_control_port: int = environment.get_proxy_database_info().get_control_port()

        for instance in environment.get_proxy_instances():
            client = Toxiproxy()
            client.update_api_consumer(instance.get_host(), proxy_control_port)
            proxies = client.proxies()
            if not bool(proxies):
                raise Exception(Messages.get_formatted("Testing.ProxyNotFound", instance.get_instance_id()))

            proxy_info = ProxyInfo(
                list(proxies.values())[0], instance.get_host(), proxy_control_port)
            environment._proxies[instance.get_instance_id()] = proxy_info

        if environment.get_proxy_database_info().get_cluster_endpoint() is not None:
            client = Toxiproxy()
            client.update_api_consumer(
                environment.get_proxy_database_info().get_cluster_endpoint(),
                proxy_control_port)

            proxy = client.get_proxy("{0}:{1}".format(
                environment.get_database_info().get_cluster_endpoint(),
                environment.get_database_info().get_cluster_endpoint_port()))

            if proxy is not None:
                proxy_info = ProxyInfo(
                    proxy,
                    environment.get_proxy_database_info().
                    get_cluster_endpoint(), proxy_control_port)
                environment._proxies[
                    environment.get_proxy_database_info().get_cluster_endpoint()] = proxy_info

        if environment.get_proxy_database_info().get_cluster_read_only_endpoint() is not None:
            client = Toxiproxy()
            client.update_api_consumer(
                environment.get_proxy_database_info().get_cluster_read_only_endpoint(),
                proxy_control_port)

            proxy = client.get_proxy("{0}:{1}".format(
                environment.get_database_info().get_cluster_read_only_endpoint(),
                environment.get_database_info().get_cluster_read_only_endpoint_port()))

            if proxy is not None:
                proxy_info = ProxyInfo(
                    proxy,
                    environment.get_proxy_database_info().
                    get_cluster_read_only_endpoint(), proxy_control_port)
                environment._proxies[
                    environment.get_proxy_database_info().get_cluster_read_only_endpoint()] = proxy_info

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

    def get_features(self):
        return self.get_info().get_request().get_features()

    def get_deployment(self) -> DatabaseEngineDeployment:
        return self.get_info().get_request().get_database_engine_deployment()

    def get_engine(self) -> DatabaseEngine:
        return self.get_info().get_request().get_database_engine()

    def get_database_info(self) -> TestDatabaseInfo:
        return self.get_info().get_database_info()

    def get_instances(self) -> List[TestInstanceInfo]:
        return self.get_database_info().get_instances()

    def get_writer(self) -> TestInstanceInfo:
        return self.get_instances()[0]

    def get_cluster_name(self) -> str:
        return self.get_info().get_db_name()

    def get_proxy_database_info(self) -> TestProxyDatabaseInfo:
        return self.get_info().get_proxy_database_info()

    def get_proxy_instances(self) -> List[TestInstanceInfo]:
        return self.get_proxy_database_info().get_instances()

    def get_proxy_writer(self) -> TestInstanceInfo:
        return self.get_proxy_instances()[0]

    def get_aurora_region(self) -> str:
        return self.get_info().get_region()

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
        features = self.get_features()
        database_engine = self.get_engine()

        if test_driver == TestDriver.MYSQL:
            driver_compatible_to_database_engine = database_engine == DatabaseEngine.MYSQL
            disabled_by_feature = TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS in features
        elif test_driver == TestDriver.PG:
            driver_compatible_to_database_engine = (
                    database_engine == DatabaseEngine.PG)
            disabled_by_feature = TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS in features
        else:
            raise UnsupportedOperationError(test_driver.value)

        if disabled_by_feature or (not driver_compatible_to_database_engine):
            return False

        return True
