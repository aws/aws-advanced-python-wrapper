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

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver
    from tests.integration.container.utils.test_instance_info import TestInstanceInfo

from socket import gethostbyname
from typing import Callable

import pytest

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverSuccessError)
from tests.integration.container.utils.conditions import (
    disable_on_features, enable_on_deployments, enable_on_features,
    enable_on_num_instances)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment


@enable_on_features([TestEnvironmentFeatures.IAM])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestAwsIamAuthentication:

    @pytest.fixture(scope='class')
    def props(self):
        p: Properties = Properties()

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    def test_iam_wrong_database_username(self, test_environment: TestEnvironment,
                                         test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        user = f"WRONG_{conn_utils.iam_user}_USER"
        params = conn_utils.get_connect_params(user=user)
        params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(
                target_driver_connect,
                **params,
                plugins="iam",
                **props)

    def test_iam_no_database_username(self, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        params = conn_utils.get_connect_params()
        params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
        params.pop("user", None)

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(target_driver_connect, **params, plugins="iam", **props)

    def test_iam_invalid_host(self, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        params = conn_utils.get_connect_params()
        params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
        params.update({"iam_host": "<>", "plugins": "iam"})

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(target_driver_connect, **params, **props)

    def test_iam_using_ip_address(self, test_environment: TestEnvironment,
                                  test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_writer()
        ip_address = self.get_ip_address(instance.get_host())

        params = conn_utils.get_connect_params(host=ip_address, user=conn_utils.iam_user,
                                               password="<anything>")
        params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
        params.update({"iam_host": instance.get_host(), "plugins": "iam"})

        self.validate_connection(target_driver_connect, **params, **props)

    def test_iam_valid_connection_properties(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        params = conn_utils.get_connect_params(user=conn_utils.iam_user, password="<anything>")
        params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
        params["plugins"] = "iam"

        self.validate_connection(target_driver_connect, **params, **props)

    def test_iam_valid_connection_properties_no_password(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        params = conn_utils.get_connect_params(user=conn_utils.iam_user)
        params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
        params.pop("password", None)
        params["plugins"] = "iam"

        self.validate_connection(target_driver_connect, **params, **props)

    @pytest.mark.parametrize("plugins", ["failover,iam", "failover_v2,iam"])
    @enable_on_num_instances(min_instances=2)
    @enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
    @disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                          TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                          TestEnvironmentFeatures.PERFORMANCE])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED, TestEnvironmentFeatures.IAM])
    def test_failover_with_iam(
            self, test_driver: TestDriver, props, conn_utils, plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        region = TestEnvironment.get_current().get_info().get_region()
        aurora_utility = RdsTestUtility(region)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        props.update({
            "plugins": plugins,
            "socket_timeout": 10,
            "connect_timeout": 10,
            "monitoring-connect_timeout": 5,
            "monitoring-socket_timeout": 5,
            "topology_refresh_ms": 10,
            "autocommit": True
        })

        with AwsWrapperConnection.connect(
                target_driver_connect, **conn_utils.get_connect_params(user=conn_utils.iam_user), **props) as aws_conn:
            # crash instance1 and nominate a new writer
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # failure occurs on Cursor invocation
            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError)

            # assert that we are connected to the new writer after failover happens and we can reuse the cursor
            current_connection_id = aurora_utility.query_instance_id(aws_conn)
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            assert current_connection_id != initial_writer_id

    def get_ip_address(self, hostname: str):
        return gethostbyname(hostname)

    def validate_connection(self, target_driver_connect: Callable, **connect_params):
        with AwsWrapperConnection.connect(target_driver_connect, **connect_params) as conn, \
                conn.cursor() as cursor:
            cursor.execute("SELECT now()")
            records = cursor.fetchall()
            assert len(records) == 1
