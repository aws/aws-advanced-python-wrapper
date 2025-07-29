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
from aws_advanced_python_wrapper.errors import AwsWrapperError
from tests.integration.container.utils.conditions import (disable_on_features,
                                                          enable_on_features)
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.test_environment import TestEnvironment


@enable_on_features([TestEnvironmentFeatures.IAM])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE,
                      TestEnvironmentFeatures.RUN_DSQL_TESTS_ONLY])
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

    def get_ip_address(self, hostname: str):
        return gethostbyname(hostname)

    def validate_connection(self, target_driver_connect: Callable, **connect_params):
        with AwsWrapperConnection.connect(target_driver_connect, **connect_params) as conn, \
                conn.cursor() as cursor:
            cursor.execute("SELECT now()")
            records = cursor.fetchall()
            assert len(records) == 1
