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

from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver
    from tests.integration.container.utils.test_environment import TestEnvironment
    from tests.integration.container.utils.test_instance_info import TestInstanceInfo

from socket import gethostbyname
from typing import Callable

import pytest

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.errors import AwsWrapperError
from tests.integration.container.utils.conditions import (
    disable_on_mariadb_driver, enable_on_features)
from tests.integration.container.utils.driver_helper import DriverHelper


@enable_on_features([TestEnvironmentFeatures.IAM])
@disable_on_mariadb_driver
class TestAwsIamAuthentication:
    def test_iam_wrong_database_username(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        user: str = f"WRONG_{conn_utils.iam_user}_USER"

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(
                target_driver_connect,
                **conn_utils.get_connect_params(user=user),
                plugins="iam")

    def test_iam_no_database_username(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params = conn_utils.get_connect_params()
        connect_params.pop("user", None)

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(target_driver_connect, **connect_params, plugins="iam")

    def test_iam_using_ip_address(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_writer()
        ip_address = self.get_ip_address(instance.get_host())

        connect_params = conn_utils.get_connect_params(host=ip_address, user=conn_utils.iam_user, password="<anything>")
        connect_params.update({"iam_host": instance.get_host(), "plugins": "iam"})

        self.validate_connection(target_driver_connect, **connect_params)

    def test_iam_valid_connection_properties(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params = conn_utils.get_connect_params(user=conn_utils.iam_user, password="<anything>")
        connect_params["plugins"] = "iam"

        self.validate_connection(target_driver_connect, **connect_params)

    def test_iam_valid_connection_properties_no_password(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params = conn_utils.get_connect_params(user=conn_utils.iam_user)
        connect_params.pop("password", None)
        connect_params["plugins"] = "iam"

        self.validate_connection(target_driver_connect, **connect_params)

    def get_ip_address(self, hostname: str):
        return gethostbyname(hostname)

    def validate_connection(self, target_driver_connect: Callable, **connect_params):
        with AwsWrapperConnection.connect(target_driver_connect, **connect_params) as conn, \
                conn.cursor() as cursor:
            cursor.execute("SELECT now()")
            records = cursor.fetchall()
            assert len(records) == 1
