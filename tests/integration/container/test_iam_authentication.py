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

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver
    from tests.integration.container.utils.test_environment import TestEnvironment
    from tests.integration.container.utils.test_instance_info import TestInstanceInfo

from socket import gethostbyname
from typing import Callable, Dict

import pytest

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.errors import AwsWrapperError
from tests.integration.container.utils.conditions import (
    disable_on_mariadb_driver, iam_required)
from tests.integration.container.utils.driver_helper import DriverHelper


class TestAwsIamAuthentication:

    @iam_required
    @disable_on_mariadb_driver
    def test_iam_wrong_database_username(self, test_environment: TestEnvironment,
                                         test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        user: str = f"WRONG_{test_environment.get_info().get_iam_user_name()}_USER"
        password: str = test_environment.get_info().get_database_info().get_password()
        connect_params: str = self.init_aws_iam_properties(test_environment, {
            "user": user,
            "password": password
        })

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(connect_params, target_driver_connect)

    @iam_required
    @disable_on_mariadb_driver
    def test_iam_no_database_username(self, test_environment: TestEnvironment,
                                      test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        password: str = test_environment.get_info().get_database_info().get_password()
        connect_params: str = self.init_aws_iam_properties(test_environment, {
            "password": password
        })

        with pytest.raises(AwsWrapperError):
            AwsWrapperConnection.connect(connect_params, target_driver_connect)

    @iam_required
    @disable_on_mariadb_driver
    def test_iam_using_ip_address(self, test_environment: TestEnvironment,
                                  test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_info().get_database_info().get_instances()[0]
        ip_address = self.get_ip_address(instance.get_host())
        user: str = test_environment.get_info().get_iam_user_name()
        connect_params: str = self.init_aws_iam_properties(test_environment, {
            "host": ip_address,
            "user": user,
            "password": "<anything>",
            "iam_host": instance.get_host()
        })

        self.validate_connection(target_driver_connect, connect_params)

    @iam_required
    @disable_on_mariadb_driver
    def test_iam_valid_connection_properties(self, test_environment: TestEnvironment,
                                             test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        user: str = test_environment.get_info().get_iam_user_name()
        connect_params: str = self.init_aws_iam_properties(test_environment, {
            "user": user,
            "password": "<anything>"
        })

        self.validate_connection(target_driver_connect, connect_params)

    @iam_required
    @disable_on_mariadb_driver
    def test_iam_valid_connection_properties_no_password(self, test_environment: TestEnvironment,
                                                         test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        db_name: str = test_environment.get_info().get_database_info().get_default_db_name()
        user: str = test_environment.get_info().get_iam_user_name()
        connect_params: str = self.init_aws_iam_properties(test_environment, {
            "dbname": db_name,
            "user": user,
        })

        self.validate_connection(target_driver_connect, connect_params)

    def init_aws_iam_properties(self, test_environment: TestEnvironment, props: Dict[str, str]) -> str:
        instance = test_environment.get_info().get_database_info().get_instances()[0]
        props["plugins"] = "iam"
        props["port"] = str(instance.get_port())

        if not props.get("host"):
            props["host"] = instance.get_host()
        if not props.get("dbname"):
            props["dbname"] = test_environment.get_info().get_database_info().get_default_db_name()

        return "plugins=iam " + " ".join([f"{key}={value}" for key, value in props.items()])

    def get_ip_address(self, hostname: str):
        return gethostbyname(hostname)

    def validate_connection(self, target_driver_connect: Callable, connect_params: str):
        with AwsWrapperConnection.connect(connect_params, target_driver_connect) as conn, \
                conn.cursor() as cursor:
            cursor.execute("SELECT now()")
            records = cursor.fetchall()
            assert len(records) == 1
