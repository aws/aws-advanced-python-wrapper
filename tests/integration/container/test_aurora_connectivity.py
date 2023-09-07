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

import pytest

from aws_wrapper import AwsWrapperConnection
from tests.integration.container.utils.aurora_test_utility import \
    AuroraTestUtility
from tests.integration.container.utils.conditions import (
    disable_on_features, enable_on_deployment, enable_on_num_instances)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@enable_on_num_instances(min_instances=2)
@enable_on_deployment(DatabaseEngineDeployment.AURORA)
@disable_on_features([TestEnvironmentFeatures.PERFORMANCE])
class TestAuroraConnectivity:
    @pytest.fixture(scope='class')
    def aurora_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_aurora_region()
        return AuroraTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        user = TestEnvironment.get_current().get_database_info().get_username()
        password = TestEnvironment.get_current().get_database_info().get_password()
        return {"plugins": "efm",  "user": user, "password": password, "connect_timeout": 10}

    def test_wrapper_connection_reader_cluster_with_efm_enabled(
            self, test_driver: TestDriver, props, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        awsconn = AwsWrapperConnection.connect(
            conn_utils.get_conn_string(conn_utils.reader_cluster_host), target_driver_connect, **props)
        awscursor = awsconn.cursor()
        awscursor.execute("SELECT 1")
        records = awscursor.fetchall()
        assert len(records) == 1

        awsconn.close()
