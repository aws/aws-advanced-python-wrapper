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

import pytest

from aws_wrapper import AwsWrapperConnection
from tests.integration.container.utils.aurora_test_utility import \
    AuroraTestUtility

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver

from .utils.driver_helper import DriverHelper
from .utils.test_environment import TestEnvironment


class TestBasicFunctionality:

    @pytest.fixture(scope='class')
    def aurora_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_aurora_region()
        return AuroraTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        return {"plugins": "host_monitoring", "connect_timeout": 10}

    def test_execute__positional_and_keyword_args(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string())
        cursor = conn.cursor()

        some_number = 1
        cursor.execute("SELECT %s", (some_number,), binary=True)
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()