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

import asyncio
from typing import TYPE_CHECKING

import pytest

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, connect_async)
from .utils.conditions import disable_on_features
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.connection_utils import ConnectionUtils
    from .utils.test_driver import TestDriver


@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE,
                      TestEnvironmentFeatures.SKIP_ASYNC_DRIVER_TESTS])
class TestBasicFunctionalityAsync:

    @pytest.fixture(scope='class')
    def props(self):
        p: Properties = Properties({
            "plugins": "aurora_connection_tracker,failover_v2",
            "connect_timeout": 10,
            "autocommit": True,
            "cluster_id": "cluster1"})

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    def test_execute__positional_and_keyword_args_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils: ConnectionUtils, props):
        async def inner() -> None:
            conn = await connect_async(test_driver=test_driver, connect_params=conn_utils.get_connect_params(), **dict(props))
            try:
                async with conn.cursor() as cur:
                    some_number = 1
                    await cur.execute("SELECT %s", params=(some_number,))
                    result = await cur.fetchone()
                    assert 1 == result[0]
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())
