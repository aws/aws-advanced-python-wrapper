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

import pytest
import pytest_asyncio

from tests.integration.container.tortoise.test_tortoise_common import (
    run_basic_read_operations, run_basic_write_operations, setup_tortoise)
from tests.integration.container.utils.conditions import (disable_on_engines,
                                                          disable_on_features,
                                                          enable_on_features)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@disable_on_engines([DatabaseEngine.PG])
@enable_on_features([TestEnvironmentFeatures.IAM])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseIamAuthentication:
    """Test class for Tortoise ORM with IAM authentication."""
    
    @pytest_asyncio.fixture
    async def setup_tortoise_iam(self, conn_utils):
        """Setup Tortoise with IAM authentication."""
        async for result in setup_tortoise(conn_utils, plugins="iam", user=conn_utils.iam_user):
            yield result

    @pytest.mark.asyncio
    async def test_basic_read_operations(self, setup_tortoise_iam):
        """Test basic read operations with IAM authentication."""
        await run_basic_read_operations()

    @pytest.mark.asyncio
    async def test_basic_write_operations(self, setup_tortoise_iam):
        """Test basic write operations with IAM authentication."""
        await run_basic_write_operations()
