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

import boto3
import pytest
import pytest_asyncio

from tests.integration.container.tortoise.models.test_models import User
from tests.integration.container.tortoise.test_tortoise_common import (
    run_basic_read_operations, run_basic_write_operations, setup_tortoise)
from tests.integration.container.utils.conditions import (
    disable_on_engines, disable_on_features, enable_on_deployments)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@disable_on_engines([DatabaseEngine.PG])
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseSecretsManager:
    """Test class for Tortoise ORM with AWS Secrets Manager authentication."""
    
    @pytest.fixture(scope='class')
    def create_secret(self, conn_utils):
        """Create a secret in AWS Secrets Manager with database credentials."""
        region = TestEnvironment.get_current().get_info().get_region()
        client = boto3.client('secretsmanager', region_name=region)
        
        secret_name = f"test-tortoise-secret-{TestEnvironment.get_current().get_info().get_db_name()}"
        secret_value = {
            "username": conn_utils.user,
            "password": conn_utils.password
        }
        
        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(secret_value)
            )
            secret_id = response['ARN']
            yield secret_id
        finally:
            try:
                client.delete_secret(
                    SecretId=secret_name,
                    ForceDeleteWithoutRecovery=True
                )
            except Exception:
                pass
    
    @pytest_asyncio.fixture
    async def setup_tortoise_secrets_manager(self, conn_utils, create_secret):
        """Setup Tortoise with Secrets Manager authentication."""
        async for result in setup_tortoise(conn_utils, 
                                            plugins="aws_secrets_manager", 
                                            secrets_manager_secret_id=create_secret):
            yield result

    @pytest.mark.asyncio
    async def test_basic_read_operations(self, setup_tortoise_secrets_manager):
        """Test basic read operations with Secrets Manager authentication."""
        await run_basic_read_operations("Secrets", "secrets")

    @pytest.mark.asyncio
    async def test_basic_write_operations(self, setup_tortoise_secrets_manager):
        """Test basic write operations with Secrets Manager authentication."""
        await run_basic_write_operations("Secrets", "secretswrite")