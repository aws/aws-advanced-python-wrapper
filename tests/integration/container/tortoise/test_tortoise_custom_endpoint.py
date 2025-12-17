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

from time import perf_counter_ns, sleep
from uuid import uuid4

import pytest
import pytest_asyncio
from boto3 import client
from botocore.exceptions import ClientError

from tests.integration.container.tortoise.test_tortoise_common import (
    run_basic_read_operations, run_basic_write_operations, setup_tortoise)
from tests.integration.container.utils.conditions import (
    disable_on_engines, disable_on_features, enable_on_deployments,
    enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures
from tests.integration.container.utils.rds_test_utility import RdsTestUtility


@disable_on_engines([DatabaseEngine.PG])
@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseCustomEndpoint:
    """Test class for Tortoise ORM with custom endpoint plugin."""
    endpoint_id = f"test-tortoise-endpoint-{uuid4()}"
    endpoint_info: dict[str, str] = {}

    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def create_custom_endpoint(self, rds_utils):
        """Create a custom endpoint for testing."""
        env_info = TestEnvironment.get_current().get_info()
        region = env_info.get_region()
        rds_client = client('rds', region_name=region)

        instance_ids = [rds_utils.get_cluster_writer_instance_id()]

        try:
            rds_client.create_db_cluster_endpoint(
                DBClusterEndpointIdentifier=self.endpoint_id,
                DBClusterIdentifier=TestEnvironment.get_current().get_cluster_name(),
                EndpointType="ANY",
                StaticMembers=instance_ids
            )

            self._wait_until_endpoint_available(rds_client)
            yield self.endpoint_info["Endpoint"]
        finally:
            try:
                rds_client.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=self.endpoint_id)
                self._wait_until_endpoint_deleted(rds_client)
            except ClientError as e:
                if e.response['Error']['Code'] != 'DBClusterEndpointNotFoundFault':
                    pass  # Ignore if endpoint doesn't exist
            rds_client.close()

    def _wait_until_endpoint_available(self, rds_client):
        """Wait for the custom endpoint to become available."""
        end_ns = perf_counter_ns() + 5 * 60 * 1_000_000_000  # 5 minutes
        available = False

        while perf_counter_ns() < end_ns:
            response = rds_client.describe_db_cluster_endpoints(
                DBClusterEndpointIdentifier=self.endpoint_id,
                Filters=[
                    {
                        "Name": "db-cluster-endpoint-type",
                        "Values": ["custom"]
                    }
                ]
            )

            response_endpoints = response["DBClusterEndpoints"]
            if len(response_endpoints) != 1:
                sleep(3)
                continue

            response_endpoint = response_endpoints[0]
            TestTortoiseCustomEndpoint.endpoint_info = response_endpoint
            available = "available" == response_endpoint["Status"]
            if available:
                break

            sleep(3)

        if not available:
            pytest.fail(f"Timed out waiting for custom endpoint to become available: {self.endpoint_id}")
    
    def _wait_until_endpoint_deleted(self, rds_client):
        """Wait for the custom endpoint to be deleted."""
        end_ns = perf_counter_ns() + 5 * 60 * 1_000_000_000  # 5 minutes
        
        while perf_counter_ns() < end_ns:
            try:
                rds_client.describe_db_cluster_endpoints(DBClusterEndpointIdentifier=self.endpoint_id)
                sleep(5)  # Still exists, keep waiting
            except ClientError as e:
                if e.response['Error']['Code'] == 'DBClusterEndpointNotFoundFault':
                    return  # Successfully deleted
                raise  # Other error, re-raise

    @pytest_asyncio.fixture
    async def setup_tortoise_custom_endpoint(self, conn_utils, create_custom_endpoint, request):
        """Setup Tortoise with custom endpoint plugin."""
        plugins, user = request.param
        user_value = getattr(conn_utils, user) if user != "default" else None
        
        kwargs = {}
        if "fastest_response_strategy" in plugins:
            kwargs["reader_host_selector_strategy"] = "fastest_response"
            
        async for result in setup_tortoise(conn_utils, plugins=plugins, host=create_custom_endpoint, user=user_value, **kwargs):
            yield result

    @pytest.mark.parametrize("setup_tortoise_custom_endpoint", [
        ("custom_endpoint,aurora_connection_tracker", "default"),
        ("failover,iam,aurora_connection_tracker,custom_endpoint,fastest_response_strategy", "iam_user")
    ], indirect=True)
    @pytest.mark.asyncio
    async def test_basic_read_operations(self, setup_tortoise_custom_endpoint):
        """Test basic read operations with custom endpoint plugin."""
        await run_basic_read_operations("Custom Test", "custom")

    @pytest.mark.parametrize("setup_tortoise_custom_endpoint", [
        ("custom_endpoint,aurora_connection_tracker", "default"),
        ("failover,iam,aurora_connection_tracker,custom_endpoint,fastest_response_strategy", "iam_user")
    ], indirect=True)
    @pytest.mark.asyncio
    async def test_basic_write_operations(self, setup_tortoise_custom_endpoint):
        """Test basic write operations with custom endpoint plugin."""
        await run_basic_write_operations("Custom", "customwrite")
