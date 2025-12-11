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

import asyncio
from time import perf_counter_ns, sleep
from uuid import uuid4

import pytest
import pytest_asyncio
from boto3 import client
from botocore.exceptions import ClientError
from tortoise import connections

from aws_advanced_python_wrapper.errors import FailoverSuccessError
from tests.integration.container.tortoise.models.test_models import \
    TableWithSleepTrigger
from tests.integration.container.tortoise.test_tortoise_common import (
    run_basic_read_operations, run_basic_write_operations, setup_tortoise)
from tests.integration.container.utils.conditions import (
    disable_on_engines, disable_on_features, enable_on_deployments,
    enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures
from tests.integration.container.utils.test_utils import get_sleep_sql, get_sleep_trigger_sql
from tests.integration.container.utils.proxy_helper import ProxyHelper


@disable_on_engines([DatabaseEngine.PG])
@enable_on_num_instances(min_instances=2)
@enable_on_features([TestEnvironmentFeatures.IAM])
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseMultiPlugins:
    """Test class for Tortoise ORM with multiple AWS wrapper plugins."""
    endpoint_id = f"test-multi-endpoint-{uuid4()}"

    endpoint_info: dict[str, str] = {}

    @pytest.fixture(scope='class')
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def create_custom_endpoint(self, aurora_utility):
        """Create a custom endpoint for testing."""
        env_info = TestEnvironment.get_current().get_info()
        region = env_info.get_region()
        rds_client = client('rds', region_name=region)

        instance_ids = [aurora_utility.get_cluster_writer_instance_id()]

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
            except ClientError as e:
                if e.response['Error']['Code'] != 'DBClusterEndpointNotFoundFault':
                    pass
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
            TestTortoiseMultiPlugins.endpoint_info = response_endpoint
            available = "available" == response_endpoint["Status"]
            if available:
                break

            sleep(3)

        if not available:
            pytest.fail(f"Timed out waiting for custom endpoint to become available: {self.endpoint_id}")

    async def _create_sleep_trigger_record(self, name_prefix="Multi Test", value="multi_value", using_db=None):
        """Create TableWithSleepTrigger record."""
        await TableWithSleepTrigger.create(name=f"{name_prefix}", value=value, using_db=using_db)

    @pytest_asyncio.fixture
    async def sleep_trigger_setup(self):
        """Setup and cleanup sleep trigger for testing."""
        connection = connections.get("default")
        db_engine = TestEnvironment.get_current().get_engine()
        trigger_sql = get_sleep_trigger_sql(db_engine, 60, "table_with_sleep_trigger")
        await connection.execute_query("DROP TRIGGER IF EXISTS table_with_sleep_trigger_sleep_trigger")
        await connection.execute_query(trigger_sql)
        yield
        await connection.execute_query("DROP TRIGGER IF EXISTS table_with_sleep_trigger_sleep_trigger")

    @pytest_asyncio.fixture
    async def setup_tortoise_multi_plugins(self, conn_utils, create_custom_endpoint):
        """Setup Tortoise with multiple plugins."""
        kwargs = {
            "topology_refresh_ms": 10000,
            "reader_host_selector_strategy": "fastest_response",
            "connect_timeout": 10,
            "monitoring-connect_timeout": 5,
            "host": create_custom_endpoint,
        }

        async for result in setup_tortoise(conn_utils,
                                           plugins="failover,iam,aurora_connection_tracker,custom_endpoint,fastest_response_strategy",
                                           user="jane_doe",
                                            **kwargs,
                                           ):
            yield result
    
    @pytest_asyncio.fixture
    async def setup_tortoise_multi_plugins_no_custom_endpoint(self, conn_utils):
        """Setup Tortoise with multiple plugins."""
        kwargs = {
            "topology_refresh_ms": 10000,
            "reader_host_selector_strategy": "fastest_response",
            "connect_timeout": 10,
            "monitoring-connect_timeout": 5,
        }

        async for result in setup_tortoise(conn_utils,
                                           plugins="failover,iam,aurora_connection_tracker,fastest_response_strategy",
                                           user="jane_doe",
                                            **kwargs,
                                           ):
            yield result

    @pytest.mark.asyncio
    async def test_basic_read_operations(self, setup_tortoise_multi_plugins):
        """Test basic read operations with multiple plugins."""
        await run_basic_read_operations("Multi", "multi")

    @pytest.mark.asyncio
    async def test_basic_write_operations(self, setup_tortoise_multi_plugins):
        """Test basic write operations with multiple plugins."""
        await run_basic_write_operations("Multi", "multi")

    @pytest.mark.asyncio
    async def test_multi_plugins_with_failover(self, setup_tortoise_multi_plugins_no_custom_endpoint, sleep_trigger_setup, aurora_utility):
        """Test multiple plugins work with failover during long-running operations."""
        insert_exception = None

        async def insert_task():
            nonlocal insert_exception
            try:
                await TableWithSleepTrigger.create(name="Multi Plugin Test", value="multi_test_value")
            except Exception as e:
                insert_exception = e

        async def failover_task():
            await asyncio.sleep(10)
            await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

        await asyncio.gather(insert_task(), failover_task(), return_exceptions=True)

        assert insert_exception is not None
        assert isinstance(insert_exception, FailoverSuccessError)

    @pytest.mark.asyncio
    async def test_concurrent_queries_with_failover(self, setup_tortoise_multi_plugins_no_custom_endpoint, sleep_trigger_setup, aurora_utility):
        """Test concurrent queries with failover during long-running operation."""
        connection = connections.get("default")

        # Run 15 concurrent select queries
        async def run_select_query(query_id):
            return await connection.execute_query(f"SELECT {query_id} as query_id")

        initial_tasks = [run_select_query(i) for i in range(15)]
        initial_results = await asyncio.gather(*initial_tasks)
        assert len(initial_results) == 15

        # Run sleep query with failover
        sleep_exception = None

        async def insert_query_task():
            nonlocal sleep_exception
            try:
                await TableWithSleepTrigger.create(name="Multi Concurrent Test", value="multi_sleep_value")
            except Exception as e:
                sleep_exception = e

        async def failover_task():
            await asyncio.sleep(5)
            await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

        await asyncio.gather(insert_query_task(), failover_task(), return_exceptions=True)

        assert sleep_exception is not None
        assert isinstance(sleep_exception, FailoverSuccessError)

        # Run another 15 concurrent select queries after failover
        post_failover_tasks = [run_select_query(i + 100) for i in range(15)]
        post_failover_results = await asyncio.gather(*post_failover_tasks)
        assert len(post_failover_results) == 15

    @pytest.mark.asyncio
    async def test_multiple_concurrent_inserts_with_failover(self, setup_tortoise_multi_plugins_no_custom_endpoint, sleep_trigger_setup, aurora_utility):
        """Test multiple concurrent insert operations with failover during long-running operations."""
        insert_exceptions = []

        async def insert_task(task_id):
            try:
                await TableWithSleepTrigger.create(name=f"Multi Concurrent Insert {task_id}", value="multi_insert_value")
            except Exception as e:
                insert_exceptions.append(e)

        async def failover_task():
            await asyncio.sleep(5)
            await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

        # Create 15 insert tasks and 1 failover task
        tasks = [insert_task(i) for i in range(15)]
        tasks.append(failover_task())

        await asyncio.gather(*tasks, return_exceptions=True)

        # Verify ALL tasks got FailoverSuccessError
        assert len(insert_exceptions) == 15, f"Expected 15 exceptions, got {len(insert_exceptions)}"
        failover_errors = [e for e in insert_exceptions if isinstance(e, FailoverSuccessError)]
        assert len(failover_errors) == 15, f"Expected all 15 tasks to get failover errors, got {len(failover_errors)}"
