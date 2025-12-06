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
import threading
import time
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
from tests.integration.container.utils.test_utils import get_sleep_trigger_sql


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
    def create_custom_endpoint(self):
        """Create a custom endpoint for testing."""
        env_info = TestEnvironment.get_current().get_info()
        region = env_info.get_region()
        rds_client = client('rds', region_name=region)

        instances = env_info.get_database_info().get_instances()
        instance_ids = [instances[0].get_instance_id()]

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
            "reader_host_selector_strategy": "fastest_response"
        }
        async for result in setup_tortoise(conn_utils,
                                           plugins="failover,iam,custom_endpoint,aurora_connection_tracker,fastest_response_strategy",
                                           user=conn_utils.iam_user, **kwargs
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
    async def test_multi_plugins_with_failover(self, setup_tortoise_multi_plugins, sleep_trigger_setup, aurora_utility):
        """Test multiple plugins work with failover during long-running operations."""
        insert_exception = None

        def insert_thread():
            nonlocal insert_exception
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._create_sleep_trigger_record())
            except Exception as e:
                insert_exception = e

        def failover_thread():
            time.sleep(5)  # Wait for insert to start
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

        # Start both threads
        insert_t = threading.Thread(target=insert_thread)
        failover_t = threading.Thread(target=failover_thread)

        insert_t.start()
        failover_t.start()

        # Wait for both threads to complete
        insert_t.join()
        failover_t.join()

        # Assert that insert thread got FailoverSuccessError
        assert insert_exception is not None
        assert isinstance(insert_exception, FailoverSuccessError)

    @pytest.mark.asyncio
    async def test_concurrent_queries_with_failover(self, setup_tortoise_multi_plugins, sleep_trigger_setup, aurora_utility):
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

        def sleep_query_thread():
            nonlocal sleep_exception
            for attempt in range(3):
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(
                        TableWithSleepTrigger.create(name=f"Multi Concurrent Test {attempt}", value="multi_sleep_value")
                    )
                except Exception as e:
                    sleep_exception = e
                    break  # Stop on first exception (likely the failover)

        def failover_thread():
            time.sleep(5)  # Wait for sleep query to start
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

        sleep_t = threading.Thread(target=sleep_query_thread)
        failover_t = threading.Thread(target=failover_thread)

        sleep_t.start()
        failover_t.start()

        sleep_t.join()
        failover_t.join()

        # Verify failover exception occurred
        assert sleep_exception is not None
        assert isinstance(sleep_exception, FailoverSuccessError)

        # Run another 15 concurrent select queries after failover
        post_failover_tasks = [run_select_query(i + 100) for i in range(15)]
        post_failover_results = await asyncio.gather(*post_failover_tasks)
        assert len(post_failover_results) == 15

    @pytest.mark.asyncio
    async def test_multiple_concurrent_inserts_with_failover(self, setup_tortoise_multi_plugins, sleep_trigger_setup, aurora_utility):
        """Test multiple concurrent insert operations with failover during long-running operations."""
        # Track exceptions from all insert threads
        insert_exceptions = []

        def insert_thread(thread_id):
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(
                    TableWithSleepTrigger.create(name=f"Multi Concurrent Insert {thread_id}", value="multi_insert_value")
                )
            except Exception as e:
                insert_exceptions.append(e)

        def failover_thread():
            time.sleep(5)  # Wait for inserts to start
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

        # Start 15 insert threads
        insert_threads = []
        for i in range(15):
            t = threading.Thread(target=insert_thread, args=(i,))
            insert_threads.append(t)
            t.start()

        # Start failover thread
        failover_t = threading.Thread(target=failover_thread)
        failover_t.start()

        # Wait for all threads to complete
        for t in insert_threads:
            t.join()
        failover_t.join()

        # Verify ALL threads got FailoverSuccessError or TransactionResolutionUnknownError
        assert len(insert_exceptions) == 15, f"Expected 15 exceptions, got {len(insert_exceptions)}"
        failover_errors = [e for e in insert_exceptions if isinstance(e, FailoverSuccessError)]
        assert len(failover_errors) == 15, f"Expected all 15 threads to get failover errors, got {len(failover_errors)}"
