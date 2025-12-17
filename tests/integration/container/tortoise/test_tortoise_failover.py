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

import pytest
import pytest_asyncio
from tortoise import connections
from tortoise.transactions import in_transaction

from aws_advanced_python_wrapper.errors import (
    FailoverSuccessError, TransactionResolutionUnknownError)
from tests.integration.container.tortoise.models.test_models import \
    TableWithSleepTrigger
from tests.integration.container.tortoise.test_tortoise_common import \
    setup_tortoise
from tests.integration.container.utils.conditions import (
    disable_on_engines, disable_on_features, enable_on_deployments,
    enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures
from tests.integration.container.utils.test_utils import get_sleep_trigger_sql


# Shared helper functions for failover tests
async def run_single_insert_with_failover(create_record_func, aurora_utility, name_prefix="Test", value="test_value"):
    """Helper to test single insert with failover."""
    insert_exception = None

    async def insert_task():
        nonlocal insert_exception
        try:
            await create_record_func(name_prefix, value)
        except Exception as e:
            insert_exception = e

    async def failover_task():
        await asyncio.sleep(6)
        await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

    await asyncio.gather(insert_task(), failover_task(), return_exceptions=True)

    assert insert_exception is not None
    assert isinstance(insert_exception, FailoverSuccessError)


async def run_concurrent_queries_with_failover(aurora_utility, record_name="Concurrent Test", record_value="sleep_value"):
    """Helper to test concurrent queries with failover."""
    connection = connections.get("default")

    async def run_select_query(query_id):
        return await connection.execute_query(f"SELECT {query_id} as query_id")

    # Run 15 concurrent select queries
    initial_tasks = [run_select_query(i) for i in range(15)]
    initial_results = await asyncio.gather(*initial_tasks)
    assert len(initial_results) == 15

    # Run insert query with failover
    sleep_exception = None

    async def insert_query_task():
        nonlocal sleep_exception
        try:
            await TableWithSleepTrigger.create(name=record_name, value=record_value)
        except Exception as e:
            sleep_exception = e

    async def failover_task():
        await asyncio.sleep(6)

        await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

    await asyncio.gather(insert_query_task(), failover_task(), return_exceptions=True)


    assert sleep_exception is not None
    assert isinstance(sleep_exception, (FailoverSuccessError, TransactionResolutionUnknownError))

    # Run another 15 concurrent select queries after failover
    post_failover_tasks = [run_select_query(i + 100) for i in range(15)]
    post_failover_results = await asyncio.gather(*post_failover_tasks)
    assert len(post_failover_results) == 15


async def run_multiple_concurrent_inserts_with_failover(aurora_utility, name_prefix="Concurrent Insert", value="insert_value"):
    """Helper to test multiple concurrent inserts with failover."""
    insert_exceptions = []

    async def insert_task(task_id):
        try:
            await TableWithSleepTrigger.create(name=f"{name_prefix} {task_id}", value=value)
        except Exception as e:
            insert_exceptions.append(e)

    async def failover_task():
        await asyncio.sleep(6)
        await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

    # Create 15 insert tasks and 1 failover task
    tasks = [insert_task(i) for i in range(15)]
    tasks.append(failover_task())

    await asyncio.gather(*tasks, return_exceptions=True)

    # Verify ALL tasks got FailoverSuccessError or TransactionResolutionUnknownError
    assert len(insert_exceptions) == 15, f"Expected 15 exceptions, got {len(insert_exceptions)}"
    failover_errors = [e for e in insert_exceptions if isinstance(e, (FailoverSuccessError, TransactionResolutionUnknownError))]
    assert len(failover_errors) == 15, f"Expected all 15 tasks to get failover errors, got {len(failover_errors)}"


@disable_on_engines([DatabaseEngine.PG])
@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseFailover:
    """Test class for Tortoise ORM with Failover."""
    @pytest.fixture(scope='class')
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    async def _create_sleep_trigger_record(self, name_prefix="Plugin Test", value="test_value", using_db=None):
        await TableWithSleepTrigger.create(name=f"{name_prefix}", value=value, using_db=using_db)

    @pytest_asyncio.fixture
    async def sleep_trigger_setup(self):
        """Setup and cleanup sleep trigger for testing."""
        connection = connections.get("default")
        db_engine = TestEnvironment.get_current().get_engine()
        trigger_sql = get_sleep_trigger_sql(db_engine, 110, "table_with_sleep_trigger")
        await connection.execute_query("DROP TRIGGER IF EXISTS table_with_sleep_trigger_sleep_trigger")
        await connection.execute_query(trigger_sql)
        yield
        await connection.execute_query("DROP TRIGGER IF EXISTS table_with_sleep_trigger_sleep_trigger")

    @pytest_asyncio.fixture
    async def setup_tortoise_with_failover(self, conn_utils, request):
        """Setup Tortoise with failover plugins."""
        plugins = request.param
        kwargs = {
            "topology_refresh_ms": 1000,
            "connect_timeout": 15,
            "monitoring-connect_timeout": 10,
            "use_pure": True,
        }
        
        # Add reader strategy if multiple plugins
        if "fastest_response_strategy" in plugins:
            kwargs["reader_host_selector_strategy"] = "fastest_response"
            user = conn_utils.iam_user
            kwargs.pop("use_pure")
        else:
            user = None
            
        async for result in setup_tortoise(conn_utils, plugins=plugins, user=user, **kwargs):
            yield result

    @pytest.mark.parametrize("setup_tortoise_with_failover", [
        "failover",
        "failover,aurora_connection_tracker,fastest_response_strategy,iam"
    ], indirect=True)
    @pytest.mark.asyncio
    async def test_basic_operations_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test failover when inserting to a single table"""
        await run_single_insert_with_failover(self._create_sleep_trigger_record, aurora_utility)

    @pytest.mark.parametrize("setup_tortoise_with_failover", [
        "failover",
        "failover,aurora_connection_tracker,fastest_response_strategy,iam"
    ], indirect=True)
    @pytest.mark.asyncio
    async def test_transaction_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test transactions with failover during long-running operations."""
        transaction_exception = None

        async def transaction_task():
            nonlocal transaction_exception
            try:
                async with in_transaction() as conn:
                    await self._create_sleep_trigger_record("TX Plugin Test", "tx_test_value", conn)
            except Exception as e:
                transaction_exception = e

        async def failover_task():
            await asyncio.sleep(6)
            await asyncio.to_thread(aurora_utility.failover_cluster_and_wait_until_writer_changed)

        await asyncio.gather(transaction_task(), failover_task(), return_exceptions=True)

        assert transaction_exception is not None
        assert isinstance(transaction_exception, (FailoverSuccessError, TransactionResolutionUnknownError))

        # Verify no records were created due to transaction rollback
        record_count = await TableWithSleepTrigger.all().count()
        assert record_count == 0

        # Verify autocommit is re-enabled after failover by inserting a record
        autocommit_record = await TableWithSleepTrigger.create(
            name="Autocommit Test",
            value="autocommit_value"
        )

        # Verify the record exists (should be auto-committed)
        found_record = await TableWithSleepTrigger.get(id=autocommit_record.id)
        assert found_record.name == "Autocommit Test"
        assert found_record.value == "autocommit_value"

        # Clean up the test record
        await found_record.delete()

    @pytest.mark.parametrize("setup_tortoise_with_failover", [
        "failover",
        "failover,aurora_connection_tracker,fastest_response_strategy,iam"
    ], indirect=True)
    @pytest.mark.asyncio
    async def test_concurrent_queries_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test concurrent queries with failover during long-running operation."""
        await run_concurrent_queries_with_failover(aurora_utility)

    @pytest.mark.parametrize("setup_tortoise_with_failover", [
        "failover",
        "failover,aurora_connection_tracker,fastest_response_strategy,iam"
    ], indirect=True)
    @pytest.mark.asyncio
    async def test_multiple_concurrent_inserts_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test multiple concurrent insert operations with failover during long-running operations."""
        await run_multiple_concurrent_inserts_with_failover(aurora_utility)
