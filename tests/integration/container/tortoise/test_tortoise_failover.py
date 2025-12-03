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
from tests.integration.container.tortoise.test_tortoise_common import \
    setup_tortoise
from tests.integration.container.tortoise.test_tortoise_models import \
    TableWithSleepTrigger
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_utils import get_sleep_trigger_sql


class TestTortoiseFailover:
    """Test class for Tortoise ORM with AWS wrapper plugins."""
    @pytest.fixture(scope='class')
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)
    
    async def _create_sleep_trigger_record(self, name_prefix="Plugin Test", value="test_value", using_db=None):
        """Create 3 TableWithSleepTrigger records."""
        await TableWithSleepTrigger.create(name=f"{name_prefix}", value=value, using_db=using_db)
        # await TableWithSleepTrigger.create(name=f"{name_prefix}2", value=value, using_db=using_db)
        # await TableWithSleepTrigger.create(name=f"{name_prefix}3", value=value, using_db=using_db)
    
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
    async def setup_tortoise_with_failover(self, conn_utils):
        """Setup Tortoise with failover plugins."""
        kwargs = {
            "topology_refresh_ms": 10000,
        }
        async for result in setup_tortoise(conn_utils, plugins="failover", **kwargs):
            yield result


    @pytest.mark.asyncio
    async def test_basic_operations_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test basic operations work with AWS wrapper plugins enabled."""
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
    async def test_transaction_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test transactions with failover during long-running operations."""
        transaction_exception = None
        
        def transaction_thread():
            nonlocal transaction_exception
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def run_transaction():
                    async with in_transaction() as conn:
                        await self._create_sleep_trigger_record("TX Plugin Test", "tx_test_value", conn)
                
                loop.run_until_complete(run_transaction())
            except Exception as e:
                transaction_exception = e
        
        def failover_thread():
            time.sleep(5)  # Wait for transaction to start
            aurora_utility.failover_cluster_and_wait_until_writer_changed()
        
        # Start both threads
        tx_t = threading.Thread(target=transaction_thread)
        failover_t = threading.Thread(target=failover_thread)
            
        tx_t.start()
        failover_t.start()
        
        # Wait for both threads to complete
        tx_t.join()
        failover_t.join()
        
        # Assert that transaction thread got FailoverSuccessError
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

    @pytest.mark.asyncio
    async def test_concurrent_queries_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test concurrent queries with failover during long-running operation."""
        connection = connections.get("default")
        
        # Step 1: Run 15 concurrent select queries
        async def run_select_query(query_id):
            return await connection.execute_query(f"SELECT {query_id} as query_id")
        
        initial_tasks = [run_select_query(i) for i in range(15)]
        initial_results = await asyncio.gather(*initial_tasks)
        assert len(initial_results) == 15
        
        # Step 2: Run sleep query with failover
        sleep_exception = None
        
        def sleep_query_thread():
            nonlocal sleep_exception
            for attempt in range(3):
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(
                        TableWithSleepTrigger.create(name=f"Concurrent Test {attempt}", value="sleep_value")
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
        assert isinstance(sleep_exception, (FailoverSuccessError, TransactionResolutionUnknownError))
        
        # Step 3: Run another 15 concurrent select queries after failover
        post_failover_tasks = [run_select_query(i + 100) for i in range(15)]
        post_failover_results = await asyncio.gather(*post_failover_tasks)
        assert len(post_failover_results) == 15
    
    @pytest.mark.asyncio
    async def test_multiple_concurrent_inserts_with_failover(self, setup_tortoise_with_failover, sleep_trigger_setup, aurora_utility):
        """Test multiple concurrent insert operations with failover during long-running operations."""
        # Track exceptions from all insert threads
        insert_exceptions = []
        
        def insert_thread(thread_id):
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(
                    TableWithSleepTrigger.create(name=f"Concurrent Insert {thread_id}", value="insert_value")
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
        failover_errors = [e for e in insert_exceptions if isinstance(e, (FailoverSuccessError, TransactionResolutionUnknownError))]
        assert len(failover_errors) == 15, f"Expected all 15 threads to get failover errors, got {len(failover_errors)}"
        print(f"Successfully verified all 15 threads got failover exceptions")