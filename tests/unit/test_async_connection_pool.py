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
import time

import pytest

from aws_advanced_python_wrapper.errors import (ConnectionReleasedError,
                                                PoolClosingError,
                                                PoolExhaustedError,
                                                PoolNotInitializedError)
from aws_advanced_python_wrapper.tortoise_orm.async_support.async_connection_pool import (
    AsyncConnectionPool, ConnectionState, PoolConfig)


class MockConnection:
    """Mock connection for testing"""

    def __init__(self, connection_id: int = 1):
        self.connection_id = connection_id
        self.closed = False
        self.is_closed = False

    async def close(self):
        self.closed = True

    def some_method(self):
        return "mock_result"


@pytest.fixture
def mock_creator():
    """Mock connection creator"""
    counter = 0

    async def creator():
        nonlocal counter
        counter += 1
        return MockConnection(counter)
    return creator


@pytest.fixture
def mock_health_check():
    """Mock health check function"""
    async def health_check(conn):
        if hasattr(conn, 'healthy') and not conn.healthy:
            raise Exception("Connection unhealthy")
    return health_check


@pytest.fixture
def pool_config():
    """Basic pool configuration"""
    return PoolConfig(
        min_size=1,
        max_size=3,
        acquire_conn_timeout=1.0,
        max_conn_lifetime=10.0,
        max_conn_idle_time=5.0,
        health_check_interval=0.1
    )


class TestAsyncConnectionPool:
    """Test cases for AsyncConnectionPool"""

    @pytest.mark.asyncio
    async def test_pool_initialization(self, mock_creator, pool_config):
        """Test pool initialization creates minimum connections"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)

        await pool.initialize()

        stats = pool.get_stats()
        assert stats["initialized"] is True
        assert stats["total_size"] >= 1
        assert stats["available_in_queue"] >= 0

        await pool.close()

    @pytest.mark.asyncio
    async def test_acquire_and_release(self, mock_creator, pool_config):
        """Test basic acquire and release operations"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        # Acquire connection
        conn = await pool.acquire()
        assert conn.connection_id == 1
        assert conn.state == ConnectionState.IN_USE

        stats = pool.get_stats()
        assert stats["in_use"] >= 1
        assert stats["available_in_queue"] == 0

        # Release connection
        await conn.release()

        stats = pool.get_stats()
        assert stats["in_use"] == 0
        assert stats["available_in_queue"] >= 1

        await pool.close()

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_creator, pool_config):
        """Test context manager automatically releases connections"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        async with pool.connection() as conn:
            assert conn.state == ConnectionState.IN_USE

        stats = pool.get_stats()
        assert stats["in_use"] == 0
        assert stats["available_in_queue"] >= 1

        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_expansion(self, mock_creator, pool_config):
        """Test pool creates new connections when needed"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        # Acquire all connections
        conn1 = await pool.acquire()
        conn2 = await pool.acquire()
        conn3 = await pool.acquire()

        stats = pool.get_stats()
        assert stats["total_size"] == 3
        assert stats["in_use"] == 3

        await conn1.release()
        await conn2.release()
        await conn3.release()
        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_exhaustion(self, mock_creator):
        """Test pool exhaustion raises PoolExhaustedError"""
        config = PoolConfig(min_size=1, max_size=1, overflow=0, acquire_conn_timeout=0.1)
        pool = AsyncConnectionPool(mock_creator, config=config)
        await pool.initialize()

        # Acquire the only connection
        conn1 = await pool.acquire()

        # Try to acquire another - should timeout
        with pytest.raises(PoolExhaustedError):
            await pool.acquire()

        await conn1.release()
        await pool.close()

    @pytest.mark.asyncio
    async def test_connection_validation_failure(self, mock_creator, mock_health_check, pool_config):
        """Test connection validation and recreation"""
        pool = AsyncConnectionPool(mock_creator, health_check=mock_health_check, config=pool_config)
        await pool.initialize()

        # Get connection and mark it unhealthy
        conn = await pool.acquire()
        conn.connection.healthy = False
        await conn.release()

        # Next acquire should recreate connection due to failed health check
        new_conn = await pool.acquire()
        assert new_conn.connection_id == 2  # New connection created

        await new_conn.release()
        await pool.close()

    @pytest.mark.asyncio
    async def test_stale_connection_detection(self, mock_creator, pool_config):
        """Test stale connection detection"""
        config = PoolConfig(min_size=1, max_size=3, max_conn_lifetime=0.1, max_conn_idle_time=0.1)
        pool = AsyncConnectionPool(mock_creator, config=config)
        await pool.initialize()

        conn = await pool.acquire()

        # Make connection stale
        conn.created_at = time.monotonic() - 1.0

        assert conn.is_stale(0.1, 0.1) is True

        await conn.release()
        await pool.close()

    @pytest.mark.asyncio
    async def test_connection_proxy_methods(self, mock_creator, pool_config):
        """Test connection proxies methods to underlying connection"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        conn = await pool.acquire()

        # Test method proxying
        result = conn.some_method()
        assert result == "mock_result"

        await conn.release()
        await pool.close()

    @pytest.mark.asyncio
    async def test_released_connection_access(self, mock_creator, pool_config):
        """Test accessing released connection raises error"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        conn = await pool.acquire()
        await conn.release()

        # Accessing released connection should raise error
        with pytest.raises(ConnectionReleasedError):
            _ = conn.some_method()

        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_not_initialized_error(self, mock_creator, pool_config):
        """Test acquiring from uninitialized pool raises error"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)

        with pytest.raises(PoolNotInitializedError):
            await pool.acquire()

    @pytest.mark.asyncio
    async def test_pool_closing_error(self, mock_creator, pool_config):
        """Test acquiring from closing pool raises error"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        # Start closing
        pool._closing = True

        with pytest.raises(PoolClosingError):
            await pool.acquire()

        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_close_cleanup(self, mock_creator, pool_config):
        """Test pool close cleans up all connections"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        # Acquire some connections
        conn1 = await pool.acquire()
        conn2 = await pool.acquire()

        await pool.close()

        # Connections should be closed
        assert conn1.connection.closed is True
        assert conn2.connection.closed is True

        stats = pool.get_stats()
        assert stats["closing"] is True

    @pytest.mark.asyncio
    async def test_maintenance_loop_creates_connections(self, mock_creator):
        """Test maintenance loop creates connections when below minimum"""
        config = PoolConfig(min_size=2, max_size=5, health_check_interval=0.05)
        pool = AsyncConnectionPool(mock_creator, config=config)
        await pool.initialize()

        # Remove a connection to go below minimum
        conn = await pool.acquire()
        await pool._close_connection(conn)

        # Wait for maintenance loop
        await asyncio.sleep(0.1)

        stats = pool.get_stats()
        assert stats["total_size"] >= 2  # Should restore minimum

        await pool.close()

    @pytest.mark.asyncio
    async def test_overflow_connections(self, mock_creator):
        """Test overflow connections beyond max_size"""
        config = PoolConfig(min_size=1, max_size=2, overflow=1, acquire_conn_timeout=0.1)
        pool = AsyncConnectionPool(mock_creator, config=config)
        await pool.initialize()

        # Acquire max + overflow connections
        conn1 = await pool.acquire()
        conn2 = await pool.acquire()
        conn3 = await pool.acquire()  # Overflow connection

        stats = pool.get_stats()
        assert stats["total_size"] == 3

        await conn1.release()
        await conn2.release()
        await conn3.release()

        # After releasing all connections, should only keep max_size (2) connections
        stats = pool.get_stats()
        assert stats["total_size"] == 2

        await pool.close()

    @pytest.mark.asyncio
    async def test_connection_creator_failure(self, pool_config):
        """Test handling of connection creation failures"""
        async def failing_creator():
            raise Exception("Connection creation failed")

        pool = AsyncConnectionPool(failing_creator, config=pool_config)

        with pytest.raises(Exception):
            await pool.initialize()

    @pytest.mark.asyncio
    async def test_health_check_timeout(self, mock_creator):
        """Test health check with timeout"""
        async def slow_health_check(conn):
            await asyncio.sleep(1)

        config = PoolConfig(min_size=1, max_size=3, health_check_timeout=0.1)
        pool = AsyncConnectionPool(mock_creator, health_check=slow_health_check, config=config)
        await pool.initialize()

        conn = await pool.acquire()
        await conn.release()

        # Next acquire should recreate due to health check timeout
        new_conn = await pool.acquire()
        # Should have created at least one new connection due to health check failure
        assert new_conn.connection_id > 1

        await new_conn.release()
        await pool.close()

    @pytest.mark.asyncio
    async def test_concurrent_acquire_release(self, mock_creator, pool_config):
        """Test concurrent acquire and release operations"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        async def acquire_release_task():
            conn = await pool.acquire()
            await asyncio.sleep(0.01)  # Simulate work
            await conn.release()

        # Run multiple concurrent tasks
        tasks = [acquire_release_task() for _ in range(10)]
        await asyncio.gather(*tasks)

        stats = pool.get_stats()
        assert stats["in_use"] == 0  # All connections should be released

        await pool.close()

    @pytest.mark.asyncio
    async def test_double_release_ignored(self, mock_creator, pool_config):
        """Test double release is safely ignored"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        conn = await pool.acquire()
        await conn.release()
        await conn.release()  # Should not raise error

        await pool.close()

    def test_get_stats_format(self, mock_creator, pool_config):
        """Test get_stats returns correct format"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)

        stats = pool.get_stats()

        expected_keys = {
            "total_size", "idle", "in_use", "available_in_queue",
            "max_size", "overflow", "min_size", "initialized", "closing"
        }
        assert set(stats.keys()) == expected_keys
        assert stats["initialized"] is False
        assert stats["closing"] is False

    @pytest.mark.asyncio
    async def test_maintenance_loop_removes_stale_connections(self, mock_creator):
        """Test maintenance loop removes stale idle connections"""
        config = PoolConfig(min_size=2, max_size=5, max_conn_idle_time=0.1, health_check_interval=0.05)
        pool = AsyncConnectionPool(mock_creator, config=config)
        await pool.initialize()

        # Acquire and release a connection to make it idle
        conn = await pool.acquire()
        await conn.release()

        # Make the connection stale by backdating its last_used time
        async with pool._lock:
            for pooled_conn in pool._all_connections.values():
                if pooled_conn.state == ConnectionState.IDLE:
                    pooled_conn.last_used = time.monotonic() - 1.0  # Make it stale

        # Wait for maintenance loop to run
        await asyncio.sleep(0.15)

        # Should have removed stale connection and created new one to maintain min_size
        stats = pool.get_stats()
        assert stats["total_size"] == config.min_size

        await pool.close()

    @pytest.mark.asyncio
    async def test_default_closer(self, mock_creator, pool_config):
        """Test default closer handles both sync and async close methods"""
        pool = AsyncConnectionPool(mock_creator, config=pool_config)
        await pool.initialize()

        conn = await pool.acquire()
        connection_id = conn.connection_id

        # Close the connection (should use default closer)
        await pool._close_connection(conn)

        # Connection should be closed and removed from tracking
        assert conn.connection.closed is True
        assert connection_id not in pool._all_connections

        await pool.close()
