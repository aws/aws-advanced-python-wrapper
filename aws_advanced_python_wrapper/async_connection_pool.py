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

"""
Generic async connection pool - manual connection management.
User controls when to acquire and release connections.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

from .errors import (
    ConnectionReleasedError,
    PoolClosingError,
    PoolExhaustedError,
    PoolHealthCheckError,
    PoolNotInitializedError,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ConnectionState(Enum):
    IDLE = "idle"
    IN_USE = "in_use"
    CLOSED = "closed"


@dataclass
class PoolConfig:
    """Pool configuration"""
    min_size: int = 1
    max_size: int = 20
    overflow: int = 0  # Additional connections beyond max_size. -1 = unlimited
    acquire_conn_timeout: float = 60.0  # Max time to wait for connection acquisition
    max_conn_lifetime: float = 3600.0  # 1 hour
    max_conn_idle_time: float = 600.0  # 10 minutes
    health_check_interval: float = 30.0
    health_check_timeout: float = 5.0  # Health check timeout
    pre_ping: bool = True


class AsyncConnectionPool:
    """
    Generic async connection pool with manual connection management.
    """

    class AsyncPooledConnectionWrapper:
        """Combined wrapper for pooled connections with metadata and user interface"""

        def __init__(self, connection: Any, connection_id: int, pool: 'AsyncConnectionPool'):
            # Pool metadata
            self.connection = connection
            self.connection_id = connection_id
            self.created_at = time.monotonic()
            self.last_used = time.monotonic()
            self.use_count = 0
            self.state = ConnectionState.IDLE
            
            # User interface
            self._pool = pool
            self._released = False

        def mark_in_use(self):
            self.state = ConnectionState.IN_USE
            self.last_used = time.monotonic()
            self.use_count += 1
            self._released = False

        def mark_idle(self):
            self.state = ConnectionState.IDLE
            self.last_used = time.monotonic()

        def mark_closed(self):
            self.state = ConnectionState.CLOSED

        @property
        def age(self) -> float:
            return time.monotonic() - self.created_at

        @property
        def idle_time(self) -> float:
            return time.monotonic() - self.last_used

        def is_stale(self, max_conn_lifetime: float, max_conn_idle_time: float) -> bool:
            return (
                self.age > max_conn_lifetime or
                (self.state == ConnectionState.IDLE and self.idle_time > max_conn_idle_time)
            )
        
        # User interface methods
        async def release(self):
            """Return connection to the pool"""
            if not self._released:
                self._released = True
                await self._pool._return_connection(self)

        async def close(self):
            """Alias for release()"""
            await self.release()

        def __getattr__(self, name):
            """Proxy attribute access to underlying connection"""
            if self._released:
                raise ConnectionReleasedError("Connection already released to pool")
            return getattr(self.connection, name)

        def __del__(self):
            """Warn if connection not released"""
            if not self._released and self.state == ConnectionState.IN_USE:
                logger.warning(
                    f"Connection {self.connection_id} was not released! "
                    f"Always call release() or use context manager."
                )

    @staticmethod
    async def _default_closer(connection: Any) -> None:
        """Default connection closer that handles both sync and async close methods"""
        if hasattr(connection, 'close'):
            close_method = connection.close
            if asyncio.iscoroutinefunction(close_method):
                await close_method()
            else:
                close_method()

    @staticmethod
    async def _default_health_check(connection: Any) -> None:
        """Default health check that verifies connection is not closed"""
        is_closed = await asyncio.to_thread(lambda: connection.is_closed)
        if is_closed:
            raise PoolHealthCheckError("Connection is closed")

    def __init__(
        self,
        creator: Callable[[], Awaitable[T]],
        health_check: Optional[Callable[[T], Awaitable[None]]] = None,
        closer: Optional[Callable[[T], Awaitable[None]]] = None,
        config: Optional[PoolConfig] = None
    ):
        self._creator = creator
        self._health_check = health_check or self._default_health_check
        self._closer = closer or self._default_closer
        self._config = config or PoolConfig()

        # Pool state - queue size accounts for overflow
        self._pool: asyncio.Queue['AsyncConnectionPool.AsyncPooledConnectionWrapper'] = asyncio.Queue()
        self._all_connections: Dict[int, 'AsyncConnectionPool.AsyncPooledConnectionWrapper'] = {}
        self._connection_counter = 0
        self._max_connection_id = 1000000  # Reset after 1M connections
        self._size = 0

        # Synchronization
        self._lock = asyncio.Lock()
        self._id_lock = asyncio.Lock()  # Separate lock for connection ID generation
        self._closing = False
        self._initialized = False

        # Background tasks
        self._maintenance_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize the pool with minimum connections"""
        if self._initialized:
            logger.warning("Pool already initialized")
            return

        logger.info(f"Initializing pool with {self._config.min_size} connections")

        try:
            # Create initial connections
            tasks = [
                self._create_connection()
                for _ in range(self._config.min_size)
            ]
            connections = await asyncio.gather(*tasks)  # Remove return_exceptions=True

            async with self._lock:
                for conn in connections:
                    self._size += 1
                    await self._pool.put(conn)

            # Start maintenance task
            self._maintenance_task = asyncio.create_task(self._maintenance_loop())

            self._initialized = True
            logger.info(f"Pool initialized with {self._size} connections")

        except Exception as e:
            logger.error(f"Failed to initialize pool: {e}")
            await self.close()
            raise

    async def _get_next_connection_id(self) -> int:
        """Get next unique connection ID with cycling to prevent overflow"""
        async with self._id_lock:
            while True:
                self._connection_counter += 1
                if self._connection_counter > self._max_connection_id:
                    self._connection_counter = 1
                
                # Check if ID is in use (avoid nested lock by checking outside)
                if self._connection_counter not in self._all_connections:
                    return self._connection_counter

    async def _create_connection(self) -> 'AsyncConnectionPool.AsyncPooledConnectionWrapper':
        """Create a new connection (caller manages size)"""
        connection_id = await self._get_next_connection_id()
        
        try:
            raw_conn = await self._creator()
            pooled_conn = AsyncConnectionPool.AsyncPooledConnectionWrapper(raw_conn, connection_id, self)

            async with self._lock:
                self._all_connections[connection_id] = pooled_conn

            logger.debug(f"Created connection {connection_id}, pool size: {self._size}")
            return pooled_conn

        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            raise

    async def _close_connection(self, pooled_conn: 'AsyncConnectionPool.AsyncPooledConnectionWrapper'):
        """Close a connection"""
        if pooled_conn.state == ConnectionState.CLOSED:
            return

        pooled_conn.mark_closed()

        try:
            # Close the underlying connection
            await self._closer(pooled_conn.connection)

            # Remove from tracking
            async with self._lock:
                self._all_connections.pop(pooled_conn.connection_id, None)
                self._size -= 1

        except Exception as e:
            logger.error(f"Error closing connection {pooled_conn.connection_id}: {e}")

    async def _validate_connection(self, pooled_conn: 'AsyncConnectionPool.AsyncPooledConnectionWrapper') -> bool:
        """Validate a connection"""
        # Check if stale
        if pooled_conn.is_stale(
            self._config.max_conn_lifetime,
            self._config.max_conn_idle_time
        ):
            logger.debug(f"Connection {pooled_conn.connection_id} is stale")
            return False

        # Run health check if configured
        if self._config.pre_ping and self._health_check:
            try:
                await asyncio.wait_for(
                    self._health_check(pooled_conn.connection),
                    timeout=self._config.health_check_timeout
                )
                return True
            except Exception as e:
                logger.warning(
                    f"Health check failed for connection "
                    f"{pooled_conn.connection_id}: {e}"
                )
                return False

        return True

    async def acquire(self) -> 'AsyncConnectionPool.AsyncPooledConnectionWrapper':
        """
        Acquire a connection from the pool.
        YOU must call release() when done!

        Returns:
            AsyncPooledConnectionWrapper: Connection with .release() method and direct attribute access
        """
        if not self._initialized:
            raise PoolNotInitializedError("Pool not initialized. Call await pool.initialize() first")

        if self._closing:
            raise PoolClosingError("Pool is closing")

        pooled_conn = None
        created_new = False

        try:
            # Try to get idle connection, create new one, or wait
            try:
                pooled_conn = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                # Atomic check and reserve slot
                async with self._lock:
                    max_total = self._config.max_size + (float('inf') if self._config.overflow == -1 else self._config.overflow)
                    
                    if self._size < max_total:
                        self._size += 1  # Reserve slot
                        create_new = True
                    else:
                        create_new = False
                
                if create_new:
                    try:
                        connection_id = await self._get_next_connection_id()
                        # Create connection with timeout to prevent hanging during failover
                        raw_conn = await asyncio.wait_for(
                            self._creator(), 
                            timeout=min(self._config.acquire_conn_timeout, 30.0)  # Cap at 30s to prevent indefinite hangs
                        )
                        pooled_conn = AsyncConnectionPool.AsyncPooledConnectionWrapper(raw_conn, connection_id, self)
                        
                        # Add to tracking
                        async with self._lock:
                            self._all_connections[connection_id] = pooled_conn
                        created_new = True
                    except Exception as e:
                        # Failed to create, decrement reserved size
                        async with self._lock:
                            self._size -= 1
                        raise
                else:
                    pooled_conn = await asyncio.wait_for(self._pool.get(), timeout=self._config.acquire_conn_timeout)

            # Validate and recreate if needed
            if not await self._validate_connection(pooled_conn):
                await self._close_connection(pooled_conn)
                # Recreate using same pattern as initial creation to avoid deadlock
                async with self._lock:
                    self._size += 1  # Reserve slot
                
                try:
                    connection_id = await self._get_next_connection_id()
                    raw_conn = await asyncio.wait_for(
                        self._creator(), 
                        timeout=min(self._config.acquire_conn_timeout, 30.0)  # Cap at 30s to prevent indefinite hangs
                    )
                    pooled_conn = AsyncConnectionPool.AsyncPooledConnectionWrapper(raw_conn, connection_id, self)
                    async with self._lock:
                        self._all_connections[connection_id] = pooled_conn
                    created_new = True
                except Exception as e:
                    async with self._lock:
                        self._size -= 1
                    raise

            pooled_conn.mark_in_use()
            return pooled_conn

        except asyncio.TimeoutError:
            raise PoolExhaustedError(
                f"Pool exhausted: timeout after {self._config.acquire_conn_timeout}s, "
                f"size: {self._size}/{self._config.max_size}, "
                f"idle: {self._pool.qsize()}"
            )
        except Exception as e:
            logger.error(f"Error acquiring connection: {e}")
            # If we created a new connection and got error, close it
            if created_new and pooled_conn:
                await self._close_connection(pooled_conn)
            raise

    async def _return_connection(self, pooled_conn: 'AsyncConnectionPool.AsyncPooledConnectionWrapper'):
        """Return connection to pool or close if excess"""
        if self._closing:
            await self._close_connection(pooled_conn)
            return
            
        # Check if we should close excess connections (lock released before close)
        should_close = False
        async with self._lock:
            should_close = self._pool.qsize() >= self._config.max_size
            
        if should_close:
            await self._close_connection(pooled_conn)
        else:
            try:
                pooled_conn.mark_idle()
                await self._pool.put(pooled_conn)
            except Exception as e:
                logger.error(f"Error returning connection: {e}")
                await self._close_connection(pooled_conn)

    @asynccontextmanager
    async def connection(self):
        """
        Context manager for automatic connection management.

        Usage:
            async with pool.connection() as conn:
                result = await conn.fetchval("SELECT 1")
        """
        pool_conn = await self.acquire()
        try:
            yield pool_conn
        finally:
            await pool_conn.release()

    async def _maintenance_loop(self):
        """Background task to maintain pool health"""
        while not self._closing:
            try:
                await asyncio.sleep(self._config.health_check_interval)
                if self._closing:
                    break

                # Create connections if below minimum
                needed = 0
                async with self._lock:
                    needed = self._config.min_size - self._size
                    if needed > 0:
                        self._size += needed  # Reserve slots
                
                if needed > 0:
                    for _ in range(needed):
                        try:
                            conn = await self._create_connection()
                            await self._pool.put(conn)
                        except Exception as e:
                            async with self._lock:
                                self._size -= 1  # Release reserved slot on failure
                            logger.error(f"Maintenance connection creation failed: {e}")

                # Remove stale idle connections (collect under lock, close outside)
                stale_conns = []
                async with self._lock:
                    stale_conns = [
                        conn for conn in self._all_connections.values()
                        if conn.state == ConnectionState.IDLE and 
                           conn.is_stale(self._config.max_conn_lifetime, self._config.max_conn_idle_time)
                    ]
                
                # Close stale connections outside the lock to avoid deadlock
                for conn in stale_conns:
                    try:
                        await self._close_connection(conn)
                    except Exception as e:
                        logger.error(f"Stale connection cleanup failed: {e}")

            except Exception as e:
                logger.error(f"Maintenance loop error: {e}")

    async def close(self):
        """Close the pool and all connections"""
        if self._closing:
            return

        self._closing = True

        # Cancel maintenance task
        if self._maintenance_task:
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass

        # Close all connections (collect under lock, close outside to avoid deadlock)
        async with self._lock:
            connections = list(self._all_connections.values())
        
        await asyncio.gather(
            *[self._close_connection(conn) for conn in connections],
            return_exceptions=True
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        # Note: This is a sync method, so we can't use async lock
        # Stats may be slightly inconsistent but that's acceptable for monitoring
        try:
            states = [conn.state for conn in self._all_connections.values()]
            idle_count = states.count(ConnectionState.IDLE)
            in_use_count = states.count(ConnectionState.IN_USE)
        except RuntimeError:  # Dictionary changed during iteration
            idle_count = in_use_count = 0
            
        return {
            "total_size": self._size,
            "idle": idle_count,
            "in_use": in_use_count,
            "available_in_queue": self._pool.qsize(),
            "max_size": self._config.max_size,
            "overflow": self._config.overflow,
            "min_size": self._config.min_size,
            "initialized": self._initialized,
            "closing": self._closing,
        }
