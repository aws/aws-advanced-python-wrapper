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

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class ThreadPoolContainer:
    """
    A container class for managing multiple named thread pools.
    Provides static methods for getting, creating, and releasing thread pools.
    """

    _pools: Dict[str, ThreadPoolExecutor] = {}
    _lock: threading.Lock = threading.Lock()
    _default_max_workers: Optional[int] = None  # Uses Python's default

    @classmethod
    def get_thread_pool(
        cls,
        name: str,
        max_workers: Optional[int] = None
    ) -> ThreadPoolExecutor:
        """
        Get an existing thread pool or create a new one if it doesn't exist.

        Args:
            name: Unique identifier for the thread pool
            max_workers: Max worker threads (only used when creating new pool)
                         If None, uses Python's default: min(32, os.cpu_count() + 4)

        Returns:
            ThreadPoolExecutor instance
        """
        with cls._lock:
            if name not in cls._pools:
                workers = max_workers or cls._default_max_workers
                cls._pools[name] = ThreadPoolExecutor(
                    max_workers=workers,
                    thread_name_prefix=name
                )
            return cls._pools[name]

    @classmethod
    def release_resources(cls, wait=False) -> None:
        """
        Shutdown all thread pools and release resources.

        Args:
            wait: If True, wait for all pending tasks to complete
        """
        with cls._lock:
            for name, pool in cls._pools.items():
                try:
                    pool.shutdown(wait=wait)
                except Exception as e:
                    logger.warning("ThreadPoolContainer.ErrorShuttingDownPool", name, e)
            cls._pools.clear()

    @classmethod
    def release_pool(cls, name: str, wait: bool = True) -> bool:
        """
        Release a specific thread pool by name.

        Args:
            name: The name of the thread pool to release
            wait: If True, wait for pending tasks to complete

        Returns:
            True if pool was found and released, False otherwise
        """
        with cls._lock:
            if name in cls._pools:
                try:
                    cls._pools[name].shutdown(wait=wait)
                    del cls._pools[name]
                    return True
                except Exception as e:
                    logger.warning("ThreadPoolContainer.ErrorShuttingDownPool", name, e)
            return False

    @classmethod
    def has_pool(cls, name: str) -> bool:
        """Check if a pool with the given name exists."""
        with cls._lock:
            return name in cls._pools

    @classmethod
    def get_pool_names(cls) -> List[str]:
        """Get a list of all active pool names."""
        with cls._lock:
            return list(cls._pools.keys())

    @classmethod
    def get_pool_count(cls) -> int:
        """Get the number of active pools."""
        with cls._lock:
            return len(cls._pools)

    @classmethod
    def set_default_max_workers(cls, max_workers: Optional[int]) -> None:
        """Set the default max workers for new pools."""
        cls._default_max_workers = max_workers
