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

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Dict, Optional, Set

from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
    AllowedAndBlockedHosts
from aws_advanced_python_wrapper.hostinfo import Topology
from aws_advanced_python_wrapper.utils.events import BatchingEventPublisher
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.monitor_service import MonitorService
from aws_advanced_python_wrapper.utils.storage.storage_service import \
    StorageService

logger = Logger(__name__)


class _ServicesContainer:
    def __init__(self) -> None:
        self._event_publisher: Optional[BatchingEventPublisher] = None
        self._storage_service: Optional[StorageService] = None
        self._monitor_service: Optional[MonitorService] = None
        self._thread_pools: Dict[str, ThreadPoolExecutor] = {}
        # Some service pools must be drained BEFORE the monitor service is released and connections are closed.
        # This prevents worker threads like the topology util threads from continuing to using connections
        # after they are closed, causing segfaults.
        self._drain_first_pools: Set[str] = set()
        self._lock = threading.Lock()

    def _ensure_initialized(self) -> None:
        if self._event_publisher is not None:
            return
        self._event_publisher = BatchingEventPublisher()
        self._storage_service = StorageService(self._event_publisher)
        self._storage_service.register(Topology, item_expiration_time=timedelta(minutes=5))
        self._storage_service.register(AllowedAndBlockedHosts, item_expiration_time=timedelta(minutes=5))
        self._monitor_service = MonitorService(self._event_publisher)

    @property
    def event_publisher(self) -> BatchingEventPublisher:
        self._ensure_initialized()
        return self._event_publisher  # type: ignore

    @property
    def storage_service(self) -> StorageService:
        self._ensure_initialized()
        return self._storage_service  # type: ignore

    @property
    def monitor_service(self) -> MonitorService:
        self._ensure_initialized()
        return self._monitor_service  # type: ignore

    def get_thread_pool(self, name: str, max_workers: Optional[int] = None, drain_first: bool = False) -> ThreadPoolExecutor:
        pool = self._thread_pools.get(name)
        if pool is not None:
            if drain_first:
                self._drain_first_pools.add(name)
            return pool
        with self._lock:
            if name not in self._thread_pools:
                self._thread_pools[name] = ThreadPoolExecutor(
                    max_workers=max_workers, thread_name_prefix=name)
            if drain_first:
                self._drain_first_pools.add(name)
            return self._thread_pools[name]

    def release_thread_pool(self, name: str, wait: bool = True) -> bool:
        with self._lock:
            pool = self._thread_pools.pop(name, None)
            self._drain_first_pools.discard(name)
            if pool is not None:
                try:
                    pool.shutdown(wait=wait)
                except Exception as e:
                    logger.warning("CoreServices.ErrorShuttingDownPool", name, e)
                return True
            return False

    def release_resources(self) -> None:
        # Some thread pools need to be drained first before shutting down the monitor services.
        # This prevents segfaults when monitor services shut down and close all the active monitoring connections.
        with self._lock:
            drain_names = list(self._drain_first_pools)
        for name in drain_names:
            pool = self._thread_pools.get(name)
            if pool is not None:
                try:
                    pool.shutdown(wait=True)
                except Exception as e:
                    logger.debug("CoreServices.ErrorShuttingDownPool", name, e)

        if self._monitor_service is not None:
            try:
                self._monitor_service.release_resources()
            except Exception as e:
                logger.debug("CoreServices.ErrorReleasingMonitorService", e)

        if self._storage_service is not None:
            try:
                self._storage_service.release_resources()
            except Exception as e:
                logger.debug("CoreServices.ErrorReleasingStorageService", e)

        if self._event_publisher is not None:
            try:
                self._event_publisher.release_resources()
            except Exception as e:
                logger.debug("CoreServices.ErrorReleasingEventPublisher", e)

        self._event_publisher = None
        self._storage_service = None
        self._monitor_service = None

        with self._lock:
            for name, pool in self._thread_pools.items():
                try:
                    pool.shutdown(wait=False)
                except Exception as e:
                    logger.debug("CoreServices.ErrorShuttingDownPool", name, e)
            self._thread_pools.clear()
            self._drain_first_pools.clear()


_instance = _ServicesContainer()
_instance._ensure_initialized()


def get_event_publisher() -> BatchingEventPublisher:
    return _instance.event_publisher


def get_storage_service() -> StorageService:
    return _instance.storage_service


def get_monitor_service() -> MonitorService:
    return _instance.monitor_service


def get_thread_pool(name: str, max_workers: Optional[int] = None, drain_first: bool = False) -> ThreadPoolExecutor:
    return _instance.get_thread_pool(name, max_workers, drain_first)


def release_thread_pool(name: str, wait: bool = True) -> bool:
    return _instance.release_thread_pool(name, wait)


def release_resources() -> None:
    _instance.release_resources()
