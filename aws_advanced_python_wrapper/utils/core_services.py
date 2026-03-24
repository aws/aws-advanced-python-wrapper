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
from typing import Dict, Optional

from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
    AllowedAndBlockedHosts
from aws_advanced_python_wrapper.hostinfo import Topology
from aws_advanced_python_wrapper.utils.events import BatchingEventPublisher
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.monitor_service import MonitorService
from aws_advanced_python_wrapper.utils.storage.storage_service import \
    StorageService

logger = Logger(__name__)

_event_publisher: Optional[BatchingEventPublisher] = None
_storage_service: Optional[StorageService] = None
_monitor_service: Optional[MonitorService] = None
_thread_pools: Dict[str, ThreadPoolExecutor] = {}
_lock = threading.Lock()


def get_event_publisher() -> BatchingEventPublisher:
    global _event_publisher
    if _event_publisher is None:
        with _lock:
            if _event_publisher is None:
                _event_publisher = BatchingEventPublisher()
    return _event_publisher


def get_storage_service() -> StorageService:
    global _storage_service
    if _storage_service is None:
        publisher = get_event_publisher()
        with _lock:
            if _storage_service is None:
                svc = StorageService(publisher)
                svc.register(Topology, item_expiration_time=timedelta(minutes=5))
                svc.register(AllowedAndBlockedHosts, item_expiration_time=timedelta(minutes=5))
                _storage_service = svc
    return _storage_service


def get_monitor_service() -> MonitorService:
    global _monitor_service
    if _monitor_service is None:
        publisher = get_event_publisher()
        with _lock:
            if _monitor_service is None:
                _monitor_service = MonitorService(publisher)
    return _monitor_service


def get_thread_pool(name: str, max_workers: Optional[int] = None) -> ThreadPoolExecutor:
    pool = _thread_pools.get(name)
    if pool is not None:
        return pool
    with _lock:
        if name not in _thread_pools:
            _thread_pools[name] = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=name)
        return _thread_pools[name]


def release_thread_pool(name: str, wait: bool = True) -> bool:
    with _lock:
        pool = _thread_pools.pop(name, None)
        if pool is not None:
            try:
                pool.shutdown(wait=wait)
            except Exception as e:
                logger.warning("CoreServices.ErrorShuttingDownPool", name, e)
            return True
        return False


def release_resources() -> None:
    global _event_publisher, _storage_service, _monitor_service

    with _lock:
        if _monitor_service is not None:
            try:
                _monitor_service.release_resources()
            except Exception as e:
                logger.debug("CoreServices.ErrorReleasingMonitorService", e)
            _monitor_service = None

        if _storage_service is not None:
            try:
                _storage_service.release_resources()
            except Exception as e:
                logger.debug("CoreServices.ErrorReleasingStorageService", e)
            _storage_service = None

        if _event_publisher is not None:
            try:
                _event_publisher.release_resources()
            except Exception as e:
                logger.debug("CoreServices.ErrorReleasingEventPublisher", e)
            _event_publisher = None

        for name, pool in _thread_pools.items():
            try:
                pool.shutdown(wait=False)
            except Exception as e:
                logger.debug("CoreServices.ErrorShuttingDownPool", name, e)
        _thread_pools.clear()
