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
from dataclasses import dataclass
from time import perf_counter_ns
from typing import (Any, Callable, Dict, FrozenSet, Optional, Protocol,
                    runtime_checkable)

from aws_advanced_python_wrapper.utils.events import (DataAccessEvent,
                                                      EventBase,
                                                      EventPublisher,
                                                      EventSubscriber,
                                                      MonitorResetEvent,
                                                      MonitorStopEvent)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.storage.sliding_expiration_cache import \
    SlidingExpirationCache

logger = Logger(__name__)


@runtime_checkable
class Monitor(Protocol):
    """Structural interface for all monitors managed by MonitorService.

    stop() signals the monitor to stop and releases all resources (calls close() internally).
    close() releases resources only — called by stop(), not directly by external code.
    """

    def stop(self) -> None: ...
    def close(self) -> None: ...

    @property
    def can_dispose(self) -> bool: ...

    @property
    def last_activity_ns(self) -> int: ...


@dataclass
class MonitorSettings:
    expiration_timeout_ns: int = 15 * 60 * 1_000_000_000  # 15 min
    inactive_timeout_ns: int = 3 * 60 * 1_000_000_000  # 3 min
    produced_data_type: Optional[type] = None


class _CacheContainer:
    def __init__(self, settings: MonitorSettings) -> None:
        self.settings = settings
        self.cache: SlidingExpirationCache = SlidingExpirationCache(
            cleanup_interval_ns=settings.expiration_timeout_ns,
            should_dispose_func=lambda monitor: monitor.can_dispose,
            item_disposal_func=lambda monitor: self._dispose(monitor))

    @staticmethod
    def _dispose(monitor: Monitor) -> None:
        try:
            monitor.stop()
        except Exception as e:
            logger.debug("MonitorService.ErrorDisposingMonitor", e)


class MonitorService(EventSubscriber):
    """Centralized monitor lifecycle manager."""

    _CLEANUP_INTERVAL_SEC = 60.0

    def __init__(self, event_publisher: EventPublisher) -> None:
        self._event_publisher = event_publisher
        self._monitor_caches: Dict[type, _CacheContainer] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()

        self._event_publisher.subscribe(self, {DataAccessEvent, MonitorStopEvent, MonitorResetEvent})

        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, daemon=True, name="MonitorService-Cleanup")
        self._cleanup_thread.start()

    def register_monitor_type(
            self,
            monitor_type: type,
            expiration_timeout_ns: int = 15 * 60 * 1_000_000_000,
            inactive_timeout_ns: int = 3 * 60 * 1_000_000_000,
            produced_data_type: Optional[type] = None) -> None:
        with self._lock:
            if monitor_type not in self._monitor_caches:
                settings = MonitorSettings(
                    expiration_timeout_ns=expiration_timeout_ns,
                    inactive_timeout_ns=inactive_timeout_ns,
                    produced_data_type=produced_data_type)
                self._monitor_caches[monitor_type] = _CacheContainer(settings)

    def run_if_absent(
            self,
            monitor_type: type,
            key: Any,
            factory: Callable[[], Any]) -> Any:
        container = self._get_or_create_container(monitor_type)
        return container.cache.compute_if_absent(
            key, lambda _: factory(), container.settings.expiration_timeout_ns)

    def run_if_absent_with_aliases(
            self,
            monitor_type: type,
            aliases: FrozenSet[str],
            factory: Callable[[], Any]) -> Any:
        container = self._get_or_create_container(monitor_type)
        return container.cache.get_or_create_for_aliases(
            aliases, factory, container.settings.expiration_timeout_ns)

    def detach(self, monitor_type: type, monitor: Any) -> None:
        container = self._monitor_caches.get(monitor_type)
        if container is None:
            return
        container.cache.detach_value(monitor)

    def get(self, monitor_type: type, key: Any) -> Optional[Any]:
        container = self._monitor_caches.get(monitor_type)
        if container is None:
            return None
        return container.cache.get(key)

    def count(self, monitor_type: type) -> int:
        container = self._monitor_caches.get(monitor_type)
        return len(container.cache) if container is not None else 0

    def stop_and_remove(self, monitor_type: type, key: Any) -> None:
        container = self._monitor_caches.get(monitor_type)
        if container is not None:
            container.cache.remove(key)

    def stop_and_remove_all(self, monitor_type: type) -> None:
        container = self._monitor_caches.get(monitor_type)
        if container is not None:
            container.cache.clear()

    def stop_all(self) -> None:
        with self._lock:
            for container in self._monitor_caches.values():
                container.cache.clear()

    def release_resources(self) -> None:
        self._stop_event.set()
        self._event_publisher.unsubscribe(self, {DataAccessEvent, MonitorStopEvent, MonitorResetEvent})
        self.stop_all()
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2.0)
        with self._lock:
            self._monitor_caches.clear()

    def process_event(self, event: EventBase) -> None:
        if isinstance(event, DataAccessEvent):
            self._on_data_access(event)
            return
        if isinstance(event, MonitorStopEvent):
            self.stop_and_remove(event.monitor_type, event.key)
            return

        with self._lock:
            containers = list(self._monitor_caches.values())
        for container in containers:
            for _key, cache_item in container.cache.items():
                monitor = cache_item.item
                if isinstance(monitor, EventSubscriber):
                    try:
                        monitor.process_event(event)
                    except Exception as e:
                        logger.debug("MonitorService.ErrorPropagatingEvent", e)

    def _on_data_access(self, event: DataAccessEvent) -> None:
        """Extend expiration of monitors whose produced_data_type matches."""
        with self._lock:
            containers = list(self._monitor_caches.values())
        for container in containers:
            if container.settings.produced_data_type == event.data_type:
                container.cache.extend_expiration(event.key)

    def _get_or_create_container(self, monitor_type: type) -> _CacheContainer:
        with self._lock:
            if monitor_type not in self._monitor_caches:
                self._monitor_caches[monitor_type] = _CacheContainer(MonitorSettings())
            return self._monitor_caches[monitor_type]

    def _cleanup_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self._CLEANUP_INTERVAL_SEC):
                break
            self._run_cleanup()

    def _run_cleanup(self) -> None:
        with self._lock:
            containers = list(self._monitor_caches.items())
        now = perf_counter_ns()
        for monitor_type, container in containers:
            try:
                # Detect and remove stuck monitors
                inactive_timeout = container.settings.inactive_timeout_ns
                for key, cache_item in container.cache.items():
                    monitor = cache_item.item
                    if now - monitor.last_activity_ns > inactive_timeout:
                        logger.debug("MonitorService.StuckMonitorDetected", monitor_type, key)
                        container.cache.remove(key)

                container.cache.cleanup()
            except Exception as e:
                logger.debug("MonitorService.ErrorDuringCleanup", e)
