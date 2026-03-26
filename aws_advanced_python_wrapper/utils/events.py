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

import weakref
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from typing import Any, Dict, Protocol, Set, Type, runtime_checkable

from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class EventBase:
    """Base class for all events."""
    immediate_delivery: bool = False


@dataclass(frozen=True, eq=True)
class DataAccessEvent(EventBase):
    """Published when data is accessed in StorageService."""
    data_type: type
    key: Any
    immediate_delivery: bool = field(default=False, compare=False, hash=False)


@dataclass(frozen=True, eq=True)
class MonitorStopEvent(EventBase):
    """Published to signal a monitor should be stopped."""
    monitor_type: type
    key: Any
    immediate_delivery: bool = field(default=True, compare=False, hash=False)


@dataclass(frozen=True, eq=True)
class MonitorResetEvent(EventBase):
    """Published during Blue/Green switchover to reset monitors holding stale connections."""
    cluster_id: str
    endpoints: frozenset
    immediate_delivery: bool = field(default=True, compare=False, hash=False)


@runtime_checkable
class EventSubscriber(Protocol):
    @abstractmethod
    def process_event(self, event: EventBase) -> None:
        ...


class EventPublisher(ABC):
    @abstractmethod
    def subscribe(self, subscriber: EventSubscriber, event_types: Set[Type[EventBase]]) -> None:
        ...

    @abstractmethod
    def unsubscribe(self, subscriber: EventSubscriber, event_types: Set[Type[EventBase]]) -> None:
        ...

    @abstractmethod
    def publish(self, event: EventBase) -> None:
        ...

    @abstractmethod
    def release_resources(self) -> None:
        ...


class BatchingEventPublisher(EventPublisher):
    _DELIVERY_INTERVAL_SEC = 30.0

    def __init__(self) -> None:
        self._subscribers: Dict[Type[EventBase], weakref.WeakSet[EventSubscriber]] = {}
        self._pending_events: Set[EventBase] = set()
        self._lock = Lock()
        self._stop_event = Event()
        self._thread = Thread(
            target=self._delivery_loop, daemon=True, name="BatchingEventPublisher")
        self._thread.start()

    def subscribe(self, subscriber: EventSubscriber, event_types: Set[Type[EventBase]]) -> None:
        with self._lock:
            for event_type in event_types:
                if event_type not in self._subscribers:
                    self._subscribers[event_type] = weakref.WeakSet()
                self._subscribers[event_type].add(subscriber)

    def unsubscribe(self, subscriber: EventSubscriber, event_types: Set[Type[EventBase]]) -> None:
        with self._lock:
            for event_type in event_types:
                ws = self._subscribers.get(event_type)
                if ws is not None:
                    ws.discard(subscriber)

    def publish(self, event: EventBase) -> None:
        if event.immediate_delivery:
            self._deliver(event)
        else:
            with self._lock:
                self._pending_events.add(event)

    def release_resources(self) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
        with self._lock:
            self._pending_events.clear()
            self._subscribers.clear()

    def _delivery_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self._DELIVERY_INTERVAL_SEC):
                break
            self._drain_and_deliver()

    def _drain_and_deliver(self) -> None:
        with self._lock:
            events = list(self._pending_events)
            self._pending_events.clear()
        for event in events:
            self._deliver(event)

    def _deliver(self, event: EventBase) -> None:
        with self._lock:
            ws = self._subscribers.get(type(event))
            if ws is None:
                return
            subscribers = list(ws)
        for subscriber in subscribers:
            try:
                subscriber.process_event(event)
            except Exception as e:
                logger.debug("BatchingEventPublisher.ErrorDeliveringEvent", e)
