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

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.util.types import AttributeValue


class TelemetryConst:
    TRACE_NAME_ANNOTATION = "trace_name"
    SOURCE_TRACE_ANNOTATION = "source_trace_id"
    PARENT_TRACE_ANNOTATION = "parent_trace_id"
    EXCEPTION_TYPE_ANNOTATION = "exception_type"
    EXCEPTION_MESSAGE_ANNOTATION = "exception_message"
    COPY_TRACE_NAME_PREFIX = "copy: "


class TelemetryTraceLevel(Enum):
    FORCE_TOP_LEVEL = auto()
    TOP_LEVEL = auto()
    NESTED = auto()
    NO_TRACE = auto()


class TelemetryContext(ABC):
    def set_success(self, success: bool):
        pass

    def set_attribute(self, key: str, value: AttributeValue):
        pass

    def set_exception(self, exception: Exception):
        pass

    def get_name(self):
        pass

    def close_context(self):
        pass


class TelemetryCounter(ABC):
    def add(self, value):
        pass

    def inc(self):
        pass


class TelemetryGauge(ABC):
    pass


class TelemetryFactory(ABC):
    @abstractmethod
    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        pass

    @abstractmethod
    def post_copy(self, context: TelemetryContext, trace_level: TelemetryTraceLevel):
        pass

    @abstractmethod
    def create_counter(self, name: str) -> TelemetryCounter:
        pass

    @abstractmethod
    def create_gauge(self, name: str, callback):
        pass
