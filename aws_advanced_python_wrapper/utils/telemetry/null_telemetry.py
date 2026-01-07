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

from typing import ClassVar

from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryContext, TelemetryCounter, TelemetryFactory, TelemetryGauge,
    TelemetryTraceLevel)


class NullTelemetryContext(TelemetryContext):
    def get_name(self):
        return "null"


class NullTelemetryCounter(TelemetryCounter):
    def get_name(self):
        return "null"


class NullTelemetryGauge(TelemetryGauge):
    def get_name(self):
        return "null"


class NullTelemetryFactory(TelemetryFactory):
    _NULL_CONTEXT: ClassVar[NullTelemetryContext] = NullTelemetryContext()
    _NULL_COUNTER: ClassVar[NullTelemetryCounter] = NullTelemetryCounter()
    _NULL_GAUGE: ClassVar[NullTelemetryGauge] = NullTelemetryGauge()

    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        return NullTelemetryFactory._NULL_CONTEXT

    def post_copy(self, context: TelemetryContext, trace_level: TelemetryTraceLevel):
        pass  # Do nothing

    def create_counter(self, name: str) -> TelemetryCounter:
        return NullTelemetryFactory._NULL_COUNTER

    def create_gauge(self, name: str, callback) -> TelemetryGauge:
        return NullTelemetryFactory._NULL_GAUGE

    def in_use(self) -> bool:
        return False
