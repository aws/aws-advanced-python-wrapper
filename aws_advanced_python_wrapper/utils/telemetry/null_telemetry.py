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

from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryContext, TelemetryCounter, TelemetryFactory, TelemetryGauge,
    TelemetryTraceLevel)


class NullTelemetryContext(TelemetryContext):
    def __init__(self, name: str):
        self.name: str = name

    def get_name(self):
        return self.name


class NullTelemetryCounter(TelemetryCounter):
    def __init__(self, name: str):
        self.name: str = name

    def get_name(self):
        return self.name


class NullTelemetryGauge(TelemetryGauge):
    def __init__(self, name: str):
        self.name: str = name

    def get_name(self):
        return self.name


class NullTelemetryFactory(TelemetryFactory):
    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        return NullTelemetryContext(name)

    def post_copy(self, context: TelemetryContext, trace_level: TelemetryTraceLevel):
        pass  # Do nothing

    def create_counter(self, name: str) -> TelemetryCounter:
        return NullTelemetryCounter(name)

    def create_gauge(self, name: str, callback):
        return NullTelemetryGauge(name)
