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

from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.telemetry.open_telemetry import \
    OpenTelemetryFactory
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryContext, TelemetryCounter, TelemetryFactory, TelemetryGauge,
    TelemetryTraceLevel)
from aws_advanced_python_wrapper.utils.telemetry.xray_telemetry import \
    XRayTelemetryFactory


class DefaultTelemetryFactory(TelemetryFactory):
    _OPEN_TELEMETRY_FACTORY: ClassVar[OpenTelemetryFactory] = OpenTelemetryFactory()
    _XRAY_TELEMETRY_FACTORY: ClassVar[XRayTelemetryFactory] = XRayTelemetryFactory()

    def __init__(self, properties: Properties):
        self._enable_telemetry = WrapperProperties.ENABLE_TELEMETRY.get(properties)
        self._telemetry_submit_toplevel = WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.get(properties)
        self._telemetry_traces_backend = WrapperProperties.TELEMETRY_TRACES_BACKEND.get(properties)
        self._telemetry_metrics_backend = WrapperProperties.TELEMETRY_METRICS_BACKEND.get(properties)

        self._traces_telemetry_factory: TelemetryFactory | None
        self._metrics_telemetry_factory: TelemetryFactory | None
        self._telemetry_in_use: bool

        if self._enable_telemetry:
            if self._telemetry_traces_backend is not None:
                traces_backend = self._telemetry_traces_backend.upper()
                if traces_backend == "OTLP":
                    self._traces_telemetry_factory = DefaultTelemetryFactory._OPEN_TELEMETRY_FACTORY
                elif traces_backend == "XRAY":
                    self._traces_telemetry_factory = DefaultTelemetryFactory._XRAY_TELEMETRY_FACTORY
                elif traces_backend == "NONE":
                    self._traces_telemetry_factory = None
                else:
                    raise RuntimeError(Messages.get_formatted(
                        "DefaultTelemetryFactory.InvalidTracingBackend", self._telemetry_traces_backend))
            else:
                raise RuntimeError(Messages.get_formatted("DefaultTelemetryFactory.NoTracingBackendProvided"))

            if self._telemetry_metrics_backend is not None:
                metrics_backend = self._telemetry_metrics_backend.upper()
                if metrics_backend == "OTLP":
                    self._metrics_telemetry_factory = DefaultTelemetryFactory._OPEN_TELEMETRY_FACTORY
                elif metrics_backend == "NONE":
                    self._metrics_telemetry_factory = None
                else:
                    raise RuntimeError(Messages.get_formatted(
                        "DefaultTelemetryFactory.InvalidMetricsBackend", self._telemetry_metrics_backend))
            else:
                raise RuntimeError(Messages.get_formatted("DefaultTelemetryFactory.NoMetricsBackendProvided"))

        else:
            self._traces_telemetry_factory = None
            self._metrics_telemetry_factory = None

        self._telemetry_in_use = self._traces_telemetry_factory is not None or self._metrics_telemetry_factory is not None

    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext | None:
        if self._traces_telemetry_factory is None:
            return None

        if not self._telemetry_submit_toplevel and trace_level == TelemetryTraceLevel.TOP_LEVEL:
            return self._traces_telemetry_factory.open_telemetry_context(name, TelemetryTraceLevel.NESTED)

        return self._traces_telemetry_factory.open_telemetry_context(name, trace_level)

    def post_copy(self, context: TelemetryContext, trace_level: TelemetryTraceLevel):
        if self._traces_telemetry_factory is None:
            return
        self._traces_telemetry_factory.post_copy(context, trace_level)

    def create_counter(self, name: str) -> TelemetryCounter | None:
        if self._metrics_telemetry_factory is None:
            return None
        return self._metrics_telemetry_factory.create_counter(name)

    def create_gauge(self, name: str, callback) -> TelemetryGauge | None:
        if self._metrics_telemetry_factory is None:
            return None
        return self._metrics_telemetry_factory.create_gauge(name, callback)

    def in_use(self) -> bool:
        return self._telemetry_in_use
