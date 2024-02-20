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

from typing import TYPE_CHECKING, Callable, Optional, Sequence, Union

if TYPE_CHECKING:
    from opentelemetry.util.types import AttributeValue

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry import trace as trace_api
from opentelemetry.metrics import (CallbackOptions, Meter, Observation,
                                   get_meter)
from opentelemetry.sdk.trace import ReadableSpan, Span, StatusCode, Tracer

from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryConst, TelemetryContext, TelemetryCounter, TelemetryFactory,
    TelemetryGauge, TelemetryTraceLevel)

logger = Logger(__name__)

INSTRUMENTATION_NAME = "aws-advanced-python-wrapper"


class OpenTelemetryContext(TelemetryContext):
    def __init__(self, tracer: Tracer, name: str, trace_level: TelemetryTraceLevel,
                 start_time: Optional[int] = None, link_span: Optional[Span] = None):
        self._name = name
        self._tracer = tracer
        self._span: Optional[Span]
        self._meter: Meter
        self._token: Optional[object] = None

        current_span: Span = trace.get_current_span()  # type: ignore
        is_root = (current_span is None or current_span == trace.INVALID_SPAN)

        if is_root and trace_level == TelemetryTraceLevel.NESTED:
            trace_level = TelemetryTraceLevel.TOP_LEVEL

        links: Sequence[trace_api.Link] = ()
        if trace_level in [TelemetryTraceLevel.FORCE_TOP_LEVEL, TelemetryTraceLevel.TOP_LEVEL]:
            if link_span is not None:
                links = [trace.Link(link_span.get_span_context())]
            else:
                if not is_root:
                    links = [trace.Link(current_span.get_span_context())]

            self._span = self._tracer.start_span(self._name, context=context_api.Context(),
                                                 links=links, start_time=start_time)  # type: ignore
            if not is_root:
                self.set_attribute(TelemetryConst.TRACE_NAME_ANNOTATION, self._name)

            ctx = trace.set_span_in_context(self._span)  # type: ignore
            self._token = context_api.attach(ctx)
            logger.debug("OpenTelemetryContext.TelemetryTraceID", self._name,
                         self._span.get_span_context().trace_id)  # type: ignore

        elif trace_level == TelemetryTraceLevel.NESTED:
            if link_span is not None:
                links = [trace.Link(link_span.get_span_context())]

            self._span = self._tracer.start_span(self._name, links=links, start_time=start_time)  # type: ignore
            ctx = trace.set_span_in_context(self._span)  # type: ignore
            self._token = context_api.attach(ctx)
            self.set_attribute(TelemetryConst.TRACE_NAME_ANNOTATION, self._name)

        elif trace_level == TelemetryTraceLevel.NO_TRACE:
            self._span = None

    def set_success(self, success: bool):
        if self._span is not None:
            self._span.set_status(StatusCode.OK if success else StatusCode.ERROR)

    def set_attribute(self, key: str, value: AttributeValue):
        if self._span is not None:
            self._span.set_attribute(key, value)

    def set_exception(self, exception: Exception):
        if self._span is not None and exception is not None:
            self._span.set_attribute(TelemetryConst.EXCEPTION_TYPE_ANNOTATION, exception.__class__.__name__)
            self._span.set_attribute(TelemetryConst.EXCEPTION_MESSAGE_ANNOTATION, str(exception))
            self._span.record_exception(exception)

    def get_name(self) -> str:
        return self._name

    def close_context(self):
        if self._token is not None:
            context_api.detach(self._token)
        if self._span is not None:
            self._span.end()

    @property
    def tracer(self) -> Tracer:
        return self._tracer

    @property
    def span(self) -> Optional[Span]:
        return self._span


def post_copy(context: OpenTelemetryContext, trace_level: TelemetryTraceLevel):
    if trace_level == TelemetryTraceLevel.NO_TRACE:
        return

    _clone_and_close_context(context, trace_level)


def _clone_and_close_context(context: OpenTelemetryContext, trace_level: TelemetryTraceLevel) -> OpenTelemetryContext:
    if not isinstance(context.span, ReadableSpan):
        raise RuntimeError(Messages.get("OpenTelemetry.InvalidContext"))

    clone = OpenTelemetryContext(
        context.tracer, TelemetryConst.COPY_TRACE_NAME_PREFIX + context.get_name(),
        trace_level, context.span.start_time, context.span)

    for key in context.span.attributes:  # type: ignore
        value = context.span.attributes[key]  # type: ignore
        clone.set_attribute(key, value)

    clone.span.set_status(context.span.status)  # type: ignore
    clone.set_attribute(TelemetryConst.SOURCE_TRACE_ANNOTATION, str(context.span.get_span_context().trace_id))
    clone.span.end(context.span.end_time)  # type: ignore

    return clone


class OpenTelemetryCounter(TelemetryCounter):
    def __init__(self, meter: Meter, name: str):
        self._meter: Meter = meter
        self._name: str = name
        self._counter = meter.create_up_down_counter(self._name, unit="1")

    def add(self, value):
        self._counter.add(value)

    def inc(self):
        self._counter.add(1)

    def get_name(self):
        return self._name


class OpenTelemetryGauge(TelemetryGauge):
    def __init__(self, meter: Meter, name: str, callback: Callable[[], Union[float, int]]):
        self._meter: Meter = meter
        self.name: str = name
        self._counter = meter.create_observable_up_down_counter(name, callbacks=[self._callback_observation], unit="1")
        self._callback: Callable[[], float] = callback

    def get_name(self):
        return self.name

    def _callback_observation(self, options: CallbackOptions):
        value: Union[int, float] = self._callback()
        observation = Observation(value)
        yield observation


class OpenTelemetryFactory(TelemetryFactory):
    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        return OpenTelemetryContext(trace.get_tracer(INSTRUMENTATION_NAME), name, trace_level)  # type: ignore

    def post_copy(self, context: TelemetryContext, trace_level: TelemetryTraceLevel):
        if isinstance(context, OpenTelemetryContext):
            post_copy(context, trace_level)
        else:
            raise RuntimeError(Messages.get_formatted("OpenTelemetryFactory.WrongParameterType", type(context)))

    def create_counter(self, name: str) -> TelemetryCounter:
        return OpenTelemetryCounter(get_meter(INSTRUMENTATION_NAME), name)

    def create_gauge(self, name: str, callback: Callable[[], Union[float, int]]):
        return OpenTelemetryGauge(get_meter(INSTRUMENTATION_NAME), name, callback)
