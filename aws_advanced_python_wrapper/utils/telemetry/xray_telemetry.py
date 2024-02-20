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

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from aws_xray_sdk.core import xray_recorder

if TYPE_CHECKING:
    from opentelemetry.util.types import AttributeValue
    from aws_xray_sdk.core.models.segment import Segment
    from aws_xray_sdk.core.models.subsegment import Subsegment

from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryConst, TelemetryContext, TelemetryCounter, TelemetryFactory,
    TelemetryTraceLevel)

logger = Logger(__name__)


class XRayTelemetryContext(TelemetryContext):
    def __init__(self, name: str, trace_level: TelemetryTraceLevel):
        self._name: str = name
        self._trace_entity: Segment | Subsegment

        if trace_level in [TelemetryTraceLevel.FORCE_TOP_LEVEL, TelemetryTraceLevel.TOP_LEVEL]:
            self._trace_entity = xray_recorder.begin_segment(self._name)
            self.is_segment = True
            logger.debug("XRayTelemetryContext.TraceID", self._name, self._trace_entity.trace_id)
        elif trace_level == TelemetryTraceLevel.NESTED:
            self._trace_entity = xray_recorder.begin_subsegment(self._name)  # type: ignore
            self.set_attribute(TelemetryConst.TRACE_NAME_ANNOTATION, self._name)
            self.is_segment = False
        elif trace_level == TelemetryTraceLevel.NO_TRACE:
            pass

    def set_success(self, success: bool):
        if self._trace_entity is not None:
            self._trace_entity.error = not success

    def set_attribute(self, key: str, value: AttributeValue):
        if self._trace_entity is not None:
            self._trace_entity.put_annotation(key, value)

    def set_exception(self, exception: Exception):
        if self._trace_entity is not None and exception is not None:
            self._trace_entity.put_annotation("exception_type", exception.__class__.__name__)
            self._trace_entity.put_annotation("exception_message", str(exception))

    def get_name(self):
        return self._name

    def close_context(self, end_time=None):
        if self._trace_entity is not None:
            if self.is_segment:
                xray_recorder.end_segment(end_time)
            else:
                xray_recorder.end_subsegment(end_time)


def post_copy(context: XRayTelemetryContext, trace_level: TelemetryTraceLevel):
    if trace_level == TelemetryTraceLevel.NO_TRACE:
        return

    if trace_level in [TelemetryTraceLevel.FORCE_TOP_LEVEL, TelemetryTraceLevel.TOP_LEVEL]:
        with ThreadPoolExecutor() as executor:
            future = executor.submit(_clone_and_close_context, context, trace_level)
            future.result()
    else:
        _clone_and_close_context(context, trace_level)


def _clone_and_close_context(context: XRayTelemetryContext, trace_level: TelemetryTraceLevel) -> XRayTelemetryContext:
    clone = XRayTelemetryContext(TelemetryConst.COPY_TRACE_NAME_PREFIX + context.get_name(), trace_level)

    clone._trace_entity.start_time = context._trace_entity.start_time

    for key in context._trace_entity.annotations.items():
        value = context._trace_entity.annotations[key]
        if key != TelemetryConst.TRACE_NAME_ANNOTATION and value is not None:
            clone.set_attribute(key, value)

    if context.is_segment and context._trace_entity.error:
        clone._trace_entity.add_error_flag()

    clone.set_attribute(TelemetryConst.SOURCE_TRACE_ANNOTATION, str(context._trace_entity.trace_id))

    if context._trace_entity.parent_id is not None:
        if trace_level == TelemetryTraceLevel.NESTED:
            clone._trace_entity.parent_id = context._trace_entity.parent_id

    clone.close_context(context._trace_entity.end_time)

    return clone


class XRayTelemetryFactory(TelemetryFactory):
    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        return XRayTelemetryContext(name, trace_level)

    def post_copy(self, context: TelemetryContext, trace_level: TelemetryTraceLevel):
        if isinstance(context, XRayTelemetryContext):
            post_copy(context, trace_level)
        else:
            raise RuntimeError(Messages.get_formatted("XRayTelemetryFactory.WrongParameterType", type(context)))

    def create_counter(self, name: str) -> TelemetryCounter:
        raise RuntimeError(Messages.get_formatted("XRayTelemetryFactory.MetricsNotSupported"))

    def create_gauge(self, name: str, callback):
        raise RuntimeError(Messages.get_formatted("XRayTelemetryFactory.MetricsNotSupported"))
