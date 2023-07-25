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

from opentelemetry import trace, context
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
from opentelemetry.sdk.trace import Span


from enum import Enum

from abc import ABC, abstractmethod

INSTRUMENTATION_NAME = "aws-advanced-python-wrapper"
EXCEPTION_TYPE_ANNOTATION = "exceptionType"
EXCEPTION_MESSAGE_ANNOTATION = "exceptionMessage"


class TelemetryTraceLevel(Enum):
    FORCE_TOP_LEVEL = 1  # always top level despite settings
    TOP_LEVEL = 2  # if allowed by settings
    NESTED = 3
    NO_TRACE = 4


class TelemetryContext(ABC):
    @abstractmethod
    def set_success(self, success: bool):
        ...

    @abstractmethod
    def set_attribute(self, key: str, value: str):
        ...

    @abstractmethod
    def set_exception(self, exception: Exception):
        ...

    @abstractmethod
    def close_context(self):
        ...

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close_context()


class TelemetryFactory(ABC):
    # TODO: get proper factory based on config
    @staticmethod
    def get_telemetry_factory():
        return OpenTelemetryFactory()

    @abstractmethod
    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        ...


class OpenTelemetryFactory(TelemetryFactory):
    def __init__(self, ):
        provider = TracerProvider(id_generator=AwsXRayIdGenerator())  # TODO: make this customizable               
        processor = BatchSpanProcessor(OTLPSpanExporter())  # TODO: make this customizable
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        self._tracer = trace.get_tracer(INSTRUMENTATION_NAME)
        # TODO: This will get the root context at the start of the wrapper and use it for top-level spans 
        # However, this will still be a child of the application context if it exists
        # And this should be handled. There are no API methods for setting a span without a parent
        self._root_context = context.get_current()

    def open_telemetry_context(self, name: str, trace_level: TelemetryTraceLevel) -> TelemetryContext:
        if trace_level == TelemetryTraceLevel.TOP_LEVEL or trace_level == TelemetryTraceLevel.FORCE_TOP_LEVEL:
            print("TEST - TOP LEVEL") 
            span = self._tracer.start_span(name, context=self._root_context)
            return OpenTelemetryContext(span)
        elif trace_level == TelemetryTraceLevel.NESTED:
            print("TEST - current context - " + str(context.get_current()))
            span = self._tracer.start_span(name, context=context.get_current())
            return OpenTelemetryContext(span)
        else:
            return NullTelemetryContext()


class NullTelemetryContext(TelemetryContext):
    def close_context(self):
        pass

    def set_success(self, success: bool):
        pass

    def set_attribute(self, key: str, value: str):
        pass

    def set_exception(self, exception: Exception):
        pass

    def get_name(self) -> str:
        pass


class OpenTelemetryContext(TelemetryContext):
    def __init__(self, span: Span):
        self._span = span
        self._open_telem_context_manager = trace.use_span(span, end_on_exit=True)
        self._open_telem_context_manager.__enter__()

    def close_context(self):
        self._open_telem_context_manager.__exit__(None, None, None)

    def set_success(self, success: bool):
        self._span.set_status(status=(StatusCode.OK if success else StatusCode.ERROR))

    def set_attribute(self, key: str, value: str):
        self._span.set_attribute(key, value)

    def set_exception(self, exception: Exception):
        self._span.set_attribute(EXCEPTION_TYPE_ANNOTATION, type(exception).__name__)
        self._span.set_attribute(EXCEPTION_MESSAGE_ANNOTATION, str(exception))
        self._span.record_exception(exception=exception)


"""
class Telemetry:
    def __init__(self):
        provider = TracerProvider(id_generator=AwsXRayIdGenerator())  # TODO: make this customizable               
        processor = BatchSpanProcessor(OTLPSpanExporter())  # TODO: make this customizable
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        self._tracer = trace.get_tracer(__name__)

    def start_span(self):
        with self._tracer.start_as_current_span("request_thing"):
            time.sleep(1)  # TODO
"""

