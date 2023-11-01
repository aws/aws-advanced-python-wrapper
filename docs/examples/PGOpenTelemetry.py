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

import logging

import psycopg
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import \
    OTLPSpanExporter
from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import \
    BatchSpanProcessor  # ConsoleSpanExporter,

from aws_advanced_python_wrapper import AwsWrapperConnection

SQL_DBLIST = "select datname from pg_database;"

if __name__ == "__main__":
    print("-- running application")
    logging.basicConfig(level=logging.DEBUG)

    provider = TracerProvider(id_generator=AwsXRayIdGenerator())
    # processor = BatchSpanProcessor(ConsoleSpanExporter())
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("python_otlp_telemetry_app") as segment:
        with AwsWrapperConnection.connect(
                psycopg.Connection.connect,
                host="db-identifier-postgres.XYZ.us-east-2.rds.amazonaws.com",
                dbname="test_db",
                user="user",
                password="password",
                plugins="failover,host_monitoring",
                wrapper_dialect="aurora-pg",
                autocommit=True,
                enable_telemetry=True,
                telemetry_submit_toplevel=False,
                telemetry_traces_backend="OTLP",
                telemetry_metrics_backend="OTLP",
                telemetry_failover_additional_top_trace=True
        ) as awsconn:
            awscursor = awsconn.cursor()
            awscursor.execute(SQL_DBLIST)
            res = awscursor.fetchall()
            for record in res:
                print(record)

    print("-- end of application")
