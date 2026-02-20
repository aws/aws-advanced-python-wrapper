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
from aws_xray_sdk import global_sdk_config
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.sampling.local.sampler import LocalSampler

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources

SQL_DBLIST = "select datname from pg_database;"

if __name__ == "__main__":
    print("-- running application")
    logging.basicConfig(level=logging.DEBUG)

    xray_recorder.configure(sampler=LocalSampler({"version": 1, "default": {"fixed_target": 1, "rate": 1.0}, "rules": []}))
    global_sdk_config.set_sdk_enabled(True)

    with xray_recorder.in_segment("python_xray_telemetry_app") as segment:
        try:
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
                    telemetry_traces_backend="XRAY",
                    telemetry_metrics_backend="NONE"
            ) as awsconn:
                awscursor = awsconn.cursor()
                awscursor.execute(SQL_DBLIST)
                res = awscursor.fetchall()
                for record in res:
                    print(record)
        finally:
            # Clean up global resources created by wrapper
            release_resources()

    print("-- end of application")
