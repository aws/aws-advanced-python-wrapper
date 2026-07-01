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

"""SQLAlchemy + AWS Advanced Python Wrapper: failover on Aurora PostgreSQL.

This example shows how to build an SQLAlchemy Engine that routes through the
AWS Advanced Python Wrapper's failover plugin, then perform a workload that
survives an Aurora failover event by catching sqlalchemy.exc.OperationalError
and retrying the unit of work.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.psycopg import connect

CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "postgresql+aws_wrapper_psycopg://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} dbname={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-pg",
            plugins="failover,host_monitoring_v2",
            cluster_id="pg_sqlalchemy",
        ),
    )


def run_workload(engine, iterations: int = 20) -> None:
    for i in range(iterations):
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT pg_catalog.aurora_db_instance_identifier()")
                ).one()
                print(f"iter {i}: connected to instance {row[0]}")
        except OperationalError as exc:
            # FailoverSuccessError is reclassified as OperationalError by the
            # wrapper; SA wraps it here. The correct response is to retry the
            # unit of work against the new writer.
            print(f"iter {i}: operational error ({type(exc.orig).__name__}); retrying")


def main() -> None:
    engine = build_engine()
    try:
        run_workload(engine)
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
