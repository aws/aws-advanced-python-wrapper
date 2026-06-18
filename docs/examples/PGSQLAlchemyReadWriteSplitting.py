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

"""SQLAlchemy + AWS Advanced Python Wrapper: read/write splitting on Aurora PostgreSQL.

Demonstrates routing read-only transactions to Aurora replicas via the
readWriteSplitting plugin, by flipping SQLAlchemy's read_only execution option.
"""

from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.psycopg import connect

CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "postgresql+psycopg://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} dbname={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-pg",
            plugins="readWriteSplitting",
        ),
    )


def instance_id(conn) -> str:
    return conn.execute(
        text("SELECT pg_catalog.aurora_db_instance_identifier()")
    ).scalar_one()


def main() -> None:
    engine = build_engine()
    try:
        # Writer by default.
        with engine.connect() as conn:
            print(f"writer: {instance_id(conn)}")
            conn.commit()

        # Read-only transaction -> router switches to a reader.
        with engine.connect().execution_options(postgresql_readonly=True) as conn:
            print(f"reader: {instance_id(conn)}")
            conn.commit()

        # Back to writer on the next non-read-only connection.
        with engine.connect() as conn:
            print(f"writer again: {instance_id(conn)}")
            conn.commit()
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
