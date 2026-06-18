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

"""SQLAlchemy + AWS Advanced Python Wrapper: read/write splitting on Aurora MySQL."""

from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import connect

CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "mysql"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} database={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-mysql",
            plugins="readWriteSplitting",
            use_pure=True,
        ),
    )


def instance_id(conn) -> str:
    return conn.execute(text("SELECT @@aurora_server_id")).scalar_one()


def main() -> None:
    engine = build_engine()
    try:
        with engine.connect() as conn:
            print(f"writer: {instance_id(conn)}")
            conn.commit()

        with engine.connect().execution_options(mysql_readonly=True) as conn:
            print(f"reader: {instance_id(conn)}")
            conn.commit()

        with engine.connect() as conn:
            print(f"writer again: {instance_id(conn)}")
            conn.commit()
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
