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

import psycopg

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources

if __name__ == "__main__":
    try:
        with AwsWrapperConnection.connect(
            psycopg.Connection.connect,
            host="limitless-cluster.limitless-xyz.us-east-1.rds.amazonaws.com",
            dbname="postgres_limitless",
            user="user",
            password="password",
            plugins="limitless",
            autocommit=True
        ) as awsconn, awsconn.cursor() as awscursor:
            awscursor.execute("SELECT * FROM pg_catalog.aurora_db_instance_identifier()")

            res = awscursor.fetchone()
            print(res)
    finally:
        # Clean up global resources created by wrapper
        release_resources()
