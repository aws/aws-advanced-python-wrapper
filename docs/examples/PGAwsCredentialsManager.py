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

import os

import boto3
import psycopg

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources
from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager


def custom_credentials_handler(host_info, props):
    return boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.environ.get('AWS_SESSION_TOKEN')
    )


if __name__ == "__main__":

    # Use a custom boto3 Session with a specific set of credentials
    AwsCredentialsManager.set_custom_handler(custom_credentials_handler)

    try:
        with AwsWrapperConnection.connect(
                psycopg.Connection.connect,
                host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
                dbname="postgres",
                user="john",
                plugins="iam",
                wrapper_dialect="aurora-pg",
                autocommit=True
        ) as awsconn, awsconn.cursor() as awscursor:
            awscursor.execute("CREATE TABLE IF NOT EXISTS bank_test (id int primary key, name varchar(40), account_balance int)")
            awscursor.execute("INSERT INTO bank_test VALUES (%s, %s, %s)", (0, "Jane Doe", 200))
            awscursor.execute("INSERT INTO bank_test VALUES (%s, %s, %s)", (1, "John Smith", 200))
            awscursor.execute("SELECT * FROM bank_test")

            res = awscursor.fetchall()
            for record in res:
                print(record)
            awscursor.execute("DROP TABLE bank_test")
    finally:
        # Clean up global resources created by wrapper
        release_resources()
