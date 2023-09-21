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

from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection

from aws_wrapper import AwsWrapperConnection

if __name__ == "__main__":
    with AwsWrapperConnection.connect(
            psycopg.Connection.connect,
            host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
            dbname="postgres",
            secrets_manager_secret_id="arn:aws:secretsmanager:<Region>:<AccountId>:secret:Secre78tName-6RandomCharacters",
            secrets_manager_region="us-east-2",
            plugins="aws_secrets_manager"
    ) as awsconn:
        cursor = awsconn.cursor()
        cursor.execute("SELECT aurora_db_instance_identifier()")
        for record in cursor.fetchone():
            print(record)
