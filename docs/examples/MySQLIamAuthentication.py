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

import mysql.connector

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources

if __name__ == "__main__":
    with AwsWrapperConnection.connect(
            mysql.connector.Connect,
            host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
            database="mysql",
            user="admin",
            plugins="iam",
            wrapper_dialect="aurora-mysql",
            autocommit=True
    ) as awsconn, awsconn.cursor() as awscursor:
        awscursor.execute("CREATE TABLE IF NOT EXISTS bank_test (id int primary key, name varchar(40), account_balance int)")
        awscursor.execute("INSERT INTO bank_test VALUES (%s, %s, %s)", (0, "Jane Doe", 200))
        awscursor.execute("INSERT INTO bank_test VALUES (%s, %s, %s)", (1, "John Smith", 200))
        awscursor.execute("SELECT * FROM bank_test")

        for record in awscursor:
            print(record)
        awscursor.execute("DROP TABLE bank_test")

    # Clean up any remaining resources created by the plugins.
    release_resources()
