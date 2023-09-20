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

from aws_wrapper import AwsWrapperConnection

if __name__ == "__main__":
    with AwsWrapperConnection.connect(
            psycopg.Connection.connect,
            host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
            dbname="postgres",
            user="john",
            plugins="iam",
            wrapper_dialect="aurora-pg",
            autocommit=True
    ) as awsconn, awsconn.cursor() as awscursor:
        awscursor.execute("CREATE TABLE IF NOT EXISTS bank_test (name varchar(40), account_balance int)")
        awscursor.execute("INSERT INTO bank_test VALUES ('Jane Doe', 200), ('john_smith', 200)")
        awscursor.execute("SELECT * FROM bank_test")
        res = awscursor.fetchall()
        for record in res:
            print(record)
        awscursor.execute("DROP TABLE bank_test")
