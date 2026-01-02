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

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.wrapper import Wrapper

if __name__ == "__main__":
    provider = SqlAlchemyPooledConnectionProvider()
    ConnectionProviderManager.set_connection_provider(provider)

    with AwsWrapperConnection.connect(
        mysql.connector.Connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        database="mysql",
        user="user",
        password="password",
        plugins="read_write_splitting,fastest_response_strategy",
        reader_host_selector_strategy="fastest_response",
        autocommit=True
    ) as conn:
        # Set up
        with conn.cursor() as setup_cursor:
            setup_cursor.execute("CREATE TABLE IF NOT EXISTS bank_test (id int primary key, name varchar(40), account_balance int)")
            setup_cursor.execute("INSERT INTO bank_test VALUES (%s, %s, %s)", (0, "Jane Doe", 200))

        conn.read_only = True
        with conn.cursor() as cursor_1:
            cursor_1.execute("SELECT * FROM bank_test")
            results = cursor_1.fetchall()
            for record in results:
                print(record)

        # Switch to writer host
        conn.read_only = False

        # Use cached host when switching back to a reader
        conn.read_only = True
        with conn.cursor() as cursor_2:
            cursor_2.execute("SELECT * FROM bank_test")
            results = cursor_2.fetchall()
            for record in results:
                print(record)

        # Tear down
        conn.read_only = False
        with conn.cursor() as teardown_cursor:
            teardown_cursor.execute("DROP TABLE bank_test")

    # Clean up any remaining resources created by the plugins.
    Wrapper.release_resources()
    # Closes all pools and removes all cached pool connections
    ConnectionProviderManager.release_resources()
