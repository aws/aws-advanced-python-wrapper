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

from typing import TYPE_CHECKING, Dict, Optional, Tuple, Union

import psycopg

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.errors import (FailoverFailedError, FailoverSuccessError,
                                TransactionResolutionUnknownError)


def configure_initial_session_states(conn: Connection):
    awscursor = conn.cursor()
    awscursor.execute("SET TIME ZONE 'UTC'")


def execute_queries_with_failover_handling(conn: Connection, sql: str, params: Optional[Union[Dict, Tuple]] = None):
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return cursor

    except FailoverSuccessError:
        # Query execution failed and AWS Advanced Python Driver successfully failed over to an available instance.
        # https://github.com/awslabs/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#FailoverFailedError---successful-failover

        # The old cursor is no longer reusable and the application needs to reconfigure sessions states.
        configure_initial_session_states(conn)
        # Retry query
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor

    except FailoverFailedError as e:
        # User application should open a new connection, check the results of the failed transaction and re-run it if needed. See:
        # https://github.com/awslabs/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#FailoverFailedError---unable-to-establish-sql-connection
        raise e

    except TransactionResolutionUnknownError as e:
        # User application should check the status of the failed transaction and restart it if needed. See:
        # https://github.com/awslabs/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#TransactionResolutionUnknownError---transaction-resolution-unknown
        raise e


if __name__ == "__main__":
    with AwsWrapperConnection.connect(
            psycopg.Connection.connect,
            host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
            dbname="postgres",
            user="john",
            password="pwd",
            plugins="failover",
            wrapper_dialect="aurora-pg",
            autocommit=True
    ) as awsconn:
        configure_initial_session_states(awsconn)
        execute_queries_with_failover_handling(
            awsconn, "CREATE TABLE IF NOT EXISTS bank_test (id int primary key, name varchar(40), account_balance int)")
        execute_queries_with_failover_handling(
            awsconn, "INSERT INTO bank_test VALUES (%s, %s, %s)", (0, "Jane Doe", 200))
        execute_queries_with_failover_handling(
            awsconn, "INSERT INTO bank_test VALUES (%s, %s, %s)", (1, "John Smith", 200))

        cursor = execute_queries_with_failover_handling(awsconn, "SELECT * FROM bank_test")
        res = cursor.fetchall()
        for record in res:
            print(record)

        execute_queries_with_failover_handling(awsconn, "DROP TABLE bank_test")
