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

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

import psycopg  # type: ignore

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources
from aws_advanced_python_wrapper.errors import (
    FailoverFailedError, FailoverSuccessError,
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
        # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#failoversuccesserror

        # The old cursor is no longer reusable and the application needs to reconfigure sessions states.
        configure_initial_session_states(conn)
        # Retry query
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor

    except FailoverFailedError as e:
        # User application should open a new connection, check the results of the failed transaction and re-run it if needed. See:
        # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#failoverfailederror
        raise e

    except TransactionResolutionUnknownError as e:
        # User application should check the status of the failed transaction and restart it if needed. See:
        # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#transactionresolutionunknownerror
        raise e


if __name__ == "__main__":
    params: Any = {
        "host": "rds-proxy-name.proxy-xyz.us-east-1.rds.amazonaws.com",
        "dbname": "postgres",
        "user": "john",
        "password": "pwd",
        "plugins": "srw,failover",
        "srw_write_endpoint": "rds-proxy-name.proxy-xyz.us-east-1.rds.amazonaws.com",
        "srw_read_endpoint": "rds-proxy-name-read-only.endpoint.proxy-xyz.us-east-1.rds.amazonaws.com",
        "srw_verify_new_connections": "True",
        "wrapper_dialect": "aurora-pg",
        "autocommit": True,
    }

    """ Setup step: open connection and create tables """
    with AwsWrapperConnection.connect(psycopg.Connection.connect, **params) as conn:
        configure_initial_session_states(conn)
        execute_queries_with_failover_handling(
            conn,
            "CREATE TABLE IF NOT EXISTS bank_test (id int primary key, name varchar(40), account_balance int)",
        )
        execute_queries_with_failover_handling(
            conn, "INSERT INTO bank_test VALUES (%s, %s, %s)", (0, "Jane Doe", 200)
        )
        execute_queries_with_failover_handling(
            conn, "INSERT INTO bank_test VALUES (%s, %s, %s)", (1, "John Smith", 200)
        )

    """ Example step: open connection and perform transaction """
    try:
        with AwsWrapperConnection.connect(psycopg.Connection.connect, **params) as conn, conn.cursor() as cursor:
            configure_initial_session_states(conn)

            execute_queries_with_failover_handling(
                conn,
                "UPDATE bank_test SET account_balance=account_balance - 100 WHERE name=%s",
                ("Jane Doe",),
            )
            execute_queries_with_failover_handling(
                conn,
                "UPDATE bank_test SET account_balance=account_balance + 100 WHERE name=%s",
                ("John Smith",),
            )

            # Internally switch to a reader connection
            conn.read_only = True
            for i in range(2):
                cursor = execute_queries_with_failover_handling(
                    conn, "SELECT * FROM bank_test WHERE id = %s", (i,)
                )
                results = cursor.fetchall()
                for record in results:
                    print(record)

    finally:
        with AwsWrapperConnection.connect(psycopg.Connection.connect, **params) as conn:
            execute_queries_with_failover_handling(conn, "DROP TABLE bank_test")
        # Clean up global resources created by wrapper
        release_resources()
