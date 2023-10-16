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
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection

import mysql.connector

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.connection_provider import (
    ConnectionProviderManager, SqlAlchemyPooledConnectionProvider)
from aws_wrapper.errors import (FailoverFailedError, FailoverSuccessError,
                                TransactionResolutionUnknownError)


def configure_pool(host_info: HostInfo, props: Dict[str, Any]) -> Dict[str, Any]:
    return {"pool_size": 5}


def get_pool_key(host_info: HostInfo, props: Dict[str, Any]) -> str:
    # Include the URL, user, and database in the connection pool key so that a new
    # connection pool will be opened for each different instance-user-database combination.
    url = host_info.url
    user = props["username"]
    db = props["dbname"]
    return f"{url}{user}{db}"


def configure_initial_session_states(conn: Connection):
    awscursor = conn.cursor()
    awscursor.execute("SET time_zone = 'UTC'")


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
    params = {
        "host": "database.cluster-xyz.us-east-1.rds.amazonaws.com",
        "database": "mysql",
        "user": "admin",
        "password": "pwd",
        "plugins": "read_write_splitting,failover",
        "wrapper_dialect": "aurora-mysql",
        "autocommit": True
    }

    """
    Optional: configure read/write splitting to use internal connection pools.
    The arguments passed to SqlAlchemyConnectionProvider are optional, see  UsingTheReadWriteSplittingPlugin.md
    for more info.
    """
    provider = SqlAlchemyPooledConnectionProvider(configure_pool, get_pool_key)
    ConnectionProviderManager.set_connection_provider(provider)

    """ Setup step: open connection and create tables """
    with AwsWrapperConnection.connect(mysql.connector.Connect, **params) as conn:
        configure_initial_session_states(conn)
        execute_queries_with_failover_handling(
            conn, "CREATE TABLE IF NOT EXISTS bank_test (id int primary key, name varchar(40), account_balance int)")
        execute_queries_with_failover_handling(
            conn, "INSERT INTO bank_test VALUES (%s, %s, %s)", (0, "Jane Doe", 200))
        execute_queries_with_failover_handling(
            conn, "INSERT INTO bank_test VALUES (%s, %s, %s)", (1, "John Smith", 200))

    """ Example step: open connection and perform transaction """
    try:
        with AwsWrapperConnection.connect(mysql.connector.Connect, **params) as conn, conn.cursor() as cursor:
            configure_initial_session_states(conn)

            execute_queries_with_failover_handling(
                conn, "UPDATE bank_test SET account_balance=account_balance - 100 WHERE name=%s", ("Jane Doe",))
            execute_queries_with_failover_handling(
                conn, "UPDATE bank_test SET account_balance=account_balance + 100 WHERE name=%s", ("John Smith",))

            # Internally switch to a reader connection
            conn.read_only = True
            for i in range(2):
                cursor = execute_queries_with_failover_handling(conn, "SELECT * FROM bank_test WHERE id = %s", (i,))
                for record in cursor:
                    print(record)

    finally:
        with AwsWrapperConnection.connect(mysql.connector.Connect, **params) as conn:
            execute_queries_with_failover_handling(conn, "DROP TABLE bank_test")

        """ If connection pools were enabled, close them here """
        ConnectionProviderManager.release_resources()
