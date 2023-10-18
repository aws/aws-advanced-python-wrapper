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
from aws_wrapper.connection_provider import (
    ConnectionProviderManager, SqlAlchemyPooledConnectionProvider)

from aws_wrapper import AwsWrapperConnection

if __name__ == "__main__":
    params = {
        # In general, you should not use instance URLs to connect. However, we will use one here to simplify this
        # example, because internal connection pools are only opened when connecting to an instance URL. Normally the
        # internal connection pool would be opened when read_only is set instead of when you are initially connecting.
        "host": "database-instance.xyz.us-east-1.rds.amazonaws.com",
        "dbname": "postgres",
        "user": "john",
        "plugins": "read_write_splitting,failover,host_monitoring",
        "autocommit": True
    }

    correct_password = "correct_password"
    incorrect_password = "incorrect_password"

    provider = SqlAlchemyPooledConnectionProvider()
    ConnectionProviderManager.set_connection_provider(provider)

    # Create an internal connection pool with the correct password
    conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params, password=correct_password)
    # Finished with connection. The connection is not actually closed here, instead it will be returned to the pool but
    # will remain open.
    conn.close()

    # Even though we use an incorrect password, the original connection 'conn' will be returned by the pool, and we can
    # still use it.
    with AwsWrapperConnection.connect(
            psycopg.Connection.connect, **params, password=incorrect_password) as incorrect_password_conn:
        incorrect_password_conn.cursor().execute("SELECT 1")

    # Closes all pools and removes all cached pool connections
    ConnectionProviderManager.release_resources()

    try:
        # Correctly throws an exception - creates a fresh connection pool which will check the password because there
        # are no longer any cached pool connections.
        with AwsWrapperConnection.connect(
                psycopg.Connection.connect, **params, password=incorrect_password) as incorrect_password_conn:
            # Will not reach - exception will be thrown
            pass
    except Exception:
        print("Failed to connect - password was incorrect")
