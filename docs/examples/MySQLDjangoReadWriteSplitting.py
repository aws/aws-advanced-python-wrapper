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

"""
Django ORM Read/Write Splitting Example with AWS Advanced Python Driver

This example demonstrates how to use the AWS Advanced Python Driver with Django ORM
to leverage Aurora features like failover handling and read/write splitting with
internal connection pooling.
"""

from typing import TYPE_CHECKING, Any, Dict

import django
from django.conf import settings
from django.db import connection, models

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.errors import (
    FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo


def configure_pool(host_info: "HostInfo", props: Dict[str, Any]) -> Dict[str, Any]:
    """Configure connection pool settings for each host."""
    return {"pool_size": 5}


def get_pool_key(host_info: "HostInfo", props: Dict[str, Any]) -> str:
    """
    Generate a unique key for connection pooling.
    Include the URL, user, and database in the connection pool key so that a new
    connection pool will be opened for each different instance-user-database combination.
    """
    url = host_info.url
    user = props.get("user", "")
    db = props.get("database", "")
    return f"{url}{user}{db}"


# Database connection configuration
DB_CONFIG = {
    'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
    'NAME': 'test_db',
    'USER': 'admin',
    'PASSWORD': 'password',
    'HOST': 'database.cluster-xyz.us-east-1.rds.amazonaws.com',
    'PORT': 3306,
}

# Django settings configuration
DJANGO_SETTINGS = {
    'DATABASES': {
        'default': {  # Writer connection
            **DB_CONFIG,
            'OPTIONS': {
                'plugins': 'read_write_splitting,failover',
                'connect_timeout': 10,
                'autocommit': True,
            },
        },
        'read': {  # Reader connection
            **DB_CONFIG,
            'OPTIONS': {
                'plugins': 'read_write_splitting,failover',
                'connect_timeout': 10,
                'autocommit': True,
                'read_only': True,  # This connection will use reader instances
                'reader_host_selector_strategy': 'least_connections',
            },
        },
    },
    'DATABASE_ROUTERS': ['__main__.ReadWriteRouter'],
}


# Database Router for Read/Write Splitting
class ReadWriteRouter:
    """
    A router to control database operations for read/write splitting.
    """

    def db_for_read(self, model, **hints):
        """
        Direct all read operations to the 'read' database.
        """
        return 'read'

    def db_for_write(self, model, **hints):
        """
        Direct all write operations to the 'default' database.
        """
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations between objects in the same database.
        """
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Allow migrations on all databases.
        """
        return True


# Configure Django settings
if not settings.configured:
    settings.configure(**DJANGO_SETTINGS)
django.setup()


class BankAccount(models.Model):
    """Example model for demonstrating read/write splitting."""
    name: str = models.CharField(max_length=100)  # type: ignore[assignment]
    account_balance: int = models.IntegerField()  # type: ignore[assignment]

    class Meta:
        app_label = 'myapp'
        db_table = 'bank_accounts'

    def __str__(self) -> str:
        return f"{self.name}: ${self.account_balance}"


def execute_query_with_failover_handling(query_func):
    """
    Execute a Django ORM query with failover error handling.

    Args:
        query_func: A callable that executes the desired query

    Returns:
        The result of the query function
    """
    try:
        return query_func()

    except FailoverSuccessError:
        # Query execution failed and AWS Advanced Python Driver successfully failed over to an available instance.
        # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#failoversuccesserror

        # The connection has been re-established. Retry the query.
        print("Failover successful! Retrying query...")

        # Retry the query
        return query_func()

    except FailoverFailedError as e:
        # Failover failed. The application should open a new connection,
        # check the results of the failed transaction and re-run it if needed.
        # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#failoverfailederror
        print(f"Failover failed: {e}")
        print("Application should open a new connection and retry the transaction.")
        raise e

    except TransactionResolutionUnknownError as e:
        # The transaction state is unknown. The application should check the status
        # of the failed transaction and restart it if needed.
        # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#transactionresolutionunknownerror
        print(f"Transaction resolution unknown: {e}")
        print("Application should check transaction status and retry if needed.")
        raise e


def create_table():
    """Create the database table with failover handling."""
    def _create():
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bank_accounts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    account_balance INT
                )
            """)
        print("Table created successfully")

    execute_query_with_failover_handling(_create)


def drop_table():
    """Drop the database table with failover handling."""
    def _drop():
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS bank_accounts")
        print("Table dropped successfully")

    execute_query_with_failover_handling(_drop)


def demonstrate_write_operations():
    """Demonstrate write operations with failover handling (uses 'default' database - writer instance)."""
    print("\n--- Write Operations (Writer Instance) ---")

    # Create new records with failover handling
    def _create1():
        account = BankAccount.objects.create(name="Jane Doe", account_balance=1000)
        print(f"Created: {account}")
        return account

    def _create2():
        account = BankAccount.objects.create(name="John Smith", account_balance=1500)
        print(f"Created: {account}")
        return account

    account1 = execute_query_with_failover_handling(_create1)
    execute_query_with_failover_handling(_create2)

    # Update a record with failover handling
    def _update():
        account1.account_balance = 1200
        account1.save()
        print(f"Updated: {account1}")
        return account1

    execute_query_with_failover_handling(_update)


def demonstrate_read_operations():
    """Demonstrate read operations with failover handling (uses 'read' database - reader instance)."""
    print("\n--- Read Operations (Reader Instance) ---")

    # Query all records with failover handling
    def _query_all():
        accounts = list(BankAccount.objects.all())
        print(f"Total accounts: {len(accounts)}")
        for account in accounts:
            print(f"  {account}")
        return accounts

    execute_query_with_failover_handling(_query_all)

    # Filter records with failover handling
    def _query_filtered():
        high_balance = list(BankAccount.objects.filter(account_balance__gte=1200))
        print("\nAccounts with balance >= $1200:")
        for account in high_balance:
            print(f"  {account}")
        return high_balance

    execute_query_with_failover_handling(_query_filtered)


def demonstrate_explicit_database_selection():
    """Demonstrate explicitly selecting which database to use with failover handling."""
    print("\n--- Explicit Database Selection ---")

    # Force read from writer database with failover handling
    def _read_from_writer():
        print("Reading from writer (default) database:")
        accounts = list(BankAccount.objects.using('default').all())
        for account in accounts:
            print(f"  {account}")
        return accounts

    execute_query_with_failover_handling(_read_from_writer)

    # Force read from reader database with failover handling
    def _read_from_reader():
        print("\nReading from reader database:")
        accounts = list(BankAccount.objects.using('read').all())
        for account in accounts:
            print(f"  {account}")
        return accounts

    execute_query_with_failover_handling(_read_from_reader)


def demonstrate_raw_sql():
    """Demonstrate raw SQL queries with Django and failover handling."""
    print("\n--- Raw SQL Queries ---")

    # Execute raw SQL query with failover handling
    def _raw_query():
        accounts = list(BankAccount.objects.raw('SELECT * FROM bank_accounts WHERE account_balance > %s', [1000]))
        print("Accounts with balance > $1000:")
        for account in accounts:
            print(f"  {account}")
        return accounts

    execute_query_with_failover_handling(_raw_query)


if __name__ == "__main__":
    # Configure read/write splitting to use internal connection pools.
    provider = SqlAlchemyPooledConnectionProvider(configure_pool, get_pool_key)
    ConnectionProviderManager.set_connection_provider(provider)

    try:
        print("Django ORM Read/Write Splitting Example with AWS Advanced Python Driver")
        print("=" * 60)

        # Create table
        create_table()

        # Demonstrate write operations (uses writer instance)
        demonstrate_write_operations()

        # Demonstrate read operations (uses reader instance)
        demonstrate_read_operations()

        # Demonstrate explicit database selection
        demonstrate_explicit_database_selection()

        # Demonstrate raw SQL
        demonstrate_raw_sql()

        # Cleanup
        print("\n--- Cleanup ---")
        drop_table()

        print("\n" + "=" * 60)
        print("Example completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up connection pools
        ConnectionProviderManager.release_resources()

        # Clean up AWS Advanced Python Driver resources
        release_resources()
