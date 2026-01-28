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
Django ORM Failover Example with AWS Advanced Python Wrapper

This example demonstrates how to handle failover events when using Django ORM
with the AWS Advanced Python Wrapper.

"""

import django
from django.conf import settings
from django.db import connection, models

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.errors import (
    FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)

# Django settings configuration
DJANGO_SETTINGS = {
    'DATABASES': {
        'default': {
            'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
            'NAME': 'test_db',
            'USER': 'admin',
            'PASSWORD': 'password',
            'HOST': 'database.cluster-xyz.us-east-1.rds.amazonaws.com',
            'PORT': 3306,
            'OPTIONS': {
                'plugins': 'failover2',
                'connect_timeout': 10,
                'autocommit': True,
            },
        },
    },
}

# Configure Django settings
if not settings.configured:
    settings.configure(**DJANGO_SETTINGS)
django.setup()


class BankAccount(models.Model):
    """Example model for demonstrating failover handling."""
    name: str = models.CharField(max_length=100)  # type: ignore[assignment]
    account_balance: int = models.IntegerField()  # type: ignore[assignment]

    class Meta:
        app_label = 'myapp'
        db_table = 'bank_test'

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
        # Query execution failed and AWS Advanced Python Wrapper successfully failed over to an available instance.
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
                CREATE TABLE IF NOT EXISTS bank_test (
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
            cursor.execute("DROP TABLE IF EXISTS bank_test")
        print("Table dropped successfully")

    execute_query_with_failover_handling(_drop)


def insert_records():
    """Insert records with failover handling."""
    print("\n--- Inserting Records ---")

    def _insert1():
        account = BankAccount.objects.create(name="Jane Doe", account_balance=200)
        print(f"Inserted: {account}")
        return account

    def _insert2():
        account = BankAccount.objects.create(name="John Smith", account_balance=200)
        print(f"Inserted: {account}")
        return account

    execute_query_with_failover_handling(_insert1)
    execute_query_with_failover_handling(_insert2)


def query_records():
    """Query records with failover handling."""
    print("\n--- Querying Records ---")

    def _query():
        accounts = list(BankAccount.objects.all())
        for account in accounts:
            print(f"  {account}")
        return accounts

    return execute_query_with_failover_handling(_query)


def update_record():
    """Update a record with failover handling."""
    print("\n--- Updating Record ---")

    def _update():
        account = BankAccount.objects.filter(name="Jane Doe").first()
        if account:
            account.account_balance = 300
            account.save()
            print(f"Updated: {account}")
        return account

    return execute_query_with_failover_handling(_update)


def filter_records():
    """Filter records with failover handling."""
    print("\n--- Filtering Records ---")

    def _filter():
        accounts = list(BankAccount.objects.filter(account_balance__gte=250))
        print(f"Found {len(accounts)} accounts with balance >= $250:")
        for account in accounts:
            print(f"  {account}")
        return accounts

    return execute_query_with_failover_handling(_filter)


if __name__ == "__main__":
    try:
        print("Django ORM Failover Example with AWS Advanced Python Wrapper")
        print("=" * 60)

        # Create table
        create_table()

        # Insert records
        insert_records()

        # Query records
        query_records()

        # Update a record
        update_record()

        # Query again to see the update
        query_records()

        # Filter records
        filter_records()

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
        # Clean up AWS Advanced Python Wrapper resources
        release_resources()
