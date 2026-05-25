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

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.errors import (
    FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)

"""
SQLAlchemy ORM Failover Example with AWS Advanced Python Wrapper

This example demonstrates how to handle failover events when using SQLAlchemy ORM
with the AWS Advanced Python Wrapper.

"""


class Base(DeclarativeBase):
    pass


class BankAccount(Base):
    """Example model for demonstrating failover handling."""
    __tablename__ = 'bank_test'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    account_balance = Column(Integer)

    def __str__(self) -> str:
        return f"{self.name}: ${self.account_balance}"


def execute_query_with_failover_handling(query_func):
    """
    Execute a SQLAlchemy ORM query with failover error handling.

    Args:
        query_func: A callable that executes the desired query

    Returns:
        The result of the query function
    """
    try:
        return query_func()

    except DBAPIError as dbapi_err:
        e = dbapi_err.orig
        if isinstance(e, FailoverSuccessError):
            # Query execution failed and AWS Advanced Python Wrapper successfully failed over to an available instance.
            # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#failoversuccesserror

            # The connection has been re-established. Retry the query.
            print("Failover successful! Retrying query...")

            # Retry the query
            return query_func()

        elif isinstance(e, FailoverFailedError):
            # Failover failed. The application should open a new connection,
            # check the results of the failed transaction and re-run it if needed.
            # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#failoverfailederror
            print(f"Failover failed: {e}")
            print("Application should open a new connection and retry the transaction.")
            raise e

        elif isinstance(e, TransactionResolutionUnknownError):
            # The transaction state is unknown. The application should check the status
            # of the failed transaction and restart it if needed.
            # https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#transactionresolutionunknownerror
            print(f"Transaction resolution unknown: {e}")
            print("Application should check transaction status and retry if needed.")
            raise e


def create_table(engine):
    """Create the database table with failover handling."""
    def _create():
        Base.metadata.create_all(engine)
        print("Table created successfully")

    execute_query_with_failover_handling(_create)


def drop_table(engine):
    """Drop the database table with failover handling."""
    def _drop():
        Base.metadata.drop_all(engine)
        print("Table dropped successfully")

    execute_query_with_failover_handling(_drop)


def insert_records(session):
    """Insert records with failover handling."""
    print("\n--- Inserting Records ---")

    def _insert1():
        account = BankAccount(name='Jane Doe', account_balance=200)
        session.add(account)
        session.commit()  # Explicit commit required
        print(f"Inserted: {account}")
        return account

    def _insert2():
        account = BankAccount(name='John Smith', account_balance=200)
        session.add(account)
        session.commit()
        print(f"Inserted: {account}")
        return account

    execute_query_with_failover_handling(_insert1)
    execute_query_with_failover_handling(_insert2)


def query_records(session):
    """Query records with failover handling."""
    print("\n--- Querying Records ---")

    def _query():
        accounts = session.query(BankAccount).all()
        for account in accounts:
            print(f"  {account}")
        return accounts

    return execute_query_with_failover_handling(_query)


def update_record(session):
    """Update a record with failover handling."""
    print("\n--- Updating Record ---")

    def _update():
        account = session.query(BankAccount).filter(BankAccount.name == "Jane Doe").first()
        if account:
            account.account_balance = 300
            session.commit()
            print(f"Updated: {account}")
        return account

    return execute_query_with_failover_handling(_update)


def filter_records(session):
    """Filter records with failover handling."""
    print("\n--- Filtering Records ---")

    def _filter():
        accounts = session.query(BankAccount).filter(BankAccount.account_balance >= 250).all()
        print(f"Found {len(accounts)} accounts with balance >= $250:")
        for account in accounts:
            print(f"  {account}")
        return accounts

    return execute_query_with_failover_handling(_filter)


if __name__ == "__main__":
    try:
        print("SQLAlchemy ORM Failover Example with AWS Advanced Python Wrapper")
        print("=" * 60)

        engine = create_engine(
            'mysql+aws_wrapper_mysqlconnector://admin:pwd@'
            'database.cluster-xyz.us-east-1.rds.amazonaws.com:3306/mysql?'
            'wrapper_plugins=failover'
        )

        # Create table
        create_table(engine)

        Session = sessionmaker(bind=engine)
        with Session() as session:
            # Insert records
            insert_records(session)

            # Query records
            query_records(session)

            # Update a record
            update_record(session)

            # Query again to see the update
            query_records(session)

            # Filter records
            filter_records(session)

            session.close()

        # Cleanup
        print("\n--- Cleanup ---")
        drop_table(engine)

        print("\n" + "=" * 60)
        print("Example completed successfully!")

        engine.dispose()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up AWS Advanced Python Wrapper resources
        release_resources()
