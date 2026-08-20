# Getting Started

## Minimum Requirements

Before using the AWS Advanced Python Wrapper, you must install:

- Python 3.10 - 3.14 (inclusive).
- The AWS Advanced Python Wrapper.
- Your choice of underlying Python driver. 
  - To use the wrapper with Aurora with PostgreSQL compatibility, install [Psycopg](https://github.com/psycopg/psycopg).
  - To use the wrapper with Aurora with MySQL compatibility, install [MySQL Connector/Python](https://github.com/mysql/mysql-connector-python).
> [!NOTE]\
> The wrapper has been verified on Psycopg 3.1.12+ and MySQL Connector/Python 8.1.0+. Compatibility with prior versions have not been tested.

## Obtaining the AWS Advanced Python Wrapper

You can install the AWS Advanced Python Wrapper and the underlying Python drivers via [pip](https://pip.pypa.io/en/stable/).
The order of installation does not matter.

To use the AWS Advanced Python Wrapper with Psycopg for Aurora PostgreSQL, run:

```shell
pip install aws-advanced-python-wrapper
pip install psycopg
```

To use the AWS Advanced Python Wrapper with MySQL Connector/Python for Aurora MySQL, run:
```shell
pip install aws-advanced-python-wrapper
pip install mysql-connector-python
```

To use the wrapper's asyncio API (`aws_advanced_python_wrapper.aio`), install the async driver for your engine. PostgreSQL async uses the same Psycopg package as sync; MySQL async is driven by [aiomysql](https://github.com/aio-libs/aiomysql):

```shell
pip install aws-advanced-python-wrapper
pip install psycopg    # PostgreSQL (sync and async)
pip install aiomysql   # MySQL async
```

Some features pull in additional packages when used with the asyncio API:

- The async Federated Authentication and Okta Authentication plugins use [aiohttp](https://docs.aiohttp.org/) for their HTTP flows: `pip install aiohttp`.
- Async SQLAlchemy (`create_async_engine`) requires SQLAlchemy's asyncio support, which includes `greenlet`: `pip install "sqlalchemy[asyncio]"`.

### Asyncio on Windows

Python's default event loop on Windows is `ProactorEventLoop`, and Psycopg's async
connection refuses to run on it:

```
psycopg.InterfaceError: Psycopg cannot use the 'ProactorEventLoop' to run in async mode
```

Every async PostgreSQL connection fails at connect until a selector event loop is
selected. Do this once, before any async code runs:

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

This is a requirement of the underlying driver rather than of the wrapper, and it does
not apply on Linux or macOS, where the default loop is already a selector loop. See
[Psycopg's asynchronous operations documentation](https://www.psycopg.org/psycopg3/docs/advanced/async.html#async).

## Using the AWS Advanced Python Wrapper

To start using the wrapper with Psycopg, you need to pass Psycopg's connect function to the `AwsWrapperConnection#connect` method as shown in the following example:

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

awsconn = AwsWrapperConnection.connect(
    Connection.connect,
    "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd",
    plugins="failover",
    autocommit=True
)
```

Similarly, to start using the wrapper with MySQL Connector/Python, you need to pass the connect function to the `AwsWrapperConnection#connect` method as shown in the following example:
```python
awsconn = AwsWrapperConnection.connect(
        mysql.connector.Connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        database="mysql",
        user="admin",
        password="pwd",
        plugins="failover",
        autocommit=True
)
```

The `AwsWrapperConnection#connect` method accepts the connection configuration through both the connection string and the keyword arguments.
You can either pass the connection configuration entirely through the connection string, entirely though the keyword arguments, or through both the connection string and the keywords arguments as shown below.

**Configuring the connection using the connection string**
```python
awsconn = AwsWrapperConnection.connect(
        Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd plugins=failover wrapper_dialect=aurora-pg"
)
```

**Configuring the connection using the keyword arguments**
```python
awsconn = AwsWrapperConnection.connect(
        Connection.connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        dbname="postgres",
        user="john",
        password="pwd",
        plugins="failover",
        wrapper_dialect="aurora-pg"
)
```

> **NOTE**: If the same configuration is specified in both the connection string and the keyword arguments, the keyword argument takes precedence.

The AWS Advanced Python Wrapper implements the [PEP 249 Database API](https://peps.python.org/pep-0249/).
After establishing a connection, you can use it in the same pattern as you would with the community Python drivers.
However, the wrapper introduces some custom errors for the Failover Plugin and the Read/Write Splitting Plugin that need to be explicitly handled.

For instance, after a successful failover, some session states may not be transferred to the new connection, so the wrapper throws a `FailoverSuccessError` to notify the application that the connection may need to be reconfigured, or to create a new cursor object.
See this simple PostgreSQL example:

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import FailoverSuccessError

with AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd"
) as awsconn:
    try:
        with awsconn.cursor() as cursor:
            cursor.execute(sql)

    except FailoverSuccessError:
        # Query execution failed and AWS Advanced Python Wrapper successfully failed over to an available instance.
        # The old cursor is no longer reusable and the application needs to reconfigure sessions states.
        reconfigure_session_states(awsconn)

        # Retry query
        with awsconn.cursor() as cursor:
            cursor.execute(sql)
```
A full PostgreSQL example is available at [PGFailover.py](./examples/PGFailover.py). A full MySQL example, [MySQLFailover.py](./examples/MySQLFailover.py), is available as well.

You can learn more about the AWS Advanced Python Wrapper specific errors in the [Using the Failover Plugin](using-the-python-wrapper/using-plugins/UsingTheFailoverPlugin.md#Failover-Errors) page.

For more detailed information about how to use and configure the AWS Advanced Python Wrapper, please visit [this page](using-the-python-wrapper/UsingThePythonWrapper.md).
