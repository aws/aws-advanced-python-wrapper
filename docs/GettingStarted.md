# Getting Started

## Minimum Requirements

Before using the AWS Advanced Python Driver, you must install:

- Python 3.8+.
- The AWS Advanced Python Driver.
- Your choice of underlying Python driver. 
  - To use the wrapper with Aurora with PostgreSQL compatibility, install [Psycopg](https://github.com/psycopg/psycopg).
  - To use the wrapper with Aurora with MySQL compatibility, install [MySQL Connector/Python](https://github.com/mysql/mysql-connector-python).

## Obtaining the AWS Advanced Python Driver

You can install the AWS Advanced Python Driver and the underlying Python drivers via [pip](https://pip.pypa.io/en/stable/).

To use the AWS Advanced Python Driver with Psycopg for Aurora PostgreSQL, run:

```shell
pip install aws-advanced-python-wrapper
pip install psycopg
```

To use the AWS Advanced Python Driver with MySQL Connector/Python for Aurora MySQL, run:
```shell
pip install aws-advanced-python-wrapper
pip install mysql-connector-python
```

## Using the AWS Advanced Python Driver

To start using the driver with Psycopg, you need to pass Psycopg's connect function to the `AwsWrapperConnection#connect` method as shown in the following example:

```python
from aws_wrapper import AwsWrapperConnection
from psycopg import Connection

awsconn = AwsWrapperConnection.connect(
        Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd",
        plugins="failover",
        wrapper_dialect="aurora-pg",
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

The AWS Advanced Python Driver implements the [PEP 249 Database API](https://peps.python.org/pep-0249/).
After establishing a connection, you can use it in the same pattern as you would with the community Python drivers.
However, the driver introduces some custom errors for the Failover Plugin and the Read/Write Splitting Plugin that need to be explicitly handled.

For instance, after a successful failover, some session states may not be transferred to the new connection, so the driver throws a `FailoverSuccessError` to notify the application that the connection may need to be reconfigured, or to create a new cursor object.
See this simple PostgreSQL example:

```python
import psycopg
from aws_wrapper import AwsWrapperConnection
from aws_wrapper.errors import FailoverSuccessError

with AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd"
) as awsconn:
    try:
        with awsconn.cursor() as cursor:
            cursor.execute(sql)
    
    except FailoverSuccessError:
        # Query execution failed and AWS Advanced Python Driver successfully failed over to an available instance.
        # The old cursor is no longer reusable and the application needs to reconfigure sessions states.
        reconfigure_session_states(awsconn)
        
        # Retry query
        with awsconn.cursor() as cursor:
            cursor.execute(sql)
```
A full PostgreSQL example is available at [PGFailover.py](./examples/PGFailover.py) and [MySQLFailover.py](./examples/MySQLFailover.py) 

You can learn more about the AWS Advanced Python Driver specific errors in the [Using the Failover Plugin](./using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md#Failover-Exception-Codes) page.

For more detailed information about how to use and configure the AWS Advanced Python Driver, please visit [this page](./using-the-python-driver/UsingThePythonDriver.md).
