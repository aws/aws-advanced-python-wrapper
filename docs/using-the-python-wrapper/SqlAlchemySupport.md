# SQLAlchemy ORM Support

> [!IMPORTANT]
> SQLAlchemy ORM support is currently only available for **MySQL databases**.

The AWS Advanced Python Wrapper provides a custom SQLAlchemy database backend that enables SQLAlchemy applications to leverage AWS and Aurora functionalities such as failover handling, IAM authentication, and read/write splitting.

## Prerequisites

- SQLAlchemy 2.0.0+

## Basic Configuration

To use the AWS Advanced Python Wrapper with SQLAlchemy, call the `create_engine` function with your database URL with the configuration settings appended:

```python
from sqlalchemy import create_engine

create_engine("mysql+aws_wrapper_mysqlconnector://your_username:your_password@your-cluster-endpoint.cluster-xyz.us-east-1.rds.amazonaws.com:your_port/your_database_name?connect_timeout=10&wrapper_plugins=aurora_connection_tracker%2Cfailover_v2")
```

See [the SQLALchemy official documentation](https://docs.sqlalchemy.org/en/20/core/engines.html) for more information on engine configuration.

### Supported Engines

| Driver | Database Dialect |
|-------------------|------------------|
| `aws_wrapper_mysqlconnector` | `mysql` |

## Using Plugins with SQLAlchemy

The AWS Advanced Python Wrapper supports a variety of plugins that enhance your SQLAlchemy application with features like failover handling, IAM authentication, and more. Most plugins can be enabled simply by adding them to the `wrapper_plugins` parameter in your database URL.

For a complete list of available plugins, see the [List of Available Plugins](./UsingThePythonWrapper.md#list-of-available-plugins) in the main driver documentation.

