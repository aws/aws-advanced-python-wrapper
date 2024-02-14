# Driver Dialects

The AWS Advanced Python Driver uses two types of dialects: driver dialects and database dialects. This page is on driver dialects. To find out more about database dialects, see [Database Dialects](./DatabaseDialects.md).

## What are driver dialects?

The AWS Advanced Python Driver is a wrapper that requires an underlying driver, and it is meant to be compatible with any Python driver. Driver dialects help the AWS Advanced Python Driver to properly pass database calls to an underlying Python driver. To function correctly, the AWS Advanced Python Driver requires details unique to specific target driver such as a database name used by the driver or whether to include some specific configuration parameters to a list of properties. These details can be defined and provided to the AWS Advanced Python Driver by using driver dialects.

By default, the driver dialect is determined based on the Connection function passed to the `AwsWrapperConnection#connect` method.

## Configuration Parameters

| Name                     | Required             | Description                                                                              | Example                                   |
|--------------------------|----------------------|------------------------------------------------------------------------------------------|-------------------------------------------|
| `wrapper_driver_dialect` | No (see notes below) | The [driver dialect code](#list-of-available-driver-codes) of the desired target driver. | `DriverDialectCodes.PSYCOPG` or `psycopg` |

> **NOTES:**
>
> The `wrapper_driver_dialect` parameter is not required. When it is not provided by the user, the AWS Advanced Python Driver will determine which of the existing target driver dialects to use based on the Connect function passed to the driver. If target driver specific implementation is not found, the AWS Advanced Python Driver will use a generic target driver dialect.


### List of Available Driver Codes

Driver Dialect codes specify which driver dialect class to use. 

| Dialect Code Reference   | Value                    |
|--------------------------|--------------------------|
| `PSYCOPG`                | `psycopg`                |
| `MYSQL_CONNECTOR_PYTHON` | `mysql-connector-python` |
| `SQLALCHEMY`             | `sqlalchemy`             |
| `GENERIC`                | `generic`                |

## Custom Driver Dialects

If you are interested in using the AWS Advanced Python Driver but your desired target driver has unique features incompatible with the generic dialect, it is possible to create a custom target driver dialect.

To create a custom target driver dialect, implement the [`DriverDialect`](../../aws_advanced_python_wrapper/driver_dialect.py) interface. See the following classes for examples:

- [PgDriverDialect](../../aws_advanced_python_wrapper/pg_driver_dialect.py)
    - This is a dialect that should work with [Psycopg](https://github.com/psycopg/psycopg).
- [MysqlTargetDriverDialect](../../aws_advanced_python_wrapper/mysql_driver_dialect.py)
    - This is a dialect that should work with [MySQL Connector/Python](https://github.com/mysql/mysql-connector-python).

Once the custom driver dialect class has been created, tell the AWS Advanced Python Driver to use it by setting the `custom_dialect` attribute in the `DriverDialectManager` class. It is not necessary to set the `wrapper_driver_dialect` parameter. See below for an example:

```python
custom_driver_dialect: DriverDialect = CustomDriverDialect()
DriverDialectManager.custom_dialect = custom_driver_dialect
```
