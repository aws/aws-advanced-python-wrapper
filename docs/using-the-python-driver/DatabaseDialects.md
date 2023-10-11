# Database Dialects

The AWS Advanced Python Driver uses two types of dialects: driver dialects and database dialects. This page is on database dialects. To find out more about driver dialects, see [Driver Dialects](./DriverDialects.md).

## What are database dialects?

The AWS Advanced Python Driver is a wrapper that requires an underlying driver, and it is meant to be compatible with any Python driver. Database dialects help the AWS Advanced Python Driver determine what kind of underlying database is being used. To function correctly, the AWS Advanced Python Driver requires details unique to specific databases such as the default port number or the method to get the current host from the database. These details can be defined and provided to the AWS Advanced Python Driver by using database dialects. 

## Configuration Parameters

| Name              | Required             | Description                                                                        | Example                                      |
|-------------------|----------------------|------------------------------------------------------------------------------------|----------------------------------------------|
| `wrapper_dialect` | No (see notes below) | The [dialect code](#list-of-available-dialect-codes) of the desired database type. | `DialectCode.AURORA_MYSQL` or `aurora-mysql` |

> **NOTES:** 
> 
> The `wrapper_dialect` parameter is not required. When it is not provided by the user, the AWS Advanced Python Driver will attempt to determine which of the existing dialects to use based on other connection details. However, if the dialect is known by the user, it is preferable to set the `wrapper_dialect` parameter because it will take time to resolve the dialect.

### List of Available Dialect Codes

Dialect codes specify what kind of database any connections will be made to.

| Dialect Code Reference | Value          | Database                                                                                       |
|------------------------|----------------|------------------------------------------------------------------------------------------------|
| `AURORA_MYSQL`         | `aurora-mysql` | Aurora MySQL                                                                                   |
| `RDS_MYSQL`            | `rds-mysql`    | Amazon RDS MySQL                                                                               |
| `MYSQL`                | `mysql`        | MySQL                                                                                          |
| `AURORA_PG`            | `aurora-pg`    | Aurora PostgreSQL                                                                              |
| `RDS_PG`               | `rds-pg`       | Amazon RDS PostgreSQL                                                                          |
| `PG`                   | `pg`           | PostgreSQL                                                                                     |
| `CUSTOM`               | `custom`       | See [custom dialects](#custom-dialects). This code is not required when using custom dialects. |
| `UNKNOWN`              | `unknown`      | Unknown. Although this code is available, do not use it as it will result in errors.           |

## Custom Dialects

If you are interested in using the AWS Advanced Python Driver but your desired database type is not currently supported, it is possible to create a custom dialect.

To create a custom dialect, implement the [`DatabaseDialect`](../../aws_wrapper/database_dialect.py) class. For databases clusters that are aware of their topology, the `TopologyAwareDatabaseDialect` interface should also be implemented. See the following classes in the [Database Dialect file](../../aws_wrapper/database_dialect.py) for examples:
- PgDialect is a generic dialect that should work with any PostgreSQL database.
- AuroraPgDialect is an extension of PgDialect, but also implements the `TopologyAwareDatabaseDialect` interface.

Once the custom dialect class has been created, tell the AWS Advanced Python Driver to use it by setting the `custom_dialect` attribute in the `DialectManager` class. It is not necessary to set the `wrapper_dialect` parameter. See below for an example:

```python
custom_dialect: Dialect = CustomDialect()
DialectManager.custom_dialect = custom_dialect
```