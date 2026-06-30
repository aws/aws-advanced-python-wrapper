# SQLAlchemy Support

The AWS Advanced Python Wrapper can be used as a PEP 249 DBAPI module with SQLAlchemy's `create_engine` via the `creator=` factory pattern, for both PostgreSQL (via psycopg v3) and MySQL (via mysql-connector-python).

## Prerequisites

- Python 3.10 – 3.14 (inclusive)
- SQLAlchemy 2.x
- One of:
  - [psycopg v3](https://www.psycopg.org/psycopg3/) for PostgreSQL
  - [mysql-connector-python](https://dev.mysql.com/doc/connector-python/en/) for MySQL

## Using the wrapper with SQLAlchemy (PostgreSQL)

```python
from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.psycopg import connect

engine = create_engine(
    "postgresql+aws_wrapper_psycopg://",
    creator=lambda: connect(
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com "
        "dbname=db user=john password=pwd",
        wrapper_dialect="aurora-pg",
        plugins="failover,host_monitoring_v2",
        cluster_id="pg_sqlalchemy",
    ),
)

try:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT pg_catalog.aurora_db_instance_identifier()")).one()
        print(row)
finally:
    engine.dispose()
    release_resources()
```

The wrapper's connection options (`wrapper_dialect`, `plugins`, etc.) are passed as kwargs to `connect`. PostgreSQL requires the wrapper's custom dialect (`postgresql+aws_wrapper_psycopg://`) even with the `creator=` pattern: SQLAlchemy's stock psycopg dialect calls `psycopg.TypeInfo.fetch()` during initialization, which `isinstance`-checks its argument against the real `psycopg.Connection` and raises `TypeError: expected Connection or AsyncConnection, got AwsWrapperConnection` on the wrapper proxy. The custom dialect unwraps to the native connection for those calls.

## Using the wrapper with SQLAlchemy (MySQL)

```python
from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import connect

engine = create_engine(
    "mysql+aws_wrapper_mysqlconnector://",
    creator=lambda: connect(
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com "
        "database=db user=john password=pwd",
        wrapper_dialect="aurora-mysql",
        plugins="failover,host_monitoring_v2",
        use_pure=True,
    ),
)

try:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT @@aurora_server_id")).one()
        print(row)
finally:
    engine.dispose()
    release_resources()
```

> **Note — `use_pure` + IAM authentication:** For Aurora MySQL, we recommend `use_pure=True` because the C extension's `is_connected` can block indefinitely on network failure. However, the [IAM Authentication Plugin](using-plugins/UsingTheIamAuthenticationPlugin.md) is incompatible with `use_pure=True` (the pure-Python driver truncates passwords at 255 chars; IAM tokens are longer). See the README's "Known Limitations" section for details.

## Using the custom SQLAlchemy dialects (URL-based)

The wrapper registers two SQLAlchemy dialects via entry-points so `create_engine` can be driven by URL alone — no `creator=` lambda needed. This is the idiomatic path for Alembic, 12-factor `DATABASE_URL` configs, and framework starters that expect a URL string.

PostgreSQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+aws_wrapper_psycopg://john:pwd@"
    "database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/db"
    "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,host_monitoring_v2"
)
```

MySQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+aws_wrapper_mysqlconnector://john:pwd@"
    "database.cluster-xyz.us-east-1.rds.amazonaws.com:3306/db"
    "?wrapper_dialect=aurora-mysql&wrapper_plugins=failover&use_pure=True"
)
```

### Naming

The wrapper registers as a **driver under SQLAlchemy's existing dialects**, following SA's `<dialect>+<driver>` URL convention (the same shape as stock `postgresql+psycopg`, `mysql+mysqlconnector`):

| Engine | URL |
|--------|-----|
| PostgreSQL | `postgresql+aws_wrapper_psycopg://` |
| MySQL | `mysql+aws_wrapper_mysqlconnector://` |

This keeps the dialect identity correct (`engine.dialect.name == "postgresql"` / `"mysql"`), so dialect-specific type compilation, reserved-word handling, and any third-party `if dialect.name == ...` checks behave as expected.

### URL parameter `wrapper_plugins` (not `plugins`)

SQLAlchemy's `create_engine` reserves the query-string `plugins=` key for its own engine-plugin loader and strips it from the URL before the dialect sees it. To pass the wrapper's `plugins` connection property via URL, spell it **`wrapper_plugins=`** — the dialect translates it back to `plugins=` before calling the wrapper's `connect()`. In the creator-pattern path (where you call `connect()` directly in Python), continue to use the normal `plugins=` kwarg; the `wrapper_plugins` alias is URL-only.

All other wrapper connection options (`wrapper_dialect`, plugin-specific parameters like `failover_timeout_sec`, auth parameters like `iam_region`, etc.) pass through the URL query string unchanged as kwargs to the underlying wrapper's `connect()`.

Both the creator-pattern (shown above) and the URL-based path remain supported. Use whichever fits your configuration surface.

## Error handling

Wrapper errors are classified so SQLAlchemy maps them to the correct `sqlalchemy.exc.*` subclass:

| Wrapper error | SQLAlchemy error |
|---|---|
| `AwsConnectError` | `sqlalchemy.exc.OperationalError` |
| `FailoverError`, `FailoverSuccessError`, `FailoverFailedError`, `TransactionResolutionUnknownError` | `sqlalchemy.exc.OperationalError` |
| `QueryTimeoutError` | `sqlalchemy.exc.OperationalError` |
| `ReadWriteSplittingError` | `sqlalchemy.exc.InterfaceError` |
| `UnsupportedOperationError` | `sqlalchemy.exc.NotSupportedError` |
| `AwsWrapperError` (generic) | `sqlalchemy.exc.DBAPIError` |

Applications writing SA retry loops can `except sqlalchemy.exc.OperationalError` and catch failover events naturally. Target-driver exceptions (e.g., `psycopg.errors.*`, `mysql.connector.errors.*`) are not remapped and flow through SA's dialect-specific classification unchanged.

## Resource cleanup

Two things must be torn down at shutdown, in this order:

1. `engine.dispose()` — drains SQLAlchemy's `QueuePool` and closes all pooled DBAPI connections.
2. `aws_advanced_python_wrapper.release_resources()` — tears down the wrapper's own background threads (topology monitor, host monitoring, internal pool cleanup).

They are complementary: `engine.dispose()` does not reach the wrapper's background machinery, and `release_resources()` does not close SA's pool.

## Plugin Compatibility

Plugins are configured identically to non-SA usage — via the `plugins` connection property (or `wrapper_plugins` in the URL) and any plugin-specific options.

| Plugin name | Plugin Code | Supported? |
|-------------------------------------------------------------------------------------------------|-------------------------------------------|-----|
| [Failover Plugin](using-plugins/UsingTheFailoverPlugin.md)                                    | `failover`                                | <span style="color:yellow;font-size:15px">&check;</span> |
| [Failover Plugin v2](using-plugins/UsingTheFailover2Plugin.md)                                | `failover_v2`                             | <span style="color:yellow;font-size:15px">&check;</span> |
| [Host Monitoring Plugin](using-plugins/UsingTheHostMonitoringPlugin.md)                       | `host_monitoring_v2` or `host_monitoring` | <span style="color:yellow;font-size:15px">&check;</span> |
| [IAM Authentication Plugin](using-plugins/UsingTheIamAuthenticationPlugin.md)                 | `iam`                                     | <span style="color:yellow;font-size:15px">&check;</span> |
| [AWS Secrets Manager Plugin](using-plugins/UsingTheAwsSecretsManagerPlugin.md)                | `aws_secrets_manager`                     | <span style="color:yellow;font-size:15px">&check;</span> |
| [Federated Authentication Plugin](using-plugins/UsingTheFederatedAuthenticationPlugin.md)     | `federated_auth`                          | <span style="color:yellow;font-size:15px">&check;</span> |
| [Okta Authentication Plugin](using-plugins/UsingTheOktaAuthenticationPlugin.md)               | `okta`                                    | <span style="color:yellow;font-size:15px">&check;</span> |
| [Custom Endpoint Plugin](using-plugins/UsingTheCustomEndpointPlugin.md)                       | `custom_endpoint`                         | <span style="color:yellow;font-size:15px">&check;</span> |
| Aurora Stale DNS Plugin                                                                        | `stale_dns`                               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Aurora Connection Tracker Plugin](using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)    | `aurora_connection_tracker`               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Fastest Response Strategy Plugin](using-plugins/UsingTheFastestResponseStrategyPlugin.md)    | `fastest_response_strategy`               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Blue/Green Deployment Plugin](using-plugins/UsingTheBlueGreenPlugin.md)                      | `bg`                                      | <span style="color:yellow;font-size:15px">&check;</span> |
| [Limitless Plugin](using-plugins/UsingTheLimitlessPlugin.md)                                  | `limitless`                               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Read Write Splitting Plugin](using-plugins/UsingTheReadWriteSplittingPlugin.md)              | `read_write_splitting`                    | <span style="color:red;font-size:20px">&cross;</span> |
| [Simple Read Write Splitting Plugin](using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | `srw`                                     | <span style="color:red;font-size:20px">&cross;</span> |

Read/write splitting is **not** supported with SQLAlchemy: the plugins switch instances by setting `read_only` on a long-lived connection, and there is currently no routing of SQLAlchemy's `execution_options(...readonly=True)` to the wrapper's `read_only` attribute. For read/write workloads, use SQLAlchemy's own session binding — see the [official SQLAlchemy documentation on the Session API](https://docs.sqlalchemy.org/en/20/orm/session_api.html).

## See also

- [Django Support](DjangoSupport.md)
- [Using the AWS Python Wrapper](UsingThePythonWrapper.md)
- Example scripts:
  - [`PGSQLAlchemyFailover.py`](../examples/PGSQLAlchemyFailover.py)
  - [`MySQLSQLAlchemyFailover.py`](../examples/MySQLSQLAlchemyFailover.py)
