# SQLAlchemy ORM Support

> [!IMPORTANT]
> SQLAlchemy ORM support is currently only available for **MySQL databases**.

The AWS Advanced Python Wrapper provides a custom SQLAlchemy database backend that enables SQLAlchemy applications to leverage AWS and Aurora functionalities such as failover handling and IAM authentication.

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

Ensure what is passed to create_engine always starts with `"mysql+aws_wrapper_mysqlconnector:..."`. Further setting of the database dialect within the wrapper can be done with the wrapper_dialect parameter, for more details see: [Database Dialects](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/DatabaseDialects.md).

## Using Plugins with SQLAlchemy

The AWS Advanced Python Wrapper supports a variety of plugins that enhance your SQLAlchemy application with features like failover handling, IAM authentication, and more. Most plugins can be enabled simply by adding them to the `wrapper_plugins` parameter in your database URL.

> [!NOTE]
> SQLAlchemy reserves the `plugins` connection parameter for its own engine, pool, and dialect event listeners.
> When using SQLAlchemy, enable AWS Advanced Python Wrapper plugins (such as the Aurora Initial Connection Strategy plugin) via `wrapper_plugins`. Otherwise, use `plugins` as usual.

For a complete list of available plugins, see the [List of Available Plugins](./UsingThePythonWrapper.md#list-of-available-plugins) in the main driver documentation.


### Failover Plugin

The Failover Plugin provides automatic failover handling for Aurora clusters. When a database instance becomes unavailable, the plugin automatically connects to a healthy instance in the cluster.

For more information about the Failover Plugin, see the [Failover Plugin documentation](./using-plugins/UsingTheFailover2Plugin.md).

#### Handling Failover Events

During a failover event, the driver will throw a `DBAPIError` exception after successfully connecting to a new instance. Your application should catch this exception, check which type of failover error occurred, and retry the failed query:

```python
from aws_advanced_python_wrapper.errors import FailoverSuccessError

def execute_query_with_failover_handling(query_func):
    try:
        return query_func()
    except DBAPIError as dbapi_err:
        err = dbapi_err.orig
        if isinstance(err, FailoverSuccessError):
            # Failover successful, retry the query
            return query_func()
```

For a complete example, see [MySQLSQLAlchemyFailover.py](../examples/MySQLSQLAlchemyFailover.py).

### Plugin Compatibility

| Plugin name | Plugin Code | Supported? |
|-------------------------------------------------------------------------------------------------|-------------------------------------------|-----|
| [Failover Plugin](./using-plugins/UsingTheFailoverPlugin.md)                                    | `failover`                                | <span style="color:yellow;font-size:15px">&check;</span> |
| [Failover Plugin v2](./using-plugins/UsingTheFailover2Plugin.md)                                | `failover_v2`                             | <span style="color:yellow;font-size:15px">&check;</span> |
| [Host Monitoring Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)                       | `host_monitoring_v2` or `host_monitoring` | <span style="color:yellow;font-size:15px">&check;</span> |
| [IAM Authentication Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)                 | `iam`                                     | <span style="color:yellow;font-size:15px">&check;</span> |
| [AWS Secrets Manager Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md)                | `aws_secrets_manager`                     | <span style="color:yellow;font-size:15px">&check;</span> |
| [Federated Authentication Plugin](./using-plugins/UsingTheFederatedAuthenticationPlugin.md)     | `federated_auth`                          | <span style="color:yellow;font-size:15px">&check;</span> |
| [Okta Authentication Plugin](./using-plugins/UsingTheOktaAuthenticationPlugin.md)               | `okta`                                    | <span style="color:yellow;font-size:15px">&check;</span> |
| [Custom Endpoint Plugin](./using-plugins/UsingTheCustomEndpointPlugin.md)                       | `customEndpoint`                          | <span style="color:yellow;font-size:15px">&check;</span> |
| Aurora Stale DNS Plugin                                                                         | `stale_dns`                               | <span style="color:yellow;font-size:15px">&check;</span> | 
| [Aurora Connection Tracker Plugin](./using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)    | `aurora_connection_tracker`               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Fastest Response Strategy Plugin](./using-plugins/UsingTheFastestResponseStrategyPlugin.md)    | `fastest_response_strategy`               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Blue/Green Deployment Plugin](./using-plugins/UsingTheBlueGreenPlugin.md)                      | `bg`                                      | <span style="color:yellow;font-size:15px">&check;</span> |
| [Limitless Plugin](./using-plugins/UsingTheLimitlessPlugin.md)                                  | `limitless`                               | <span style="color:yellow;font-size:15px">&check;</span> |
| [Read Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)              | `read_write_splitting`                    | <span style="color:red;font-size:20px">&cross;</span>  |
| [Simple Read Write Splitting Plugin](./using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | `srw`                                     | <span style="color:red;font-size:20px">&cross;</span>  |

For Read Write Splitting, SQLAlchemy provides session binding. See the [official SQLAlchemy documentation on the Session API](https://docs.sqlalchemy.org/en/20/orm/session_api.html) for more information on session binds.
