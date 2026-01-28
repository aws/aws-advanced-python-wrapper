# Using the AWS Advanced Python Wrapper

The AWS Advanced Python Wrapper leverages community Python drivers and enables support of AWS and Aurora functionalities.
Currently, [Psycopg](https://github.com/psycopg/psycopg) 3.1.12+ and [MySQL Connector/Python](https://github.com/mysql/mysql-connector-python) 8.1.0+ are supported. Compatibility with prior versions of these drivers has not been tested.


### Using the AWS Advanced Python Wrapper with RDS Multi-AZ database Clusters
The [AWS RDS Multi-AZ DB Clusters](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) are capable of switching over the current writer host to another host in the cluster within approximately 1 second or less, in case of minor engine version upgrade or OS maintenance operations.
The AWS Advanced Python Wrapper has been optimized for such fast-failover when working with AWS RDS Multi-AZ DB Clusters.

With the Failover plugin, the downtime during certain DB cluster operations, such as engine minor version upgrades, can be reduced to one second or even less with finely tuned parameters. It supports both MySQL and PostgreSQL clusters.

Visit [this page](./SupportForRDSMultiAzDBCluster.md) for more details.


## Using the AWS Advanced Python Wrapper with plain RDS databases

It is possible to use the AWS Advanced Python Wrapper with plain RDS databases, but individual features may or may not be compatible. For example, failover handling and enhanced failure monitoring are not compatible with plain RDS databases and the relevant plugins must be disabled. Plugins can be enabled or disabled as seen in the [Connection Plugin Manager Parameters](#connection-plugin-manager-parameters) section. Please note that some plugins have been enabled by default. Plugin compatibility can be verified in the [plugins table](#list-of-available-plugins).

## AWS Advanced Python Wrapper Parameters

These parameters are applicable to any instance of the AWS Advanced Python Wrapper.

| Parameter                        | Description                                                                                                                                                                                                                                                                                                                                                                                 | Required | Default Value |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| wrapper_dialect                  | A unique identifier for the supported database dialect.                                                                                                                                                                                                                                                                                                                                     | False    | None          |
| wrapper_driver_dialect           | A unique identifier for the target driver dialect.                                                                                                                                                                                                                                                                                                                                          | False    | None          |
| auxiliary_query_timeout_sec      | Network timeout, in seconds, used for auxiliary queries to the database. This timeout applies to queries executed by the wrapper driver to gain info about the connected database. It does not apply to queries requested by the driver client.                                                                                                                                             | False    | 5             |
| topology_refresh_ms              | Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection.                                                                                                                                                                            | False    | 30000         |
| cluster_id                       | A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. If unspecified, a cluster id is automatically created for AWS RDS clusters.                                                                                                                                                                                                       | False    | None          |
| cluster_instance_host_pattern    | The cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for cluster instance names. This pattern is required to be specified for IP address or custom domain connections to AWS RDS clusters. Otherwise, if unspecified, the  pattern will be automatically created for AWS RDS clusters. | False    | 30000         |
| transfer_session_state_on_switch | Enables transferring the session state to a new connection.                                                                                                                                                                                                                                                                                                                                 | False    | `True`        |
| reset_session_state_on_close     | Enables resetting the session state before closing connection.                                                                                                                                                                                                                                                                                                                              | False    | `True`        |
| rollback_on_switch               | Enables rolling back a current transaction, if any in effect, before switching to a new connection.                                                                                                                                                                                                                                                                                         | False    | `True`        |

### Network Timeouts

| Parameter                     | Description                                                                                                                                                                                                                                                                                                                                                                                 | Required | Default Value |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| connect_timeout               | Max number of seconds to wait for a connection to be established before timing out.                                                                                                                                                                                                                                                                                                         | False    | None          |
| socket_timeout                | Max number of seconds to wait for a SQL query to complete before timing out.                                                                                                                                                                                                                                                                                                                | False    | None          |
| tcp_keepalive                 | Enable TCP keepalive functionality.                                                                                                                                                                                                                                                                                                                                                         | False    | None          |
| tcp_keepalive_time            | Number of seconds to wait before sending an initial keepalive probe.                                                                                                                                                                                                                                                                                                                        | False    | None          |
| tcp_keepalive_interval        | Number of seconds to wait before sending additional keepalive probes after the initial probe has been sent.                                                                                                                                                                                                                                                                                 | False    | None          |
| tcp_keepalive_probes          | Number of keepalive probes to send before concluding that the connection is invalid.                                                                                                                                                                                                                                                                                                        | False    | None          |

## Resource Management

The AWS Advanced Python Wrapper creates background threads and thread pools for various plugins during operations such as host monitoring and connection management. To ensure proper cleanup and prevent resource leaks, it's important to release these resources when your application shuts down.

### Cleaning Up Resources

Call the following methods before your application terminates:

```python
from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources

try:
    # Your application code here
    conn = AwsWrapperConnection.connect(...)
    # ... use connection
finally:
    # Clean up all resources before application exit
    release_resources()
```

> [!IMPORTANT]
> Always call `release_resources()` at application shutdown to ensure:
> - All monitoring threads are properly terminated
> - Thread pools are shut down gracefully
> - No resource leaks occur
> - The application exits cleanly without hanging

## Plugins

The AWS Advanced Python Wrapper uses plugins to execute database API calls. You can think of a plugin as an extensible code module that adds extra logic around any database API calls. The AWS Advanced Python Wrapper has a number of [built-in plugins](#list-of-available-plugins) available for use. 

Plugins are loaded and managed through the Connection Plugin Manager and may be identified by a `str` name in the form of plugin code.

### Connection Plugin Manager Parameters

| Parameter                        | Value | Required | Description                                                                                                                                                                               | Default Value                          |
|----------------------------------|-------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| `plugins`                        | `str` | No       | Comma separated list of connection plugin codes. <br><br>Example: `failover,efm`                                                                                                          | `auroraConnectionTracker,failover,efm` | 
| `auto_sort_wrapper_plugin_order` | `str` | No       | Certain plugins require a specific plugin chain ordering to function correctly. When enabled, this property automatically sorts the requested plugins into the correct order.             | `True`                                 |

### List of Available Plugins

The AWS Advanced Python Wrapper has several built-in plugins that are available to use. Please visit the individual plugin page for more details.

| Plugin name                                                                                            | Plugin Code                               | Database Compatibility   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Additional Required Dependencies                                     |
|--------------------------------------------------------------------------------------------------------|-------------------------------------------|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)                                | `failover`                                | Aurora                   | Enables the failover functionality supported by Amazon Aurora clusters. Prevents opening a wrong connection to an old writer host dues to stale DNS after failover event. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                      | None                                                                 |                             
| [Failover Connection Plugin v2](./using-plugins/UsingTheFailover2Plugin.md)                            | `failover_v2`                             | Aurora                   | The next version (v2) of the [Failover Plugin](./using-plugins/UsingTheFailoverPlugin.md)                                                                                                                                                                                                                                                                                                                                                                                                                         | None                                                                 |
| [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)                   | `host_monitoring_v2` or `host_monitoring` | Aurora                   | Enables enhanced host connection failure monitoring, allowing faster failure detection rates. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                                                                                                  | None                                                                 |
| [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)             | `iam`                                     | Any database             | Enables users to connect to their Amazon Aurora clusters using AWS Identity and Access Management (IAM).                                                                                                                                                                                                                                                                                                                                                                                                          | [Boto3 - AWS SDK for Python](https://aws.amazon.com/sdk-for-python/) |
| [AWS Secrets Manager Connection Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md)            | `aws_secrets_manager`                     | Any database             | Enables fetching database credentials from the AWS Secrets Manager service.                                                                                                                                                                                                                                                                                                                                                                                                                                       | [Boto3 - AWS SDK for Python](https://aws.amazon.com/sdk-for-python/) |
| [Federated Authentication Connection Plugin](./using-plugins/UsingTheFederatedAuthenticationPlugin.md) | `federated_auth`                          | Any database             | Enables users to authenticate via Federated Identity and then database access via IAM.                                                                                                                                                                                                                                                                                                                                                                                                                            | [Boto3 - AWS SDK for Python](https://aws.amazon.com/sdk-for-python/) |
| Aurora Stale DNS Plugin                                                                                | `stale_dns`                               | Aurora                   | Prevents incorrectly opening a new connection to an old writer host when DNS records have not yet updated after a recent failover event. <br><br> :warning:**Note:** Contrary to `failover` plugin, `stale_dns` plugin doesn't implement failover support itself. It helps to eliminate opening wrong connections to an old writer host after cluster failover is completed. <br><br> :warning:**Note:** This logic is already included in `failover` plugin so you can omit using both plugins at the same time. | None                                                                 |
| [Aurora Connection Tracker Plugin](./using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)           | `aurora_connection_tracker`               | Aurora                   | Tracks all the opened connections. In the event of a cluster failover, the plugin will close all the impacted connections to the host. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                                                         | None                                                                 |
| [Read Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)                     | `read_write_splitting`                    | Aurora                   | Enables read write splitting functionality where users can switch between database reader and writer instances.                                                                                                                                                                                                                                                                                                                                                                                                   | None                                                                 |
| [Simple Read Write Splitting Plugin](./using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md)        | `srw`                                     | Any database             | Enables read write splitting functionality where users can switch between reader and writer endpoints.                                                                                                                                                                                                                                                                                                                                                                                                            | None                                                                 |
| [Fastest Response Strategy Plugin](./using-plugins/UsingTheFastestResponseStrategyPlugin.md)           | `fastest_response_strategy`               | Aurora                   | A host selection strategy plugin that uses a host monitoring service to monitor each reader host's response time and choose the host with the fastest response.                                                                                                                                                                                                                                                                                                                                                   | None                                                                 |
| [Blue/Green Deployment Plugin](./using-plugins/UsingTheBlueGreenPlugin.md)                             | `bg`                                      | Aurora, <br>RDS Instance | Enables client-side Blue/Green Deployment support.                                                                                                                                                                                                                                                                                                                                                                                                                                                                | None                                                                 |
| [Limitless Plugin](using-plugins/UsingTheLimitlessPlugin.md)                                           | `limitless`                               | Aurora                   | Enables client-side load-balancing of Transaction Routers on Amazon Aurora Limitless Databases.                                                                                                                                                                                                                                                                                                                                                                                                                   | None                                                                 |

In addition to the built-in plugins, you can also create custom plugins more suitable for your needs.
For more information, see [Custom Plugins](../development-guide/LoadablePlugins.md#using-custom-plugins).

## Logging

The AWS Advanced Python Wrapper uses the standard [Python logging module](https://docs.python.org/3/library/logging.html) to log information. To configure logging for the AWS Advanced Python Wrapper, refer to [this section of the Python logging docs](https://docs.python.org/3/howto/logging.html#configuring-logging). Please note the following:
- As mentioned in a warning in [this section](https://docs.python.org/3/howto/logging.html#configuring-logging), existing loggers will be disabled by default when `fileConfig()` or `dictConfig()` is called. We recommend that you pass `disable_existing_loggers=False` when calling either of these functions.
- The AWS Advanced Python Wrapper uses module-level loggers, as recommended in the Python docs. You can configure a specific module's logger by referring to its module path. Here is an example `logging.conf` file that configures logging for the failover plugin:
```
[loggers]
keys=root,failover

[handlers]
keys=consoleHandler

[formatters]
keys=simpleFormatter

[logger_root]
level=INFO
handlers=consoleHandler

[logger_failover]
level=DEBUG
handlers=consoleHandler
qualname=aws_advanced_python_wrapper.failover_plugin
propagate=0

[handler_consoleHandler]
class=StreamHandler
level=DEBUG
formatter=simpleFormatter
args=(sys.stdout,)

[formatter_simpleFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
```