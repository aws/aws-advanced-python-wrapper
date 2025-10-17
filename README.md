# Amazon Web Services (AWS) Advanced Python Driver

[![Python Checks and Unit Tests](https://github.com/aws/aws-advanced-python-wrapper/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/aws/aws-advanced-python-wrapper/actions/workflows/main.yml)
[![Integration Tests](https://github.com/aws/aws-advanced-python-wrapper/actions/workflows/integration_tests.yml/badge.svg)](https://github.com/aws/aws-advanced-python-wrapper/actions/workflows/integration_tests.yml)
![PyPI - Package Version](http://img.shields.io/pypi/v/aws-advanced-python-wrapper.svg?label=aws-advanced-python-wrapper)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-advanced-python-wrapper)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## About the Driver

Hosting a database cluster in the cloud via Aurora is able to provide users with sets of features and configurations to obtain maximum performance and availability, such as database failover. However, at the moment, most existing drivers do not currently support those functionalities or are not able to entirely take advantage of it.

The main idea behind the AWS Advanced Python Driver is to add a software layer on top of an existing Python driver that would enable all the enhancements brought by Aurora, without requiring users to change their workflow with their databases and existing Python drivers.

### What is Failover?

In an Amazon Aurora database cluster, **failover** is a mechanism by which Aurora automatically repairs the cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS Advanced Python Driver is designed to understand the situation and coordinate with the cluster in order to provide minimal downtime and allow connections to be very quickly restored in the event of a DB instance failure.

### Benefits of the AWS Advanced Python Driver

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS Advanced Python Driver allows customers to continue using their existing community drivers in addition to having the AWS Advanced Python Driver fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora DB, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS Advanced Python Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible.

### Enhanced Failure Monitoring

Since a database failover is usually identified by reaching a network or a connection timeout, the AWS Advanced Python Driver introduces an enhanced and customizable manner to faster identify a database outage.

Enhanced Failure Monitoring (EFM) is a feature available from the [Host Monitoring Connection Plugin](./docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md#enhanced-failure-monitoring) that periodically checks the connected database host's health and availability. If a database host is determined to be unhealthy, the connection is aborted (and potentially routed to another healthy host in the cluster).

### Using the AWS Advanced Python Driver with plain RDS databases

The AWS Advanced Python Driver also works with RDS provided databases that are not Aurora.

Please visit [this page](./docs/using-the-python-driver/UsingThePythonDriver.md#using-the-aws-python-driver-with-plain-rds-databases) for more information.

## Getting Started

To start using the driver with Psycopg, you need to pass Psycopg's connect function to the `AwsWrapperConnection#connect` method as shown in the following example:

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

with AwsWrapperConnection.connect(
        Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd",
        plugins="failover",
        wrapper_dialect="aurora-pg",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT pg_catalog.aurora_db_instance_identifier()")
    row = awscursor.fetchone()
    print(row)
```
The `AwsWrapperConnection#connect` method accepts the connection configuration through both the connection string and the keyword arguments.

You can either pass the connection configuration entirely through the connection string, entirely though the keyword arguments, or through a mixture of both.

To use the driver with MySQL Connector/Python, see the following example:

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from mysql.connector import Connect

with AwsWrapperConnection.connect(
        Connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com database=db user=john password=pwd",
        plugins="failover",
        wrapper_dialect="aurora-mysql",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT @@aurora_server_id")
    row = awscursor.fetchone()
    print(row)
```

For more details on how to download the AWS Advanced Python Driver, minimum requirements to use it, 
and how to integrate it within your project and with your Python driver of choice, please visit the 
[Getting Started page](./docs/GettingStarted.md).

### Connection Properties
The following table lists the connection properties used with the AWS Advanced Python Wrapper.

| Parameter                                    |                                                      Documentation Link                                                       |
|----------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------:|
| `auxiliary_query_timeout_sec`                |       [Driver Parameters](./docs/using-the-python-driver/UsingThePythonDriver.md#aws-advanced-python-driver-parameters)       |
| `topology_refresh_ms`                        |       [Driver Parameters](./docs/using-the-python-driver/UsingThePythonDriver.md#aws-advanced-python-driver-parameters)       |
| `cluster_id`                                 |       [Driver Parameters](./docs/using-the-python-driver/UsingThePythonDriver.md#aws-advanced-python-driver-parameters)       |
| `cluster_instance_host_pattern`              |       [Driver Parameters](./docs/using-the-python-driver/UsingThePythonDriver.md#aws-advanced-python-driver-parameters)       |
| `wrapper_dialect`                            |              [Dialects](./docs/using-the-python-driver/DatabaseDialects.md), and whether you should include it.               |
| `wrapper_driver_dialect`                     |            [Driver Dialect](./docs/using-the-python-driver/DriverDialects.md), and whether you should include it.             |
| `plugins`                                    |   [Connection Plugin Manager](./docs/using-the-python-driver/UsingThePythonDriver.md#connection-plugin-manager-parameters)    | 
| `auto_sort_wrapper_plugin_order`             |   [Connection Plugin Manager](./docs/using-the-python-driver/UsingThePythonDriver.md#connection-plugin-manager-parameters)    |
| `profile_name`                               |   [Connection Plugin Manager](./docs/using-the-python-driver/UsingThePythonDriver.md#connection-plugin-manager-parameters)    |
| `connect_timeout`                            |                  [Network Timeouts](./docs/using-the-python-driver/UsingThePythonDriver.md#network-timeouts)                  |
| `socket_timeout`                             |                  [Network Timeouts](./docs/using-the-python-driver/UsingThePythonDriver.md#network-timeouts)                  |
| `tcp_keepalive`                              |                  [Network Timeouts](./docs/using-the-python-driver/UsingThePythonDriver.md#network-timeouts)                  |
| `tcp_keepalive_time`                         |                  [Network Timeouts](./docs/using-the-python-driver/UsingThePythonDriver.md#network-timeouts)                  |
| `tcp_keepalive_interval`                     |                  [Network Timeouts](./docs/using-the-python-driver/UsingThePythonDriver.md#network-timeouts)                  |
| `tcp_keepalive_probes`                       |                  [Network Timeouts](./docs/using-the-python-driver/UsingThePythonDriver.md#network-timeouts)                  |
| `enable_failover`                            |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `failover_mode`                              |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `cluster_instance_host_pattern`              |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `failover_cluster_topology_refresh_rate_sec` |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `failover_reader_connect_timeout_sec`        |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `failover_timeout_sec`                       |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `failover_writer_reconnect_interval_sec`     |                   [Failover Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFailoverPlugin.md)                   |
| `failure_detection_count`                    |            [Host Monitoring Plugin](./docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md)             |
| `failure_detection_enabled`                  |            [Host Monitoring Plugin](./docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md)             |
| `failure_detection_interval_ms`              |            [Host Monitoring Plugin](./docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md)             |
| `failure_detection_time_ms`                  |            [Host Monitoring Plugin](./docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md)             |
| `monitor_disposal_time_ms`                   |            [Host Monitoring Plugin](./docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md)             |
| `iam_default_port`                           |         [IAM Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)          |
| `iam_host`                                   |         [IAM Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)          |
| `iam_region`                                 |         [IAM Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)          |
| `iam_expiration`                             |         [IAM Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)          |
| `secrets_manager_secret_id`                  |           [Secrets Manager Plugin](./docs/using-the-python-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)           |
| `secrets_manager_region`                     |           [Secrets Manager Plugin](./docs/using-the-python-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)           |
| `secrets_manager_endpoint`                   |           [Secrets Manager Plugin](./docs/using-the-python-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)           |
| `secrets_manager_secret_username`            |           [Secrets Manager Plugin](./docs/using-the-python-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)           |
| `secrets_manager_secret_password`            |           [Secrets Manager Plugin](./docs/using-the-python-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)           |
| `reader_host_selector_strategy`              | [Connection Strategy](./docs/using-the-python-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#connection-strategies) |
| `db_user`                                    |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `idp_username`                               |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `idp_password`                               |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `idp_endpoint`                               |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `iam_role_arn`                               |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `iam_idp_arn`                                |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `iam_region`                                 |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `idp_name`                                   |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `idp_port`                                   |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `rp_identifier`                              |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `iam_host`                                   |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `iam_default_port`                           |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `iam_token_expiration`                       |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `http_request_connect_timeout`               |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |
| `ssl_secure`                                 |   [Federated Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheFederatedAuthenticationPlugin.md)    |

### Using the AWS Advanced Python Driver

Technical documentation regarding the functionality of the AWS Advanced Python Driver will be maintained in this GitHub repository. Since the AWS Advanced Python Driver requires an underlying Python driver, please refer to the individual driver's documentation for driver-specific information.
To find all the documentation and concrete examples on how to use the AWS Advanced Python Driver, please refer to the [AWS Advanced Python Driver Documentation](./docs/README.md) page.

### Known Limitations

#### Amazon RDS Blue/Green Deployments

Support for Blue/Green deployments using the AWS Advanced Python Driver requires specific metadata tables. The following service versions provide support for Blue/Green Deployments:

- Supported RDS PostgreSQL Versions: `rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21)` and above.
- Supported Aurora PostgreSQL Versions: Engine Release `17.5, 16.9, 15.13, 14.18, 13.21` and above.
- Supported Aurora MySQL Versions: Engine Release `3.07` and above.

Please note that Aurora Global Database and RDS Multi-AZ clusters with Blue/Green deployments is currently not supported. For detailed information on supported database versions, refer to the [Blue/Green Deployment Plugin Documentation](./docs/using-the-python-driver/using-plugins/UsingTheBlueGreenPlugin.md).

#### MySQL Connector/Python C Extension

When connecting to Aurora MySQL clusters, it is recommended to use the Python implementation of the MySQL Connector/Python driver by setting the `use_pure` connection argument to `True`.
The AWS Advanced Python Driver internally calls the MySQL Connector/Python's `is_connected` method to verify the connection. The [MySQL Connector/Python's C extension](https://dev.mysql.com/doc/connector-python/en/connector-python-cext.html) uses a network blocking implementation of the `is_connected` method.
In the event of a network failure where the host can no longer be reached, the `is_connected` call may hang indefinitely and will require users to forcibly interrupt the application.

#### MySQL Support for the IAM Authentication Plugin

The official MySQL Connector/Python [offers a Python implementation and a C implementation](https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html#:~:text=Using%20the%20Connector/Python%20Python%20or%20C%20Extension) of the driver that can be toggled using the `use_pure` connection argument.
The [IAM Authentication Plugin](./docs/using-the-python-driver/using-plugins/UsingTheIamAuthenticationPlugin.md) is incompatible with the Python implementation of the driver due to its 255-character password limit.
The IAM Authentication Plugin generates a temporary AWS IAM token to authenticate users. Passing this token to the Python implementation of the driver will result in error messages similar to the following:
```
Error occurred while opening a connection: int1store requires 0 <= i <= 255
```
or 
```
struct.error: ubyte format requires 0 <= number <= 255
```
To avoid this error, we recommend you set `use_pure` to `False` when using the IAM Authentication Plugin.
However, as noted in the [MySQL Connector/Python C Extension](#mysql-connectorpython-c-extension) section, doing so may cause the application to indefinitely hang if there is a network failure.
Unfortunately, due to conflicting limitations, you will need to decide if using the IAM plugin is worth this risk for your application.

#### Toxiproxy network failure behavior

In the AWS Advanced Python Driver test suite, we have two methods of simulating database/network failure:
- method 1: initiate failover using the AWS RDS SDK.
- method 2: use a test dependency called Toxiproxy. 

Toxiproxy creates proxy network containers that sit between the test and the actual database. The test code connects to the proxy containers instead of the database. Toxiproxy can then be used to disable network activity between the driver and the database via the proxy. This network failure simulation is stricter than scenario 1. It does not allow any communication between the driver and the database, and does not give the database a chance to break off any connections.

We have observed different behavior when testing with method 1 vs method 2. With method 1, the server failover is detected without unexpected side effects. With method 2, we have observed some side effects. We have only observed these side effects when using Toxiproxy, and have never observed them in a real-world scenario. 

Psycopg and MySQL Connector/Python do not provide client-side query timeouts. When querying a host that has been disabled via Toxiproxy, the query will hang indefinitely until the host is re-enabled. Whether this behavior would ever occur in a real world scenario is uncertain. We reached out to the Psycopg team, who indicated they have not seen this issue (see full discussion [here](https://github.com/psycopg/psycopg/discussions/676)). They also believe that Toxiproxy tests for stricter conditions than would occur in a real-world scenario. However, we will list the side effects we have noticed during testing due to this behavior:

1. The EFM plugin and `socket_timeout` connection parameter use helper threads to execute queries used to detect if the network is working properly. These helper threads are executed with a timeout so that failure can be detected if the network is down. If the host was disabled using Toxiproxy, although the timeout will return control to the main thread, the helper thread will be stuck in a loop waiting on results from the server. It will be stuck until the host is re-enabled with Toxiproxy. There is no mechanism to cancel a running thread from another thread in Python, so the thread will consume resources until the host is re-enabled. Using an Event to signal the thread to stop is not an option, as the loop occurs inside the underlying driver code. Note that although the helper thread will be stuck, control will still be returned to the main thread so that the driver is still usable.
2. As a consequence of side effect 1, if a query is executed against a host that has been disabled with Toxiproxy, the Python program will not exit if the host is not re-enabled. The EFM helper threads mentioned in side effect 1 are run using a ThreadPoolExecutor. Although the ThreadPoolExecutor implementation uses daemon threads, it also joins all threads at Python exit. Because the helper thread is stuck in this scenario, the Python application will hang waiting the thread to join. This behavior has only been observed when using the MySQL Connector/Python driver.
3. As a consequence of side effect 1, if a query is executed against a host that has been disabled with Toxiproxy, and the host is still disabled when the Python program exits, a segfault may occur. This occurs because the helper thread is stuck in a loop attempting to read a connection pointer. When the program is exiting, the pointer is destroyed. The helper thread may try to read from the pointer after it is destroyed, leading to a segfault. This behavior has only been observed when using the Psycopg driver.

## Getting Help and Opening Issues

If you encounter a bug with the AWS Advanced Python Driver, we would like to hear about it.
Please search the [existing issues](https://github.com/awslabs/aws-advanced-python-wrapper/issues) to see if others are also experiencing the issue before reporting the problem in a new issue. GitHub issues are intended for bug reports and feature requests. 

When opening a new issue, please fill in all required fields in the issue template to help expedite the investigation process.

For all other questions, please use [GitHub discussions](https://github.com/awslabs/aws-advanced-python-wrapper/discussions).

## How to Contribute

1. Set up your environment by following the directions in the [Development Guide](./docs/development-guide/DevelopmentGuide.md).
2. To contribute, first make a fork of this project. 
3. Make any changes on your fork. Make sure you are aware of the requirements for the project (e.g. do not require Python 3.7 if we are supporting Python 3.8 - 3.11 (inclusive)).
4. Create a pull request from your fork. 
5. Pull requests need to be approved and merged by maintainers into the main branch. <br />

> [!NOTE]\
> Before making a pull request, [run all tests](./docs/development-guide/DevelopmentGuide.md#running-the-tests) and verify everything is passing.

### Code Style

The project source code is written using the [PEP 8 Style Guide](https://peps.python.org/pep-0008/), and the style is strictly enforced in our automation pipelines. Any contribution that does not respect/satisfy the style will automatically fail at build time.

## Releases

The `aws-advanced-python-wrapper` has a regular monthly release cadence. A new release will occur during the last week of each month. However, if there are no changes since the latest release, then a release will not occur.

## Aurora Engine Version Testing

This `aws-advanced-python-wrapper` is being tested against the following Community and Aurora database versions in our test suite:

| Database          | Versions                                                                                                                                                                                                                                                                                                                         |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL             | 8.4.0                                                                                                                                                                                                                                                                                                                            |
| PostgreSQL        | 16.2                                                                                                                                                                                                                                                                                                                             |
| Aurora MySQL      | - LTS version, see [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Updates.Versions.html#AuroraMySQL.Updates.LTS) for more details. <br><br> - Latest release, as shown on [this page](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraMySQLReleaseNotes/AuroraMySQL.Updates.30Updates.html). |
| Aurora PostgreSQL | - LTS version, see [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Updates.LTS.html) for more details. <br><br> - Latest release, as shown on [this page](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/AuroraPostgreSQL.Updates.html).)                        |

The `aws-advanced-python-wrapper` is compatible with MySQL 5.7 and MySQL 8.0 as per MySQL Connector/Python.
> [!WARNING]\
> Due to recent internal changes with the `v9.0.0` MySQL Connector/Python driver in regards to connection handling, the AWS Advanced Python Wrapper is not recommended for usage with `v9.0.0`. The AWS Advanced Python Wrapper will be updated in the future for `v9.0.0` compatibility with the community driver.

## License

This software is released under the Apache 2.0 license.
