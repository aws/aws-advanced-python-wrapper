# Aurora Global Databases

The AWS Advanced Python Wrapper provides support for [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/).

## Overview

Aurora Global Database is a feature that allows a single Aurora database to span multiple AWS regions. It provides fast replication across regions with minimal impact on database performance, enabling disaster recovery and serving read traffic from multiple regions.

The AWS Advanced Python Wrapper supports Global Writer Endpoint recognition.

## Configuration

The following instructions are recommended by AWS Service Teams for Aurora Global Database connections.

### Writer Connections

**Connection String:**
Use the global cluster endpoint:
```
<global-db-name>.global-<XYZ>.global.rds.amazonaws.com
```

**Configuration Parameters:**

| Parameter                                | Value                                                                          | Notes                                                                                       |
|------------------------------------------|--------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `cluster_id`                             | `1`                                                                            | See [cluster_id parameter documentation](./ClusterId.md)                                    |
| `wrapper_dialect`                        | `global-aurora-mysql` or `global-aurora-pg`                                    |                                                                                             |
| `plugins`                                | `initial_connection,failover_v2,host_monitoring_v2`                                            | Without connection pooling                                                                  |
|                                          | `aurora_connection_tracker,initial_connection,failover_v2,host_monitoring_v2`                  | With connection pooling                                                                     |
| `global_cluster_instance_host_patterns`  | `us-east-2:?.XYZ1.us-east-2.rds.amazonaws.com,us-west-2:?.XYZ2.us-west-2.rds.amazonaws.com` | See [documentation](./using-plugins/UsingTheFailover2Plugin.md)                             |

> **Note:** Add additional plugins as needed for your use case.

> **Warning:** The plugin lists above include `host_monitoring_v2`, which requires a driver that
> supports aborting a connection from a separate thread. The MySQL Connector/Python driver does not,
> so **omit `host_monitoring_v2` when connecting to Aurora MySQL with `mysql-connector-python`** —
> loading it raises `Aborting connections from a separate thread is not supported for the detected
> driver dialect` and the connection never opens. The asynchronous MySQL driver (`aiomysql`) does
> support it. See [Host Monitoring Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md) and the
> [plugin compatibility matrix](./PluginChainCompatibility.md).

### Reader Connections

**Connection String:**
Use the cluster reader endpoint:
```
<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com
```

**Configuration Parameters:**

| Parameter                                | Value                                                                          | Notes                                    |
|------------------------------------------|--------------------------------------------------------------------------------|------------------------------------------|
| `cluster_id`                             | `1`                                                                            | Use the same value as writer connections |
| `wrapper_dialect`                        | `global-aurora-mysql` or `global-aurora-pg`                                    |                                          |
| `plugins`                                | `initial_connection,failover_v2,host_monitoring_v2`                                            | Without connection pooling               |
|                                          | `aurora_connection_tracker,initial_connection,failover_v2,host_monitoring_v2`                  | With connection pooling                  |
| `global_cluster_instance_host_patterns`  | Same as writer configuration                                                   |                                          |
| `failover_mode`                          | `strict-reader` or `reader-or-writer`                                          | Depending on system requirements         |

> **Note:** Add additional plugins as needed for your use case.

> **Warning:** As with writer connections, omit `host_monitoring_v2` when connecting to Aurora MySQL
> with `mysql-connector-python`. See the warning under [Writer Connections](#writer-connections).

## Example Configuration

### PostgreSQL Example

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

# Writer connection
with AwsWrapperConnection.connect(
        Connection.connect,
        "host=my-global-db.global-xyz.global.rds.amazonaws.com dbname=mydb user=admin password=pwd",
        plugins="initial_connection,failover_v2,host_monitoring_v2",
        wrapper_dialect="global-aurora-pg",
        cluster_id="1",
        global_cluster_instance_host_patterns="us-east-1:?.abc123.us-east-1.rds.amazonaws.com,us-west-2:?.def456.us-west-2.rds.amazonaws.com",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT pg_catalog.aurora_db_instance_identifier()")
    print(awscursor.fetchone())

# Reader connection
with AwsWrapperConnection.connect(
        Connection.connect,
        "host=my-cluster.cluster-ro-abc123.us-east-1.rds.amazonaws.com dbname=mydb user=admin password=pwd",
        plugins="initial_connection,failover_v2,host_monitoring_v2",
        wrapper_dialect="global-aurora-pg",
        cluster_id="1",
        global_cluster_instance_host_patterns="us-east-1:?.abc123.us-east-1.rds.amazonaws.com,us-west-2:?.def456.us-west-2.rds.amazonaws.com",
        failover_mode="strict-reader",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT pg_catalog.aurora_db_instance_identifier()")
    print(awscursor.fetchone())
```

### MySQL Example

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from mysql.connector import Connect

# Writer connection
with AwsWrapperConnection.connect(
        Connect,
        "host=my-global-db.global-xyz.global.rds.amazonaws.com database=mydb user=admin password=pwd",
        # host_monitoring_v2 is intentionally absent: mysql-connector-python cannot abort a
        # connection from a separate thread, so the plugin refuses to load on this driver.
        plugins="initial_connection,failover_v2",
        wrapper_dialect="global-aurora-mysql",
        cluster_id="1",
        global_cluster_instance_host_patterns="us-east-1:?.abc123.us-east-1.rds.amazonaws.com,us-west-2:?.def456.us-west-2.rds.amazonaws.com",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT @@aurora_server_id")
    print(awscursor.fetchone())
```

## Important Considerations

### Plugin Selection
- **Connection Pooling**: Include the `aurora_connection_tracker` plugin when using connection pooling.

### Global Cluster Instance Host Patterns
The `global_cluster_instance_host_patterns` parameter is **required** for Aurora Global Databases. Each entry uses the format `<aws-region>:<host-pattern>` or `<aws-region>:<host-pattern>:<port>`. It should contain:
- Comma-separated list of region-prefixed host patterns for each region
- Different cluster identifiers for each region (e.g., `XYZ1`, `XYZ2`)
- Example: `us-east-2:?.XYZ1.us-east-2.rds.amazonaws.com,us-west-2:?.XYZ2.us-west-2.rds.amazonaws.com`

### Restricting to Accessible Regions
If your application can only reach a subset of the regions the global cluster spans, use the `gdb_accessible_regions` property to restrict host selection to those regions. See [Restricting Aurora Global Databases to Accessible Regions](./using-plugins/UsingGlobalAuroraAccessibleRegions.md).

### Monitoring Connection Priority
To control which host role or region the topology monitor uses for its background connection, use the `monitoring_connection_priority` / `gdb_monitoring_connection_priority` properties. See [Monitoring Connection Priority](./using-plugins/UsingMonitoringConnectionPriority.md).

### Authentication Plugins Compatible with GDB
- [IAM Authentication Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)
- [Federated Authentication Plugin](./using-plugins/UsingTheFederatedAuthPlugin.md)
- [Okta Authentication Plugin](./using-plugins/UsingTheOktaAuthPlugin.md)
