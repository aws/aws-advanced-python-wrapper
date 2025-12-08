# Simple Read/Write Splitting Plugin

The Simple Read/Write Splitting Plugin adds functionality to switch between endpoints by setting the `read_only` attribute of an `AwsWrapperConnection`. Based on the values provided in the properties, upon setting `read_only` to `True`, the plugin will connect to the specified endpoint for read operations. When setting `read_only` to `False`, the plugin will connect to the specified endpoint for write operations. Future `read_only` settings will switch between the established writer and reader connections according to the `read_only` setting.

The plugin will use the current connection, which may be the writer or initial connection, as a fallback if the reader connection is unable to be established. This includes when `srw_verify_new_connections` is set to True and no *verified* reader connection is able to be established.

The plugin does not rely on cluster topology. It relies purely on the provided endpoints and their DNS resolution.

## Loading the Simple Read/Write Splitting Plugin

The Simple Read/Write Splitting Plugin is not loaded by default. To load the plugin, include `srw` in the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) connection parameter:

```python
params = {
    "plugins": "srw,failover,host_monitoring_v2",
    # Add other connection properties below...
}

# If using MySQL:
conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)

# If using Postgres:
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
```

## Simple Read/Write Splitting Plugin Parameters
| Parameter                            |  Value  | Required | Description                                                                                                                                                                                                           | Default Value | Example Values                                               |
|--------------------------------------|:-------:|:--------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------------|
| `srw_write_endpoint`                 | String  |   Yes    | The endpoint to connect to upon setting `read_only` to `False`.                                                                                                                                                       | `None`        | `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`    |
| `srw_read_endpoint`                  | String  |   Yes    | The endpoint to connect to upon setting `read_only` to `True`.                                                                                                                                                        | `None`        | `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com` |
| `srw_verify_new_connections`         | Boolean |    No    | Enables writer/reader verification for new connections made by the Simple Read/Write Splitting Plugin. More details below.                                                                                            | `True`        | `True`, `False`                                              |
| `srw_verify_initial_connection_type` | String  |    No    | If `srw_verify_new_connections` is set to `True` and this value is set, the wrapper will verify whether the initial connection matches the specified role. Valid values are 'reader' or 'writer'. More details below. | `None`        | `writer`, `reader`                                           |
| `srw_connect_retry_timeout_ms`       | Integer |    No    | If `srw_verify_new_connections` is set to `True`, this parameter sets the maximum allowed time in milliseconds for retrying connection attempts. More details below.                                                  | `6000`        | `60000`                                                      |
| `srw_connect_retry_interval_ms`      | Integer |    No    | If `srw_verify_new_connections` is set to `True`, this parameter sets the time delay in milliseconds between each retry of opening a connection. More details below.                                                  | `100`         | `1000`                                                       |

> [!NOTE]\
> The following values are only used when verifying new connections with the Simple Read/Write Splitting Plugin:
> - `srw_verify_initial_connection_type`
> - `srw_connect_retry_timeout_ms`
> - `srw_connect_retry_interval_ms` 
> 
> When `srw_verify_new_connections` is set to `False`, these values will have no impact.

## How the Simple Read/Write Splitting Plugin Verifies Connections

The property `srw_verify_new_connections` is enabled by default. This means that when new connections are made with the Simple Read/Write Splitting Plugin, a query is sent to the new connection to verify its role. If the connection cannot be verified as having the correct role, ie. a write connection is not connected to a writer, or a read connection is not connected to a reader, the plugin will retry the connection up to the time limit of `srw_connect_retry_timeout_ms`. 

The values of `srw_connect_retry_timeout_ms` and `srw_connect_retry_interval_ms` control the timing and aggressiveness of the plugin's retries.

Additionally, to consistently ensure the role of connections made with the plugin, the plugin also provides role verification for the initial connection. When connecting with an RDS writer cluster or reader cluster endpoint, the plugin will retry the initial connection up to `srw_connect_retry_timeout_ms` until it has verified the intended role of the endpoint.
If it is unable to return a verified initial connection, it will log a message and continue with the normal workflow of the other plugins.
When connecting with custom endpoints and other non-standard URLs, role verification on the initial connection can also be triggered by providing the expected role through the `srw_verify_initial_connection_type` parameter. Set this to `writer` or `reader` accordingly.

## Limitations When Verifying Connections

#### Non-RDS clusters
The verification step determines the role of the connection by executing a query against it. If the endpoint is not part of an Aurora or RDS cluster, the plugin will not be able to verify the role, so `srw_verify_new_connections` *must* be set to `False`.

#### Autocommit
The verification logic results in errors such as `Cannot change transaction read-only property in the middle of a transaction` from the underlying driver when:
- autocommit is set to false 
- setReadOnly is called 
- as part of setReadOnly, a new connection is opened
- that connection's role is verified

This is a result of the plugin executing the role-verification query against a new connection, and when autocommit is false, this opens a transaction. 

If autocommit is essential to a workflow, either ensure the plugin has connected to the desired target connection of the setReadOnly query before setting autocommit to false or disable `srw_verify_new_connections`.

## Using the Simple Read/Write Splitting Plugin with RDS Proxy

RDS Proxy provides connection pooling and management that significantly improves application scalability by reducing database connection overhead and enabling thousands of concurrent connections through
connection multiplexing. Connecting exclusively through the proxy endpoint ensures consistent connection management, automatic failover handling, and centralized monitoring, while protecting the underlying database from connection exhaustion
and providing a stable abstraction layer that remains consistent even when database topology changes. To take full advantage of the benefits of RDS Proxy, it is recommended to only connect through RDS Proxy endpoints. By providing the read/write endpoint and a read-only endpoint to the Simple Read/Write Splitting Plugin, the AWS Advanced Python Driver will connect using 
these endpoints any time setReadOnly is called. 

There are limitations with the AWS Advanced Python Driver and RDS Proxy. This is currently intended, by design, since RDS Proxy decides which database instance is used based on many criteria (on a per-request basis). Due to this, functionality like [Failover](./UsingTheFailoverPlugin.md), [Enhanced Failure Monitoring](./UsingTheHostMonitoringPlugin.md), and topology-based [Read/Write Splitting](./UsingTheReadWriteSplittingPlugin.md) is not compatible since the driver relies on cluster topology and RDS Proxy handles this automatically. However, the driver can still be used to handle authentication workflows and to perform the endpoint-based read/write splitting of the Simple Read/Write Splitting Plugin. 

## Using the Simple Read/Write Splitting Plugin against non-RDS clusters

The Simple Read/Write Splitting Plugin can be used to switch between any two endpoints. If the endpoints do not direct to an RDS cluster, ensure the property `srw_verify_new_connections` is set to `False`. See [Limitations of srw_verify_new_connections](UsingTheSimpleReadWriteSplittingPlugin.md#non-rds-clusters) for details.

## Limitations

### General plugin limitations

When a `Cursor` is created, it is bound to the database connection established at that moment. Consequently, if the Simple Read/Write Splitting Plugin switches the internal connection, any `Cursor` objects created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any `Cursor` objects created before a change in internal connection. To solve this problem, please ensure you create new `Cursor` objects after switching between the writer/reader.

### Failover

Immediately following a failover event, due to DNS caching, an RDS cluster endpoint may connect to the previous writer, and the read-only endpoint may connect to the new writer instance. 

To avoid stale DNS connections, enable `srw_verify_new_connections`, as this will retry the connection until the role has been verified. Service for Aurora clusters is typically restored in less than 60 seconds, and often less than 30 seconds. RDS Proxy endpoints to Aurora databases can update in as little as 3 seconds. Depending on your configuration and cluster availability `srw_connect_retry_timeout_ms` and `srw_connect_retry_interval_ms` may be set to customize the timing of the retries.

Following failover, endpoints that point to specific instances will be impacted if their target instance was demoted to a reader or promoted to a writer. The Simple Read/Write Splitting Plugin always connects to the endpoint provided in the initial connection properties when the `read_only` attribute is set. We suggest using endpoints that return connections with a specific role such as cluster or read-only endpoints, or using the [Read/Write Splitting Plugin](UsingTheReadWriteSplittingPlugin.md) to connect to instances based on the cluster's current topology.

### Session state

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the Simple Read/Write Splitting Plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

For Postgres:
- read_only
- autocommit
- transaction isolation level

For MySQL:
- autocommit

All other session state attributes will be lost when switching connections between the writer/reader.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes each time that you switch between the writer/reader.

### Sample Code
[Postgres simple read/write splitting sample code](../../examples/PGSimpleReadWriteSplitting.py)  
[MySQL simple read/write splitting sample code](../../examples/MySQLSimpleReadWriteSplitting.py)  

