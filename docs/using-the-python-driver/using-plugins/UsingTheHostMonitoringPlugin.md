# Host Monitoring Plugin

## Enhanced Failure Monitoring

The figure that follows shows a simplified Enhanced Failure Monitoring (EFM) workflow. Enhanced Failure Monitoring is a feature available from the Host Monitoring Connection Plugin. The Host Monitoring Connection Plugin periodically checks the connected database host's health or availability. If a database host is determined to be unhealthy, the connection will be aborted. The Host Monitoring Connection Plugin uses the [Enhanced Failure Monitoring Parameters](#enhanced-failure-monitoring-parameters) and a database host's responsiveness to determine whether a host is healthy.

<div style="text-align:center"><img src="../../images/enhanced_failure_monitoring_diagram.png"/></div>

### The Benefits of Enhanced Failure Monitoring

Enhanced Failure Monitoring helps user applications detect failures earlier. When a user application executes a query, EFM may detect that the connected database host is unavailable. When this happens, the query is cancelled and the connection will be aborted. This allows queries to fail fast instead of waiting indefinitely or failing due to a timeout.

One use case is to pair EFM with the [Failover Connection Plugin](./UsingTheFailoverPlugin.md). When EFM discovers a database host failure, the connection will be aborted. Without the Failover Connection Plugin, the connection would be terminated up to the user application level. With the Failover Connection Plugin, the AWS Advanced Python Driver can attempt to failover to a different, healthy database host where the query can be executed.

### Using the Host Monitoring Connection Plugin

The Host Monitoring Connection Plugin will be loaded by default if the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter is not specified. The Host Monitoring Connection Plugin can also be explicitly loaded by adding the plugin code `host_monitoring` to the [`plugins`](../UsingThePythonDriver.md#aws-advanced-python-driver-parameters) parameter. Enhanced Failure Monitoring is enabled by default when the Host Monitoring Connection Plugin is loaded, but it can be disabled by setting the `failure_detection_enabled` parameter to `False`.

### Enhanced Failure Monitoring Parameters

<div style="text-align:center"><img src="../../images/monitor_process.png" /></div>

The parameters `failure_detection_time_ms`, `failure_detection_interval_ms`, and `failure_detection_count` are similar to TCP Keepalive parameters. Each connection has its own set of parameters. `failure_detection_time_ms` controls how long the monitor waits after a SQL query is executed before sending a probe to the database host. `failure_detection_interval_ms` controls how often the monitor sends probes to the database after the initial probe. `failure_detection_count` controls how many times a monitor probe can go unacknowledged before the database host is deemed unhealthy. 

To determine the health of a database host: 
1. The monitor will first wait for a time equivalent to the `failure_detection_time_ms`. 
2. Then, every `failure_detection_interval_ms`, the monitor will send a probe to the database host. 
3. If the probe is not acknowledged by the database host, a counter is incremented. 
4. If the counter reaches the `failure_detection_count`, the database host will be deemed unhealthy and the connection will be aborted.

If a more aggressive approach to failure checking is necessary, all of these parameters can be reduced to reflect that. However, increased failure checking may also lead to an increase in false positives. For example, if the `failure_detection_interval_ms` was shortened, the plugin may complete several connection checks that all fail. The database host would then be considered unhealthy, but it may have been about to recover and the connection checks were completed before that could happen.

| Parameter                       |  Value  | Required | Description                                                                                                  | Default Value |
|---------------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------|---------------|
| `failure_detection_enabled`     | Boolean |    No    | Set to `True` to enable Enhanced Failure Monitoring. Set to `False` to disable it.                           | `True`        |
| `monitor_disposal_time_ms`      | Integer |    No    | Interval in milliseconds specifying how long to wait before an inactive monitor should be disposed.          | `60000`       |
| `failure_detection_count`       | Integer |    No    | Number of failed connection checks before considering database host as unhealthy.                            | `3`           |
| `failure_detection_interval_ms` | Integer |    No    | Interval in milliseconds between probes to database host.                                                    | `5000`        |
| `failure_detection_time_ms`     | Integer |    No    | Interval in milliseconds between sending a SQL query to the server and the first probe to the database host. | `30000`       |

The Host Monitoring Connection Plugin may create new monitoring connections to check the database host's availability. You can configure these connections with driver-specific configurations by adding the `monitoring-` prefix to the configuration parameters, as in the following example:

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection

conn = AwsWrapperConnection.connect(
    psycopg.Connection.connect,
    host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
    dbname="postgres",
    user="john",
    password="pwd",
    plugins="host_monitoring",
    # Configure the timeout values for all non-monitoring connections.
    connect_timeout=30, socket_timeout=30,
    # Configure different timeout values for the monitoring connections.
    monitoring - connect_timeout = 10, monitoring - socket_timeout = 10)
```

> [!IMPORTANT]\
> **If specifying a monitoring- prefixed timeout, always ensure you provide a non-zero timeout value**

>[!WARNING]\
> Warnings About Usage of the AWS Advanced Python Driver with RDS Proxy
> We recommend you either disable the Host Monitoring Connection Plugin or avoid using RDS Proxy endpoints when the Host Monitoring Connection Plugin is active.
>
> Although using RDS Proxy endpoints with the AWS Advanced Python Driver with Enhanced Failure Monitoring doesn't cause any critical issues, we don't recommend this approach. The main reason is that RDS Proxy transparently re-routes requests to a single database instance. RDS Proxy decides which database instance is used based on many criteria (on a per-request basis). Switching between different instances makes the Host Monitoring Connection Plugin useless in terms of instance health monitoring because the plugin will be unable to identify which instance it's connected to, and which one it's monitoring. This could result in false positive failure detections. At the same time, the plugin will still proactively monitor network connectivity to RDS Proxy endpoints and report outages back to a user application if they occur.
