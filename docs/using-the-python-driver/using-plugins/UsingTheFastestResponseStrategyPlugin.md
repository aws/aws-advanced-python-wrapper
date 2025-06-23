## Fastest Response Strategy Plugin
The Fastest Response Strategy Plugin is a host selection strategy plugin monitoring the response time of each reader host, and returns the host with the fastest response time. The plugin stores the fastest host in a cache so it can easily be retrieved again.

The host response time is measured at an interval set by `response_measurement_interval_ms`, at which time the old cache expires and is updated with the current fastest host.

## Using the Fastest Response Strategy Plugin

The plugin can be loaded by adding the plugin code `fastest_response_strategy` to the [`plugins`](../UsingThePythonDriver.md#aws-advanced-python-driver-parameters) parameter. The Fastest Response Strategy Plugin is not loaded by default, and must be loaded along with the [`read_write_splitting`](./UsingTheReadWriteSplittingPlugin.md) plugin.

> [!IMPORTANT]\
> **`reader_response_strategy` must be set to `fastest_reponse` when using this plugin. Otherwise an error will be thrown:** 
> `Unsupported host selector strategy: random. To use the fastest response strategy plugin, please ensure the property reader_host_selector_strategy is set to fastest_response.`

```python
params = {
    "plugins": "read_write_splitting,fastest_response_strategy,failover,host_monitoring",
    "reader_response_strategy": "fastest_response"
    # Add other connection properties below...
}

# If using MySQL:
conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)

# If using Postgres:
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
```

## Configuration Parameters
| Parameter                          |   Value   | Required | Description                                                                                                                          | Default Value |
|------------------------------------|:---------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `reader_response_strategy`         | `String`  |   Yes    | Setting to `fastest_reponse` sets the reader response strategy to choose the fastest host using the Fastest Response Strategy Plugin | `random`      |
| `response_measurement_interval_ms` | `Integer` |    No    | Interval in milliseconds between measuring response time to a database host                                                          | `30_000`      |


## Host Response Time Monitor
The Host Response Time Monitor measures the host response time in a separate monitoring thread. If the monitoring thread has not been called for a response time for 10 minutes, the thread is stopped. When the topology changes, the new hosts will be added to monitoring.

The host response time monitoring thread creates new database connections. By default it uses the same set of connection parameters provided for the main connection, but you can customize these connections with the `frt-` prefix, as in the following example:

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection

conn = AwsWrapperConnection.connect(
    psycopg.Connection.connect,
    host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
    dbname="postgres",
    user="john",
    password="pwd",
    plugins="read_write_splitting,fastest_response_strategy",
    "reader_response_strategy": "fastest_response",
    # Configure the timeout values for all non-monitoring connections.
    connect_timeout=30,
    # Configure different timeout values for the host response time monitoring connection.
    frt-connect_timeout=10)
```

> [!IMPORTANT]\
> **When specifying a frt- prefixed timeout, always ensure you provide a non-zero timeout value**

### Sample Code
[Postgres fastest response strategy sample code](../../examples/PGFastestResponseStrategy.py)
[MySQL fastest response strategy sample code](../../examples/MySQLFastestResponseStrategy.py)
