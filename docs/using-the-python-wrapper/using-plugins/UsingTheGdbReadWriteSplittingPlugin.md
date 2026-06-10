# Global Database (GDB) Read/Write Splitting Plugin

The GDB Read/Write Splitting Plugin extends the functionality of the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) and adds settings tailored to Aurora Global Databases.

The plugin introduces the concept of a *home region* and lets you constrain new connections to that region. Such restrictions are helpful in environments where remote AWS regions add latency that cannot be tolerated.

Unless otherwise stated, all recommendations, configurations, and code examples for the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) apply to the GDB Read/Write Splitting Plugin.

## Loading the GDB Read/Write Splitting Plugin

The GDB Read/Write Splitting Plugin is not loaded by default. To load it, include `gdb_rw` in the [`plugins`](../UsingThePythonWrapper.md#connection-plugin-manager-parameters) connection parameter.
```python
params = {
    "plugins": "gdb_rw,failover_v2,host_monitoring_v2",
    # Add other connection properties below...
}

# If using MySQL:
conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)

# If using Postgres:
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
```

If you would like to use the GDB Read/Write Splitting Plugin without the failover plugin, list `gdb_rw` on its own:

```python
params = {
    "plugins": "gdb_rw",
    # Add other connection properties below...
}
```

> [!WARNING]
> Do not use the `read_write_splitting`, `srw`, and/or `gdb_rw` plugins (or any combination of them) at the same time on the same connection.

## Using the GDB Read/Write Splitting Plugin against non-GDB clusters

The GDB Read/Write Splitting Plugin can be used against Aurora and RDS clusters. However, since these cluster types are single-region, configuring a home region adds little value. Instead, we recommend using the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md).

## Configuration Parameters

| Parameter                              |  Value  |                                                                  Required                                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                        | Default Value                                                                                                                    |
|----------------------------------------|:-------:|:------------------------------------------------------------------------------------------------------------------------------------------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `reader_host_selector_strategy`        | String  |                                                                     No                                                                     | The name of the strategy used to select a new reader host. For more information on the available reader selection strategies, see the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md#connection-strategies) docs.                                                                                                                                                             | `random`                                                                                                                         |
| `gdb_rw_home_region`                   | String  | If connecting using an IP address, a custom domain URL, a Global Database endpoint, or any other endpoint without a region: Yes<br><br>Otherwise: No | Defines the home region.<br><br>Examples: `us-west-2`, `us-east-1`.<br><br>If this parameter is omitted, the value is parsed from the connection URL. For regional cluster endpoints and instance endpoints, it is set to the region of the provided endpoint. If the endpoint has no region (for example, a Global Database endpoint or an IP address), the configuration parameter is mandatory. | For regional cluster endpoints and instance endpoints, it is set to the region of the provided endpoint.<br><br>Otherwise: `None` |
| `gdb_rw_restrict_writer_to_home_region`| Boolean |                                                                     No                                                                     | If set to `True`, prevents following or connecting to a writer node outside the defined home region. An exception will be raised when a connection to a writer outside the home region is requested.                                                                                                                                                                                               | `True`                                                                                                                           |
| `gdb_rw_restrict_reader_to_home_region`| Boolean |                                                                     No                                                                     | If set to `True`, prevents connecting to a reader node outside the defined home region. If no reader nodes in the home region are available, an exception will be raised.                                                                                                                                                                                                                          | `True`                                                                                                                           |
| `gdb_enable_global_write_forwarding`   | Boolean |                                                                     No                                                                     | If set to `True`, allows connections in the secondary region to forward write queries to the primary global region. This is useful when your home region is the secondary global region. This functionality requires [Global Write Forwarding](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-write-forwarding.html) to be enabled on the cluster.            | `False`                                                                                                                          |

Refer to the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) docs for more details on error handling, connection pooling, and sample code; all of that information applies here.
