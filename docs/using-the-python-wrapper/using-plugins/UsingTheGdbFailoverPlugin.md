# Global Database (GDB) Failover Plugin
The AWS Advanced Python Wrapper uses the GDB Failover Plugin to provide minimal downtime in the event of a DB instance failure for [Amazon Aurora Global Databases](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database.html). The plugin is based on the [Failover Plugin v2](./UsingTheFailover2Plugin.md) and unless explicitly stated otherwise, most of the information and suggestions for the [Failover Plugin](./UsingTheFailoverPlugin.md) and [Failover Plugin v2](./UsingTheFailover2Plugin.md) are applicable to the GDB Failover Plugin.

## Differences between the GDB Failover Plugin and the Failover Plugin v2

The GDB Failover Plugin introduces the notion of a *home region* and extends the configuration parameters to allow setting different failover logic for **in-home** and **out-of-home** cases.

Driver configuration for the GDB Failover Plugin should include a home region defined with an AWS region name. This introduces two cases:
- **in-home**: when the primary region of the Global Database is the same as the specified home region.
- **out-of-home**: when GDB switches over to another region and the primary region is not the same as the specified home region.

Users are allowed to specify different failover logic individually for the **in-home** and **out-of-home** cases. The following are two examples where in-home and out-of-home cases impact the failover logic.

**Example 1**

When a user application needs a writer connection, it makes sense to define the failover mode to follow the writer (`strict-writer`). However, some applications may choose not to follow a writer host when cross-region failover occurs (see [Configuration Example 3](#configuration-example-3) below).
- **in-home**: when in-region failover occurs, the wrapper reconnects to a new writer host and continues serving the user application with a write connection.
- **out-of-home**: the application turns to inactive mode and stops performing writes, prioritizing performance due to reduced connection latency over being connected to a writer host in another region.

**Example 2**

When a user application needs a reader connection, prioritize reader connections in the home region to reduce connection latency (see [Configuration Example 2](#configuration-example-2) below).
- **in-home**: the primary region of the Global Database is the same as the specified home region; connect to any such reader.
- **out-of-home**: connect to readers that are in the specified home region; these readers are no longer part of the primary region of the Global Database.

The mentioned scenarios can be configured with the GDB Failover Plugin. The [Failover Plugin v2](./UsingTheFailover2Plugin.md) and [Failover Plugin](./UsingTheFailoverPlugin.md) don't support the home region and can't be used for the cases mentioned above.

## Using the GDB Failover Plugin
The GDB Failover Plugin will not be enabled by default. To enable it, explicitly include the plugin code `gdb_failover` in the [`plugins`](../UsingThePythonWrapper.md#connection-plugin-manager-parameters) value, or add it to the current [driver profile](../UsingThePythonWrapper.md#connection-plugin-manager-parameters). After you load the plugin, the failover feature will be enabled.

Please refer to the [failover configuration guide](../FailoverConfigurationGuide.md) for tips to keep in mind when using the failover plugins.

> [!WARNING]
> Do not use any combination of the `gdb_failover`, `failover`, and/or `failover_v2` plugins at the same time for the same connection!

### GDB Failover Plugin Configuration Parameters
In addition to the parameters that you can configure for the underlying driver, you can pass the following parameters for the AWS Advanced Python Wrapper to specify additional failover behavior.

| Parameter                                | Value   |                                                                  Required                                                                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                              | Default Value                                                                                                                                              |
|------------------------------------------|:--------|:------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `failover_home_region`                   | String  | If connecting using an IP address, a custom domain URL, a Global Database endpoint, or another endpoint with no region: Yes<br><br>Otherwise: No | Defines a home region. Examples: `us-west-2`, `us-east-1`. If this parameter is omitted, the value is parsed from the connection url. For regional cluster endpoints and instance endpoints, it's set to the region of the provided endpoint. If the provided endpoint has no region (for example, a Global Database endpoint or IP address) this parameter is mandatory.                                                                                  |                                                                                                                                                            |
| `active_home_failover_mode`              | String  |                                                                     No                                                                      | Defines the failover mode when the GDB primary region **is the home region**. Possible values: `strict-writer`, `strict-home-reader`, `strict-out-of-home-reader`, `strict-any-reader`, `home-reader-or-writer`, `out-of-home-reader-or-writer`, `any-reader-or-writer`. See [Failover Modes](#failover-modes) below.                                                                                                                                     | Depends on connection url. For an Aurora writer cluster endpoint or Global Database endpoint, it's `strict-writer`. Otherwise, it's `home-reader-or-writer`. |
| `inactive_home_failover_mode`            | String  |                                                                     No                                                                      | Defines the failover mode when the GDB primary region **is not the home region**. Possible values are the same as for `active_home_failover_mode`.                                                                                                                                                                                                                                                                                                       | Depends on connection url. For an Aurora writer cluster endpoint or Global Database endpoint, it's `strict-writer`. Otherwise, it's `home-reader-or-writer`. |
| `global_cluster_instance_host_patterns`  | String  |                                               For Global Databases: Yes<br><br>Otherwise: No                                                | A comma-separated list of instance host patterns, one per region of the Global Database. A `?` character in each pattern is a placeholder for the DB instance identifiers. Each entry uses the format `region:host:port` or `region:host`. Example: `us-east-2:?.XYZ1.us-east-2.rds.amazonaws.com,us-west-2:?.XYZ2.us-west-2.rds.amazonaws.com`.                                                                                                          |                                                                                                                                                            |
| `failover_timeout_sec`                   | Integer |                                                                     No                                                                      | Maximum allowed time in seconds to attempt reconnecting to a new writer or reader instance after a cluster failover is initiated.                                                                                                                                                                                                                                                                                                                        | `300`                                                                                                                                                      |
| `cluster_topology_high_refresh_rate_ms`  | Integer |                                                                     No                                                                      | Interval of time in milliseconds to wait between attempts to update cluster topology after the writer has come back online following a failover event.                                                                                                                                                                                                                                                                                                  | `100`                                                                                                                                                      |
| `failover_reader_host_selector_strategy` | String  |                                                                     No                                                                      | Strategy used to select a reader host during failover. For more information on the available reader selection strategies, see this [table](../ReaderSelectionStrategies.md).                                                                                                                                                                                                                                                                             | `random`                                                                                                                                                   |
| `telemetry_failover_additional_top_trace`| Boolean |                                                                     No                                                                      | Allows the wrapper to produce an additional telemetry span associated with failover.                                                                                                                                                                                                                                                                                                                                                                    | `False`                                                                                                                                                    |

### Failover Modes
The `active_home_failover_mode` and `inactive_home_failover_mode` parameters accept the following values:

- `strict-writer` - Failover follows the writer host and connects to a new writer (in any region) when it changes.
- `strict-home-reader` - Connect to any available reader host in the home region. If no reader is available, raise an error.
- `strict-out-of-home-reader` - Connect to any available reader host that is *not* in the home region. If no reader is available, raise an error.
- `strict-any-reader` - Connect to any available reader host in any region. If no reader is available, raise an error.
- `home-reader-or-writer` - Connect to any available reader host in the home region. If no such reader is available, connect to a writer host (in any region).
- `out-of-home-reader-or-writer` - Connect to any available reader host that is *not* in the home region. If no such reader is available, connect to a writer host (in any region).
- `any-reader-or-writer` - Connect to any available reader or writer host in any region.

Please refer to the original [Failover Plugin](./UsingTheFailoverPlugin.md) and [Failover Plugin v2](./UsingTheFailover2Plugin.md) for more details about error codes, configurations, connection pooling, and sample code.

### Failover Configuration Examples

#### Configuration Example 1
**Goal:** Provide a user application with a writer connection. The application is deployed in the `us-west-1` region and connects to a Global Database with the `us-east-1`, `us-east-2`, and `us-west-1` regions.

**Solution:** Use the following configuration parameters:
- `failover_home_region=us-west-1`
- `active_home_failover_mode=strict-writer`
- `inactive_home_failover_mode=strict-writer`
- `global_cluster_instance_host_patterns=us-east-1:?.XYZ1.us-east-1.rds.amazonaws.com,us-east-2:?.XYZ2.us-east-2.rds.amazonaws.com,us-west-1:?.XYZ3.us-west-1.rds.amazonaws.com` (replace `XYZ1`, `XYZ2`, `XYZ3` with values that correspond to your database)
- `dialect=global-aurora-mysql` (or `global-aurora-pg`)
- `plugins=initial_connection,gdb_failover,host_monitoring_v2`
- use the Global Database endpoint in your connection string

#### Configuration Example 2
**Goal:** Provide a user application with a reader connection in `us-west-1`. The application is deployed in the `us-west-1` region. If the GDB primary region switches over, prioritize reader connections from the `us-west-1` region.

**Solution:** Use the following configuration parameters:
- `failover_home_region=us-west-1`
- `active_home_failover_mode=strict-home-reader`
- `inactive_home_failover_mode=strict-home-reader`
- `global_cluster_instance_host_patterns=us-east-1:?.XYZ1.us-east-1.rds.amazonaws.com,us-east-2:?.XYZ2.us-east-2.rds.amazonaws.com,us-west-1:?.XYZ3.us-west-1.rds.amazonaws.com`
- `dialect=global-aurora-mysql` (or `global-aurora-pg`)
- `plugins=initial_connection,gdb_failover,host_monitoring_v2`
- use the cluster reader endpoint in region `us-west-1` in your connection string

#### Configuration Example 3
**Goal:** Provide a user application with a writer connection when the primary region is closest to the deployed region. When the primary region switches over to another region and network latency becomes unacceptable, the application deployment in region `us-west-1` becomes inactive and lets applications in other regions process business transactions.

**Solution:** Use the following configuration parameters for the application deployment in the `us-west-1` region:
- `failover_home_region=us-west-1`
- `active_home_failover_mode=strict-writer`
- `inactive_home_failover_mode=strict-any-reader`
- `global_cluster_instance_host_patterns=us-east-1:?.XYZ1.us-east-1.rds.amazonaws.com,us-east-2:?.XYZ2.us-east-2.rds.amazonaws.com,us-west-1:?.XYZ3.us-west-1.rds.amazonaws.com`
- `dialect=global-aurora-mysql` (or `global-aurora-pg`)
- `plugins=initial_connection,gdb_failover,host_monitoring_v2`
- use the cluster writer endpoint in region `us-west-1` in your connection string

**Explanation:** While the primary region of the GDB is `us-west-1`, the application deployed in this region needs writable connections. By using the cluster writer endpoint, the wrapper opens connections to a writer host. If in-region failover occurs, the wrapper reconnects to a new writer host in the same `us-west-1` region (in-home, `active_home_failover_mode=strict-writer`). When a cross-region failover event occurs and the new writer is, for example, in `us-east-1`, the wrapper uses `inactive_home_failover_mode=strict-any-reader` and serves a reader connection from any available region.

> [!WARNING]
> These examples cover failover and failover-related settings only. A complete driver configuration may require settings for other plugins.
