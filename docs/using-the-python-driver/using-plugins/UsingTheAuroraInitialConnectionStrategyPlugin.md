# Aurora Initial Connection Strategy Plugin
The Aurora Initial Connection Strategy Plugin allows users to configure their initial connection strategy, and it can also be used to obtain a connection more reliably if DNS is updating by replacing an out of date endpoint. When the Aurora Initial Connection Strategy Plugin attempts to make a connection, it may retry the connection attempt if there is a failure. Users are able to configure how often to retry a connection and the maximum allowed time to obtain a connection using the connection parameters.

When this plugin is enabled, if the initial connection is to a reader cluster endpoint, the connected reader host will be chosen based on the configured strategy. The [initial connection strategy](../ReaderSelectionStrategies.md) specifies how the driver determines which available reader to connect to.

This plugin also helps retrieve connections more reliably. When a user connects to a cluster endpoint, the actual instance for a new connection is resolved by DNS. During failover, the cluster elects another instance to be the writer. While DNS is updating, which can take up to 40-60 seconds, if a user tries to connect to the cluster endpoint, they may be connecting to an old node. This plugin helps by replacing the out of date endpoint if DNS is updating.

## Enabling the Aurora Initial Connection Strategy Plugin

To enable the Aurora Initial Connection Strategy Plugin, add `initial_connection` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) value.

## Aurora Initial Connection Strategy Connection Parameters

The following properties can be used to configure the Aurora Initial Connection Strategy Plugin.

| Parameter                                          |  Value  | Required | Description                                                                                                                                                                                                              | Example            | Default Value |
|----------------------------------------------------|:-------:|:--------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------|
| `reader_initial_connection_host_selector_strategy` | String  |    No    | The strategy that will be used to select a new reader host when opening a new connection. <br><br> For more information on the available reader selection strategies, see this [table](../ReaderSelectionStrategies.md). | `leastConnections` | `random`      |
| `open_connection_retry_timeout_ms`                 | Integer |    No    | The maximum allowed time for retries when opening a connection in milliseconds.                                                                                                                                          | `40000`            | `30000`       |
| `open_connection_retry_interval_ms`                | Integer |    No    | The time between retries when opening a connection in milliseconds.                                                                                                                                                      | `2000`             | `1000`        |
