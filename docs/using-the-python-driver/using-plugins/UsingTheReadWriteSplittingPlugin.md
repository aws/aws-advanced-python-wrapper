## Read/Write Splitting Plugin

The Read/Write Splitting Plugin adds functionality to switch between writer and reader instances by setting the `read_only` attribute of an `AwsWrapperConnection`. When setting `read_only` to `True`, the plugin will establish a connection to a reader instance and direct subsequent queries to this instance. Future `read_only` settings will switch the underlying connection between the established writer and reader according to the `read_only` setting.

### Loading the Read/Write Splitting Plugin

The Read/Write Splitting Plugin is not loaded by default. To load the plugin, include `read_write_splitting` in the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) connection parameter:

```python
params = {
    "plugins": "read_write_splitting,failover,host_monitoring",
    # Add other connection properties below...
}

# If using MySQL:
conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)

# If using Postgres:
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
```

### Supplying the connection string

When using the Read/Write Splitting Plugin against Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply only the URL for the initial connection. The Read/Write Splitting Plugin will automatically discover the URLs for the other instances in the cluster and will use this info to switch between the writer/reader when `read_only` is set. Note however that you must set the [`cluster_instance_host_pattern`](./UsingTheFailoverPlugin.md#failover-parameters) if you are connecting using an IP address or custom domain.

> ![IMPORTANT]\
> you must set the [`cluster_instance_host_pattern`](./UsingTheFailoverPlugin.md#failover-parameters) if you are connecting using an IP address or custom domain.

### Using the Read/Write Splitting Plugin against non-Aurora clusters

The Read/Write Splitting Plugin is not currently supported for non-Aurora clusters.

### Internal connection pooling

> [!IMPORTANT]\
> If internal connection pools are enabled, database passwords may not be verified with every connection request. The initial connection request for each database instance in the cluster will verify the password, but subsequent requests may return a cached pool connection without re-verifying the password. This behavior is inherent to the nature of connection pools and not a bug with the driver. `ConnectionProviderManager.release_resources` can be called to close all pools and remove all cached pool connections. Take a look at the [Postgres](../../examples/PGInternalConnectionPoolPasswordWarning.py) or [MySQL](../../examples/MySQLInternalConnectionPoolPasswordWarning.py) sample code for more details.

When `read_only` is first set on an `AwsWrapperConnection` object, the Read/Write Splitting Plugin will internally open a physical connection to a reader. The reader connection will then be cached for the given `AwsWrapperConnection`. Future `read_only` settings for the same `AwsWrapperConnection` will not open a new physical connection. However, other `AwsWrapperConnection` objects will need to open their own reader connections when `read_only` is first set. If your application frequently sets `read_only` on many `AwsWrapperConnection` objects, you can enable internal connection pooling to improve performance. When enabled, the AWS Advanced Python Driver will maintain an internal connection pool for each instance in the cluster. This allows the Read/Write Splitting Plugin to reuse connections that were established by previous `AwsWrapperConnection` objects.

> [!NOTE]\
> Initial connections to a cluster URL will not be pooled because it can be problematic to pool a URL that resolves to different instances over time. The main benefit of internal connection pools occurs when `read_only` is set. When `read_only` is set, an internal pool will be created for the instance that the plugin switches to, regardless of the initial connection URL. Connections for that instance can then be reused in the future.

The AWS Advanced Python Driver currently uses [SqlAlchemy](https://docs.sqlalchemy.org/en/20/core/pooling.html) to create and maintain its internal connection pools. The [Postgres](../../examples/PGReadWriteSplitting.py) or [MySQL](../../examples/MySQLReadWriteSplitting.py) sample code provides a useful example of how to enable this feature. The steps are as follows:

1.  Create an instance of `SqlAlchemyPooledConnectionProvider`, passing in any optional arguments if desired:
    - The first optional argument takes in a `Callable` that can be used to return custom parameter settings for the connection pool. See [here](https://docs.sqlalchemy.org/en/20/core/pooling.html#sqlalchemy.pool.QueuePool) for a list of available parameters. Note that you should not set the `creator` parameter - this parameter will be set automatically by the AWS Advanced Python Driver. This is done to follow desired behavior and ensure that the Read/Write Splitting Plugin can successfully establish connections to new instances.
    - The second optional argument takes in a `Callable` that can be used to define custom key generation for the internal connection pools. Key generation is used to define when new connection pools are created; a new pool will be created each time a connection is requested with a unique key. By default, a new pool will be created for each unique instance-user combination. If you would like to define a different key system, you should pass in a `Callable` defining this logic. The `Callable` should take in a `HostInfo` and `Dict` specifying the connection properties and return a string representing the desired connection pool key. A simple example is shown below.

    > [!IMPORTANT]\
    > If you do not include the username in the connection pool key, connection pools may be shared between different users. As a result, an initial connection established with a privileged user may be returned to a connection request with a lower-privilege user without re-verifying credentials. This behavior is inherent to the nature of connection pools and not a bug with the driver. `ConnectionProviderManager.release_resources` can be called to close all pools and remove all cached pool connections.

    ```python
    def get_pool_key(host_info: HostInfo, props: Dict[str, Any]):
      # Include the URL, user, and database in the connection pool key so that a new
      # connection pool will be opened for each different instance-user-database combination.
      url = host_info.url
      user = props["username"];
      db = props["dbname"]
      return f"{url}{user}{db}"
    
    provider = SqlAlchemyPooledConnectionProvider(
        lambda host_info, props: {"pool_size": 5},
        get_pool_key)
    ConnectionProviderManager.set_connection_provider(provider)
    ```

2. Call `ConnectionProviderManager.set_connection_provider`, passing in the `SqlAlchemyPooledConnectionProvider` you created in step 1.

3. By default, the Read/Write Splitting Plugin randomly selects a reader instance when `read_only` is first called. If you would like the plugin to select a reader based on a different connection strategy, please see the [Connection Strategies](#connection-strategies) section for more information.

4. Continue as normal: create connections and use them as needed.

5. When you are finished using all connections, call `ConnectionProviderManager.release_resources()`.

> [!IMPORTANT]\
> You must call `ConnectionProviderManager.release_resources` to close the internal connection pools when you are finished using all connections. Unless `ConnectionProviderManager.release_resources` is called, the AWS Advanced Python Driver will keep the pools open so that they can be shared between connections.

### Sample Code
[Postgres read/write splitting sample code](../../examples/PGReadWriteSplitting.py)  
[MySQL read/write splitting sample code](../../examples/MySQLReadWriteSplitting.py)
[Postgres internal connection pool password warning sample code](../../examples/PGInternalConnectionPoolPasswordWarning.py)
[MySQL internal connection pool password warning sample code](../../examples/MySQLInternalConnectionPoolPasswordWarning.py)

### Connection Strategies
By default, the Read/Write Splitting Plugin randomly selects a reader instance the first time that `read_only` is set. To balance connections to reader instances more evenly, different connection strategies can be used. The following table describes the currently available connection strategies and any relevant configuration parameters for each strategy.

To indicate which connection strategy to use, the `reader_host_selector_strategy` parameter can be set to one of the connection strategies in the table below. The following is an example of enabling the least connections strategy:

```python
params = {
    "reader_host_selector_strategy": "least_connections",
    # Add other connection properties below...
}

# If using MySQL:
conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)

# If using Postgres:
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
```

| Connection Strategy | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Default Value |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `random`            | The random strategy is the default connection strategy. When switching to a reader connection, the reader instance will be chosen randomly from the available database instances.                                                                                                                                                                                                                                                                                                                                                                                 | N/A           |
| `least_connections` | The least connections strategy will select reader instances based on which database instance has the least number of currently active connections. Note that this strategy is only available when internal connection pools are enabled - if you set the connection property without enabling internal pools, an exception will be thrown.                                                                                                                                                                                                                        | N/A           |
| `round_robin`        | See the following rows for configuration parameters.  | The round robin strategy will select a reader instance by taking turns with all available database instances in a cycle. A slight addition to the round robin strategy is the weighted round robin strategy, where more connections will be passed to reader instances based on user specified connection properties.                                                                                                                                                                                                                                             | N/A           |
|                     | `round_robin_host_weight_pairs`                           | This parameter value must be a `string` type comma separated list of database host-weight pairs in the format `<host>:<weight>`. The host represents the database instance name, and the weight represents how many connections should be directed to the host in one cycle through all available hosts. For example, the value `instance-1:1,instance-2:4` means that for every connection to `instance-1`, there will be four connections to `instance-2`. <br><br> **Note:** The `<weight>` value in the string must be an integer greater than or equal to 1. | `null`        |
|                     | `round_robin_default_weight`                             | This parameter value must be an integer value. This parameter represents the default weight for any hosts that have not been configured with the `round_robin_host_weight_pairs` parameter. For example, if a connection were already established and host weights were set with `round_robin_host_weight_pairs` but a new reader node was added to the database, the new reader node would use the default weight. <br><br> **Note:** This value must be an integer greater than or equal to 1.                                                | 1           |

### Limitations

#### General plugin limitations

When a `Cursor` is created, it is bound to the database connection established at that moment. Consequently, if the Read/Write Splitting Plugin switches the internal connection, any `Cursor` objects created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any `Cursor` objects created before a change in internal connection. To solve this problem, please ensure you create new `Cursor` objects after switching between the writer/reader.

#### Session state limitations

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the Read/Write Splitting Plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

For Postgres:
- read_only
- autocommit
- transaction isolation level

For MySQL:
- autocommit

All other session state attributes will be lost when switching connections between the writer/reader.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes each time that you switch between the writer/reader.
