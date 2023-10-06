## Read-Write Splitting Plugin

The read-write splitting plugin adds functionality to switch between writer and reader instances by setting the `read_only` property of an `AwsWrapperConnection`. When setting `read_only` to True, the plugin will establish a connection to a reader instance and direct subsequent queries to this instance. Future `read_only` settings will switch the underlying connection between the established writer and reader according to the `read_only` setting.

### Loading the Read-Write Splitting Plugin

The read-write splitting plugin is not loaded by default. To load the plugin, include `read_write_splitting` in the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) connection parameter. If you would like to load the read-write splitting plugin with the failover and host monitoring plugins, the read-write splitting plugin must be listed before these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example below to properly load the read-write splitting plugin with these plugins.

```
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

When using the read-write splitting plugin against Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply only the URL for the initial connection. The read-write splitting plugin will automatically discover the URLs for the other instances in the cluster and will use this info to switch between the writer/reader when `read_only` is set. Note however that you must set the [`cluster_instance_host_pattern`](./UsingTheFailoverPlugin.md#failover-parameters) if you are connecting using an IP address or custom domain.

### Using the Read-Write Splitting Plugin against non-Aurora clusters

The read-write splitting plugin is not currently supported for non-Aurora clusters.

### Internal connection pooling

> :warning: If internal connection pools are enabled, database passwords may not be verified with every connection request. The initial connection request for each database instance in the cluster will verify the password, but subsequent requests may return a cached pool connection without re-verifying the password. This behavior is inherent to the nature of connection pools and not a bug with the driver. `ConnectionProviderManager.releaseResources` can be called to close all pools and remove all cached pool connections. See [InternalConnectionPoolPasswordWarning.java](../../examples/InternalConnectionPoolPasswordWarning.py) for more details.

When `read_only` is first set on an `AwsWrapperConnection` object, the read-write plugin will internally open a physical connection to a reader. The reader connection will then be cached for the given `AwsWrapperConnection`. Future `read_only` settings for the same `AwsWrapperConnection` will not open a new physical connection. However, other `AwsWrapperConnection` objects will need to open their own reader connections when `read_only` is first set. If your application frequently sets `read_only` on many `AwsWrapperConnection` objects, you can enable internal connection pooling to improve performance. When enabled, the wrapper driver will maintain an internal connection pool for each instance in the cluster. This allows the read-write plugin to reuse connections that were established by previous `AwsWrapperConnection` objects.

> Note: Initial connections to a cluster URL will not be pooled because it can be problematic to pool a URL that resolves to different instances over time. The main benefit of internal connection pools occurs when `read_only` is set. When `read_only` is set, an internal pool will be created for the instance that the plugin switches to, regardless of the initial connection URL. Connections for that instance can then be reused in the future.

The wrapper driver currently uses [SqlAlchemy](https://docs.sqlalchemy.org/en/20/core/pooling.html) to create and maintain its internal connection pools. The [Postgres](../../examples/PGReadWriteSplitting.py) or [MySQL](../../examples/MySQLReadWriteSplitting.py) sample code provides a useful example of how to enable this feature. The steps are as follows:

1.  Create an instance of `SqlAlchemyPooledConnectionProvider`, passing in any optional arguments if desired:
    - The first optional argument takes in a `Callable` that can be used to return custom parameter settings for the connection pool. See [here](https://docs.sqlalchemy.org/en/20/core/pooling.html#sqlalchemy.pool.QueuePool) for a list of available parameters. Note that you should not set the `creator` parameter - this parameter will be set automatically by the wrapper driver. This is done to follow desired behavior and ensure that the read-write plugin can successfully establish connections to new instances.
    - The second optional argument takes in a `Callable` that can be used to define custom key generation for the internal connection pools. Key generation is used to define when new connection pools are created; a new pool will be created each time a connection is requested with a unique key. By default, a new pool will be created for each unique instance-user combination. If you would like to define a different key system, you should pass in a `Callable` defining this logic. The `Callable` should take in a HostInfo and Dict and return a string representing the desired connection pool key. A simple example is shown below. Please see the [Postgres](../../examples/PGReadWriteSplitting.py) or [MySQL](../../examples/MySQLReadWriteSplitting.py) sample code for a full example.

    > :warning: If you do not include the username in the connection pool key, connection pools may be shared between different users. As a result, an initial connection established with a privileged user may be returned to a connection request with a lower-privilege user without re-verifying credentials. This behavior is inherent to the nature of connection pools and not a bug with the driver. `ConnectionProviderManager.releaseResources` can be called to close all pools and remove all cached pool connections.

    ```
    def get_pool_key(host_info: HostInfo, props: Dict[str, Any]):
      # Include the URL, user, and database in the connection pool key so that a new
      # connection pool will be opened for each different instance-user-database combination.
      url = host_info.url
      user = props["username"];
      db = props["dbname"]
      return f"{url}{user}{db}"
    
    provider = SqlAlchemyPooledConnectionProvider(
        lambda host_info, props: {"pool_size": 1},
        get_pool_key)
    ConnectionProviderManager.set_connection_provider(provider)
    ```

2. Call `ConnectionProviderManager.set_connection_provider`, passing in the `SqlAlchemyPooledConnectionProvider` you created in step 1.

3. By default, the read-write plugin randomly selects a reader instance when `read_only` is first called. If you would like the plugin to select a reader based on a different connection strategy, please see the [Connection Strategies](#connection-strategies) section for more information.

4. Continue as normal: create connections and use them as needed.

5. When you are finished using all connections, call `ConnectionProviderManager.release_resources`.

> :warning: **Note:** You must call `ConnectionProviderManager.release_resources` to close the internal connection pools when you are finished using all connections. Unless `ConnectionProviderManager.release_resources` is called, the wrapper driver will keep the pools open so that they can be shared between connections.

### Example
[Postgres read-write splitting sample code](../../examples/PGReadWriteSplitting.py)
[MySQL read-write splitting sample code](../../examples/MySQLReadWriteSplitting.py)

### Connection Strategies
By default, the read-write plugin randomly selects a reader instance the first time that `read_only` is set. To balance connections to reader instances more evenly, different connection strategies can be used. The following table describes the currently available connection strategies and any relevant configuration parameters for each strategy.

To indicate which connection strategy to use, the `reader_host_selector_strategy` parameter can be set to one of the connection strategies in the table below. The following is an example of enabling the least connections strategy:

```
params = {
    "reader_host_selector_strategy": "least_connections",
    # Add other connection properties below...
}

# If using MySQL:
conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)

# If using Postgres:
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
```

| Connection Strategy | Configuration Parameter                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Default Value |
|---------------------|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `random`            | This strategy does not have configuration parameters. | The random strategy is the default connection strategy. When switching to a reader connection, the reader instance will be chosen randomly from the available database instances.                                                                                                                                                                                                                                                                                                                                                                                 | N/A           |
| `least_connections` | This strategy does not have configuration parameters. | The least connections strategy will select reader instances based on which database instance has the least number of currently active connections. Note that this strategy is only available when internal connection pools are enabled - if you set the connection property without enabling internal pools, an exception will be thrown.                                                                                                                                                                                                                        | N/A           |


### Limitations

#### General plugin limitations

When a `Cursor` is created, it is bound to the database connection established at that moment. Consequently, if the read-write plugin switches the internal connection, any `Cursor` objects created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any `Cursor` objects created before a change in internal connection. To solve this problem, please ensure you create new Cursor objects after switching between the writer/reader.

#### Session state limitations

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the read-write splitting plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

For Postgres:
- read_only
- autocommit
- transaction isolation level

For MySQL:
- autocommit

All other session state attributes will be lost when switching connections between the writer/reader.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes each time that you switch between the writer/reader.
