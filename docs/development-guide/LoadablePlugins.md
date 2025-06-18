# Plugins As Loadable Modules
Plugins are loadable and extensible modules that add extra logic around Python method calls.

Plugins let users:
- monitor connections
- handle exceptions during executions
- log execution details, such as SQL statements executed
- cache execution results
- measure execution time
- and more

The AWS Advanced Python Driver has several built-in plugins; you can [see the list here](../../docs/using-the-python-driver/UsingThePythonDriver.md#list-of-available-plugins).

## Available Services

Plugins are notified by the connection plugin manager when changes to the database connection occur, and utilize the [plugin service](./PluginService.md) to establish connections and retrieve host information. 

## Using Custom Plugins

To use a custom plugin, you must:
1. Create the custom plugin.
2. Create a corresponding plugin factory.
3. Follow the steps in [Registering a Custom Plugin](#registering-a-custom-plugin).

A short example of these steps is provided [below](#Example).

### Creating Custom Plugins

To create a custom plugin, create a new class that extends the [Plugin](../../aws_advanced_python_wrapper/plugin.py) class.

The `Plugin` class provides a simple implementation for all `Plugin` methods. By default, requested Python database methods will be called without additional operations. This is helpful when the custom plugin only needs to override one (or a few) methods from the `Plugin` interface.
See the following classes for examples:

- [IamAuthPlugin](../../aws_advanced_python_wrapper/iam_plugin.py)
    - The `IamAuthPlugin` class only overrides the `connect` method because the plugin is only concerned with creating
      database connections with IAM database credentials.

- [ExecuteTimePlugin](../../aws_advanced_python_wrapper/execute_time_plugin.py)
    - The `ExecuteTimePlugin` only overrides the `execute` method because it is only concerned with elapsed time during execution. It does not establish new connections or set up any host list provider.

A `PluginFactory` implementation is also required for the new custom plugin. This factory class is used to register and initialize custom plugins. See [ExecuteTimePluginFactory](../../aws_advanced_python_wrapper/execute_time_plugin.py) for a simple implementation example.

### Subscribed Methods

When executing a Python method, the plugin manager will only call a specific plugin method if the Python method is within its set of subscribed methods. For example, the [ReadWriteSplittingPlugin](../../aws_advanced_python_wrapper/read_write_splitting_plugin.py) subscribes to Python methods and setters that change the read-only value of the connection, but does not subscribe to other common `Connection` or `Cursor` methods. Consequently, this plugin will not be triggered by method calls like `Connection.commit` or `Cursor.execute`.

The `subscribed_methods` attribute specifies the set of Python methods that a plugin is subscribed to in the form of a set of strings (`Set[str]`). All plugins must implement/define the `subscribed_methods` attribute.

Plugins can subscribe to any of the standard PEP249 [Connection methods](https://peps.python.org/pep-0249/#connection-methods) or [Cursor methods](https://peps.python.org/pep-0249/#cursor-methods). They can also subscribe to the target driver methods listed in the corresponding driver dialect's `_network_bound_methods` attribute:
- [Postgres network bound methods](../../aws_advanced_python_wrapper/pg_driver_dialect.py)
- [MySQL network bound methods](../../aws_advanced_python_wrapper/mysql_driver_dialect.py)

Plugins can also subscribe to specific [pipelines](./Pipelines.md) by including the subscription key in their `subscribed_methods` attribute and implementing the equivalent pipeline method:

| Pipeline                                                                                            | Method Name / Subscription Key |
|-----------------------------------------------------------------------------------------------------|:------------------------------:|
| [Host list provider pipeline](./Pipelines.md#host-list-provider-pipeline)                           |       init_host_provider       |
| [Connect pipeline](./Pipelines.md#connect-pipeline)                                                 |            connect             |
| [Connection changed notification pipeline](./Pipelines.md#connection-changed-notification-pipeline) |   notify_connection_changed    |
| [Host list changed notification pipeline](./Pipelines.md#host-list-changed-notification-pipeline)   |    notify_host_list_changed    |                                                                      

### Tips on Creating a Custom Plugin

A custom plugin can subscribe to all Python methods being executed by setting the Plugin's `subscribed_methods` attribute to `{"*"}`. In this case, the plugin will be active in every workflow. We recommend that you be aware of the performance impact of subscribing to all Python methods, especially if your plugin regularly performs demanding tasks for common Python method calls.

### Registering a Custom Plugin
To register a custom plugin, follow these steps:
- Import and call `PluginManager.register_plugin(plugin_code: str, plugin_factory: Type[PluginFactory], weight: int = WEIGHT_RELATIVE_TO_PRIOR_PLUGIN)` with the appropriate arguments: 
  - The first argument specifies a short name for the plugin that will be used when specifying the `plugins` connection parameter. The name should not contain spaces. In the example below, we will use `custom_plugin`.
  - The second argument should be the `PluginFactory` class you created for the custom plugin. Note that the class itself should be passed rather than an instance of the class.
  - The third (optional) argument specifies a weight for the custom plugin. The weight will determine the plugin's ordering in the plugin chain if the `auto_sort_wrapper_plugin_order` property is enabled. All plugins with unspecified weight will be ordered according to the `plugins` parameter setting. More information on this property can be found [here](../../docs/using-the-python-driver/UsingThePythonDriver.md#connection-plugin-manager-parameters).
- When creating a connection, in the `plugins` parameter, include the plugin name that you specified as the first argument to `register_plugin`. This will ensure that your plugin is included in the plugin chain.

### Example
```python
    from aws_advanced_python_wrapper.plugin_service import PluginManager

    # In custom_plugin.py
    class CustomPlugin(Plugin):
        def __init__(self, plugin_service: PluginService, props: Properties):
            self._plugin_service = plugin_service
            self._props = props
            
        @property
        def subscribed_methods(self) -> Set[str]:
            return {"notify_connection_changed"}
        
        def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
            print("The connection has changed.")
            return OldConnectionSuggestedAction.NO_OPINION
        
        
    class CustomPluginFactory(PluginFactory):
        def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
            return CustomPlugin(plugin_service, props)

    # In app.py
    PluginManager.register_plugin("custom_plugin", CustomPluginFactory)
    params = {
        "plugins": "aurora_connection_tracker,custom_plugin"
        # Add other connection properties below...
    }
    
    # If using MySQL:
    conn = AwsWrapperConnection.connect(mysql.connector.connect, **params)
    
    # If using Postgres:
    conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
    
```

## What is Not Allowed in Plugins

When creating custom plugins, it is important to **avoid** the following bad practices in your plugin implementation:

1. Keeping local copies of shared information:
   - information like current connection, or the host list provider are shared across all plugins
   - shared information may be updated by any plugin at any time and should be retrieved via the plugin service when required
2. Using driver-specific properties or objects:
   - the AWS Advanced Python Driver may be used with multiple drivers, therefore plugins must ensure implementation is not restricted to a specific driver
3. Making direct connections:
   - the plugin should always call the pipeline lambdas (i.e. `connect_func: Callable` or `force_connect_func: Callable`)
4. Running long tasks synchronously:
   - the Python method calls are executed by all subscribed plugins synchronously; if one plugin runs a long task during the execution it blocks the execution for the other plugins

See the following examples for more details:

<details><summary>❌ <strong>Bad Example</strong></summary>

```python
class BadPlugin(Plugin):
    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props

        # Bad Practice #1: keeping local copies of items
        # Plugins should not keep local copies of the host list provider, the topology or the connection.
        # The host list provider is stored in the Plugin Service and can be modified by other plugins,
        # therefore it should be retrieved by accessing plugin_service.host_list_provider when it is needed.
        self._host_list_provider = self._plugin_service.host_list_provider

    def subscribed_methods(self) -> Set[str]:
        return {"*"}

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        # Bad Practice #2: using driver-specific parameters.
        # Not all drivers support the same configuration parameters. For instance, MySQL Connector/Python uses the
        # "database" parameter to specify which database to connect to, but psycopg uses "dbname".
        if props.get("dbname") is None:
            props["dbname"] = "default_database"

        # Bad Practice #3: Making direct connections
        return psycopg.Connection.connect(**props)
```
</details>

<details><summary>✅ <strong>Good Example</strong></summary>

```python
class GoodPlugin(Plugin):
    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props

    def subscribed_methods(self) -> Set[str]:
        return {"*"}

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        if len(self._plugin_service.hosts) == 0:
            # Re-fetch host info if it is empty.
            self._plugin_service.force_refresh_host_list()

        return execute_func()

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        # Use the DATABASE wrapper property. This property will be converted to the correct target driver property by
        # the current DriverDialect.
        if props.get(WrapperProperties.DATABASE.name) is None:
            props[WrapperProperties.DATABASE.name] = "default_database"

        # Call the pipeline lambda to connect.
        return connect_func()
```
</details>
