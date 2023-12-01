## Developer Plugin

> :warning: The plugin is NOT intended to be used in production environments. It's designed for the purpose of testing.

The Developer Plugin allows developers to inject an exception to a connection and to verify how an application handles it. 

Since some exceptions raised by the drivers rarely happen, testing for those might be difficult and require a lot of effort in building a testing environment. Exceptions associated with network outages are a good example of those exceptions. It may require substantial efforts to design and build a testing environment where such timeout exceptions could be produced with 100% accuracy and 100% guarantee. If a test suite can't produce and verify such cases with 100% accuracy it significantly decreases the value of such tests and makes the tests unstable and flaky. The Developer Plugin simplifies testing of such scenarios as shown below.

The `dev` plugin code should be added to the connection plugins parameter in order to be able to intercept Python calls and raise a test exception when conditions are met.

### Simulate an exception while opening a new connection

The plugin introduces a new class `ExceptionSimulationManager` that will handle how a given exception will be passed to the connection to be tested.

In order to raise a test exception while opening a new connection, first create an instance of the exception to be tested, then use `raise_exception_on_next_connect` in `ExceptionSimulationManager` so it will be triggered at next connection attempt.

Once the exception is raised, it will be cleared and will not be raised again. This means that the next opened connection will not raise the exception again.

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.pep249 import Error

params = {
    "host": "database.cluster-xyz.us-east-1.rds.amazonaws.com",
    "dbname": "postgres",
    "plugins": "dev",
    "wrapper_dialect": "pg"
}
exception: Error = Error("test")
exception_simulator_manager = ExceptionSimulatorManager()
exception_simulator_manager.raise_exception_on_next_connect(exception)

AwsWrapperConnection.connect(psycopg.Connection.connect, **params) // this throws the exception

AwsWrapperConnection.connect(psycopg.Connection.connect, **params) // goes as usual with no exception
```

### Simulate an exception with already opened connection

It is possible to also simulate an exception thrown in a connection after the connection has been opened.

Similar to previous case, the exception is cleared up once it's raised and subsequent Python calls should behave normally.


```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection

props = Properties({"plugins": "dev", "dialect": "pg"})
container: PluginServiceManagerContainer = PluginServiceManagerContainer()
plugin_service = container.plugin_service
plugin_manager: PluginManager = PluginManager(container, props)

wrapper = AwsWrapperConnection(plugin_service, plugin_manager)
simulator = wrapper.unwrap(ExceptionSimulator)

exception: RuntimeError = RuntimeError("test")
simulator.raise_exception_on_next_call("Connection.cursor", exception)

wrapper.cursor() // this throws the exception

wrapper.cursor() // goes as usual with no exception
```

It's possible to use a callback functions to check Python call parameters and decide whether to return an exception or not. Check `ExceptionSimulatorManager.set_callback` and `ExceptionSimulator.set_callback` for more details.