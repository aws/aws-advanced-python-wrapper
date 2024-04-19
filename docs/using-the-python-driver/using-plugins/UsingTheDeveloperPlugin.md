# Developer Plugin

> [!WARNING]\
> The plugin is NOT intended to be used in production environments. It's designed for the purpose of testing.

The Developer Plugin allows developers to simulate failures by providing a mechanism to raise exceptions when connection operations occur. By simulating these failures, developers can verify that their application handles these scenarios correctly.

Since some exceptions raised by the drivers rarely happen, testing for those might be difficult and require a lot of effort in building a testing environment. Exceptions associated with network outages are a good example of those exceptions. It may require substantial efforts to design and build a testing environment where such timeout exceptions could be produced with 100% accuracy and 100% guarantee. If a test suite can't produce and verify such cases with 100% accuracy it significantly decreases the value of such tests and makes the tests unstable and flaky. The Developer Plugin simplifies testing of such scenarios as shown below.

The `dev` plugin code should be added to the connection plugins parameter in order to be able to intercept Python calls and raise a test exception when conditions are met.

## Simulate an exception while opening a new connection

The plugin introduces a new class `ExceptionSimulatorManager` that will handle how a given exception will be passed to the connection to be tested.

In order to raise a test exception while opening a new connection, first create an instance of the exception to be tested, then use `raise_exception_on_next_connect` in `ExceptionSimulatorManager` so it will be triggered at next connection attempt.

Once the exception is raised, it will be cleared and will not be raised again. This means that the next opened connection will not raise the exception again.

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.pep249 import Error

params = {
    "plugins": "dev",
    "dialect": "aurora-pg"
}
exception: Error = Error("test")
ExceptionSimulatorManager.raise_exception_on_next_connect(exception)

AwsWrapperConnection.connect(psycopg.Connection.connect, **params) # this throws the exception
AwsWrapperConnection.connect(psycopg.Connection.connect, **params) # goes as usual with no exception
```

## Simulate an exception with already opened connection

It is possible to also simulate an exception thrown in a connection after the connection has been opened.

Similar to the previous case, the exception is cleared up once it's raised and subsequent Python calls should behave normally.

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection

params = {
    "plugins": "dev",
    "dialect": "aurora-pg"
}
exception: RuntimeError = RuntimeError("test")
ExceptionSimulatorManager.raise_exception_on_next_method("Connection.cursor", exception)
conn = AwsWrapperConnection.connect(psycopg.Connection.connect, **params)
conn.cursor() # this throws the exception
conn.cursor() # goes as usual with no exception
```

It's possible to use callback functions to check Python call parameters and decide whether to return an exception or not. Check `ExceptionSimulatorManager.set_connect_callback` and `ExceptionSimulatorManager.set_method_callback` for more details.
