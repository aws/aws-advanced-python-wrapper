# Plugin Service

<div style="center"><img src="../images/plugin_service.png" alt="diagram for the plugin service design"/></div>

The plugin service retrieves and updates the current connection and its relevant host information.

It also keeps track of the host list provider in use, and notifies it to update its host list.

It is expected that the plugins do not establish a connection themselves, but rather call `PluginService.connect()`
to establish connections.

## The Call Context

A plugin may need to change the arguments that `Cursor.execute` or
`Cursor.executemany` passes to the target driver, for example to replace a
parameter value or to rewrite the statement. The call context covers these
two methods only.

While one of them runs through the plugins,
`PluginService.current_call_context` holds the call's arguments as a
`PluginCallContext` with two attributes, `args` (a tuple) and `kwargs` (a
mapping). A plugin that changes them **assigns** a new tuple to `args` and a
new mapping to `kwargs` before it calls `execute_func`:

```python
def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
    context = self._plugin_service.current_call_context
    if context is not None:
        context.args, context.kwargs = self._rewrite(args, kwargs)  # a new tuple and a new dict
    return execute_func()
```

The plugins after it in the chain, and the target driver, receive exactly the
new arguments, even when they are empty; the plugins before it keep
theirs. Changing `args` or `kwargs` in place is not allowed, and the
caller's own objects must never be modified.

`current_call_context` is set whenever a plugin handles one of these two
methods through the wrapper. A plugin that must change the arguments, rather
than one that may, should raise when it finds `None` instead of calling
`execute_func` with the arguments unchanged.

A replacement lasts until the plugin that made it returns. A plugin before it
that calls `execute_func` again starts again from the arguments it saw, so the
replacement is made once per run. The arguments the target driver receives
are fixed when the call reaches `DefaultPlugin`, the last plugin in the chain,
so a driver call that `socket_timeout` runs on a worker thread sends them even
if it starts late. That covers `DefaultPlugin` only: a plugin that runs
`execute_func` on a thread of its own must wait for that call before it returns,
since a call that reaches `DefaultPlugin` after the plugin that made a
replacement has returned sends the arguments without it.

Each `Cursor.execute` and `Cursor.executemany` call has its own context, a
call made from inside a plugin included, and the enclosing call's context is
current again when it returns. Other methods get no context of their own:
during a `Cursor.fetchone` called from inside an `execute`, for example,
`current_call_context` still returns the enclosing call's context. So read it
only while handling `Cursor.execute` or `Cursor.executemany`. It is `None`
when no such call is running on the thread.

The context belongs to the calling thread, not to the connection's plugin
service. Another thread sees it only through a copy of the thread's context
taken during the call (`contextvars.copy_context()`, or a thread that inherits
its creator's context), and such a copy holds no arguments once the call has
returned.

The call context is available to synchronous plugins only.

## Host List Providers

The plugin service uses the host list provider to retrieve the most recent host information or topology information about the database.

The Python Wrapper has two host list providers, the `ConnectionStringHostListProvider` and the `RdsHostListPovider`.

The `ConnectionStringHostListProvider` is the default provider. 

The `RdsHostListProvider` provides information of the Aurora cluster.
It uses the current connection to track the available hosts and their roles in the cluster.

The `ConnectionStringHostListProvider` is a static host list provider, whereas the `RdsHostListProvider` is a dynamic host list provider.
A static host list provider will fetch the host list during initialization and does not update the host list afterwards,
whereas a dynamic host list provider will update the host list information based on database status.
When implementing a custom host list provider, implement either the `StaticHostListProvider` or the `DynamicHostListProvider` marker interfaces to specify its provider type.

