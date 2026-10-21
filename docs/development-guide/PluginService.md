# Plugin Service

<div style="center"><img src="../images/plugin_service.png" alt="diagram for the plugin service design"/></div>

The plugin service retrieves and updates the current connection and its relevant host information.

It also keeps track of the host list provider in use, and notifies it to update its host list.

It is expected that the plugins do not establish a connection themselves, but rather call `PluginService.connect()`
to establish connections.

## The Routed Host

A plugin may connect to a host other than the one it was asked for. The Aurora
initial connection strategy plugin substitutes an instance for a cluster
endpoint, and the limitless plugin picks a transaction router.

A plugin that does this assigns `PluginService.routed_host_info`, and the
wrapper adopts that host as the current host for the connection.

A plugin that wants to know where the connect was routed should **read** the
property. Connect hooks unwind from the innermost plugin outwards, so a plugin
that consumed the value would hide it from every plugin outside it and from
the wrapper. Only the wrapper consumes it, with `take_routed_host_info()`.

The value belongs to a single connect. `PluginService.connect()` clears it on
entry and `set_current_connection()` clears it once a host has been adopted,
so a plugin never reads a host left over from an earlier connect.

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

