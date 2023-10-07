# Aurora Connection Tracker Plugin

This plugin tracks all the opened connections. In the event of a cluster failover, this plugin will close all the impacted connections.
This plugin is enabled by default. It can also be explicitly included by adding the plugin code `aurora_connection_tracker` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) value.

## Use Case
User applications can have two types of connections:

1. active connections that are used to execute statements or perform other types of database operations.
2. idle connections that the application holds references to but are not used for any operations.

For instance, a user application has an active connection and an idle connection to host A where host A is a writer instance. The user application executes DML statements against host A when a cluster failover occurs. A different host is promoted as the writer, so host A is now a reader. The driver will failover the active connection to the new writer, but it will not modify the idle connection.

When the application tries to continue the workflow with the idle connection that is still pointing to a host that has changed roles, i.e. host A, users may get an error caused by unexpected behaviour, such as `cannot execute UPDATE in a read-only transaction`.

Since the Aurora Connection Tracker Plugin keeps track of all the open connections, the plugin can close all impacted connections after failover.
When the application tries to use the outdated idle connection, the application will get a `connection closed` error instead.
