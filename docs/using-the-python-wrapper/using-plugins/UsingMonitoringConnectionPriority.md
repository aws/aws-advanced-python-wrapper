# Monitoring Connection Priority

The monitoring connection priority properties let you control which kind of host the topology monitor connects to for its background monitoring connection. This is useful both for standard Aurora clusters and for [Aurora Global Databases](../GlobalDatabases.md), where you may want to keep monitoring traffic on a particular host role or region.

## Overview

The topology monitor maintains a background connection it uses to observe cluster topology changes. By default it prefers a **writer** connection, which provides the most accurate and timely topology information. These properties let you change that preference.

Two properties are available:

- **`monitoring_connection_priority`** — for standard Aurora clusters. Selects the host role used for the monitoring connection.
- **`gdb_monitoring_connection_priority`** — for Aurora Global Databases. Extends the standard property with region-aware and primary/secondary-aware values.

Both accept a **comma-separated, ordered priority list**. The monitor accepts whatever connection it obtains first, then **asynchronously upgrades** to a higher-priority host (see [Async upgrade behavior](#async-upgrade-behavior)) without blocking the monitoring loop.

## Configuration Properties

| Property                              |  Value   | Required | Description                                                                                                                                                        | Default                   |
| ------------------------------------- | :------: | :------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------- |
| `monitoring_connection_priority`      | `String` |    No    | Comma-separated ordered priority list for the topology monitor's background connection. Values: `strict-writer`, `strict-reader`, `writer-or-reader`.               | `strict-writer`           |
| `gdb_monitoring_connection_priority`  | `String` |    No    | Comma-separated, region-aware ordered priority list for the Global Database topology monitor. See [GDB values](#gdb-values-gdb_monitoring_connection_priority) below. | `strict-writer-primary`   |

Only one of these applies at a time: `gdb_monitoring_connection_priority` is used by the Global Aurora topology monitor; `monitoring_connection_priority` is used by the standard Aurora topology monitor.

## Priority Values

### Standard values (`monitoring_connection_priority`)

| Value              | Description                                                                                             |
| ------------------ | ------------------------------------------------------------------------------------------------------- |
| `strict-writer`    | Prefer a **writer** host for the monitoring connection.                                                 |
| `strict-reader`    | Prefer a **reader** host for the monitoring connection.                                                 |
| `writer-or-reader` | Any host is acceptable (no role preference).                                                            |

Parsing notes:

- Unrecognized tokens are ignored. Duplicate values are dropped, order preserved.
- If the value is unset, empty, or no token parses to a known value, it defaults to `strict-writer`.

### GDB values (`gdb_monitoring_connection_priority`)

| Value                       | Description                                                                                                    |
| --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `strict-writer-primary`     | A **writer** in the **primary** region of the Global Database.                                                 |
| `strict-reader-primary`     | A **reader** in the **primary** region.                                                                        |
| `strict-reader-secondary`   | A **reader** in any **secondary** (non-primary) region.                                                        |
| `strict-writer-<region>`    | A **writer** in the named region, e.g. `strict-writer-us-east-1`.                                              |
| `strict-reader-<region>`    | A **reader** in the named region, e.g. `strict-reader-us-west-2`.                                              |
| `<region-name>`             | Any host (writer or reader) in the named AWS region, e.g. `us-west-2`.                                         |

Parsing notes:

- **`strict-writer-secondary` is rejected** — a writer cannot exist in a secondary region of a Global Database (only the primary region has a writer). The token is skipped.
- There is **no** `writer-or-reader-primary` / `writer-or-reader-secondary` value. To target any host in a region, use the bare `<region-name>` form.
- Any token that is not one of the `strict-writer-*` / `strict-reader-*` forms is treated as a **bare region literal**. A typo (for example `strict-wrtier-primary`) is therefore silently accepted as a region name that will simply never match — double-check spelling.
- Unlike the standard property, duplicate GDB values are **not** dropped (order preserved).
- If the value is unset, empty, or no token parses, it defaults to `strict-writer-primary`.

The "primary region" is the region of the current writer host; a "secondary region" is any other region the global cluster spans.

## Usage

### Standard Aurora cluster

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

with AwsWrapperConnection.connect(
        Connection.connect,
        "host=my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com dbname=mydb user=admin password=pwd",
        plugins="failover2,efm2",
        monitoring_connection_priority="writer-or-reader",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT 1")
    print(awscursor.fetchone())
```

### Aurora Global Database

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

with AwsWrapperConnection.connect(
        Connection.connect,
        "host=my-global-db.global-xyz.global.rds.amazonaws.com dbname=mydb user=admin password=pwd",
        plugins="initial_connection,gdb_failover,efm2",
        wrapper_dialect="global-aurora-pg",
        failover_home_region="us-west-2",
        global_cluster_instance_host_patterns="us-east-1:?.abc123.us-east-1.rds.amazonaws.com,us-west-2:?.def456.us-west-2.rds.amazonaws.com",
        gdb_monitoring_connection_priority="strict-writer-primary",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT pg_catalog.aurora_db_instance_identifier()")
    print(awscursor.fetchone())
```

### Keeping monitoring traffic in a specific region

```python
# Direct the monitoring connection to any host in us-west-2 to reduce
# cross-region monitoring latency.
gdb_monitoring_connection_priority="us-west-2"
```

## Async upgrade behavior

The monitor does not block waiting for its preferred host. It accepts the first connection it can obtain, then in the background attempts to upgrade to a higher-priority host from the current priority list. For example, with `strict-writer-primary` configured but the primary writer temporarily unreachable, the monitor may connect to another host and upgrade once the primary writer becomes reachable. The upgrade runs on a background worker and never stalls the monitoring loop.

## Interaction with accessible regions

When [`gdb_accessible_regions`](./UsingGlobalAuroraAccessibleRegions.md) is configured, the accessible-regions filter is applied **first**: upgrade candidates and monitored hosts in inaccessible regions are excluded before the priority list is consulted.

> [!WARNING]
> If `gdb_monitoring_connection_priority` names a region (or a `strict-*-<region>` value) that is not in `gdb_accessible_regions`, that priority can never match a monitored host. Keep the two properties consistent.

When the writer lives in an inaccessible region and no host monitor can reach it directly, the monitor exits panic mode by adopting a harvested reader connection (reader-consensus / stable-reader-topology exit), so monitoring still proceeds against reachable hosts.

## Tuning guidance

- Use `strict-writer` (the default) for most applications — writer connections give the most accurate, timely topology.
- Use `strict-reader` to keep monitoring load off the writer, accepting slightly more delayed topology updates.
- Use `writer-or-reader` for maximum monitoring availability when any host is acceptable.
- For Global Databases, prefer `strict-writer-primary` to read topology from the primary region's writer.
- Use a bare `<region-name>` to keep monitoring traffic local and reduce cross-region latency.
