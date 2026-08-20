# Restricting Aurora Global Databases to Accessible Regions

When using [Aurora Global Databases](../GlobalDatabases.md), an application may only be able to reach a subset of the regions the global cluster spans (for example, because of network routing, VPC peering, or security constraints). The `gdb_accessible_regions` property restricts the AWS Advanced Python Wrapper to a set of reachable AWS regions, excluding hosts in all other regions from host selection.

> [!IMPORTANT]\
> **Currently supported in the synchronous wrapper only.** 

## `gdb_accessible_regions`

| Property                   | Value                                                                                                              | Default                                 |
|----------------------------|--------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `gdb_accessible_regions`   | Comma-separated list of AWS region names the application can reach (for example, `us-east-1,us-west-2`). Region names are matched case-insensitively and surrounding whitespace is trimmed. | Unset — **all regions are accessible** (no restriction). |

When the property is unset (or empty), no filtering is applied and every region in the global cluster is treated as accessible.

**Key constraint:** the **home region must be included** in `gdb_accessible_regions`. If it is not, the connection fails at initialization with an error, because the home region must always be reachable.

## Behavior by Component

The accessible-regions filter is applied **before** all other selection logic — role preference, failover mode, home-region restriction, and initial-connection strategy all operate on the already-filtered host list.

| Component                                                             | Behavior                                                                                                                                     |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| [GDB Failover plugin](./UsingTheGdbFailoverPlugin.md)                 | Validates the home region at init. In `strict-writer` mode, **fails loudly** (`FailoverFailedError`) if the new writer is in an inaccessible region. In all other modes, filters out inaccessible-region hosts before candidate selection. |
| [GDB Read/Write Splitting plugin](./UsingTheGdbReadWriteSplittingPlugin.md) | Validates the home region at init. Rejects (`ReadWriteSplittingError`) a writer in an inaccessible region. Filters readers by accessible region **before** applying the home-region restriction. |
| Aurora Initial Connection Strategy plugin                             | Excludes inaccessible-region hosts before selecting a host by strategy.                                                                       |
| Global Aurora topology monitor                                        | Skips host-monitoring workers for hosts in inaccessible regions, and fails if the initial host is itself in an inaccessible region.          |

### Fail-loud, not silent fallback

The filter is a **hard restriction**. When the writer is in an inaccessible region, the wrapper raises an error rather than silently connecting to an unreachable or unintended host. When reader filtering leaves no candidates in accessible regions, the wrapper does **not** fall back to the unfiltered host list.

## Example

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

with AwsWrapperConnection.connect(
        Connection.connect,
        "host=my-global-db.global-xyz.global.rds.amazonaws.com dbname=mydb user=admin password=pwd",
        plugins="initial_connection,failover_v2,host_monitoring_v2",
        wrapper_dialect="global-aurora-pg",
        cluster_id="1",
        global_cluster_instance_host_patterns="us-east-1:?.abc123.us-east-1.rds.amazonaws.com,us-west-2:?.def456.us-west-2.rds.amazonaws.com",
        # Only us-east-1 and us-west-2 are reachable from this application.
        gdb_accessible_regions="us-east-1,us-west-2",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT pg_catalog.aurora_db_instance_identifier()")
    print(awscursor.fetchone())
```

> **Note:** the home region (derived from the connection endpoint or `failover_home_region` / `gdb_rw_home_region`) must appear in `gdb_accessible_regions`.
