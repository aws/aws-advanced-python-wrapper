# Cross plugin compatibility

This document is part of the [Compatibility Guide](./Compatibility.md) and explains compatibility between various plugins — both the *logical* plugin-vs-plugin matrix and the **Python-specific** constraints (async/sync availability, driver/runtime limitations, plugin ordering, and recommended canonical chains) that the matrix can't capture.

- [Database type compatibility](./CompatibilityDatabaseTypes.md)
- [Database URL type compatibility](./CompatibilityEndpoints.md)

While combining plugins in a single driver configuration is common, some plugins may not work properly together. Such incompatibilities can arise from either plugin design constraints or logical conflicts.

For example, the `failover` plugin is incompatible with `failover_v2`. Both plugins support database cluster failover but implement this functionality differently. Combining them in a single configuration causes interference between their operations, leading to instability. Similarly, the `limitless` plugin and `custom_endpoint` plugin are incompatible because Limitless Database does not support custom endpoints.

> **Runtime note (Python-specific):** The AWS Advanced Python Wrapper does **not** raise at construction time for an incompatible chain — the chain is accepted but behaves incorrectly at runtime (silent fallback to a writer-only connection, monitor threads that can't abort, etc.). Check the relevant section below before composing a chain you haven't used before.

## Mutually incompatible groups

Only **one** plugin from each of the following groups may be used at a time:

- **Failover:** `failover`, `failover_v2`, `gdb_failover`
- **Host monitoring (EFM):** `host_monitoring`, `host_monitoring_v2`
- **Read/write splitting:** `read_write_splitting`, `srw`, `gdb_rw`
- **Authentication:** `iam`, `aws_secrets_manager`, `federated_auth`, `okta`

## Other incompatibilities

- `initial_connection` is incompatible with `stale_dns` and with `srw`.
- `stale_dns` is incompatible with `srw` and `gdb_rw`.
- `limitless` is incompatible with all failover plugins, all read/write-splitting plugins, `custom_endpoint`, `aurora_connection_tracker`, `initial_connection`, `stale_dns`, `fastest_response_strategy`, and `bg`.

## Plugin-vs-plugin matrix

Legend: ✅ compatible &nbsp;|&nbsp; ❌ incompatible

<br>

| Plugin code / Plugin code | host_monitoring | host_monitoring_v2 | failover | failover_v2 | gdb_failover |
|---|:---:|:---:|:---:|:---:|:---:|
| host_monitoring_v2 | ❌ | | | | |
| failover | ✅ | ✅ | | | |
| failover_v2 | ✅ | ✅ | ❌ | | |
| gdb_failover | ✅ | ✅ | ❌ | ❌ | |
| iam | ✅ | ✅ | ✅ | ✅ | ✅ |
| aws_secrets_manager | ✅ | ✅ | ✅ | ✅ | ✅ |
| federated_auth | ✅ | ✅ | ✅ | ✅ | ✅ |
| okta | ✅ | ✅ | ✅ | ✅ | ✅ |
| read_write_splitting | ✅ | ✅ | ✅ | ✅ | ✅ |
| srw | ✅ | ✅ | ✅ | ✅ | ✅ |
| gdb_rw | ✅ | ✅ | ✅ | ✅ | ✅ |
| custom_endpoint | ✅ | ✅ | ✅ | ✅ | ✅ |
| aurora_connection_tracker | ✅ | ✅ | ✅ | ✅ | ✅ |
| initial_connection | ✅ | ✅ | ✅ | ✅ | ✅ |
| stale_dns | ✅ | ✅ | ✅ | ✅ | ✅ |
| connect_time | ✅ | ✅ | ✅ | ✅ | ✅ |
| fastest_response_strategy | ✅ | ✅ | ✅ | ✅ | ✅ |
| limitless | ✅ | ✅ | ❌ | ❌ | ❌ |
| bg | ✅ | ✅ | ✅ | ✅ | ✅ |

<br>

| Plugin code / Plugin code | iam | aws_secrets_manager | federated_auth | okta |
|---|:---:|:---:|:---:|:---:|
| aws_secrets_manager | ❌ | | | |
| federated_auth | ❌ | ❌ | | |
| okta | ❌ | ❌ | ❌ | |
| read_write_splitting | ✅ | ✅ | ✅ | ✅ |
| srw | ✅ | ✅ | ✅ | ✅ |
| gdb_rw | ✅ | ✅ | ✅ | ✅ |
| custom_endpoint | ✅ | ✅ | ✅ | ✅ |
| aurora_connection_tracker | ✅ | ✅ | ✅ | ✅ |
| initial_connection | ✅ | ✅ | ✅ | ✅ |
| stale_dns | ✅ | ✅ | ✅ | ✅ |
| connect_time | ✅ | ✅ | ✅ | ✅ |
| fastest_response_strategy | ✅ | ✅ | ✅ | ✅ |
| limitless | ✅ | ✅ | ✅ | ✅ |
| bg | ✅ | ✅ | ✅ | ✅ |

<br>

| Plugin code / Plugin code | read_write_splitting | srw | gdb_rw |
|---|:---:|:---:|:---:|
| srw | ❌ | | |
| gdb_rw | ❌ | ❌ | |
| custom_endpoint | ✅ | ✅ | ✅ |
| aurora_connection_tracker | ✅ | ✅ | ✅ |
| initial_connection | ✅ | ❌ | ✅ |
| stale_dns | ✅ | ❌ | ❌ |
| connect_time | ✅ | ✅ | ✅ |
| fastest_response_strategy | ✅ | ✅ | ✅ |
| limitless | ❌ | ❌ | ❌ |
| bg | ✅ | ✅ | ✅ |

<br>

| Plugin code / Plugin code | custom_endpoint | aurora_connection_tracker | initial_connection | stale_dns |
|---|:---:|:---:|:---:|:---:|
| aurora_connection_tracker | ✅ | | | |
| initial_connection | ✅ | ✅ | | |
| stale_dns | ✅ | ✅ | ❌ | |
| connect_time | ✅ | ✅ | ✅ | ✅ |
| fastest_response_strategy | ✅ | ✅ | ✅ | ✅ |
| limitless | ❌ | ❌ | ❌ | ❌ |
| bg | ✅ | ✅ | ✅ | ✅ |

<br>

| Plugin code / Plugin code | connect_time | fastest_response_strategy | limitless | bg |
|---|:---:|:---:|:---:|:---:|
| fastest_response_strategy | ✅ | | | |
| limitless | ✅ | ❌ | | |
| bg | ✅ | ✅ | ❌ | |

## Plugin order

Plugins are initialized and executed in the order they are listed in the `plugins` property. By default, the wrapper re-sorts them into a safe order.

- **`plugins`** — comma-separated list of plugin codes. Default:
  `initial_connection,aurora_connection_tracker,failover_v2,host_monitoring_v2`
  (for `mysql-connector-python`, the default omits `host_monitoring_v2`:
  `initial_connection,aurora_connection_tracker,failover_v2`).
- **`auto_sort_wrapper_plugin_order`** — `Boolean`, default `True`. Lets the wrapper sort
  connection plugins by weight to prevent misconfiguration. Set to `False` to preserve the
  exact order you specify.

When auto-sort is enabled, plugins are ordered by ascending weight (lowest first). The
built-in weights are:

| Plugin code | Weight |
|---|---|
| `custom_endpoint` | 40 |
| `initial_connection` | 50 |
| `aurora_connection_tracker` | 100 |
| `stale_dns` | 200 |
| `read_write_splitting` | 300 |
| `srw` | 310 |
| `gdb_rw` | 320 |
| `failover` | 400 |
| `failover_v2` | 410 |
| `gdb_failover` | 420 |
| `host_monitoring` | 500 |
| `host_monitoring_v2` | 510 |
| `bg` | 550 |
| `fastest_response_strategy` | 600 |
| `iam` | 700 |
| `aws_secrets_manager` | 800 |
| `federated_auth` | 900 |
| `limitless` | 950 |
| `okta` | 1000 |
| `connect_time`, `execute_time`, `dev` | relative to prior plugin (keep listed position) |

## Required pairings

| Plugin | Requires | Why |
|---|---|---|
| `read_write_splitting` | `failover_v2` (not `failover`) | `failover_v2` starts the cluster topology monitor on the **initial** connect. With plain `failover`, the host list stays at `{writer-only}` until a later failover signal fires, and `conn.read_only = True` flips silently fall back to the writer rather than splitting to a reader. |

## Async / sync availability

Every plugin code is registered in **both** the synchronous and asynchronous (`asyncio`) wrappers, so the plugin-vs-plugin, database-type, and endpoint matrices above apply identically to both. The only differences are the target driver and a few async behavioral gaps:

| Delta | Sync | Async |
|---|---|---|
| Target driver | `psycopg` (PG), `mysql-connector-python` (MySQL) | `psycopg` async (PG), `aiomysql` (MySQL) |
| EFM (`host_monitoring` / `host_monitoring_v2`) on MySQL | ❌ not supported ([why](#driver-specific-incompatibilities)) | ✅ supported (`aiomysql`) |
| `federated_auth` / `okta` IdP HTTP client | `requests` (bundled) | `aiohttp` (install separately) |
| `gdb_failover` / `gdb_rw` accessible-regions filtering + monitoring-connection-priority | ✅ supported | ❌ sync-only today |

## Driver-specific incompatibilities

These constraints are specific to the Python target drivers and are **not** reflected in the logical compatibility matrices.

| Plugin | Incompatible with | Why |
|---|---|---|
| `host_monitoring` (EFM v1) | sync MySQL (`mysql-connector-python`) | EFM requires a thread-based connection abort that `mysql-connector-python` doesn't expose. Symptoms: monitor threads hang on shutdown, "Python hangs on exit" when a host is unreachable. Use the chain **without EFM**, or switch to the async driver (`aiomysql`). |
| `host_monitoring_v2` (EFM v2) | sync MySQL (`mysql-connector-python`) | Same root cause as v1 — EFM v2 still depends on thread-based abort. |
| `iam` | MySQL `use_pure=True` (pure-Python connector) | The pure-Python connector truncates passwords at 255 chars; IAM tokens are typically longer. Expect `int1store requires 0 <= i <= 255` or `struct.error: ubyte format requires 0 <= number <= 255`. See README "Known Limitations". |

## Recommended canonical chains

| Use case | Sync chain | Async chain |
|---|---|---|
| Aurora PG — R/W splitting + failover + EFM | `read_write_splitting,failover_v2,host_monitoring_v2` | `read_write_splitting,failover_v2,host_monitoring_v2` |
| Aurora MySQL — R/W splitting + failover | `read_write_splitting,failover_v2` ¹ | `read_write_splitting,failover_v2,host_monitoring_v2` |
| Aurora PG — failover only | `failover_v2,host_monitoring_v2` | `failover_v2,host_monitoring_v2` |
| Aurora MySQL — failover only | `failover_v2` ¹ | `failover_v2,host_monitoring_v2` |
| Aurora Global Database — GDB failover + EFM | `gdb_failover,host_monitoring_v2` | `gdb_failover,host_monitoring_v2` |
| Aurora Global Database — GDB R/W splitting + GDB failover | `gdb_rw,gdb_failover` | `gdb_rw,gdb_failover` |

¹ Sync MySQL (`mysql-connector-python`) omits EFM — see [Async / sync availability](#async--sync-availability).

> **Note (async federated/Okta auth):** the async `federated_auth` and `okta` plugins perform their
> IdP HTTP round-trips with [`aiohttp`](https://docs.aiohttp.org/), which is not a runtime dependency
> of this package — install it alongside your async driver (`pip install aiohttp`) when using either
> plugin in an asyncio application. The sync plugins use `requests` and are unaffected.

## Related docs

- [UsingTheReadWriteSplittingPlugin.md](../using-plugins/UsingTheReadWriteSplittingPlugin.md)
- [UsingTheFailover2Plugin.md](../using-plugins/UsingTheFailover2Plugin.md)
- [UsingTheFailoverPlugin.md](../using-plugins/UsingTheFailoverPlugin.md) (v1, not recommended for new code per the table above)
- [UsingTheHostMonitoringPlugin.md](../using-plugins/UsingTheHostMonitoringPlugin.md)
- [UsingTheIamAuthenticationPlugin.md](../using-plugins/UsingTheIamAuthenticationPlugin.md)
- [FailoverConfigurationGuide.md](../FailoverConfigurationGuide.md)
