# Plugin Chain Compatibility

Some plugins depend on or conflict with others. This page captures
non-obvious pairings that aren't enforced by code but will silently
degrade behavior at runtime if violated.

If a plugin's combination is **incompatible**, the wrapper does not raise
at construction time — the chain is accepted but behavior at runtime is
incorrect (silent fallback to a writer-only connection, monitor threads
that can't abort, etc.). Check the relevant row below before composing
a chain you haven't used before.

## Required pairings

| Plugin              | Requires                          | Why |
|---------------------|-----------------------------------|-----|
| `read_write_splitting` | `failover_v2` (not `failover`) | `failover_v2` starts the cluster topology monitor on the **initial** connect. With plain `failover`, the host list stays at `{writer-only}` until a later failover signal fires, and `conn.read_only = True` flips silently fall back to the writer rather than splitting to a reader. |

## Driver-specific incompatibilities

| Plugin                       | Incompatible with                              | Why |
|------------------------------|------------------------------------------------|-----|
| `host_monitoring` (EFM v1)   | sync MySQL (`mysql-connector-python`)          | EFM requires a thread-based connection abort that `mysql-connector-python` doesn't expose. Symptoms: monitor threads hang on shutdown, "Python hangs on exit" when a host is unreachable. Use the chain **without EFM**, or switch to the async driver (`aiomysql`). |
| `host_monitoring_v2` (EFM v2) | sync MySQL (`mysql-connector-python`)          | Same root cause as v1 — EFM v2 still depends on thread-based abort. |
| `iam_authentication`         | MySQL `use_pure=True` (pure-Python connector)  | The pure-Python connector truncates passwords at 255 chars; IAM tokens are typically longer. Expect `int1store requires 0 <= i <= 255` or `struct.error: ubyte format requires 0 <= number <= 255`. See README "Known Limitations". |

## Recommended canonical chains

| Use case                                                       | Sync chain                                              | Async chain                                              |
|----------------------------------------------------------------|---------------------------------------------------------|----------------------------------------------------------|
| Aurora PG — R/W splitting + failover + EFM                     | `read_write_splitting,failover_v2,host_monitoring_v2`   | `read_write_splitting,failover_v2,host_monitoring_v2`    |
| Aurora MySQL (sync, `mysql-connector-python`) — R/W splitting + failover | `read_write_splitting,failover_v2` *(no EFM)*  | —                                                        |
| Aurora MySQL (async, `aiomysql`) — R/W splitting + failover + EFM | —                                                    | `read_write_splitting,failover_v2,host_monitoring_v2`    |
| Aurora PG — failover only                                       | `failover_v2,host_monitoring_v2`                       | `failover_v2,host_monitoring_v2`                         |
| Aurora MySQL (sync) — failover only                             | `failover_v2`                                          | —                                                        |

> **Note (async federated/Okta auth):** the async `federated_auth` and `okta` plugins perform their
> IdP HTTP round-trips with [`aiohttp`](https://docs.aiohttp.org/), which is not a runtime dependency
> of this package — install it alongside your async driver (`pip install aiohttp`) when using either
> plugin in an asyncio application. The sync plugins use `requests` and are unaffected.

## Related docs

- [UsingTheReadWriteSplittingPlugin.md](./using-plugins/UsingTheReadWriteSplittingPlugin.md)
- [UsingTheFailover2Plugin.md](./using-plugins/UsingTheFailover2Plugin.md)
- [UsingTheFailoverPlugin.md](./using-plugins/UsingTheFailoverPlugin.md) (v1, not recommended for new code per the table above)
- [UsingTheHostMonitoringPlugin.md](./using-plugins/UsingTheHostMonitoringPlugin.md)
- [UsingTheIamAuthenticationPlugin.md](./using-plugins/UsingTheIamAuthenticationPlugin.md)
- [FailoverConfigurationGuide.md](./FailoverConfigurationGuide.md) — retry-budget knobs at the SQLAlchemy / Django boundary
