# Using Stateless Mode

## What it is

Stateless mode is a configuration of the AWS Advanced Python Wrapper that strips out the long-lived background machinery — host monitoring threads, topology caches, dialect probing, async upgrade attempts — and keeps only the parts that work for short-lived, per-task connections:

- The DB-API connection wrapper itself
- Failover **error mapping** (the wrapper still raises `FailoverSuccessError` / `FailoverFailedError` / `TransactionStateUnknownError` when it sees the cluster behave that way during a single connection's lifetime)
- IAM authentication (token generation per-connect)
- TLS / driver-protocol passthrough

It does **not** give you transparent reconnection across failover, RW splitting, or proactive host health monitoring. Use it when those features cost more than they're worth — e.g., when your runtime opens a connection, runs one transaction, and closes it.

## When to use it

Use stateless mode when **all** of these are true:

- Connection lifetime is measured in seconds, not minutes (Spark partition tasks, Lambda invocations without container reuse, Glue PySpark `mapPartitions`, Step Functions iterations).
- You don't share a single connection across many transactions.
- You don't need RW splitting (you connect to writer or reader, not both, on the same connection).
- You're willing to handle reconnection at a higher layer (Spark task retry, Step Functions retry, your own loop).

Use the **default (stateful) configuration** when:

- The connection lives for minutes or hours and serves many transactions.
- You want host monitoring to detect a dead writer faster than your TCP keepalives will.
- You use the `read_write_splitting` plugin to swap reader/writer on the same logical connection.

## How to enable it

Stateless mode is a curated set of properties — no single switch. The recipe:

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
import psycopg

STATELESS_PROPS = {
    # Plugins: only failover error mapping. No host_monitoring, no read_write_splitting.
    "plugins": "failover",

    # Don't spawn topology refresh threads.
    "cluster_topology_refresh_rate_ms": "0",

    # Disable the host availability tracker's background work.
    "monitoring_enabled": "false",

    # Failover behavior: surface errors to the caller; don't loop trying to find a writer.
    "failover_mode": "strict-writer",
    "failover_timeout_ms": "5000",       # short — let the orchestrator retry
    "failover_writer_reconnect_interval_ms": "1000",
    "failover_reader_connect_timeout_ms": "5000",
}

conn = AwsWrapperConnection.connect(
    psycopg.Connection.connect,
    "host=mycluster.cluster-xyz.us-east-1.rds.amazonaws.com "
    "dbname=mydb user=appuser",
    **STATELESS_PROPS,
)
```

## What you get and what you don't

| Feature | Stateful (default) | Stateless |
|---|---|---|
| Failover *error contract* (the four exceptions) | Yes | Yes |
| Transparent reconnection across failover (same connection object survives) | Yes | No — connection is closed; caller retries |
| Host monitoring (background liveness probes) | Yes | No |
| Read/write splitting on the same connection | Yes | No |
| Topology cache shared across operations on the same connection | Yes | Per-connect only |
| IAM auth | Yes | Yes |
| Per-connect overhead | Higher (cache warm-up, monitor thread spawn) | Lower |
| Background thread count | 1+ per connection | 0 |
| Suitable for `mapPartitions` / per-task connections | No | Yes |

## Error handling in stateless mode

The same exception contract applies, but the *meaning* shifts. Without a host monitor or topology refresh, the wrapper detects failover when a query fails with a state that implies the writer is gone (read-only error, connection lost, etc.). It maps that to `FailoverSuccessError` / `FailoverFailedError` / `TransactionStateUnknownError` exactly as it does in stateful mode.

The difference: in stateless mode the wrapper does **not** transparently reconnect you to the new writer on the same connection object. The connection is closed. Your caller is expected to:

```python
from aws_advanced_python_wrapper.errors import (
    FailoverSuccessError,
    FailoverFailedError,
    TransactionStateUnknownError,
)

def run_with_retry(work, retries=3):
    for attempt in range(retries):
        conn = open_connection()  # fresh wrapper instance
        try:
            with conn.cursor() as cur:
                work(cur)
            conn.commit()
            return
        except FailoverSuccessError:
            # Connection is dead. Open a new one — DNS/topology will route to new writer.
            continue
        except TransactionStateUnknownError:
            if work.is_idempotent:
                continue
            raise
        except FailoverFailedError:
            raise  # let the orchestrator decide
        finally:
            try:
                conn.close()
            except Exception:
                pass
    raise RuntimeError("transaction did not complete after retries")
```

In a Spark partition task, the equivalent is to let the task fail and Spark's `task.maxFailures` retry it on a different executor. The fresh executor opens a fresh connection, which resolves to the new writer via DNS.

## Limitations

- **No proactive failure detection.** Stateless mode learns about failover only when a query fails. If your connection is idle when failover happens, the next query absorbs the latency hit.
- **No RW splitting.** If you set `plugins=read_write_splitting,failover` you re-introduce the state machine that stateless mode is trying to avoid. Don't mix them.
- **No connection-internal reconnect.** A `FailoverSuccessError` from stateless mode means "this connection is done." Open a new one.
- **Per-task connection overhead.** Each fresh connect re-runs IAM token generation (if used), TCP handshake, TLS handshake, and dialect detection. For very short tasks (<1s of work) this overhead may dominate. Consider RDS Proxy in front of the cluster — it amortizes connection setup and handles failover at its own layer.

## Combining with RDS Proxy

For Glue PySpark and similar short-task workloads, the most reliable production setup is:

```
PySpark executor → aws-advanced-python-wrapper (stateless) → RDS Proxy → Aurora
```

RDS Proxy gives you connection pooling and transparent failover at the proxy layer; the wrapper above it gives you the typed exception contract for your application code. You lose RW splitting, but you weren't getting it in stateless mode anyway.

## See also

- [Glue PySpark Guidance](./UsingFromGluePySpark.md)
- [Not For Executors](./NotForExecutors.md)
- [Failover plugin reference](./using-plugins/UsingTheFailoverPlugin.md)
