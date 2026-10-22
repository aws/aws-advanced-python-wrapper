# Not For Executors: When the Python Wrapper Doesn't Fit

This page is the honest version of the "limitations" section. If you're considering using the AWS Advanced Python Wrapper inside Spark executors, AWS Lambda functions with very short timeouts, or any other short-lived per-task process, read this first.

## The runtime assumption

The wrapper's headline features — failover handling, host monitoring, read/write splitting, topology caching — were designed around a runtime that looks like this:

- **One process**, long-lived (minutes to hours).
- **A small number of connections**, each used for many transactions.
- **Background threads are welcome** (host monitoring, async upgrade attempts).
- **In-process caches pay off** because they're hit many times.

This is the shape of an application server, an ECS task, a long-running EC2 process, a Glue Python Shell job, or a Lambda function with sustained warm-container reuse.

This is **not** the shape of:

- A Spark executor running a partition task (seconds, then gone).
- A Glue PySpark `mapPartitions` / `foreachPartition` callback.
- A Lambda function that opens and closes a connection per invocation with cold containers.
- An AWS Batch container that runs one job and exits.

## What breaks (or wastes resources) at executor scale

### Host monitoring threads outlive their usefulness — but only barely

The `host_monitoring` plugin spawns a background thread per connection that probes liveness. The thread takes some time to detect a problem — short by application standards, eternal by partition-task standards. A partition task that runs for 2 seconds gets zero benefit from a monitor that needs more than that to pronounce a host dead. You pay the thread spawn cost, the inter-thread coordination cost, the shutdown cost, and you get nothing.

### Topology caches are per-process, so they get cold N times

Each executor JVM/Python process has its own topology cache. A 100-executor job means 100 cold caches and 100 topology refresh queries against your cluster — within seconds of each other. From RDS's perspective this looks like a thundering herd. From your perspective, each executor pays the cache-warm latency on its first task.

### Read/write splitting needs connection lifetime to amortize

The `read_write_splitting` plugin's value is being able to flip a single connection between writer and reader as your code switches between writes and reads. That assumes the connection will see many such flips. In a partition task, you connect, you do one thing (write OR read, rarely both), you disconnect. The plugin's machinery never gets used.

### Failover transparent reconnection conflicts with task-retry semantics

In stateful mode, when the wrapper sees failover, it transparently reconnects you to the new writer and raises `FailoverSuccessError` so you can retry your transaction. That's the right behavior in a long-lived process — your connection object is salvaged, your session state is mostly retained, and you re-run a transaction.

In a Spark task, this is the wrong layer. The task is going to fail anyway (Spark needs the exception to count toward `task.maxFailures`), the executor is going to retry on a fresh connection anyway, and the new connection will route to the new writer via DNS anyway. The wrapper's transparent-reconnect code path is doing work that's about to be thrown away.

### Connections aren't shareable across executors

You can't open a connection on the driver and broadcast it. Connections aren't picklable; the wrapper's per-process caches don't survive serialization either. Every executor (and in PySpark, every partition-function call) opens its own connection.

## What the symptoms look like

If you put the Python wrapper in stateful mode inside Spark executors, you'll typically see one or more of:

- **High connection setup latency on first task per executor.** You're paying topology discovery on every cold executor.
- **A burst of `SELECT … FROM aurora_replica_status` (or equivalent) queries against your cluster** at job start, one per executor.
- **Background threads that briefly outlive their task** and produce log noise during executor shutdown.
- **Zombie connections** if a partition task fails before `close()` runs and the monitor thread holds a reference. The cleaner eventually catches them, but RDS sees connection-count blips.
- **`FailoverSuccessError` raised by the wrapper, then immediately swallowed by Spark's task-retry**, leaving you to wonder what value the wrapper added on that path.

None of these are correctness bugs. They're all "the wrapper is doing work that doesn't help in this runtime."

## What to use instead

| Problem | Recommendation |
|---|---|
| Bulk read/write from PySpark to Aurora | **JDBC wrapper** via `--extra-jars`. The JDBC wrapper sits at the layer Spark drives bulk traffic through; its plugins fit per-executor JDBC connections naturally. |
| Per-partition queries that need failover-aware error handling | **Python wrapper, [stateless mode](./UsingStatelessMode.md)**. Same exception contract; no background threads or caches. |
| Per-partition queries where you'd rather not think about it | **RDS Proxy + plain driver (no wrapper)**. The proxy handles failover at the connection layer; you get connection pooling for free. You lose the typed exception contract and RW splitting. |
| Driver-side control queries (watermarks, manifests, DDL) | **Python wrapper, stateful** (default). The driver is one long-lived process, which is exactly the shape the wrapper was built for. |

## A hard rule and a soft rule

**Hard rule:** never enable `host_monitoring` on connections that will live for less than ~30 seconds. The thread doesn't pay for itself.

**Soft rule:** if you find yourself wanting to use `read_write_splitting` inside a partition task, you probably want two separate connections, or two separate jobs, instead.

## "Then why does the Python wrapper exist for Glue at all?"

Two reasons:

1. **The Glue driver is a Python process.** Anything you run before/after `spark.read` / `df.write` runs on the driver, and the driver lives for the whole job. That's the natural home for the Python wrapper. Lookups, watermarks, manifests, DDL, post-load verification — all valid uses.

2. **Glue Python Shell jobs exist.** They're a single-VM Python runtime with no Spark. Every feature of the Python wrapper works there exactly as designed. If your data-integration work fits in Python Shell (no need for distributed compute), use it and use the wrapper to its full extent.

The mismatch is specifically with **Spark executor lifecycle**, not with Glue overall. Inside an executor, the Python wrapper is at the wrong layer; outside it (driver, Python Shell), the Python wrapper is exactly the right tool.

## See also

- [Stateless Mode](./UsingStatelessMode.md) — the supported way to use the Python wrapper in short-lived contexts
- [Glue PySpark Guidance](./UsingFromGluePySpark.md) — concrete patterns for mixing Python wrapper (driver) and JDBC wrapper (bulk I/O)
- [JDBC wrapper](https://github.com/aws/aws-advanced-jdbc-wrapper) — the right tool for executor-side bulk I/O
