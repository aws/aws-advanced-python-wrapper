# Using the AWS Advanced Python Wrapper from AWS Glue PySpark

This guide is opinionated. It tells you what works, what doesn't, and where to use the **JDBC** wrapper instead of the Python one.

## Decision matrix: which wrapper, where

| Workload in your Glue job | Use |
|---|---|
| **Bulk read** from Aurora into a DataFrame (`spark.read.format("jdbc")`) | **JDBC wrapper** via `--extra-jars` |
| **Bulk write** from a DataFrame to Aurora (`df.write.format("jdbc")`) | **JDBC wrapper** via `--extra-jars` |
| **Glue DynamicFrame** read/write to Aurora (`glueContext.create_dynamic_frame.from_options`) | **JDBC wrapper** (Glue passes the JDBC URL through) |
| Driver-side queries: lookup a watermark, write a manifest, read config rows | **Python wrapper** on the driver |
| Per-partition queries inside `mapPartitions` / `foreachPartition` | **Python wrapper in [stateless mode](./UsingStatelessMode.md)** |
| Streaming jobs that hold a connection per microbatch | **Python wrapper, stateful** (microbatches are long enough) |

The short version: **for bulk movement of data, use the JDBC wrapper**. The Python wrapper is for control-plane code on the driver and (in stateless mode) for short per-partition work. See [Not For Executors](./NotForExecutors.md) for why.

## Setup

### Option A — Bulk I/O via the JDBC wrapper (recommended for read/write at scale)

In your Glue job parameters:

```
--extra-jars  s3://your-bucket/jars/aws-advanced-jdbc-wrapper-2.x.x.jar
```

Then in your job:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:aws-wrapper:postgresql://"
                   "mycluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb")
    .option("driver", "software.amazon.jdbc.Driver")
    .option("user", "appuser")
    .option("password", get_secret())
    .option("dbtable", "(select id, payload from events where ts > '2026-01-01') t")
    # JDBC wrapper plugins
    .option("wrapperPlugins", "failover,efm2,iam")
    .option("wrapperDialect", "aurora-pg")
    .load()
)
```

This is the path Glue and Spark are designed around. Each executor gets its own JDBC connection; the wrapper's plugins fit the per-executor lifecycle naturally.

### Option B — Driver-side control queries via the Python wrapper

Useful for "look up a value before the run" / "write a manifest after the run" / DDL.

```
--additional-python-modules  aws-advanced-python-wrapper,psycopg[binary]
```

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
import psycopg

# Runs on the Spark driver — long-lived, one process. Stateful mode is fine here.
conn = AwsWrapperConnection.connect(
    psycopg.Connection.connect,
    f"host={cluster_endpoint} dbname=mydb user=appuser",
    plugins="failover,host_monitoring,iam",
    wrapper_dialect="aurora-pg",
    iam_region="us-east-1",
)

with conn.cursor() as cur:
    cur.execute("SELECT max(processed_ts) FROM watermarks WHERE job=%s", (JOB_ID,))
    watermark = cur.fetchone()[0]
```

### Option C — Per-partition queries via the Python wrapper in stateless mode

Sometimes you need to look up something per row/partition that isn't worth a Spark join (e.g., per-partition external API calls plus a quick DB write). Use the Python wrapper in [stateless mode](./UsingStatelessMode.md):

```
--additional-python-modules  aws-advanced-python-wrapper,pg8000
```

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
import pg8000

STATELESS_PROPS = {
    "plugins": "failover",
    "cluster_topology_refresh_rate_ms": "0",
    "monitoring_enabled": "false",
    "failover_timeout_ms": "5000",
}

def write_partition(rows):
    conn = AwsWrapperConnection.connect(
        pg8000.dbapi.connect,
        host=CLUSTER_ENDPOINT, database="mydb", user="appuser",
        **STATELESS_PROPS,
    )
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("INSERT INTO sink (id, payload) VALUES (%s, %s)",
                            (row.id, row.payload))
        conn.commit()
    finally:
        conn.close()

df.foreachPartition(write_partition)
```

`pg8000` is a pure-Python driver — no native build, easiest to ship via `--additional-python-modules`. Use `psycopg[binary]` if you need higher throughput and your wheel set tolerates the native dep.

## Configuration that matters in Glue

### Job role → DB user (IAM auth)

Don't put DB passwords in job parameters. Use the `iam` plugin and let your Glue job's IAM role become the DB principal:

```python
plugins="failover,iam"
iam_region="us-east-1"
# user is the DB user mapped to the IAM role; no password needed
```

The Glue job role needs `rds-db:connect` on the resource ARN of your DB user.

### Secrets, if you must

If you can't use IAM auth, fetch from Secrets Manager on the driver and broadcast — never include the password in `--default-arguments`:

```python
secret = boto3.client("secretsmanager").get_secret_value(SecretId="...")
broadcast_pw = sc.broadcast(json.loads(secret["SecretString"])["password"])
```

### Pick the right cluster endpoint

- **Writer cluster endpoint** for jobs that write or need read-after-write.
- **Reader cluster endpoint** for read-only bulk loads — DNS load-balances across readers, and per-executor connections will spread out.
- **Custom endpoints** if you want to pin to a specific reader subset.

## Failover handling in PySpark

The exception contract is the same as anywhere else in the Python wrapper, but **the unit of retry is the Spark task**, not your Python loop. Don't try to retry a transaction inside `mapPartitions` — let it fail, and configure Spark to retry the task:

```python
spark.conf.set("spark.task.maxFailures", "4")
```

Inside the partition function, surface failover errors as exceptions:

```python
from aws_advanced_python_wrapper.errors import (
    FailoverSuccessError, FailoverFailedError, TransactionStateUnknownError,
)

def write_partition(rows):
    conn = open_stateless()
    try:
        ...
        conn.commit()
    except (FailoverSuccessError, FailoverFailedError):
        # Let the task fail. Spark retries on a different executor; the new
        # connection resolves to the new writer via DNS.
        raise
    except TransactionStateUnknownError:
        # COMMIT may or may not have applied. Make your write idempotent
        # (INSERT ... ON CONFLICT, MERGE, partitioned by run-id) and re-raise
        # to let Spark retry.
        raise
    finally:
        conn.close()
```

**Make every per-partition write idempotent.** Spark task retry is at-least-once; combined with `TransactionStateUnknownError`, you can see the same partition apply twice. Idempotency at the SQL level (`ON CONFLICT DO NOTHING`, `MERGE`, dedup by run-id) is the only safe answer.

## Things that don't work the way you'd hope

- **Sharing one Python wrapper connection across executors.** You can't. Connections are not picklable; even if they were, the wrapper's caches are per-process. Open per-executor (or per-partition) connections.
- **`read_write_splitting` plugin in `mapPartitions`.** The plugin's value is reusing a single connection for both roles over time — but partition tasks are short-lived. You're paying its overhead for none of its benefit.
- **`host_monitoring` plugin per partition.** Spawns a thread that gets reaped before it does anything useful. Disable it in stateless mode.
- **Setting `cluster_topology_refresh_rate_ms` low to "react faster" in PySpark.** Each executor has its own cache; lowering the rate just multiplies refresh queries against your cluster. If you want fast detection, use RDS Proxy.

## RDS Proxy as an alternative

For PySpark workloads where stateless-mode exception handling is more contract than you need, point Spark at RDS Proxy instead of the cluster. The proxy handles failover at the connection layer and Spark sees a connection drop / reconnect. You lose RW splitting and the typed exception contract; you gain a runtime model that matches Spark's lifecycle.

```
spark.read.format("jdbc")
  .option("url", "jdbc:postgresql://my-proxy.proxy-xyz.us-east-1.rds.amazonaws.com:5432/mydb")
```

This works fine without either wrapper. Pick this path if your team prefers fewer moving parts.

## Sample job: end-to-end

```python
import sys, json, boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import FailoverSuccessError
import psycopg

args = getResolvedOptions(sys.argv, ["JOB_NAME", "CLUSTER_ENDPOINT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

CLUSTER = args["CLUSTER_ENDPOINT"]

# 1. Driver-side: read the watermark via the Python wrapper (stateful — long-lived).
def read_watermark():
    conn = AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        f"host={CLUSTER} dbname=mydb user=appuser",
        plugins="failover,iam",
        wrapper_dialect="aurora-pg",
        iam_region="us-east-1",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(ts) FROM watermarks WHERE job=%s",
                        (args["JOB_NAME"],))
            return cur.fetchone()[0]
    finally:
        conn.close()

watermark = read_watermark()

# 2. Bulk read from Aurora via the JDBC wrapper.
df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:aws-wrapper:postgresql://{CLUSTER}:5432/mydb")
    .option("driver", "software.amazon.jdbc.Driver")
    .option("user", "appuser")
    .option("dbtable", f"(SELECT * FROM events WHERE ts > '{watermark}') t")
    .option("wrapperPlugins", "failover,efm2,iam")
    .option("wrapperDialect", "aurora-pg")
    .load()
)

# 3. Process and write back via the JDBC wrapper.
processed = df.transform(my_transform)

(
    processed.write.format("jdbc")
    .option("url", f"jdbc:aws-wrapper:postgresql://{CLUSTER}:5432/mydb")
    .option("driver", "software.amazon.jdbc.Driver")
    .option("user", "appuser")
    .option("dbtable", "events_out")
    .option("wrapperPlugins", "failover,efm2,iam")
    .mode("append")
    .save()
)
```

The Python wrapper sits where it's most useful (the driver). The JDBC wrapper sits where bulk movement actually happens (the executors).

## See also

- [Stateless Mode](./UsingStatelessMode.md)
- [Not For Executors](./NotForExecutors.md)
- [JDBC wrapper documentation](https://github.com/aws/aws-advanced-jdbc-wrapper)
