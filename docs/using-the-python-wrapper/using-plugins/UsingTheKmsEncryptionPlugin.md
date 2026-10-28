# KMS Encryption Plugin

The KMS Encryption Plugin (`kms_encryption`) encrypts the values an application binds for configured columns before the driver sends them, and decrypts them when they are read back. Each column has its own data key, which the database stores only in a form encrypted by an AWS Key Management Service (KMS) master key, so neither the plaintext nor a usable data key reaches the database.

The plugin works with the synchronous wrapper, on PostgreSQL with psycopg and on MySQL with MySQL Connector/Python, on Python 3.10 to 3.14. It has no async support.

> [!WARNING]\
> Every encrypted column needs a database-side trigger. The plugin sees only the statements sent on connections that enable it, and it finds encrypted columns by reading the SQL, so some statements reach the database without it (see [What the plugin does not see](#what-the-plugin-does-not-see)). The trigger runs in the database and rejects an `INSERT` or `UPDATE` that stores anything but a valid encrypted value in the column. See [Enforce encryption in the database](#enforce-encryption-in-the-database).

## Prerequisites

- Install the `encryption` extra, which adds `cryptography` and `sqlglot`, for PostgreSQL and MySQL alike: `pip install "aws-advanced-python-wrapper[encryption]"`. Install the target driver (`psycopg` or `mysql-connector-python`) as usual.
- AWS credentials, found as [AWS Credentials](../AwsCredentials.md) describes. At run time the application needs only `kms:Decrypt` on the master keys it uses (see [Hardening](#hardening)).
- Each encrypted column is binary: `BYTEA` on PostgreSQL, `VARBINARY` or `BLOB` on MySQL. A stored value is 65 bytes longer than the encoded value, which for a string is its UTF-8 bytes, so an 11-character ASCII string takes 76 bytes; size a `VARBINARY` column for the longest value (MySQL refuses a longer one with error 1406 and stores nothing).
- On PostgreSQL, a `TEXT` column, or a `VARCHAR` wide enough for it, accepts the plugin's writes and stores each encrypted value as hex text, which the plugin then refuses to read with `IntegrityCheckError`. Declare the column `BYTEA`, or convert it with `ALTER TABLE <table> ALTER COLUMN <column> TYPE bytea USING <column>::bytea`. On MySQL a text column refuses the write (error 1366).
- The two metadata tables, `encryption_metadata` and `key_storage`, in the schema named by `encryption_metadata_schema` (default `encrypt`). An administrator creates them once with the SQL in [Create the metadata tables](#create-the-metadata-tables). On MySQL that account needs the global `CREATE` privilege, which an application user usually lacks (error 1044). Create them exactly as that SQL does. Nothing validates the schema: a missing table or column makes the plugin's metadata reads raise `MetadataError`, and a missing primary key, unique constraint or foreign key goes unnoticed.
- At run time the plugin only reads the metadata schema, so the application's database user needs only `SELECT` on it, besides its privileges on its own tables. On MySQL the account that installs the trigger SQL needs `SELECT` there too, and the application user needs no grant for the trigger.
- `key_storage` holds the only copy of each column's encrypted data key and its HMAC key, so back it up with the data. Without its row, a column's values can't be read (`InvalidKeyMetadataError`).
- The HMAC key in `key_storage` is stored unencrypted. A role that can read `key_storage` can compute a tag the trigger accepts, so it can store values that pass the trigger and then fail to decrypt. A role that can write it can replace a column's HMAC key with one of its own and forge values the trigger accepts. Neither can decrypt a stored value, because the data key is stored encrypted under the master key. Grant `SELECT` on `key_storage` only to the roles that need it (the application's role and, on MySQL, the account that installed the trigger SQL), and write access only to the administrator who runs `KeyManagementUtility`.

## Create the metadata tables

Create the schema and both tables once, before setting up any encrypted column. The statements below use the default schema name `encrypt`; replace it if `encryption_metadata_schema` is set to something else. On MySQL a schema is a database, so `CREATE SCHEMA` creates a database named `encrypt`. `encryption_metadata.key_id` references `key_storage.id`, not `key_storage.key_id`.

```sql
-- PostgreSQL
CREATE SCHEMA IF NOT EXISTS encrypt;

CREATE TABLE encrypt.key_storage (
    id SERIAL PRIMARY KEY,
    key_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key BYTEA NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
```

```sql
-- MySQL
CREATE SCHEMA IF NOT EXISTS encrypt;

CREATE TABLE encrypt.key_storage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    key_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key VARBINARY(32) NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
```

Then install the trigger SQL, as [Enforce encryption in the database](#enforce-encryption-in-the-database) describes, and set up each column with `KeyManagementUtility`.

## Set up an encrypted column

`KeyManagementUtility` creates a master key in KMS, generates the column's data key and records the column in the metadata tables. Run it as an administrator, from a setup or migration script:

```python
import boto3
import psycopg

from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.key_management_utility import KeyManagementUtility
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager

config = EncryptionConfig.from_props({"encryption_kms_region": "us-east-1"}, require_allowed_master_keys=False)
kms = KeyManager.create_kms_client(boto3.Session(), config)

with psycopg.connect("host=db.example.com dbname=mydb user=admin password=...") as conn:
    utility = KeyManagementUtility(conn, kms, config)
    master_key_arn = utility.create_master_key("users.ssn encryption")
    utility.initialize_encryption_for_column("users", "ssn", master_key_arn)
    conn.commit()
```

On MySQL, open the connection with `mysql.connector.connect(...)` instead; the rest is the same.

- Call `conn.commit()`. The utility runs its statements in the connection's transaction and never commits. psycopg's `with` block commits when it ends without an error, but mysql-connector's closes the connection without committing, so on MySQL the setup is lost without `commit()` and the KMS key it created is left unused.
- To use an existing KMS key, pass its ARN to `initialize_encryption_for_column` and skip `create_master_key`.
- Add the master key's ARN to `encryption_allowed_master_key_arns` on the application's connections, or the plugin refuses to read or write the column (`UnauthorizedMasterKeyError`).
- Run the utility on a DB-API connection. From a SQLAlchemy engine, keep the connection `engine.raw_connection()` returns and give the utility its `driver_connection`, or pass `engine=` (`"postgresql"` or `"mysql"`). After `commit()`, close the connection `raw_connection()` returned, not its `driver_connection`, so the pool gets a working connection back.
- The utility doesn't create the trigger. Create the column's trigger in the same change, as [Enforce encryption in the database](#enforce-encryption-in-the-database) describes, and check that it exists in the engine's trigger list. The version queries there show only that the trigger SQL is installed.

```sql
-- PostgreSQL: tgenabled is O while the trigger is enabled, D after ALTER TABLE ... DISABLE TRIGGER
SELECT tgname, tgenabled FROM pg_trigger WHERE tgrelid = 'users'::regclass AND NOT tgisinternal;
-- MySQL: the BEFORE INSERT and BEFORE UPDATE triggers of each column
SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION FROM information_schema.TRIGGERS
 WHERE EVENT_OBJECT_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = 'users';
```

### Rotate a column's data key

```python
utility.rotate_data_key("users", "ssn", master_key_arn)
conn.commit()
```

`rotate_data_key(table_name, column_name, new_master_key_arn=None)` gives the column a new data key, encrypted by `new_master_key_arn`. Values written before keep decrypting with the key they were written with, and new writes use the new key. Leaving out `new_master_key_arn` keeps the column's current master key, which needs `encryption_allowed_master_key_arns` in the utility's configuration. To rotate onto another master key, add its ARN to `encryption_allowed_master_key_arns` before the rotation (on the application's connections, and in the utility's configuration if it has the parameter), and keep the old ARN listed while values written under it remain.

A connection opened before the rotation keeps the old data key until its next metadata refresh (`encryption_metadata_cache_refresh_interval_sec`, 300 seconds by default) or until its cached metadata expires. Until then its writes to the column use the old key, and the trigger, which checks each new value against the column's current key, rejects them. To pick up a rotation sooner, set `encryption_metadata_cache_refresh_interval_sec` to a smaller value above `0` and keep `encryption_metadata_cache_expiration_sec` above it.

## Enable the plugin

To enable the KMS Encryption Plugin, add the plugin code `kms_encryption` to the [`plugins`](../UsingThePythonWrapper.md#connection-plugin-manager-parameters) value. This example connects to PostgreSQL with the default plugins and `kms_encryption`:

```python
import psycopg

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources

try:
    with AwsWrapperConnection.connect(
            psycopg.Connection.connect,
            "host=db.example.com dbname=mydb user=app password=...",
            plugins="initial_connection,aurora_connection_tracker,failover_v2,host_monitoring_v2,kms_encryption",
            encryption_kms_region="us-east-1",
            encryption_allowed_master_key_arns="arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
            autocommit=True,
    ) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO users (name, ssn) VALUES (%s, %s)", ("Jane Doe", "000-00-0000"))
        cur.execute("SELECT ssn FROM users WHERE name = %s", ("Jane Doe",))
        print(cur.fetchone())
finally:
    release_resources()
```

- `plugins` replaces the default plugin list, so list the plugins you use together with `kms_encryption`. The example lists the PostgreSQL defaults; on MySQL the defaults are the same without `host_monitoring_v2`.
- Keep `auto_sort_wrapper_plugin_order` on (the default), or list `kms_encryption` before the failover plugins, so the plugin's errors keep their class (see [Plugin Chain Compatibility](../PluginChainCompatibility.md)).
- SQLAlchemy: pass the list as `connect_args={"plugins": "..."}`, or as `?wrapper_plugins=...` in the URL. SQLAlchemy removes a `?plugins=` query parameter, so with it the plugin is silently not loaded (see [SQLAlchemy Support](../SqlAlchemySupport.md)).
- Django (MySQL only): put `"plugins": "..."` and the `encryption_*` parameters in the database's `OPTIONS` (see [Django Support](../DjangoSupport.md)).
- There is no `aws_credentials_provider` parameter, and the driver rejects it at connect: use `aws_profile` or the default credential chain.
- Each connection loads the metadata at its first statement, decrypts the data keys it needs with KMS, and keeps up to two background threads until it closes. The metadata load, each background refresh and the first read of a value written under an earlier key open a separate short-lived connection with the application connection's properties. Under any connection limit (a PostgreSQL role's `CONNECTION LIMIT`, MySQL's `max_user_connections`, the server's `max_connections`), leave room for one more connection per application connection (with `encryption_metadata_cache_enabled` off, one for each thread running a statement on that connection at the same time); otherwise the first statement raises `MetadataError`. If the limit is reached later, each background refresh logs a warning, and once the cached copy expires, every statement that names a table raises `MetadataError` until a connection can be opened again. A background refresh can run while a read looks up a `key_storage` row, so for that moment the application connection uses two short-lived connections, plus one for each other thread reading through it at the same time. A read that finds no room raises `KeyManagementError`. An application that opens a connection per request should use a connection pool, or Django's `CONN_MAX_AGE`.
- Closing the connection stops the plugin's background threads, wipes its cached data keys and closes its KMS client. If one of these steps raises, the plugin logs a warning that names it and still runs the others, and `close()` doesn't raise for it.
- In a process that forks after using the plugin (a gunicorn or Celery prefork worker, `multiprocessing` with the `fork` start method), the child has none of the plugin's background threads. Open new connections in the child, which start their own, and don't use a connection the parent opened. While a connection that used the plugin is open, its threads run, so on Python 3.12 and later each `fork()` emits a `DeprecationWarning`; the `forkserver` and `spawn` start methods avoid it.

## Configuration Parameters

| Parameter | Value | Required | Description | Default Value |
|---|---|---|---|---|
| `encryption_kms_region` | String | Yes, unless `AWS_REGION` or `AWS_DEFAULT_REGION` is set | AWS region of the KMS calls. When it isn't set, the plugin uses the `AWS_REGION`, then the `AWS_DEFAULT_REGION` environment variable. A region set only in the AWS config file, for the default profile or the one `aws_profile` names, isn't read: connecting raises `EncryptionConfigError` ("[EncryptionConfig] No KMS region configured: set encryption_kms_region, or the AWS_REGION or AWS_DEFAULT_REGION environment variable."), so set `encryption_kms_region` as well. | `None` |
| `encryption_kms_endpoint` | String | No | KMS endpoint URL override. | `None` |
| `encryption_allowed_master_key_arns` | String or list | Yes | The master key ARNs the plugin may use, as a comma-separated string or a list. Each must equal a `key_storage.master_key_arn` value exactly; a data key encrypted by any other master key is refused before KMS is called. | `None` |
| `encryption_metadata_schema` | String | No | Schema (database, on MySQL) of the `encryption_metadata` and `key_storage` tables: a letter or `_`, then letters, digits and `_`. | `encrypt` |
| `encryption_metadata_cache_enabled` | Boolean | No | Keep the metadata in memory. When off, each statement that names a table reads the tables on one new connection, which closes before the statement returns. | `True` |
| `encryption_metadata_cache_expiration_sec` | Float | No | How long the cached metadata answers lookups, in seconds. Keep it above `encryption_metadata_cache_refresh_interval_sec`. Once the cached copy has expired, the next statement that names a table reads the tables again on one new connection and caches them, whether or not the background refresh is on; if they can't be read, that statement raises `MetadataError` (see [Errors](#errors)). | `3600.0` |
| `encryption_metadata_cache_refresh_interval_sec` | Float | No | How often the metadata is refreshed in the background, in seconds. `0` turns the background refresh off. | `300.0` |
| `encryption_data_key_cache_enabled` | Boolean | No | Keep decrypted data keys in memory. When off, the plugin calls KMS `Decrypt` for every encrypted value it writes or reads, which adds latency and cost and can reach KMS request-rate limits; leave it on in production. | `True` |
| `encryption_data_key_cache_max_size` | Integer | No | Most decrypted data keys held in memory. | `1000` |
| `encryption_data_key_cache_expiration_sec` | Float | No | How long a decrypted data key stays cached, in seconds. | `300.0` |
| `encryption_key_management_max_retries` | Integer | No | Retries of a throttled or failed KMS call before it raises `KeyManagementError`. Each attempt waits up to 5 seconds to connect and 10 seconds for KMS to answer, so at the default a KMS endpoint that stops answering holds a statement for about 40 seconds before the error. | `3` |
| `encryption_key_management_retry_backoff_base_sec` | Float | No | Base delay of the exponential backoff between KMS retries, in seconds. | `0.1` |
| `encryption_audit_logging_enabled` | Boolean | No | Accepted. When true, each connection logs one warning that no audit records are written: "[KmsEncryptionPlugin] encryption_audit_logging_enabled is true, but this release writes no audit records." | `False` |
| `encryption_return_unverified_data` | Boolean | No | **Not for production.** See [Reading data written before encryption](#reading-data-written-before-encryption). | `False` |

Boolean parameters are on for `True`, for the string `"true"` in any case, and for the integer `1`. The settings are read when the connection is created, and a missing or invalid value raises `EncryptionConfigError`, whose message names the parameter. Two setup failures show only at the first statement (see [Errors](#errors)).

## What the plugin encrypts and decrypts

**Writes.** On `execute` and `executemany`, the plugin encrypts each bound parameter that goes into an encrypted column: `INSERT` (multi-row `VALUES` included), `UPDATE`, upserts (`ON CONFLICT`, `ON DUPLICATE KEY UPDATE`), `MERGE` and data-modifying CTEs. Parameters can be positional (`%s`) or named (`%(name)s`); a repeated `%(name)s` is encrypted once, with the first column's key, and a name also used for a column that isn't encrypted, a comparison with one, or a place with no column (such as `LIMIT`) is refused with `MetadataError`, so give each use its own name. The parameter objects you pass are never changed, and `None` stays `NULL`. `executemany` reads an iterable once, before the plugin's checks and its setup, so pass a list if you may retry the call. Each stored value carries an HMAC-SHA256 tag, the column's `key_storage` id, a type marker, the AES-GCM nonce, the ciphertext and the GCM tag, at least 65 bytes in all. Encryption is randomized, so equal values are stored differently.

**Comparisons.** A bound parameter compared with an encrypted column by `=`, `IN (...)`, `= ANY(...)`, `LIKE`, `ILIKE`, `<=>` or `IS NOT DISTINCT FROM` is encrypted too, so the comparison never matches, when it sits in the `WHERE` or `HAVING` of a query or in the `ON` of an inner join and is joined to the rest of the condition only by `AND` and `OR`. Django `filter()` and SQLAlchemy `filter_by()` then find nothing, and Django `get()` raises `DoesNotExist`. On PostgreSQL, an `ILIKE` comparison fails on the server with `operator does not exist: bytea ~~* bytea`, because `bytea` has no `ILIKE`; the value is still sent encrypted. `execute()` raises psycopg's `UndefinedFunction`, or under a failover plugin `AwsWrapperError` with it as `__cause__` (see [Errors](#errors)). A list bound to `= ANY(%s)` raises `EncryptionFailedError` instead, because a list has no stored form (see **Refused**). A sub-select whose select list is only the encrypted column counts as that column, so `%s IN (SELECT ssn FROM users)`, `%s = ANY (SELECT ssn FROM users)` and `%s = (SELECT ssn FROM users WHERE id = 1)` are encrypted in the same places. The same holds in `UPDATE` and `DELETE`, in a sub-select that `IN`, `EXISTS`, `= ANY` or `=` reads, and in a derived table or CTE that its query reads in `FROM`, `USING`, a `JOIN` or the table list of a MySQL multi-table `UPDATE`, unless that query has an outer join or the derived table or CTE comes first in `USING` or in the table list of a MySQL multi-table `UPDATE` or `DELETE`. In a write the plugin passes through, such as an `INSERT ... SELECT` (see **Passed through**), such a comparison is neither refused nor encrypted: its value is sent as written, and the database's statement log holds it. Any other comparison of a parameter with an encrypted column is refused with `MetadataError` before anything is sent, because there a comparison that never matches would select every row or arbitrary rows. That covers `<>`, `!=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `NOT IN`, `NOT LIKE`, `IS DISTINCT FROM`, `<> ALL(...)` and `= ALL(...)`, also with a sub-select of the column (`%s NOT IN (SELECT ssn FROM users)`, `%s <> ALL (SELECT ssn FROM users)`). It also covers a comparison under `NOT` or `IS` (`IS FALSE`, `IS NOT TRUE`), inside `CASE`, `IF`, `COALESCE` or another function, or in a select list, `ORDER BY` or `SET` value; one in the `ON` of an outer join or a `MERGE`, in `ON CONFLICT ... WHERE`, or in any join or derived table of a query that has an outer join; and one in a sub-select that `NOT IN`, `NOT EXISTS` or `EXCEPT` reads, or that groups, aggregates, calls a window function or has a `WINDOW` clause, as in `DELETE FROM users WHERE id NOT IN (SELECT id FROM users WHERE ssn = %s)`. A sub-select whose select list, `ORDER BY` or `DISTINCT ON` calls a function the plugin's SQL parser doesn't know, such as a user-defined aggregate or `to_jsonb`, counts as one that aggregates. Filter, look up and join on a column that isn't encrypted. For the same reason, a unique index or an upsert's conflict target on an encrypted column never detects a duplicate. A value compared with something the plugin can't tie to the column is sent as written and isn't refused, and the database's statement log holds it. Examples are an expression over the column (`COALESCE(ssn, alt) = %s`) or over the value (a function other than `CAST` or `CONVERT`, `COLLATE`, a sub-select or, on PostgreSQL, `||` around it, as in `ssn = lower(%s)`), a sub-select of the column that is a set operation or selects anything but the column (`%s IN (SELECT ssn FROM users UNION SELECT ssn FROM archive)`, `%s IN (SELECT lower(ssn) FROM users)`), a row value, `%s IN (ssn, alt)`, `ssn = ANY(ARRAY[%s])`, `SIMILAR TO`, a regular expression, a column inside a parenthesized join, or on MySQL inside a parenthesized table or table list, that is qualified by its alias, or that is unqualified in a query that reads exactly one table outside the parentheses (see [What the plugin does not see](#what-the-plugin-does-not-see)), and, on PostgreSQL, a sub-select under `ALL` or `SOME` followed by `FETCH FIRST` inside the function's parentheses (`%s <> ALL ((SELECT ssn FROM users) FETCH FIRST 5 ROWS ONLY)`), which the plugin's SQL parser drops. With `<>` or another operator the plugin refuses, such a comparison matches every non-NULL row or arbitrary rows.

**Annotations.** An `/*@encrypt:table.column*/` comment directly before a placeholder names the column its value is for, and wins over the column the plugin infers. Leading schema names are ignored, and `/*@encrypt:ssn*/` names that column of the table the statement writes, or of the only table it reads. An annotation doesn't make an unbound write acceptable: `SET ssn = upper(/*@encrypt:users.ssn*/ %s)` is refused.

**Types.** A value reads back as the Python type it was written as: `str`, `bool`, `int`, `float`, `Decimal`, `date`, `datetime` or `bytes` (`bytearray` and `memoryview` read back as `bytes`). An aware `datetime` reads back in UTC with millisecond precision, and a naive one reads back with its microseconds. A `datetime.time` reads back as ISO text, and a `str` subclass, such as a `(str, Enum)` member, is stored as its own text and reads back as a plain `str`. An `IntEnum` or `IntFlag` member reads back as a plain `int`, and an `int` outside the signed 64-bit range or a `Decimal` NaN or infinity raises `EncryptionFailedError`.

**SQL types.** SQL passed as `bytes` and psycopg `sql` objects are analyzed like `str` SQL. A `bytes` statement that the connection's encoding doesn't decode exactly passes through with the same warning as other SQL the plugin can't analyze: with PostgreSQL's `SHIFT_JIS_2004`, whose Python codec reads the bytes of `\` and `~` as a yen sign and an overline, every `bytes` statement does. On MySQL only `bytes` SQL that is all ASCII is analyzed, and other `bytes` SQL passes through with that warning. Send such SQL as `str`, which is analyzed.

**Refused.** These raise before anything is sent, on every cursor kind, psycopg `RawCursor` and prepared cursors included:

- A write to an encrypted column of anything but a bind parameter (a literal, an expression, `DEFAULT` or another column), and a `COPY ... FROM` sent through `execute`, raise `MetadataError` with `table_name` and `column_name` set. Bind the value instead. A `NULL` in the SQL text is stored.
- A value with no exact stored form (a UUID, a plain `Enum` member, a list, tuple or dict, an IP address, a `timedelta`, psycopg `Jsonb` or `psycopg.Binary`), written to or compared with an encrypted column, raises `EncryptionFailedError` naming its type and the column. Convert it first; for `psycopg.Binary`, pass `bytes`.
- An annotation that names a column without encryption metadata (a plain column, a typo, a table that doesn't exist), on a value written to or compared with an encrypted column, raises `MetadataError` naming the encrypted column.
- In a statement that reads several tables, one of which has an encrypted column of that name, a value compared with a column the plugin can't tie to a table (a derived table's or CTE's column joined with another table, or an unqualified column in a join) raises `MetadataError` naming the encrypted column, also when the value's annotation names no table the statement uses. To run the statement, annotate the value with the table and column it is compared with, a plain column's included (`/*@encrypt:archive.ssn*/ %s`).
- A comparison of a bound value with an encrypted column where a comparison that never matches would still select rows (see **Comparisons**) raises `MetadataError` naming the column, with `table_name` and `column_name` set, also in a write that is otherwise passed through. Compare a column that isn't encrypted instead.
- A multi-statement string is analyzed as one statement, so a literal written to an encrypted column in any of its statements is refused too.

**Passed through.** A write to a table with encrypted columns whose target columns the plugin can't list runs unchanged, with a warning: an `INSERT` without a column list, `INSERT ... SELECT`, and on MySQL an `UPDATE` of several tables whose `SET` names any column without its table (qualify every column it sets, as in `SET u.ssn = %s`, and the plugin encrypts the encrypted ones). A multi-statement string that holds such a write is sent whole, the other statements' values included. SQL objects that aren't `str`, `bytes` or a psycopg `sql` object, such as `bytearray`, `memoryview` and Python 3.14 template strings, pass through in the same way; what the driver then does with them is the driver's. The warning is logged once per connection for each statement shape, from a bounded set, and says what the plugin couldn't analyze, as in "(the statement does not name the columns it writes)". The trigger rejects any plaintext such a statement writes. A value it compares with an encrypted column reaches the database's statement log, unless the comparison is one the plugin refuses (see **Comparisons**). On mysql-connector, the first 40 characters of the statement as sent, values included, also appear in the `AwsWrapperError` the cursor raises if it is used again after failover or read-write splitting has changed the connection.

**Reads.** `fetchone`, `fetchmany`, `fetchall`, `next(cursor)`, `for row in cursor` and psycopg's `cur.execute(query).fetchone()` return decrypted values. `cursor.description` is the driver's, unchanged: an encrypted column reports its storage type (on PostgreSQL `bytea`, type code 17; on MySQL the binary column's type, 253 for `VARBINARY`) while the row holds the decrypted value, so don't infer a value's type from `type_code`. Result columns are matched to their source column, so aliases, joins, `RETURNING`, derived tables and CTEs decrypt; an expression, a set operation's column and a select list with `*` are matched by name only (see [What the plugin does not see](#what-the-plugin-does-not-see)). psycopg row factories other than the default, `dict_row` and `namedtuple_row` raise `MetadataError` on a result with an encrypted column. A `for` loop over a cursor decrypts its rows for the statement that was current when the loop started, so finish the loop before you call `nextset()` or run another statement on the same cursor. If you don't, the loop's later rows can come back still encrypted or raise `IntegrityCheckError`. In psycopg's pipeline mode (`with conn.pipeline():`), the plugin reads a result's columns when its rows arrive, so the row-factory check and a loop's decoding happen then.

### Errors

Import the error classes from `aws_advanced_python_wrapper.errors`; `EncryptionPluginError` catches every one of them. Django doesn't translate them into `django.db` errors. SQLAlchemy wraps them in its own classes instead: `IntegrityCheckError` arrives as `sqlalchemy.exc.IntegrityError`, and `MetadataError`, `EncryptionFailedError` and `KeyManagementError` as `sqlalchemy.exc.OperationalError`, with the plugin's error as `orig`; the connection stays usable. A column whose key row is missing or unusable raises `InvalidKeyMetadataError` on write and on read (a value written under an earlier key still decrypts with that key), and a column whose `encryption_metadata` row names an algorithm other than exactly `AES-256-GCM` or `AES-128-GCM` raises `InvalidAlgorithmError`, naming the column and the algorithm, before KMS is called; a master key outside the allow-list raises `UnauthorizedMasterKeyError`, a `KeyManagementError`, before KMS is called; and KMS failing after the retries raises `KeyManagementError`. Its message quotes KMS's error with the ARNs in it masked; so do the `DEBUG` record logged before each retry and the warning that `create_master_key` logs when it can't create an alias. The AWS SDK's own error, kept as its `__cause__`, quotes KMS in full. That can include the master key's ARN (a disabled key, a key pending deletion, a key policy that denies the call) and, for an access denial, the ARN of the role session the application runs as, and a traceback that prints the cause shows them. With the metadata cache on, if the metadata tables can't be read at a connection's first statement, that statement and every later one, including a `SELECT 1`, raise `MetadataError` until the tables can be read. If they can't be read after the cached copy expires (`encryption_metadata_cache_expiration_sec`), or at any lookup with the metadata cache off, every statement that names a table raises `MetadataError` instead of passing through, even if the table has no encrypted columns; until the cached copy expires, it keeps answering. In each of these cases the plugin raises the `MetadataError` itself, before the statement reaches a failover plugin, so it doesn't start failover. If the database host can't be reached, close the connection (with SQLAlchemy, call `invalidate()` on it) and open a new one.

The plugin's own errors keep their classes with or without a failover plugin in the chain (on mysql-connector, see the note on unread rows below). A trigger's rejection reaches the application as the driver's own error (`psycopg.errors.RaiseException`, `mysql.connector.errors.DatabaseError`) when no failover plugin is in the chain. Under `failover` or `failover_v2`, which the default plugin lists include, or under `gdb_failover`, `execute()` raises it as `AwsWrapperError` with the driver's error as `__cause__`, unless [`preserve_driver_exception_type`](./UsingTheFailover2Plugin.md#failover-plugin-v2-configuration-parameters) is on: then it is the driver's own error. From `executemany()` it is still the driver's own error, because the failover plugins don't intercept `executemany`.

A setup failure at the first statement, such as an `aws_profile` that doesn't exist or a malformed `encryption_kms_endpoint`, raises the AWS SDK's or Python's own error (`botocore.exceptions.ProfileNotFound`, `ValueError`), not a plugin error class. Nothing is sent, and the next statement tries the setup again.

When a stored value can't be read, the errors for a missing or unusable key row, for an unsupported algorithm, for a value whose key id names no usable key row (`IntegrityCheckError`) and for a value that isn't binary (`IntegrityCheckError`) name the column. A value that is too short, fails its HMAC check or doesn't decrypt raises `IntegrityCheckError` or `DecryptionError` without naming the column.

On mysql-connector's default cursor, a read error can leave the rest of the result unread. Until it is read, the driver raises its own `InternalError` ("Unread result found") when that cursor closes and when the connection opens its next cursor (under the default plugins, as `AwsWrapperError`), so leaving a `with conn.cursor()` block raises the driver's error instead of the plugin's. Connect with `consume_results=True`, or open such cursors with `buffered=True`, to get the plugin's error and keep using the connection; `conn.rollback()` also discards the unread rows. The driver behaves the same after any fetch that stops before the end of a result.

### Reading data written before encryption

When a column already holds plaintext, reading it through the plugin raises `IntegrityCheckError`. Encrypt the existing rows first: read each value without the plugin and write it back through the plugin. Only for a short, controlled migration, set `encryption_return_unverified_data=True`: a value the plugin can't verify as the column's encrypted value then comes back as the driver returned it (usually `bytes`), next to the decrypted values, and the plugin logs nothing; a value that verifies but doesn't decrypt still raises. While the column's own key row is missing or unusable, a value shorter than 36 bytes, or one that carries the column's key id, raises `InvalidKeyMetadataError` with this setting too. **Don't enable it in production:** a tampered value then reads back without an error. While a column still holds such values, each read of one that is 36 bytes or longer looks up the `key_storage` row its bytes 32 to 35 name, on a new database connection each time and in both modes, so a database statement log records those four bytes as a number on every read: migrate such values first.

## What the plugin does not see

The plugin doesn't see the statements and results below, except where a line says otherwise. Plaintext written through them is stopped only by the trigger. A value they compare with an encrypted column is sent as written, so the database's statement log holds it. Encrypted values read through them come back as stored bytes.

- psycopg `cursor.copy()`, so a `COPY ... FROM` into an encrypted column is stopped by the trigger, not refused by the plugin; `cursor.stream()` (to stream large results, use a named cursor with `fetchmany`); `cursor.results()`; and `cursor.set_result()`. Read each set from the cursor that `results()` yields or `set_result()` returns, which returns stored bytes. A fetch from the cursor you called them on decodes each set it reads after its first with the columns of that first set, so a value can come back as stored bytes, and the fetch can raise `IntegrityCheckError`, `IndexError` or `KeyError`.
- Result sets that `nextset()` moves to come back as stored bytes, except those of an `executemany`, whose columns are matched again for each set. psycopg's `executemany(..., returning=True)` sets all come from one statement and decrypt like the first. The first result set of a multi-statement string is matched by name only, as a select list with `*` is (see below), and so is each later set when mysql-connector's `executemany` runs such a string with one parameter set. An encrypted column in such a set decrypts only if no other table the string names has an encrypted column of that name (`a.users` and `b.users` count as one table, see below) and no other column of the set has that name. Run one `SELECT` per `execute()` when it reads encrypted columns.
- `callproc()`, mysql-connector `stored_results()` and `fetchsets()`; SQL `PREPARE` and `EXECUTE` (on PostgreSQL, a `PREPARE` of a write gets the pass-through warning); `LOAD DATA`; stored routines and triggers.
- psycopg `RawCursor` (`$1`) and `?` placeholders on mysql-connector prepared cursors are not supported for encrypted columns: a write that names an encrypted column is refused, and a write whose columns the plugin can't list goes to the trigger. mysql-connector prepared cursors with `%s` placeholders are encrypted and decrypted like default cursors.
- Connections taken from `target_connection`, the driver's cursor that `cursor.target_cursor` returns, reads through a view, and connections whose `plugins` list doesn't include `kms_encryption`.
- A column renamed by a set operation (`SELECT ssn AS x FROM users UNION ALL ...`) and an expression over an encrypted column, also behind a derived table, unless the expression keeps the column's name. In `WITH RECURSIVE a AS (SELECT ssn FROM users), users AS (...) SELECT ssn FROM a`, `a` reads the CTE `users` on PostgreSQL and the table `users` on MySQL, and the plugin takes it for the CTE on both engines. So on MySQL, when no other table the statement reads has an encrypted `ssn`, a value compared with `users.ssn` is sent as written, and `ssn` comes back as stored bytes. When exactly one does, `users.ssn` is matched to that one by name: an encrypted `users.ssn` decrypts, and a plain one raises `IntegrityCheckError`. Don't name a CTE like a table. The columns of a `SELECT` or `RETURNING` whose list has `*` or `t.*` are matched by name in any letter case: an encrypted column there decrypts only under its own name, only if no other result column has that name, and only if no other table the statement reads has an encrypted column of that name. So it comes back as stored bytes from a `*` over a join whose tables share its name, beside another column of that name (`SELECT *, ssn FROM users`, `SELECT *, upper(name) AS SSN FROM users`), under an alias or a derived table's new name (`SELECT *, ssn AS x FROM users`, `SELECT * FROM (SELECT ssn AS x FROM users) s`), and when the `*` is in a derived table or CTE whose column the outer query renames (`SELECT s.ssn AS social FROM (SELECT * FROM users) s`). The metadata names a table without its schema, so `a.users` and `b.users` count as one table (on MySQL, so do `users` and `USERS`), and `ssn` decrypts in `SELECT * FROM a.users WHERE id IN (SELECT id FROM b.users)`. The name-matching rule doesn't check which table a column comes from, so a plain column that meets it is decoded as the encrypted column of its name: with a plain `archive.ssn`, reading `SELECT a.* FROM archive a JOIN users u ON u.id = a.user_id` raises `IntegrityCheckError`, which can name `users.ssn`; with `encryption_return_unverified_data=True`, the read returns the value as is. List the columns instead of `*`.
- A value compared with a column that a derived table or CTE renames (`SELECT s.x FROM (SELECT ssn AS x FROM users) s WHERE s.x = %s`, or a CTE column list), or with an unqualified column that a subquery takes from the outer query, is sent as written, isn't refused, and the database's statement log holds it. With `=` it matches no row; with an operator the plugin refuses on an encrypted column, such as `<>`, it matches every non-NULL row or arbitrary rows. Compare the table's own column, name its table (`u.ssn`), or annotate the value.
- In a statement that reads several tables, an annotation that names a table the statement uses is trusted. One that names that table's plain column, a misspelled column, or the right column on the wrong table, on a value compared with an encrypted column the plugin can't tie to a table, is sent as written in the same way. Annotate with the table whose column the value is compared with.
- A parenthesized join, as in `FROM orders o JOIN (users u JOIN items i ON i.uid = u.id) ON o.uid = u.id`, and on MySQL a parenthesized table or table list, as in `FROM (users u)` or `JOIN (users u, items i) ON ...`. A value compared with a column inside the parentheses that is qualified by its alias (`u.ssn`), or that is unqualified in a query that reads exactly one table outside the parentheses, is sent as written, isn't refused, and the database's statement log holds it; with `<>` it matches every non-NULL row. An encrypted column read in either of those two ways comes back as stored bytes. On MySQL, an `UPDATE` whose tables are all inside parentheses, as in `UPDATE (users u) SET u.ssn = %s`, sends the values it sets as written, with no warning, so only the trigger stops them, and the statement log holds them. Write the tables without the parentheses.
- Statements the plugin can't tie to any table run unchanged with no warning: `EXPLAIN ANALYZE` of a write, MySQL `/*! ... */` executable comments (`INSERT /*! IGNORE */ INTO ...`), and SQL the plugin can't parse. A statement with a placeholder inside a quoted literal, quoted identifier or comment is refused with `MetadataError` when the plugin can tie it to a table with encrypted columns, because the driver binds that placeholder too and the plugin can't tell which value goes to which column. On other tables, and in the forms above that the plugin can't tie to a table, it runs unchanged.
- Servers that don't use the default quoting rules: MySQL `NO_BACKSLASH_ESCAPES` or `ANSI_QUOTES`, PostgreSQL `standard_conforming_strings=off`.
- The `encryption` extra installs sqlglot's pure-Python build. sqlglot's compiled build, the `sqlglotc` package that `sqlglot[c]` and `sqlglot[rs]` install, can crash on very deeply nested SQL, so don't install it alongside the plugin.
- Keep text decoding on for the plugin's connections: the mysql-connector connection options `use_unicode=False` and `raw=True` also apply to the plugin's own metadata reads and aren't supported (a `raw=True` cursor works); on PostgreSQL, use a UTF-8 client encoding (on a `SQL_ASCII` connection, psycopg returns text as bytes, so reads and writes of encrypted columns raise `InvalidAlgorithmError`, whose message shows the algorithm's name as bytes, for example `b'AES-256-GCM'`).

## Enforce encryption in the database

Every encrypted column needs a trigger that checks each value written to it. The trigger needs no help from the application: the HMAC key that signs each value is stored unencrypted in `key_storage`, so the database can check that a value carries a valid tag without holding the data key. The trigger SQL ships with the wrapper, in the `aws_advanced_python_wrapper/encryption/sql` folder of the package:

- [`trigger_pg.sql`](../../../aws_advanced_python_wrapper/encryption/sql/trigger_pg.sql) installs the `pgcrypto` extension if it isn't installed, and creates the `enforce_encrypted_column()` trigger function. The function reads the metadata tables as `encrypt.encryption_metadata` and `encrypt.key_storage`: replace `encrypt` in the file if `encryption_metadata_schema` is set to something else. If `pgcrypto` is already installed in a schema other than `public`, replace `public` in `public.hmac` with that schema.
- [`trigger_mysql.sql`](../../../aws_advanced_python_wrapper/encryption/sql/trigger_mysql.sql) builds HMAC-SHA256 from `SHA2()`, because MySQL has no HMAC function, and creates the two functions and two validation procedures that the per-column triggers call. Follow the MySQL steps below before you run it.

Run the file for your engine once per database, in a single `cursor.execute()`. psycopg runs `trigger_pg.sql` with no parameters (passed `params=()`, it reads the file's `%` signs as placeholders and fails), and mysql-connector 9.2 or later runs `trigger_mysql.sql` with its `DELIMITER` lines (older versions reject the file with error 1064); read every result set it returns:

```python
from importlib.resources import files

sql_dir = files("aws_advanced_python_wrapper.encryption") / "sql"

# PostgreSQL
with conn.cursor() as cur:
    cur.execute((sql_dir / "trigger_pg.sql").read_text(encoding="utf-8"))
conn.commit()

# MySQL, with SCHEMA_NAME replaced by the metadata schema (step 2 below)
with conn.cursor() as cur:
    cur.execute((sql_dir / "trigger_mysql.sql").read_text(encoding="utf-8").replace("SCHEMA_NAME", "encrypt"))
    while cur.nextset():
        pass
conn.commit()
```

Then create a trigger for every column that you want to encrypt. On PostgreSQL that is one trigger per column, and on MySQL a `BEFORE INSERT` and a `BEFORE UPDATE` trigger per column. Through a driver, send each `CREATE TRIGGER` in its own `cursor.execute()`; the `DELIMITER` lines and the `$$` after each `END` are for the `mysql` command-line client.

```sql
-- PostgreSQL
CREATE TRIGGER users_ssn_encrypted
BEFORE INSERT OR UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION enforce_encrypted_column('ssn');
```

```sql
-- MySQL
DELIMITER $$
CREATE TRIGGER users_ssn_hmac_check
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$

CREATE TRIGGER users_ssn_hmac_check_update
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_update('users', 'ssn', NEW.ssn, OLD.ssn);
END$$
DELIMITER ;
```

Each file records its version in a database comment, which the queries below return. The text ends with `kms_encryption trigger v2` for `trigger_pg.sql` and `kms_encryption trigger v1` for `trigger_mysql.sql`, the versions this guide describes. An empty result is a copy from before versions were recorded: on PostgreSQL, run the file again; on MySQL it is equivalent to v1. When a later release of the wrapper ships a newer version of a file, its CHANGELOG entry says so: run the file again. That replaces the functions and procedures without changing data, and the per-column triggers keep working. On MySQL a write to an encrypted column can fail while a routine is dropped and re-created, so run it while writes are quiet.

```sql
-- PostgreSQL
SELECT obj_description('enforce_encrypted_column'::regproc, 'pg_proc');
-- MySQL, in the schema the routines were created in
SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES
 WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert';
```

On MySQL, also do the following:

1. With binary logging on, an account without `SUPER` can't create the routines (error 1419). On a self-managed server it can't create the per-column triggers either (error 1419), and if a `CREATE TRIGGER` fails, the column is unprotected and plaintext is stored without an error. On Aurora MySQL the master user can create the per-column triggers without the routines; every write to the column then fails with error 1305 and stores nothing until the routines are installed. Binary logging is on by default since MySQL 8.0; on Aurora MySQL it is off unless the DB cluster parameter group sets `binlog_format`. Where it is on, have an administrator set `log_bin_trust_function_creators` to `1` first. On a self-managed server:

   ```sql
   SET GLOBAL log_bin_trust_function_creators = 1;
   ```

   On Aurora MySQL, the master user can't run that statement (error 1227: it needs `SUPER` or `SYSTEM_VARIABLES_ADMIN`). Set the parameter in the DB cluster parameter group instead.

2. Replace `SCHEMA_NAME` in `trigger_mysql.sql` with your metadata schema, and run the file in every schema that holds an encrypted table: the per-column triggers call the routines unqualified (error 1305 otherwise). A file run with `SCHEMA_NAME` left in installs, then fails every write: with error 1142 when the account that ran it can't read `SCHEMA_NAME.*`, and with error 1146 when it can, as an Aurora master user's global `SELECT` allows.
3. The routines run as their definer, the account that ran the file (`schema_owner` below). Grant it read access to the metadata schema:

   ```sql
   GRANT SELECT ON encrypt.* TO 'schema_owner'@'%';
   ```

To stop encrypting a column, drop its triggers in the same change, or every later write to it fails:

```python
with conn.cursor() as cur:
    # PostgreSQL. On MySQL, drop the column's BEFORE INSERT and BEFORE UPDATE triggers: DROP TRIGGER <name>, without ON.
    cur.execute("DROP TRIGGER users_ssn_encrypted ON users")
utility.remove_encryption_for_column("users", "ssn")
conn.commit()
```

### Updates after a key rotation

The trigger checks a new value against the column's current HMAC key, and rotating a column's data key also gives it a new HMAC key. Values written before the rotation keep the earlier key's tag, which stays valid for them: the plugin reads them with the key they were written with. So the trigger doesn't check an `UPDATE` that leaves the encrypted value unchanged. Without that, every update of a row written before the rotation, even one that changes only another column, would be rejected until the row's values were encrypted again. An `INSERT`, or an `UPDATE` that changes the encrypted column, is checked and must carry the current key's tag, which the plugin's writes do once the connection has the new key (see [Rotate a column's data key](#rotate-a-columns-data-key)).

## Hardening

Three controls decide how much the encryption protects: which KMS keys the application can use, who can write the metadata schema, and who can change the trigger. The first three steps cover them, and the fourth covers what none of them stops.

- **KMS keys.** The plugin takes a column's master key from its `key_storage` row, not from its own settings, so the allow-list and the application's IAM policy keep a changed row from pointing it at another key. List every master key the columns use in `encryption_allowed_master_key_arns`, in the form `key_storage.master_key_arn` holds. The plugin refuses any other key before calling KMS (`UnauthorizedMasterKeyError`), on reads and writes alike, and `encryption_return_unverified_data` doesn't change that. Add a new master key before rotating a column onto it, and keep the old one listed, with `kms:Decrypt` allowed, while values written under it remain. Give the application's credentials only `kms:Decrypt`, on those key ARNs, never `"Resource": "*"`, and use a separate key per environment. Run `KeyManagementUtility`, which also calls `kms:GenerateDataKey`, `kms:CreateKey`, `kms:CreateAlias` and `kms:DescribeKey`, under a separate administrative credential. Don't add a `kms:ViaService` condition to these policies. It matches only requests an AWS service makes on your behalf, and the plugin calls KMS directly, so every `Decrypt` would be denied.

  ```json
  {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "kms:Decrypt",
    "Resource": "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"}]}
  ```

- **Metadata schema.** A role that can write `key_storage` can replace a column's HMAC key and forge values the trigger accepts, or delete or swap a column's key rows and make its values unreadable; the allow-list stops only a switch to an unlisted master key. Grant the application's database role only `SELECT` on the metadata schema (PostgreSQL: `GRANT USAGE ON SCHEMA encrypt TO app_role; GRANT SELECT ON ALL TABLES IN SCHEMA encrypt TO app_role;`, MySQL: `GRANT SELECT ON encrypt.* TO 'app_user'@'%';`). Grant `INSERT`, `UPDATE` and `DELETE` there only to the administrative role that runs `KeyManagementUtility`.
- **Triggers.** Create the encrypted tables, the trigger SQL and the per-column triggers as an administrative role, and don't run the application as that role or as the master user. A table's owner can disable or drop its triggers (`ALTER TABLE users DISABLE TRIGGER users_ssn_encrypted` on PostgreSQL). A role with the `TRIGGER` privilege can add a trigger that runs after the check and stores plaintext, and on MySQL can drop triggers. The owner of the trigger function on PostgreSQL, or an account with `ALTER ROUTINE` on the routines on MySQL (their creator has it), can replace them. A superuser, the RDS master user included, can do all of this, and on PostgreSQL can skip triggers with `SET session_replication_role = replica`. Give the application's role data privileges only (PostgreSQL: `GRANT SELECT, INSERT, UPDATE, DELETE ON users TO app_role; GRANT USAGE ON SEQUENCE users_id_seq TO app_role;`, MySQL: `GRANT SELECT, INSERT, UPDATE, DELETE ON mydb.users TO 'app_user'@'%';`), never `TRIGGER` or `ALTER ROUTINE`.
- **Rows.** An encrypted value isn't tied to its row. A value copied into another row of the same column passes the trigger and decrypts there, so a role that can `UPDATE` the table, the application's own included (and so any SQL injection), can move values between rows without a key. A value moved to another column fails the trigger, because each column has its own keys. Grant `UPDATE` on encrypted tables only where it is needed. Where a value must belong to its row, store the row's key inside the value and check it after reading.
- The plugins that run before `kms_encryption` receive your unencrypted parameters; the plugins after it and the driver receive the encrypted values. With `auto_sort_wrapper_plugin_order` on, the plugins before it are `custom_endpoint`, `initial_connection`, `aurora_connection_tracker`, `stale_dns`, `read_write_splitting`, `srw` and `gdb_rw`; `connect_time`, `execute_time` and `dev` sort right after the plugin listed before them, so they run before it when that plugin does or when they are listed first. No built-in plugin logs parameters; if you write your own plugin, don't log them either.
- Keep the root logger and the `botocore` logger above `DEBUG` in production (see [Logging](../UsingThePythonWrapper.md#logging)). At `DEBUG`, botocore logs every AWS request with its access key id, session token and signature, and, with a profile that assumes a role, the temporary credentials it fetches. Once the plugin has created its KMS client, or a script has built one with `KeyManager.create_kms_client` (as the setup example above does for `KeyManagementUtility`), the wrapper drops from then on, for every KMS client in the process (the application's included), the botocore records of KMS calls that would carry a data key or these credentials, and for every AWS service botocore's separate record of each request's signature and its record of any response body that contains the quoted word `"Plaintext"`. It drops no other records. A KMS client built any other way turns none of this on: if a setup script passes one to `KeyManagementUtility` before the plugin or `create_kms_client` has built a client in the process, botocore's `DEBUG` log records the column's new data key.
- SQLAlchemy's error messages include bound values unless the engine is created with `hide_parameters=True`: set it, and leave `echo` off. Keep Django's `django.db.backends` logger above `DEBUG`.
- Error reporters that record local variables (Sentry's default) capture plaintext values from the wrapper's frames, the plugin's included: a statement's values when it raises (a refused write, or the driver's own error such as a duplicate key), and, when a read fails, the values already decrypted from that row. Turn that off, or scrub every frame of the `aws_advanced_python_wrapper` package.
- Data keys are wiped from memory after use on a best-effort basis; Python and `cryptography` may keep copies.

## Using SQLAlchemy and Django

On PostgreSQL, SQLAlchemy casts typed parameters (`%(ssn)s::VARCHAR`), which the server rejects for a `bytea` column, and nothing is stored. Declare encrypted columns with a pass-through type, which works with any value type:

```python
from sqlalchemy.types import UserDefinedType


class EncryptedValue(UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kw):
        return "BYTEA"
```

`LargeBinary` also works, but only with `bytes` values. On MySQL, declare encrypted time columns as `String`: a `Time` column fails on read, and a `String` one reads back the ISO text.

On MySQL the plugin adds `_binary` before each encrypted value, so the server raises no warning 1300, and its SQL shows `_binary '...'`. On `executemany`, a placeholder whose value is `None` in any parameter set gets no prefix in any set, so that call keeps warning 1300, and with `raise_on_warnings=True` it raises the driver's `DatabaseError`. For an `INSERT ... VALUES`, which mysql-connector sends as one statement, the error comes after every row is written. Any other statement is sent once per parameter set, so the error comes after the first set that warns, and the later sets aren't sent. Write `_binary` directly before the placeholder or leave it out: with a comment between them, an `INSERT`'s value is refused as an unbound write, and a comparison or `UPDATE` isn't seen.

Django (MySQL only): `DEBUG=True` works. `DateField` reads back as `str`. `DateTimeField` and `TimeField` aren't supported on encrypted columns: they read back as `bytes`, and with `USE_TZ=True` reading a `DateTimeField` raises. `bulk_update()` on an encrypted column is refused, because it writes the value inside a `CASE` expression.

On MySQL, connections that write encrypted columns should use the default `utf8mb4` or a single-byte client character set whose MySQL name Python accepts as a codec name, such as `latin1`. With `sjis`, `cp932`, `big5` or `gbk`, use mysql-connector's C extension, not `use_pure=True`, and don't use `gb18030` on either build: there the driver changes some of the encrypted values it sends, so the trigger refuses those writes (without the trigger they are stored, and reading them raises `IntegrityCheckError`), and other writes fail with the driver's own error. Set the character set with mysql-connector's `charset` connection argument, not with `SET NAMES` sent as SQL: after such a statement, mysql-connector keeps reporting the one it negotiated, and in a session switched that way to `swe7`, a value bound for an encrypted table or column whose name holds É, Ä, Ö, Å, Ü or their lowercase forms can be sent unencrypted with no warning. Given as the `charset` argument, a character set whose MySQL name isn't a Python codec name, such as `koi8r` (Python calls it `koi8_r`) or `swe7` (Python has no codec for it), makes the first statement raise `MetadataError` ("[MetadataManager] Failed to read the kms_encryption metadata: unknown encoding: koi8r").
