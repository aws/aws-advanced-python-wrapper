# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [3.1.0] - 2026-08-24
### :magic_wand: Added
* Python 3.14 support. ([PR #1252](https://github.com/aws/aws-advanced-python-wrapper/pull/1252))
* Aurora Global Database support. Adds the `gdb_failover` plugin, which is aware of a home region and selects a failover target using the `failover_home_region`, `active_home_failover_mode` and `inactive_home_failover_mode` parameters, and the `gdb_rw` plugin, which restricts read/write splitting to the home region and can defer writes to Global Write Forwarding. Global writer endpoints are recognized, and cross-region topology is discovered through the `global-aurora-pg` / `global-aurora-mysql` dialects using `global_cluster_instance_host_patterns`. See [Aurora Global Databases](./docs/using-the-python-wrapper/GlobalDatabases.md). ([PR #1243](https://github.com/aws/aws-advanced-python-wrapper/pull/1243), [PR #1246](https://github.com/aws/aws-advanced-python-wrapper/pull/1246))
* `gdb_accessible_regions`, restricting host selection to the AWS regions an application can reach, and `monitoring_connection_priority` / `gdb_monitoring_connection_priority`, controlling which host role or region the topology monitor uses for its background connection. Both are currently supported in the synchronous driver only. ([PR #1266](https://github.com/aws/aws-advanced-python-wrapper/pull/1266))
* Async (asyncio) counterpart of the wrapper (`aws_advanced_python_wrapper.aio`), targeting sync parity for the shipped plugins: failover v2, read/write splitting, EFM v2, IAM auth, AWS Secrets Manager, federated + Okta auth, Aurora connection tracker, cluster topology monitor, custom endpoint, stale DNS, Aurora initial connection strategy, simple read/write splitting, developer plugin, blue/green deployment, limitless, fastest-response strategy. Backed by psycopg async and aiomysql, with async SQLAlchemy support via `create_async_engine` (`postgresql+aws_wrapper_psycopg://` serves both sync and async; MySQL async uses `mysql+aws_wrapper_aiomysql://`). Includes `AsyncConnectionProvider`/`AsyncPooledConnectionProvider`, `AsyncSessionStateService`, an async IdP factory registry, and `release_resources_async()` for background-task teardown. ([PR #1257](https://github.com/aws/aws-advanced-python-wrapper/pull/1257))

### :bug: Fixed
* Cross-thread use-after-free (SIGSEGV) when an offloaded query times out (e.g. during failover) and the connection is later closed or reused while the query is still running: on timeout the driver dialects now shut down the connection socket and wait for the worker to unwind before propagating, and leak rather than close a connection whose worker cannot be drained. ([PR #1252](https://github.com/aws/aws-advanced-python-wrapper/pull/1252))
* Schema-qualified every function, operator, cast and catalog reference in the PostgreSQL queries the driver issues internally, so a session `search_path` cannot redirect them. This covers the queries issued by the asynchronous Aurora host list provider as well. ([PR #1262](https://github.com/aws/aws-advanced-python-wrapper/pull/1262), [PR #1270](https://github.com/aws/aws-advanced-python-wrapper/pull/1270))
* Connection properties whose name indicates a credential are now masked when connection properties are logged. Previously only `password` was masked, so properties such as `idp_password` and the `monitoring-` and `blue-green-monitoring-` prefixed passwords were logged in clear text. The Secrets Manager properties remain readable, since they identify a secret rather than hold one. ([PR #1270](https://github.com/aws/aws-advanced-python-wrapper/pull/1270))
* `opentelemetry-api` is now declared as a runtime dependency rather than a development one. ([PR #1261](https://github.com/aws/aws-advanced-python-wrapper/pull/1261))
* `boto3-stubs` and `types_aws_xray_sdk` are no longer declared as runtime dependencies. Both are type-stub distributions with no runtime effect, and declaring them meant their version ranges constrained applications that pin those packages themselves. ([PR #1274](https://github.com/aws/aws-advanced-python-wrapper/pull/1274))
* `create_engine("postgresql+aws_wrapper_psycopg://")` now resolves. The PostgreSQL SQLAlchemy dialect entry point registered by 3.0.0 pointed at a module that was not present in the distribution, so constructing the engine raised `ModuleNotFoundError` before any connection was attempted. ([Issue #1260](https://github.com/aws/aws-advanced-python-wrapper/issues/1260), [Issue #1273](https://github.com/aws/aws-advanced-python-wrapper/issues/1273), [PR #1252](https://github.com/aws/aws-advanced-python-wrapper/pull/1252))
* Query timeouts are now threaded through the failover paths, and pooled connections are invalidated in the Aurora connection tracker. ([PR #1255](https://github.com/aws/aws-advanced-python-wrapper/pull/1255))
* `pool_pre_ping` is now supported in the SQLAlchemy ORM MySQL dialect. ([PR #1245](https://github.com/aws/aws-advanced-python-wrapper/pull/1245))
* Issues found while aligning the synchronous and asynchronous implementations ([PR #1256](https://github.com/aws/aws-advanced-python-wrapper/pull/1256)):
  * The Aurora Initial Connection Strategy Plugin computed its retry deadline from `open_connection_retry_interval_ms` rather than `open_connection_retry_timeout_ms`, so it gave up retrying much earlier than configured.
  * The Limitless Plugin discarded the result of its login-exception check, so an authentication failure while fetching transaction routers was never recognized as one.
  * The Limitless Plugin did not read the connection from its routing context before using it.
  * Resetting the session state transfer handler assigned to a name that did not exist, leaving a previously registered handler installed.
  * A message key was raised in place of its resolved text when the current host could not be determined.
  * Added two missing Blue/Green deployment log messages.
* Documentation corrections. ([PR #1244](https://github.com/aws/aws-advanced-python-wrapper/pull/1244))
* Corrected documented examples that could not run as written: the plugin codes `failover2` and `efm2` (the registered codes are `failover_v2` and `host_monitoring_v2`), `dialect` in place of the `wrapper_dialect` parameter, and a MySQL Global Database chain containing `host_monitoring_v2`, which cannot be loaded on `mysql-connector-python`. Also documents the event loop requirement for asyncio on Windows, and notes that `gdb_accessible_regions` is currently supported in the synchronous driver only. ([PR #1270](https://github.com/aws/aws-advanced-python-wrapper/pull/1270))

### :cloud: Changed
* The SQLAlchemy dialects moved to `aws_advanced_python_wrapper.sqlalchemy_dialects` and register as drivers under SQLAlchemy's existing dialects: `postgresql+aws_wrapper_psycopg://`, `mysql+aws_wrapper_mysqlconnector://` and `mysql+aws_wrapper_aiomysql://`. The URL driver names are unchanged, so URL-based and `creator=` configurations continue to work without modification. `aws_advanced_python_wrapper.sqlalchemy.mysql_orm_dialect.SqlAlchemyOrmMysqlDialect` remains importable as a deprecated alias for `sqlalchemy_dialects.mysql.AwsWrapperMySQLConnectorDialect` and will be removed in the next major version. Note that the replacement class does not inject a default `aurora_connection_tracker,failover_v2` plugin chain when `wrapper_plugins` is absent; the wrapper's own default chain applies instead, which for `mysql-connector-python` is `initial_connection,aurora_connection_tracker,failover_v2`. See [SQLAlchemy Support](./docs/using-the-python-wrapper/SqlAlchemySupport.md). ([PR #1274](https://github.com/aws/aws-advanced-python-wrapper/pull/1274))
* Reworked the Aurora initial connection strategy plugin. ([PR #1253](https://github.com/aws/aws-advanced-python-wrapper/pull/1253), [PR #1264](https://github.com/aws/aws-advanced-python-wrapper/pull/1264))
* Updated the Community and Aurora database versions the test suite runs against. ([PR #1248](https://github.com/aws/aws-advanced-python-wrapper/pull/1248))

## [3.0.0] - 2026-06-02

### :crab: Breaking Changes
> [!WARNING]
> 3.0.0 changes the default `cluster_id` behavior. Applications connecting to **multiple database clusters** must now explicitly set a unique `cluster_id` for each cluster. See the [Cluster ID documentation](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/ClusterId.md) for details.
>
> #### Migration
>
> | Scenario | Action Required |
> |---|---|
> | Single database cluster | No changes required |
> | Multiple database clusters | Review all connection strings and add a unique `cluster_id` parameter per cluster. See the [Cluster ID documentation](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/ClusterId.md) for configuration guidance. |

### :magic_wand: Added
* [SQLAlchemy ORM support](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/SqlAlchemySupport.md).

### :bug: Fixed
* New pooled connections created with stale credentials, and PostgreSQL error handler unable to correctly handle auth errors nested in connection errors ([PR #1231](https://github.com/aws/aws-advanced-python-wrapper/pull/1231)).
* Read/Write Splitting plugins not subscribed to the execute pipeline, causing idle connections to not be correctly closed during failover ([PR #1117](https://github.com/aws/aws-advanced-python-wrapper/pull/1117)).

### :crab: Changed
* Updated the default plugin list:
  * Added the [Aurora Initial Connection Strategy Plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) for both PostgreSQL and MySQL driver dialects.
  * Removed the [Host Monitoring Plugin V2](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheHostMonitoringPlugin.md) from the default plugins for MySQL driver dialect.

## [2.1.0] - 2026-02-11
### :magic_wand: Added
* [Failover v2 Plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheFailoverPlugin.md), an improved version of the failover plugin with enhanced reliability ([PR #1079](https://github.com/aws/aws-advanced-python-wrapper/pull/1079)).
* [Django support for MySQL](https://github.com/aws/aws-advanced-python-wrapper/blob/c1e33f9d4468993063439bacaae7993ebe89d691/docs/using-the-python-wrapper/DjangoSupport.md) ([PR #1077](https://github.com/aws/aws-advanced-python-wrapper/pull/1077)).

### :bug: Fixed
* Properly handling nested errors in auth plugins ([PR #1092](https://github.com/aws/aws-advanced-python-wrapper/pull/1092)).
* Populate opened connection queue with url ([PR #1094](https://github.com/aws/aws-advanced-python-wrapper/pull/1094)).
* Spawning unnecessary threads due to ClassVars ([PR #1090](https://github.com/aws/aws-advanced-python-wrapper/pull/1090)).
* [Incorrect cleanup thread sleep time issue](https://github.com/aws/aws-advanced-python-wrapper/issues/1087) ([PR #1090](https://github.com/aws/aws-advanced-python-wrapper/pull/1090)).
* Aurora connection tracker and writer host comparison ([PR #1081](https://github.com/aws/aws-advanced-python-wrapper/pull/1081)).
* Sliding expiration cache concurrent access exceptions ([PR #1089](https://github.com/aws/aws-advanced-python-wrapper/pull/1089)).
* Stale DNS plugin when connected to reader ([PR #1086](https://github.com/aws/aws-advanced-python-wrapper/pull/1086)).
* Read/write splitting + custom endpoint plugin issue when switching to writer ([PR #1080](https://github.com/aws/aws-advanced-python-wrapper/pull/1080)).
* Move `conn.release_resources()` to close method instead of `__del__` to avoid relying on GC to release resources ([PR #1078](https://github.com/aws/aws-advanced-python-wrapper/pull/1078)).

### :crab: Changed
* Performance optimization for auth plugins by caching clients and sessions ([PR #1084](https://github.com/aws/aws-advanced-python-wrapper/pull/1084)).
* Update documentation for AWS credentials requirements for plugins using the AWS SDK ([PR #1093](https://github.com/aws/aws-advanced-python-wrapper/pull/1093)).

## [2.0.0] - 2026-01-14
### :crab: Breaking Changes
> [!WARNING]
> - 2.0 removes support for Python 3.8 and 3.9.

### :magic_wand: Added
* Python 3.12 and 3.13 support ([PR #1052](https://github.com/aws/aws-advanced-python-wrapper/pull/1052)).
* [Simple Read/Write Splitting Plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) (`srw`). This plugin adds functionality to switch between endpoints via calls to the Connection#setReadOnly method. It does not rely on cluster topology. It relies purely on the provided endpoints and their DNS resolution ([PR #1048](https://github.com/aws/aws-advanced-python-wrapper/pull/1048)).
* Wrapper resource cleanup method `aws_advanced_python_wrapper.release_resources()`. This method should be called at program exit to properly clean up background threads and resources ([PR #1066](https://github.com/aws/aws-advanced-python-wrapper/pull/1066)).

### :bug: Fixed
* Sliding expiration cache bug which causes delay upon exit ([PR #1043](https://github.com/aws/aws-advanced-python-wrapper/pull/1043)).
* Unnecessary boto3 call to verify region in IAM plugin which causes performance issues ([PR #1042](https://github.com/aws/aws-advanced-python-wrapper/pull/1042)).
* MySQL connections hanging during garbage collection ([PR #1063](https://github.com/aws/aws-advanced-python-wrapper/pull/1063)).
* Incorrect MySQL host alias query ([PR #1051](https://github.com/aws/aws-advanced-python-wrapper/pull/1051)).
* `ImportError` when MySQL Connector/Python C Extension isn't available ([PR #1038](https://github.com/aws/aws-advanced-python-wrapper/pull/1038)).
* Background threads being created at import time ([PR #1066](https://github.com/aws/aws-advanced-python-wrapper/pull/1066)).

### :crab: Changed
* Refactor host list provider ([PR #1065](https://github.com/aws/aws-advanced-python-wrapper/pull/1065)).
* Performance optimizations ([PR #1072](https://github.com/aws/aws-advanced-python-wrapper/pull/1072)).
* Update documentation with required db user permissions for Multi-AZ DB Cluster and Blue/Green support ([PR #1061](https://github.com/aws/aws-advanced-python-wrapper/pull/1061)).

## [1.4.0] - 2025-10-17
### :magic_wand: Added
* [EFM v2](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheHostMonitoringPlugin.md#host-monitoring-plugin-v2), an improved alternate version of the `efm` plugin which addresses issues such as garbage collection and monitoring stability, is now live!

### :bug: Fixed
* Update subscribed methods to explicit methods ([PR #960](https://github.com/aws/aws-advanced-python-wrapper/pull/960))
* Limitless Connection Plugin to properly round the load metric values for Limitless transaction routers ([PR #988](https://github.com/aws/aws-advanced-python-wrapper/pull/988)).

### :crab: Changed
* Update documentation for Limitless Plugin ([PR #914](https://github.com/aws/aws-advanced-python-wrapper/pull/914)).
* Update documentation for Blue/Green Support ([PR #995](https://github.com/aws/aws-advanced-python-wrapper/pull/995)).
* Add qualifiers to PostgreSQL SQL statements ([PR #1007](https://github.com/aws/aws-advanced-python-wrapper/pull/1007)).

## [1.3.0] - 2025-07-28
### :magic_wand: Added
* [Blue/Green Plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheBlueGreenPlugin.md), which adds support for blue/green deployments ([PR #911](https://github.com/aws/aws-advanced-python-wrapper/pull/911)).
* Limitless Plugin, which adds support for limitless deployments ([PR #912](https://github.com/aws/aws-advanced-python-wrapper/pull/912)).
* Add weighted random host selection strategy ([PR #907](https://github.com/aws/aws-advanced-python-wrapper/pull/907)).
* Add expiration time for secrets cache in the Secrets Manager Plugin ([PR #906](https://github.com/aws/aws-advanced-python-wrapper/pull/906)).
* Allow custom secret keys for database credentials retrieval ([PR #843](https://github.com/aws/aws-advanced-python-wrapper/pull/843)).

### :bug: Fixed
* Separate plugin chain cache based on whether a plugin needs to be skipped or not ([PR #916](https://github.com/aws/aws-advanced-python-wrapper/pull/916)).
* Check the cached token and exception type before retrying connection in the auth plugins ([PR #902](https://github.com/aws/aws-advanced-python-wrapper/pull/902)).
* Set the default SSL Secure setting to True ([PR #848](https://github.com/aws/aws-advanced-python-wrapper/pull/848)).

### :crab: Changed
* Use poetry version compatible with Python 3.8 ([PR #913](https://github.com/aws/aws-advanced-python-wrapper/pull/913)).
* Port over PluginService API changes from JDBC ([PR #901](https://github.com/aws/aws-advanced-python-wrapper/pull/901)).
* Verify links in markdown documentation ([PR #909](https://github.com/aws/aws-advanced-python-wrapper/pull/909)).
* Replace poetry installation with bash for GitHub actions ([PR #903](https://github.com/aws/aws-advanced-python-wrapper/pull/903)).
* Update python requirement and environment variable information in documentation([PR #900](https://github.com/aws/aws-advanced-python-wrapper/pull/900)).

## [1.2.0] - 2024-12-12
### :magic_wand: Added
* [Custom endpoint plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheCustomEndpointPlugin.md), which adds support for RDS custom endpoints.

## [1.1.1] - 2024-10-18
### :magic_wand: Added
* Support for MySQL version 9+ ([PR #713](https://github.com/aws/aws-advanced-python-wrapper/pull/713)).

### :bug: Fixed
* Extended support for China endpoints ([Issue #700](https://github.com/aws/aws-advanced-python-wrapper/issues/700)).
* Removed unused SQLAlchemy dialect from documentation ([PR #714](https://github.com/aws/aws-advanced-python-wrapper/pull/714)).

## [1.1.0] - 2024-07-31
### :magic_wand: Added
* Okta authentication support. See the [documentation](docs/using-the-python-wrapper/using-plugins/UsingTheOktaAuthenticationPlugin.md) for more details and sample code.

## [1.0.0] - 2024-05-23
The Amazon Web Services (AWS) Advanced Python Wrapper allows an application to take advantage of the features of clustered Aurora databases.

### :magic_wand: Added
* Support for PostgreSQL
* Support for MySQL

[3.1.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/3.0.0...3.1.0
[3.0.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/2.1.0...3.0.0
[2.1.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/2.0.0...2.1.0
[2.0.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.4.0...2.0.0
[1.4.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.3.0...1.4.0
[1.3.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.2.0...1.3.0
[1.2.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/aws/aws-advanced-python-wrapper/releases/tag/1.0.0
