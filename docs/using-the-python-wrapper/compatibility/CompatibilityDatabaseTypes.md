# Database type compatibility

This document is part of the [Compatibility Guide](./Compatibility.md) and explains plugin compatibility with various database types and deployments. Some plugins require specific metadata from particular database types to function properly.

For example, the `limitless` plugin is incompatible with [Aurora Global Database](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database.html) because it's built on different architectural principles than [Limitless Database](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/limitless-architecture.html). Aurora Global Database doesn't use transaction routers and doesn't provide the transaction routers' metadata. This lack of required metadata makes it incompatible with the `limitless` plugin.

For Aurora Global Database configuration details, see [Aurora Global Databases](../GlobalDatabases.md).

Legend: ✅ compatible &nbsp;|&nbsp; ❌ incompatible / no added value

| Plugin code / Database type | Aurora Global Database <br>(MySQL and PG) | Aurora Cluster <br>(MySQL and PG) | RDS Multi-AZ DB Cluster (3 instances) <br>(MySQL and PG) |
|---|:---:|:---:|:---:|
| [custom_endpoint](../using-plugins/UsingTheCustomEndpointPlugin.md) | ✅ | ✅ | ✅ |
| [host_monitoring](../using-plugins/UsingTheHostMonitoringPlugin.md) (EFM v1) | ✅ | ✅ | ✅ |
| [host_monitoring_v2](../using-plugins/UsingTheHostMonitoringPlugin.md) (EFM v2) | ✅ | ✅ | ✅ |
| [failover](../using-plugins/UsingTheFailoverPlugin.md) | ✅ | ✅ | ✅ |
| [failover_v2](../using-plugins/UsingTheFailover2Plugin.md) | ✅ | ✅ | ✅ |
| [gdb_failover](../using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ | ✅ | ✅ |
| [iam](../using-plugins/UsingTheIamAuthenticationPlugin.md) | ✅ | ✅ | ✅ |
| [aws_secrets_manager](../using-plugins/UsingTheAwsSecretsManagerPlugin.md) | ✅ | ✅ | ✅ |
| [federated_auth](../using-plugins/UsingTheFederatedAuthenticationPlugin.md) | ✅ | ✅ | ✅ |
| [okta](../using-plugins/UsingTheOktaAuthenticationPlugin.md) | ✅ | ✅ | ✅ |
| stale_dns | ✅ | ✅ | ✅ |
| [read_write_splitting](../using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ |
| [srw](../using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ |
| [gdb_rw](../using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ |
| [aurora_connection_tracker](../using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ | ✅ |
| connect_time | ✅ | ✅ | ✅ |
| [fastest_response_strategy](../using-plugins/UsingTheFastestResponseStrategyPlugin.md) | ✅ | ✅ | ✅ |
| [initial_connection](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | ✅ | ✅ | ✅ |
| [limitless](../using-plugins/UsingTheLimitlessPlugin.md) | ❌ | ✅ (PostgreSQL only) | ✅ |
| [bg](../using-plugins/UsingTheBlueGreenPlugin.md) | ❌ | ✅ | ❌ |

<br>

| Plugin code / Database type | RDS Multi-AZ DB Instance (2 instances) <br>(MySQL and PG) | RDS Single-AZ Instance (1 instance) <br>(MySQL and PG) | Community Database <br>(MySQL and PG) |
|---|:---:|:---:|:---:|
| [custom_endpoint](../using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ | ❌ | ❌ |
| [host_monitoring](../using-plugins/UsingTheHostMonitoringPlugin.md) (EFM v1) | ✅ | ✅ | ✅ |
| [host_monitoring_v2](../using-plugins/UsingTheHostMonitoringPlugin.md) (EFM v2) | ✅ | ✅ | ✅ |
| [failover](../using-plugins/UsingTheFailoverPlugin.md) | ❌ | ❌ | ❌ |
| [failover_v2](../using-plugins/UsingTheFailover2Plugin.md) | ❌ | ❌ | ❌ |
| [gdb_failover](../using-plugins/UsingTheGdbFailoverPlugin.md) | ❌ | ❌ | ❌ |
| [iam](../using-plugins/UsingTheIamAuthenticationPlugin.md) | ✅ | ✅ | ❌ |
| [aws_secrets_manager](../using-plugins/UsingTheAwsSecretsManagerPlugin.md) | ✅ | ✅ | ❌ |
| [federated_auth](../using-plugins/UsingTheFederatedAuthenticationPlugin.md) | ✅ | ✅ | ❌ |
| [okta](../using-plugins/UsingTheOktaAuthenticationPlugin.md) | ✅ | ✅ | ❌ |
| stale_dns | ❌ | ❌ | ❌ |
| [read_write_splitting](../using-plugins/UsingTheReadWriteSplittingPlugin.md) | ❌ | ❌ | ❌ |
| [srw](../using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | ✅ | ❌ | ✅ |
| [gdb_rw](../using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ❌ | ❌ | ❌ |
| [aurora_connection_tracker](../using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) | ❌ | ❌ | ❌ |
| connect_time | ✅ | ✅ | ✅ |
| [fastest_response_strategy](../using-plugins/UsingTheFastestResponseStrategyPlugin.md) | ❌ | ❌ | ❌ |
| [initial_connection](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | ❌ | ❌ | ❌ |
| [limitless](../using-plugins/UsingTheLimitlessPlugin.md) | ❌ | ❌ | ❌ |
| [bg](../using-plugins/UsingTheBlueGreenPlugin.md) | ✅ | ✅ | ❌ |

> The `connect_time`, `execute_time`, and [`dev`](../using-plugins/UsingTheDeveloperPlugin.md) plugins are compatible with every database type (see [Universally Compatible Plugins](./Compatibility.md#universally-compatible-plugins)).
