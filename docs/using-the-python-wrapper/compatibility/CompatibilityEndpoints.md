# Database URL types compatibility

This document is part of the [Compatibility Guide](./Compatibility.md) and explains plugin compatibility with various database endpoints.

There are many different URL types (endpoints) that can be used with the AWS Advanced Python Wrapper, but certain URL types are not compatible with certain plugins. This page outlines the various URL types and which plugins are compatible with each type.

- [Aurora Global Database Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-connecting.html) - `<global-db-name>.global-<XYZ>.global.rds.amazonaws.com`
- [Aurora Cluster Writer Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Cluster.html) - `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Cluster Reader Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Reader.html) - `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Cluster Custom Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Custom.html) - `<custom-endpoint-name>.cluster-custom-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Instance Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Instance.html) - `<instance-name>.<XYZ>.<region>.rds.amazonaws.com`
- [RDS Multi-AZ DB Cluster Writer Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`
- [RDS Multi-AZ DB Cluster Reader Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com`
- [RDS Instance Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - `<instance-name>.<XYZ>.<region>.rds.amazonaws.com`
- [RDS Proxy Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html) - `<proxy-name>.proxy-<XYZ>.<region>.rds.amazonaws.com`
- [DB Shard Group Endpoint (Aurora Limitless)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/limitless-shard.html) - `<shard-group-name>.shardgrp-<XYZ>.<region>.rds.amazonaws.com`
- [IP address](https://en.wikipedia.org/wiki/IP_address) - IPv4 or IPv6 addresses, for example: `8.8.0.0`
- User custom domain (CNAME alias) - any other domain names, for example: `my-database.my-domain.com`

Legend: ✅ compatible &nbsp;|&nbsp; ❌ incompatible

<br>

| Plugin code / Database URL type | Aurora Global Database Endpoint |
|---|:---:|
| [custom_endpoint](../using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ |
| [host_monitoring](../using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ (requires `initial_connection` plugin) |
| [host_monitoring_v2](../using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ (requires `initial_connection` plugin) |
| [failover](../using-plugins/UsingTheFailoverPlugin.md) | ✅ |
| [failover_v2](../using-plugins/UsingTheFailover2Plugin.md) | ✅ |
| [gdb_failover](../using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ |
| [iam](../using-plugins/UsingTheIamAuthenticationPlugin.md) | ✅ (requires `initial_connection` plugin) |
| [aws_secrets_manager](../using-plugins/UsingTheAwsSecretsManagerPlugin.md) | ✅ |
| [federated_auth](../using-plugins/UsingTheFederatedAuthenticationPlugin.md) | ✅ |
| [okta](../using-plugins/UsingTheOktaAuthenticationPlugin.md) | ✅ |
| stale_dns | ✅ |
| [read_write_splitting](../using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ |
| [srw](../using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | ✅ |
| [gdb_rw](../using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ |
| [aurora_connection_tracker](../using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) | ✅ |
| connect_time | ✅ |
| [fastest_response_strategy](../using-plugins/UsingTheFastestResponseStrategyPlugin.md) | ✅ |
| [initial_connection](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | ✅ |
| [limitless](../using-plugins/UsingTheLimitlessPlugin.md) | ❌ |
| [bg](../using-plugins/UsingTheBlueGreenPlugin.md) | ❌ |

<br>

| Plugin code / Database URL type | Aurora Cluster Writer Endpoint | Aurora Cluster Reader Endpoint | Aurora Cluster Custom Endpoint | Aurora / RDS Instance Endpoint |
|---|:---:|:---:|:---:|:---:|
| [custom_endpoint](../using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ | ❌ | ✅ | ❌ |
| [host_monitoring](../using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ (requires `initial_connection`) | ✅ (requires `initial_connection`) | ✅ | ✅ |
| [host_monitoring_v2](../using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ (requires `initial_connection`) | ✅ (requires `initial_connection`) | ✅ | ✅ |
| [failover](../using-plugins/UsingTheFailoverPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [failover_v2](../using-plugins/UsingTheFailover2Plugin.md) | ✅ | ✅ | ✅ | ✅ |
| [gdb_failover](../using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [iam](../using-plugins/UsingTheIamAuthenticationPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [aws_secrets_manager](../using-plugins/UsingTheAwsSecretsManagerPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [federated_auth](../using-plugins/UsingTheFederatedAuthenticationPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [okta](../using-plugins/UsingTheOktaAuthenticationPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| stale_dns | ✅ | ❌ | ❌ | ❌ |
| [read_write_splitting](../using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅[^1] | ✅[^1] |
| [srw](../using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [gdb_rw](../using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅[^1] | ✅[^1] |
| [aurora_connection_tracker](../using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| connect_time | ✅ | ✅ | ✅ | ✅ |
| [fastest_response_strategy](../using-plugins/UsingTheFastestResponseStrategyPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [initial_connection](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | ✅ | ✅ | ❌ | ❌ |
| [limitless](../using-plugins/UsingTheLimitlessPlugin.md) | ✅ | ✅ | ✅ | ❌ |
| [bg](../using-plugins/UsingTheBlueGreenPlugin.md) | ✅ | ✅ | ✅ | ✅ |

<br>

| Plugin code / Database URL type | RDS Multi-AZ Cluster Writer Endpoint | RDS Multi-AZ Cluster Reader Endpoint | RDS Proxy Endpoint | DB Shard Group Endpoint (Limitless) |
|---|:---:|:---:|:---:|:---:|
| [custom_endpoint](../using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ | ❌ | ❌ | ❌ |
| [host_monitoring](../using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ (requires `initial_connection`) | ✅ (requires `initial_connection`) | ❌ | ❌ |
| [host_monitoring_v2](../using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ (requires `initial_connection`) | ✅ (requires `initial_connection`) | ❌ | ❌ |
| [failover](../using-plugins/UsingTheFailoverPlugin.md) | ✅ | ✅ | ✅ | ❌ |
| [failover_v2](../using-plugins/UsingTheFailover2Plugin.md) | ✅ | ✅ | ✅ | ❌ |
| [gdb_failover](../using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ | ✅ | ✅ | ❌ |
| [iam](../using-plugins/UsingTheIamAuthenticationPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [aws_secrets_manager](../using-plugins/UsingTheAwsSecretsManagerPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [federated_auth](../using-plugins/UsingTheFederatedAuthenticationPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [okta](../using-plugins/UsingTheOktaAuthenticationPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| stale_dns | ✅ | ❌ | ❌ | ❌ |
| [read_write_splitting](../using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ | ✅ | ❌ | ❌ |
| [srw](../using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [gdb_rw](../using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ | ✅ | ❌ | ❌ |
| [aurora_connection_tracker](../using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ | ✅ | ❌ |
| connect_time | ✅ | ✅ | ✅ | ✅ |
| [fastest_response_strategy](../using-plugins/UsingTheFastestResponseStrategyPlugin.md) | ✅ | ✅ | ✅ | ❌ |
| [initial_connection](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | ✅ | ✅ | ❌ | ❌ |
| [limitless](../using-plugins/UsingTheLimitlessPlugin.md) | ❌ | ❌ | ❌ | ✅ |
| [bg](../using-plugins/UsingTheBlueGreenPlugin.md) | ❌ | ❌ | ✅ | ❌ |

<br>

| Plugin code / Database URL type | IP address | User custom domain (CNAME alias) |
|---|:---:|:---:|
| [custom_endpoint](../using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ | ❌ |
| [host_monitoring](../using-plugins/UsingTheHostMonitoringPlugin.md) | ❌ | ❌ |
| [host_monitoring_v2](../using-plugins/UsingTheHostMonitoringPlugin.md) | ❌ | ❌ |
| [failover](../using-plugins/UsingTheFailoverPlugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| [failover_v2](../using-plugins/UsingTheFailover2Plugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| [gdb_failover](../using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| [iam](../using-plugins/UsingTheIamAuthenticationPlugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| [aws_secrets_manager](../using-plugins/UsingTheAwsSecretsManagerPlugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| [federated_auth](../using-plugins/UsingTheFederatedAuthenticationPlugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| [okta](../using-plugins/UsingTheOktaAuthenticationPlugin.md) | ✅ (requires special configuration) | ✅ (requires special configuration) |
| stale_dns | ❌ | ❌ |
| [read_write_splitting](../using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅[^1] | ✅[^1] |
| [srw](../using-plugins/UsingTheSimpleReadWriteSplittingPlugin.md) | ✅ | ✅ |
| [gdb_rw](../using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅[^1] | ✅[^1] |
| [aurora_connection_tracker](../using-plugins/UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ |
| connect_time | ✅ | ✅ |
| [fastest_response_strategy](../using-plugins/UsingTheFastestResponseStrategyPlugin.md) | ✅ | ✅ |
| [initial_connection](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) | ❌ | ❌ |
| [limitless](../using-plugins/UsingTheLimitlessPlugin.md) | ✅ | ✅ |
| [bg](../using-plugins/UsingTheBlueGreenPlugin.md) | ✅ (requires special configuration) | ❌ |

<br>

[^1]: For custom-endpoint and instance endpoints, keep connection-role verification enabled (`verify_opened_connection_role`, default on). The actual host role may differ from the assumed role, and disabling verification can cause incorrect read/write splitting behavior. See [UsingTheAuroraInitialConnectionStrategyPlugin.md](../using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) for details.
