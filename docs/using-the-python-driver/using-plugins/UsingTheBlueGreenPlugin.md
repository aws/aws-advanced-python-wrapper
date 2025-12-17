# Blue/Green Deployment Plugin

## What is Blue/Green Deployment?

The [Blue/Green Deployment](https://docs.aws.amazon.com/whitepapers/latest/blue-green-deployments/introduction.html) technique enables organizations to release applications by seamlessly shifting traffic between two identical environments running different versions of the application. This strategy effectively mitigates common risks associated with software deployment, such as downtime and limited rollback capability.

The AWS Python Driver leverages the Blue/Green Deployment approach by intelligently managing traffic distribution between blue and green hosts, minimizing the impact of stale DNS data and connectivity disruptions on user applications.


## Prerequisites

> [!WARNING]\
> Currently Supported Database Deployments:
>
> - Aurora MySQL and PostgreSQL clusters
> - RDS MySQL and PostgreSQL instances
>
> Unsupported Database Deployments and Configurations:
>
> - RDS MySQL and PostgreSQL Multi-AZ clusters
> - Aurora Global Database for MySQL and PostgreSQL
>
> Additional Requirements:
>
> - AWS cluster and instance endpoints must be directly accessible from the client side
> - :warning: If connecting with non-admin users, permissions must be granted to the users so that the blue/green metadata table/function can be properly queried. If the permissions are not granted, the metadata table/function will not be visible and blue/green plugin functionality will not work properly. Please see the [Connecting with non-admin users](#connecting-with-non-admin-users) section below.
> - Connecting to database nodes using CNAME aliases is not supported
>
> **Blue/Green Support Behaviour and Version Compatibility:**
>
> The AWS Advanced Python Driver now includes enhanced full support for Blue/Green Deployments. This support requires a minimum database version that includes a specific metadata table. The metadata will be accessible provided the green deployment satisfies the minimum version compatibility requirements. This constraint **does not** apply to RDS MySQL.
>
> For RDS Postgres, you will also need to manually install the `rds_tools` extension using the following DDL so that the metadata required by the wrapper is available:
>
> ```sql
> CREATE EXTENSION rds_tools;
> ```
>
> If your database version does **not** support this table, the driver will automatically detect its absence and fallback to its previous behaviour. In this fallback mode, Blue/Green handling is subject to the same limitations listed above.
>
> **No action is required** if your database does not include the new metadata table -- the driver will continue to operate as before. If you have questions or encounter issues, please open an issue in this repository.
>
> Supported RDS PostgreSQL Versions: `rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21)` and above.<br>
> Supported Aurora PostgreSQL Versions: Engine Release `17.5, 16.9, 15.13, 14.18, 13.21` and above.<br>
> Supported Aurora MySQL Versions: Engine Release `3.07` and above.


## What is the Blue/Green Deployment Plugin?

During a [Blue/Green switchover](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html), several significant changes occur to your database configuration:
- Connections to blue hosts terminate at a specific point during the transition
- Host connectivity may be temporarily impacted due to reconfigurations and potential host restarts
- Cluster and instance endpoints are redirected to different database hosts
- Internal database host names undergo changes
- Internal security certificates are regenerated to accommodate the new host names


All factors mentioned above may cause application disruption. The AWS Advanced Python Driver aims to minimize application disruption during Blue/Green switchover by performing the following actions:
- Actively monitors Blue/Green switchover status and implements appropriate measures to suspend, pass-through, or re-route database traffic
- Prior to Blue/Green switchover initiation, compiles a comprehensive inventory of cluster and instance endpoints for both blue and green hosts along with their corresponding IP addresses
- During the active switchover phase, temporarily suspends database traffic to connected blue hosts, which helps unload database hosts and reduces transaction lag for green hosts, thereby enhancing overall switchover performance
- Substitutes provided hostnames with corresponding IP addresses when establishing new blue connections, effectively eliminating stale DNS data and ensuring connections to the current blue hosts
- During the brief post-switchover period, continuously monitors DNS entries, confirms that blue endpoints have been reconfigured, and discontinues hostname-to-IP address substitution since it is no longer unnecessary
- Automatically rejects new connection requests to green hosts when the switchover is completed, although DNS entries for green hosts remain temporarily available
- Intelligently detects switchover failures and rollbacks to the original state, implementing appropriate connection handling measures to maintain application stability


## How do I use the Blue/Green Deployment Plugin with the AWS Python Driver?

To enable the Blue/Green Deployment functionality, add the plugin code `bg` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter value.
The Blue/Green Deployment Plugin supports the following configuration parameters:

| Parameter                         |  Value  |                           Required                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                             | Example Value            | Default Value |
|-----------------------------------|:-------:|:------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|---------------|
| `bg_id`                           | String  | If using multiple Blue/Green Deployments, yes; otherwise, no | This parameter is optional and defaults to `1`. When supporting multiple Blue/Green Deployments (BGDs), this parameter becomes mandatory. Each connection string must include the `bg_id` parameter with a value that can be any number or string. However, all connection strings associated with the same Blue/Green Deployment must use identical `bg_id` values, while connection strings belonging to different BGDs must specify distinct values. | `1234`, `abc-1`, `abc-2` | `1`           |
| `bg_connect_timeout_ms`           | Integer |                              No                              | Maximum waiting time (in milliseconds) for establishing new connections during a Blue/Green switchover when blue and green traffic is temporarily suspended.                                                                                                                                                                                                                                                                                            | `30000`                  | `30000`       |
| `bg_interval_baseline_ms`         | Integer |                              No                              | The baseline interval (ms) for checking the Blue/Green Deployment status. It's highly recommended to keep this parameter below 900000ms (15 minutes).                                                                                                                                                                                                                                                                                                   | `60000`                  | `60000`       |
| `bg_interval_increased_ms`        | Integer |                              No                              | The increased-frequency interval (ms) for checking the Blue/Green Deployment status. Configure this parameter within the range of 500-2000 milliseconds.                                                                                                                                                                                                                                                                                                | `1000`                   | `1000`        |
| `bg_interval_high_ms`             | Integer |                              No                              | The high-frequency interval (ms) for checking the Blue/Green Deployment status. Configure this parameter within the range of 50-500 milliseconds.                                                                                                                                                                                                                                                                                                       | `100`                    | `100`         |
| `bg_switchover_timeout_ms`        | Integer |                              No                              | Maximum duration (in milliseconds) allowed for switchover completion. If the switchover process stalls or exceeds this timeframe, the driver will automatically assume completion and resume normal operations.                                                                                                                                                                                                                                         | `180000`                 | `180000`      |
| `bg_suspend_new_blue_connections` | Boolean |                              No                              | Enables Blue/Green Deployment switchover to suspend new blue connection requests while the switchover process is in progress.                                                                                                                                                                                                                                                                                                                           | `false`                  | `false`       |

The plugin establishes dedicated monitoring connections to track Blue/Green Deployment status. To apply specific configurations to these monitoring connections, add the `blue-green-monitoring-` prefix to any configuration parameter, as shown in the following example:

```python
props = Properties()
// Configure the timeout values for all, non-monitoring connections.
props["connect_timeout"] = 30
// Configure different timeout values for the Blue/Green monitoring connections.
props["blue-green-monitoring-connect_timeout"] = 10
```

> [!WARNING]\
> **Always ensure you provide a non-zero connect timeout value to the Blue/Green Deployment Plugin**
>

## Connecting with non-admin users
> [!WARNING]\
> If connecting with non-admin users, permissions must be granted to the users so that the blue/green metadata table/function can be properly queried. If the permissions are not granted, the metadata table/function will not be visible and blue/green plugin functionality will not work properly.

| Environment       | Required permission statements                                                                                        |
|-------------------|-----------------------------------------------------------------------------------------------------------------------|
| Aurora Postgresql | None                                                                                                                  |
| RDS Postgresql    | `GRANT USAGE ON SCHEMA rds_tools TO your_user;`<br>`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rds_tools TO your_user;` |
| Aurora MySQL      | `GRANT SELECT ON mysql.rds_topology TO 'your_user'@'%';`<br>`FLUSH PRIVILEGES;`                                       |
| RDS MySQL         | `GRANT SELECT ON mysql.rds_topology TO 'your_user'@'%';`<br>`FLUSH PRIVILEGES;`                                       |

## Plan your Blue/Green switchover in advance

To optimize Blue/Green switchover support with the AWS Python Driver, advance planning is essential. Please follow these recommended steps:

1. Create a Blue/Green Deployment for your database.
2. Configure your application by incorporating the `bg` plugin along with any additional parameters of your choice, then deploy your application to the corresponding environment.
3. The order of steps 1 and 2 is flexible and can be performed in either sequence.
4. Allow sufficient time for the deployed application with the active Blue/Green plugin to collect deployment status information. This process typically requires several minutes.
5. Initiate the Blue/Green Deployment switchover through the AWS Console, CLI, or RDS API.
6. Monitor the process until the switchover completes successfully or rolls back. This may take several minutes.
7. Review the switchover summary in the application logs. This requires setting the log level to `DEBUG` for the `aws_advanced_python_wrapper.blue_green_plugin` module.
8. Update your application by deactivating the `bg` plugin through its removal from your application configuration. Redeploy your application afterward. Note that an active Blue/Green plugin produces no adverse effects once the switchover has been completed.
9. Delete the Blue/Green Deployment through the appropriate AWS interface.
10. The sequence of steps 8 and 9 is flexible and can be executed in either order based on your preference.

Here's an example of a switchover summary. Time zero corresponds to the beginning of the active switchover phase. Time offsets indicate the start time of each specific switchover phase.
```
2025-07-24 16:37:47,439.439 aws_advanced_python_wrapper.blue_green_plugin:debug [DEBUG   ] - BlueGreenMonitorThread - [bg_id: '1']
----------------------------------------------------------------------------------
timestamp                         time offset (ms)                           event
----------------------------------------------------------------------------------
  2025-07-24 16:36:15.103679             -37650 ms                     NOT_CREATED
  2025-07-24 16:36:15.265241             -37489 ms                         CREATED
  2025-07-24 16:36:50.472846              -2281 ms                     PREPARATION
  2025-07-24 16:36:52.752912                  0 ms                     IN_PROGRESS
  2025-07-24 16:36:55.258827               2505 ms                            POST
  2025-07-24 16:37:02.684998               9932 ms          Green topology changed
  2025-07-24 16:37:11.369040              18616 ms                Blue DNS updated
  2025-07-24 16:37:46.550696              53797 ms               Green DNS removed
  2025-07-24 16:37:47.439025              54685 ms                       COMPLETED
----------------------------------------------------------------------------------
```
