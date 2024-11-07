# Custom Endpoint Plugin

The Custom Endpoint Plugin adds support for RDS custom endpoints. When the Custom Endpoint Plugin is in use, the driver will analyse custom endpoint information to ensure instances used in connections are part of the custom endpoint being used. This includes connections used in failover and read-write splitting.

## Prerequisites
- This plugin requires the AWS SDK for Python, [Boto3](https://pypi.org/project/boto3/). Boto3 is a runtime dependency and must be resolved. It can be installed via pip like so: `pip install boto3`.

## How to use the Custom Endpoint Plugin with the AWS Advanced Python Driver

### Enabling the Custom Endpoint Plugin

1. If needed, create a custom endpoint using the AWS RDS Console:
    - If needed, review the documentation about [creating a custom endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-custom-endpoint-creating.html).
2. Add the plugin code `custom_endpoint` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter value, or to the current [driver profile](../UsingThePythonDriver.md#connection-plugin-manager-parameters).
3. If you are using the failover plugin, set the failover parameter `failover_mode` according to the custom endpoint type. For example, if the custom endpoint you are using is of type `READER`, you can set `failover_mode` to `strict_reader`, or if it is of type `ANY`, you can set `failover_mode` to `reader_or_writer`.
4. Specify parameters that are required or specific to your case.

### Custom Endpoint Plugin Parameters

| Parameter                                    |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                          | Default Value         | Example Value |
|----------------------------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|---------------|
| `custom_endpoint_info_refresh_rate_ms`       | Integer |    No    | Controls how frequently custom endpoint monitors fetch custom endpoint info, in milliseconds.                                                                                                                                                                                                                                        | `30000`               | `20000`       |
| `custom_endpoint_idle_monitor_expiration_ms` | Integer |    No    | Controls how long a monitor should run without use before expiring and being removed, in milliseconds.                                                                                                                                                                                                                               | `900000` (15 minutes) | `600000`      |
| `wait_for_custom_endpoint_info`              | Boolean |    No    | Controls whether to wait for custom endpoint info to become available before connecting or executing a method. Waiting is only necessary if a connection to a given custom endpoint has not been opened or used recently. Note that disabling this may result in occasional connections to instances outside of the custom endpoint. | `true`                | `true`        |
| `wait_for_custom_endpoint_info_timeout_ms`   | Integer |    No    | Controls the maximum amount of time that the plugin will wait for custom endpoint info to be made available by the custom endpoint monitor, in milliseconds.                                                                                                                                                                         | `5000`                | `7000`        |
