# AWS Secrets Manager Plugin

The AWS Advanced Python Driver supports usage of database credentials stored as secrets in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Connection Plugin. When you create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created with the credentials inside that secret.

## Enabling the AWS Secrets Manager Connection Plugin
> [!WARNING]\
> To use this plugin, you must install [`boto3`](https://aws.amazon.com/sdk-for-python/) in your project. These parameters are required for the AWS Advanced Python Driver to pass database credentials to the underlying driver.
> You can install it via `pip install boto3`.

The AWS Secret Manager plugin requires authentication via AWS Credentials. These credentials can be defined in `~/.aws/credentials` or set as environment variables. All users must set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Users who are using temporary security credentials will also need to additionally set `AWS_SESSION_TOKEN`.

To enable the AWS Secrets Manager Connection Plugin, add the plugin code `aws_secrets_manager` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) value.

## AWS Secrets Manager Connection Plugin Parameters
The following properties are required for the AWS Secrets Manager Connection Plugin to retrieve database credentials from the AWS Secrets Manager.

> [!IMPORTANT]\
>To use this plugin, you will need to set the following AWS Secrets Manager specific parameters.

| Parameter                   | Value  |                          Required                           | Description                                             | Example     | Default Value |
|-----------------------------|:------:|:-----------------------------------------------------------:|:--------------------------------------------------------|:------------|---------------|
| `secrets_manager_secret_id` | String |                             Yes                             | Set this value to be the secret name or the secret ARN. | `secret_id` | `None`        |
| `secrets_manager_region`    | String | Yes unless the `secrets_manager_secret_id` is a Secret ARN. | Set this value to be the region your secret is in.      | `us-east-2` | `us-east-1`   |

*NOTE* A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:Secre78tName-6RandomCharacters`

## Secret Data
The plugin assumes that the secret contains the following properties `username` and `password`

### Example

The following code snippet shows how you can establish a PostgreSQL connection with the AWS Secrets Manager Plugin.

Note that the `secrets_manager_region` is not a required parameter. If it is not provided, the default region `us-east-1` or the parsed region from the `secrets_manager_secret_id` will be used.

```python
awsconn = AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        dbname="postgres",
        secrets_manager_secret_id="secret_name",
        secrets_manager_region="us-east-2",
        plugins="aws_secrets_manager"
)
```

If you specify a secret ARN as the `secrets_manager_secret_id`, the AWS Advanced Python Driver will parse the region from the ARN and set it as the `secrets_manager_region` value.
```python
awsconn = AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        dbname="postgres",
        secrets_manager_secret_id="arn:aws:secretsmanager:us-east-2:<AccountId>:secret:Secre78tName-6RandomCharacters",
        plugins="aws_secrets_manager"
)
```

You can find a full example for [PostgreSQL](../../examples/PGSecretsManager.py), and a full example for [MySQL](../../examples/MySQLSecretsManager.py).
