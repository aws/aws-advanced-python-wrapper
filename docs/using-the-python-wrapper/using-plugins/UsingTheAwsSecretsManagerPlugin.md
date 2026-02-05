# AWS Secrets Manager Plugin

The AWS Advanced Python Wrapper supports usage of database credentials stored as secrets in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Connection Plugin. When you create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created with the credentials inside that secret.

## Enabling the AWS Secrets Manager Connection Plugin
> [!WARNING]\
> This plugin requires the AWS SDK for Python - [Boto3](https://pypi.org/project/boto3/). Boto3 is a runtime dependency and must be resolved. It can be installed via `pip install boto3`.

> [!WARNING]\
> To use this plugin, you must provide valid AWS credentials. The AWS SDK relies on the AWS SDK credential provider chain to authenticate with AWS services. If you are using temporary credentials (such as those obtained through AWS STS, IAM roles, or SSO), be aware that these credentials have an expiration time. AWS SDK exceptions will occur and the plugin will not work properly if your credentials expire without being refreshed or replaced. To avoid interruptions:
> - Ensure your credential provider supports automatic refresh (most AWS SDK credential providers do this automatically)
> - Monitor credential expiration times in production environments
> - Configure appropriate session durations for temporary credentials
> - Implement proper error handling for credential-related failures
> 
> For more information on configuring AWS credentials, see our [AWS credentials documentation](../AwsCredentials.md).

To enable the AWS Secrets Manager Connection Plugin, add the plugin code `aws_secrets_manager` to the [`plugins`](../UsingThePythonWrapper.md#connection-plugin-manager-parameters) value.

## AWS Secrets Manager Connection Plugin Parameters
The following properties are required for the AWS Secrets Manager Connection Plugin to retrieve database credentials from the AWS Secrets Manager.

> [!IMPORTANT]\
>To use this plugin, you will need to set the following AWS Secrets Manager specific parameters.

| Parameter                         | Value  |                          Required                           | Description                                                                                                                                                                                                                      | Example                 | Default Value |
|-----------------------------------|:------:|:-----------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------|---------------|
| `secrets_manager_secret_id`       | String |                             Yes                             | Set this value to be the secret name or the secret ARN.                                                                                                                                                                          | `secret_id`             | `None`        |
| `secrets_manager_region`          | String | Yes unless the `secrets_manager_secret_id` is a Secret ARN. | Set this value to be the region your secret is in.                                                                                                                                                                               | `us-east-2`             | `us-east-1`   |
| `secrets_manager_endpoint`        | String |                             No                              | Set this value to be the endpoint override to retrieve your secret from. This parameter value should be in the form of a URL, with a valid protocol (ex. `http://`) and domain (ex. `localhost`). A port number is not required. | `http://localhost:1234` | `None`        |
| `secrets_manager_secret_username` | String |                             No                              | Set this value to be the key in the JSON secret that contains the username for database connection.                                                                                                                              | `username_key`          | `username`    |
| `secrets_manager_secret_password` | String |                             No                              | SSet this value to be the key in the JSON secret that contains the password for database connection.                                                                                                                             | `password_key`          | `password`    |
| `secrets_manager_expiration`      |  int   |                             No                              | Set this value to be the expiration time in seconds the secret is stored in the cache. If the value is below 0, sets the expiration time to one year in seconds.                                                                 | 500                     | 31536000      |

*NOTE* A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:Secre78tName-6RandomCharacters`

## Secret Data
The secret stored in the AWS Secrets Manager should be a JSON object containing the properties `username` and `password`. If the secret contains different key names, you can specify them with the `secrets_manager_secret_username` and `secrets_manager_secret_password` parameters.

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

If you specify a secret ARN as the `secrets_manager_secret_id`, the AWS Advanced Python Wrapper will parse the region from the ARN and set it as the `secrets_manager_region` value.
```python
awsconn = AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        dbname="postgres",
        secrets_manager_secret_id="arn:aws:secretsmanager:us-east-2:<AccountId>:secret:Secre78tName-6RandomCharacters",
        plugins="aws_secrets_manager"
)
```

If you specify `secrets_manager_secret_username` and `secrets_manager_secret_password`, the AWS Advanced Python Wrapper will parse the secret searching for those specified keys.
```python
awsconn = AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        dbname="postgres",
        secrets_manager_secret_id="secret_name",
        secrets_manager_secret_username="custom_username_key",
        secrets_manager_secret_password="custom_password_key",
        plugins="aws_secrets_manager"
)
```
In this case the secret should have the following format:
```json
{
  "custom_username_key": "the database username",
  "custom_password_key": "the database password"
}
```

You can find a full example for [PostgreSQL](../../examples/PGSecretsManager.py), and a full example for [MySQL](../../examples/MySQLSecretsManager.py).
