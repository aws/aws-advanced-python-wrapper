# AWS IAM Authentication Plugin

## What is IAM?
AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports granular permissions, giving you the ability to grant different permissions to different users. For more information on IAM and its use cases, please refer to the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## Prerequisites
> [!WARNING]\
> To preserve compatibility with customers using the community driver, IAM Authentication requires the AWS SDK for Python; [Boto3](https://pypi.org/project/boto3/). Boto3 is a runtime dependency and must be resolved. It can be installed via pip like so: `pip install boto3`.

The IAM Authentication plugin requires authentication via AWS Credentials. These credentials can be defined in `~/.aws/credentials` or set as environment variables. All users must set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Users who are using temporary security credentials will also need to additionally set `AWS_SESSION_TOKEN`.

To enable the IAM Authentication Connection Plugin, add the plugin code `iam` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter.

> [!WARNING]\
> The `iam` plugin must NOT be specified when using the `iam_dsql` plugin.

## AWS IAM Database Authentication
The AWS Python Driver supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>i.e. `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`

IAM database authentication use is limited to certain database engines. For more information on limitations and recommendations, please [review the IAM documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

## How do I use IAM with the AWS Python Driver?
1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
    1. If needed, review the documentation about [creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
    2. If needed, review the documentation about [modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
2. Set up an [AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication.
3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication. This will be the user specified in the connection string or connection properties.
    1. Connect to your database of choice using primary logins.
        1. For a MySQL database, use the following command to create a new user:<br>
           `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`
        2. For a PostgreSQL database, use the following command to create a new user:<br>
           `CREATE USER db_userx;
           GRANT rds_iam TO db_userx;`
4. Add the plugin code `iam` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter value.

| Parameter          |  Value  | Required | Description                                                                                                                                                                                                                                                                                            | Example Value                                       |
|--------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `iam_default_port` | String  |    No    | This property will override the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for PostgreSQL and MySQL. Target drivers with different protocols will require users to provide a default port. | `1234`                                              |
| `iam_host`         | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string. This parameter is required when users are connecting with custom endpoints.                                                                       | `database.cluster-hash.us-east-1.rds.amazonaws.com` |
| `iam_region`       | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string.                                                                                                                                                        | `us-east-2`                                         |
| `iam_expiration`   | Integer |    No    | This property determines how long an IAM token is kept in the driver cache before a new one is generated. The default expiration time is set to 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.                                                 | `600`                                               |

## Sample code

[PGIamAuthentication.py](../../examples/PGIamAuthentication.py)
[MySQLIamAuthentication.py](../../examples/MySQLIamAuthentication.py)
