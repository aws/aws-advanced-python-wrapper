# AWS Aurora DSQL IAM Authentication Plugin

This plugin enables connecting to AWS Aurora DSQL databases through AWS Identity and Access Management (IAM).

## What is IAM?
AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports granular permissions, giving you the ability to grant different permissions to different users. For more information on IAM and its use cases, please refer to the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## Prerequisites
> [!WARNING]\
> To preserve compatibility with customers using the community driver, IAM Authentication requires the AWS SDK for Python; [Boto3](https://pypi.org/project/boto3/). Boto3 is a runtime dependency and must be resolved. It can be installed via pip like so: `pip install boto3`.

The DSQL IAM Authentication plugin requires authentication via AWS Credentials. These credentials can be defined in `~/.aws/credentials` or set as environment variables. All users must set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Users who are using temporary security credentials will also need to additionally set `AWS_SESSION_TOKEN`.

To enable the AWS Aurora DSQL IAM Authentication Plugin, add the plugin code `iam_dsql` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter.

> [!WARNING]\
> The `iam` plugin must NOT be specified when using the `iam_dsql` plugin.

## AWS IAM Database Authentication
The AWS Python Driver supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid AWS Aurora DSQL endpoint, and not a custom domain or an IP address.
<br>i.e. `cluster-identifier.dsql.us-east-1.on.aws`

Connections established by the `iamDsql` plugin are beholden to the [Cluster quotas and database limits in Amazon Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html). In particular, applications need to consider the maximum transaction duration, and maximum connection duration limits. Ensure connections are returned to the pool regularly, and not retained for long periods.


## How do I use IAM with the AWS Python Driver?
1. Configure IAM roles for the cluster according to [Using database roles and IAM authentication](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/using-database-and-iam-roles.html).
2. Add the plugin code `iam_dsql` to the [`plugins`](../UsingThePythonDriver.md#connection-plugin-manager-parameters) parameter value.

| Parameter          |  Value  | Required | Description                                                                                                                                                                                                                                                                                            | Example Value                                       |
|--------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `iam_host`         | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string. This parameter is required when users are connecting with custom endpoints.                                                                       | `cluster-identifier.dsql.us-east-1.on.aws` |
| `iam_region`       | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string where possible. Some connection string formats may not be supported, and the `iam_region` must be provided in these cases.                                                                                                                                                     | `us-east-2`                                         |
| `iam_expiration`   | Integer |    No    | This property determines how long an IAM token is kept in the driver cache before a new one is generated. The default expiration time is set to 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.                                                 | `600`                                               |

## Sample code

[DSQLIamAuthentication.py](../../examples/DSQLIamAuthentication.py)

