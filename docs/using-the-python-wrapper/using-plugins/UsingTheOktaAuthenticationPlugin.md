# Okta Authentication Plugin

The Okta Authentication Plugin adds support for authentication via Federated Identity and then database access via IAM.

## What is Federated Identity
Federated Identity allows users to use the same set of credentials to access multiple services or resources across different organizations. This works by having Identity Providers (IdP) that manage and authenticate user credentials, and Service Providers (SP) that are services or resources that can be internal, external, and/or belonging to various organizations. Multiple SPs can establish trust relationships with a single IdP.

When a user wants access to a resource, it authenticates with the IdP. From this a security token generated and is passed to the SP then grants access to said resource.
In the case of AD FS, the user signs into the AD FS sign in page. This generates a SAML Assertion which acts as a security token. The user then passes the SAML Assertion to the SP when requesting access to resources. The SP verifies the SAML Assertion and grants access to the user.

## Prerequisites
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

## How to use the Okta Authentication Plugin with the AWS Advanced Python Wrapper 

### Enabling the Okta Authentication Plugin
> [!NOTE]\
> AWS IAM database authentication is needed to use the Okta Authentication Plugin. This is because after the plugin
> acquires SAML assertion from the identity provider, the SAML Assertion is then used to acquire an AWS IAM token. The AWS
> IAM token is then subsequently used to access the database.

1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
   - If needed, review the documentation about [IAM authentication for MariaDB, MySQL, and PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).
2. Configure Okta as the AWS identity provider.
   - If needed, review the documentation about [Amazon Web Services Account Federation](https://help.okta.com/en-us/content/topics/deploymentguides/aws/aws-deployment.htm) on Okta's documentation.
3. Add the plugin code `okta` to the [`plugins`](../UsingThePythonWrapper.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingThePythonWrapper.md#connection-plugin-manager-parameters).
4. Specify parameters that are required or specific to your case.

### Federated Authentication Plugin Parameters
| Parameter                      |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                        | Default Value | Example Value                                          |
|--------------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------|
| `db_user`                      | String  |   Yes    | The user name of the IAM user with access to your database. <br>If you have previously used the IAM Authentication Plugin, this would be the same IAM user. <br>For information on how to connect to your Aurora Database with IAM, see this [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.html). | `None`        | `some_user_name`                                       |
| `idp_username`                 | String  |   Yes    | The user name for the `idp_endpoint` server. If this parameter is not specified, the plugin will fallback to using the `user` parameter.                                                                                                                                                                                                                           | `None`        | `jimbob@example.com`                                   |
| `idp_password`                 | String  |   Yes    | The password associated with the `idp_endpoint` username. If this parameter is not specified, the plugin will fallback to using the `password` parameter.                                                                                                                                                                                                          | `None`        | `some_random_password`                                 |
| `idp_endpoint`                 | String  |   Yes    | The hosting URL for the service that you are using to authenticate into AWS Aurora.                                                                                                                                                                                                                                                                                | `None`        | `ec2amaz-ab3cdef.example.com`                          |
| `iam_role_arn`                 | String  |   Yes    | The ARN of the IAM Role that is to be assumed to access AWS Aurora.                                                                                                                                                                                                                                                                                                | `None`        | `arn:aws:iam::123456789012:role/adfs_example_iam_role` |
| `iam_idp_arn`                  | String  |   Yes    | The ARN of the Identity Provider.                                                                                                                                                                                                                                                                                                                                  | `None`        | `arn:aws:iam::123456789012:saml-provider/adfs_example` |
| `iam_region`                   | String  |   Yes    | The IAM region where the IAM token is generated.                                                                                                                                                                                                                                                                                                                   | `None`        | `us-east-2`                                            |
| `iam_host`                     | String  |    No    | Overrides the host that is used to generate the IAM token.                                                                                                                                                                                                                                                                                                         | `None`        | `database.cluster-hash.us-east-1.rds.amazonaws.com`    |
| `iam_default_port`             | String  |    No    | This property overrides the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for PostgreSQL and MySQL. Target drivers with different protocols will require users to provide a default port.                                                                 | `None`        | `1234`                                                 |
| `iam_token_expiration`         | Integer |    No    | Overrides the default IAM token cache expiration in seconds                                                                                                                                                                                                                                                                                                        | `870`         | `123`                                                  |
| `http_request_connect_timeout` | Integer |    No    | The timeout value in seconds to send the HTTP request data used by the FederatedAuthPlugin.                                                                                                                                                                                                                                                                        | `60`          | `60`                                                   |
| `ssl_secure`                   | Boolean |    No    | Whether the SSL session is to be secure and the server's certificates will be verified                                                                                                                                                                                                                                                                             | `True`        | `False`                                                |

## Sample code
[MySQLOktaAuthentication.py](../../examples/MySQLOktaAuthentication.py)
[PGOktaAuthentication.py](../../examples/PGOktaAuthentication.py)
