# AWS Credentials Requirement

Valid AWS credentials are required by the wrapper plugins that use the AWS SDK. The wrapper uses the AWS SDK's default credentials provider chain to authenticate with AWS services. If you are using temporary credentials, you will need to ensure that they are automatically refreshed before they expire. This document provides guidance to ensure that the wrapper has access to valid credentials. For a list of plugins that require the AWS SDK and valid AWS credentials, please see [this table](./UsingThePythonWrapper.md#list-of-available-plugins).

## Credential Configuration

There are several ways to provide AWS credentials to the AWS SDK, some which support automatic refresh, and some which do not (see below). The AWS SDK automatically searches for credentials in the following order:
1. Environment variables
2. Assume role provider
3. Assume role with web identity provider
4. AWS IAM Identity Center credential provider
5. Shared credential file (~/.aws/credentials)
6. AWS config file (~/.aws/config)
7. Boto2 config file (/etc/boto.cfg and ~/.boto)
8. Container credential provider
9. Instance metadata service on an Amazon EC2 instance that has an IAM role configured.

## Credential Types

- **Long-term credentials** (IAM user access keys) - Do not expire and require no refresh. Suitable for local development but not recommended for production due to security risks.
- **Temporary credentials** (IAM roles, STS, SSO, credential process) - Include a session token and expiration time. **Automatic refresh only works when credentials are obtained through specific mechanisms** (see below).

## Automatic Credential Refresh

The AWS SDK can automatically refresh temporary credentials before they expire, but **only for certain credential sources**:

**Auto-refresh is supported for:**
- **IAM roles** (EC2 instance profiles, Lambda execution roles) - The SDK retrieves fresh credentials from the instance metadata service
- **ECS task roles** - The SDK retrieves fresh credentials from the container metadata endpoint
- **Credential process** (configured in `~/.aws/config`) - The SDK calls the external process again when credentials are about to expire
- **SSO** (configured in `~/.aws/config`) - The SDK refreshes SSO credentials automatically
- **Assume role** (configured in `~/.aws/config` with `role_arn` and `source_profile`) - The SDK automatically calls STS AssumeRole to get fresh credentials
- **Web identity tokens** - The SDK refreshes credentials obtained from OIDC providers

**Auto-refresh is NOT supported for:**
- **Environment variables** - Even if they contain temporary credentials with a session token, these are static values that won't refresh
- **Hardcoded credentials in `~/.aws/credentials` or `~/.aws/config`** - Even if these contain temporary credentials with expiration times, they're treated as static values and won't auto-refresh
- **Boto2 config file (/etc/boto.cfg and ~/.boto)**

## Important Notes

- Operations will fail with authentication errors if temporary credentials expire without being refreshed.
- Environment variables take precedence over configuration files. If using a credential process or assume role configuration for automatic refresh, ensure these higher-priority sources are not set with static credentials.
- For production deployments, use IAM roles (EC2 instance profiles, ECS task roles, Lambda execution roles) rather than long-term access keys or manually configured temporary credentials.

## Common Pitfalls to Avoid

- **Setting temporary credentials in environment variables** - If you set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` as environment variables (e.g., from `aws sts assume-role` output), these credentials will not auto-refresh when they expire. Use IAM roles or configure assume role in `~/.aws/config` instead.
- **Mixing credential sources** - Having both environment variables and a credential process configured will cause the environment variables to be used (higher priority), bypassing your credential process entirely.
- **Hardcoding temporary credentials** - Storing temporary credentials in `~/.aws/credentials` (e.g., from `aws sso login`) creates static credentials that won't refresh automatically, even though they have an expiration time. Configure SSO properly in `~/.aws/config` instead.
- **Forgetting session tokens** - When manually configuring temporary credentials, omitting the session token will cause authentication failures even if the access key and secret key are correct.

For more information on configuring AWS credentials, see the [AWS docs](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html).