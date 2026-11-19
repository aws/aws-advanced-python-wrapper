# Integration Tests

This documentation walks through the requirements and steps required to run and debug the integration tests locally.

## Prerequisites

- Docker Desktop:
    - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
    - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
- [Environment variables](#Environment-Variables)
- Poetry is installed and `poetry install` has been executed
  - Mac/Linux:
    - `curl -sSL https://install.python-poetry.org | python3 -`
    - `poetry install`
  - Windows (Powershell):
    - `(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -`
    - `poetry install`

### Aurora Test Requirements

- An AWS account with:
    - RDS permissions.
    - EC2 permissions so integration tests can add the current IP address in the Aurora cluster's EC2 security group.
    - XRay and Cloudwatch permissions for the integration tests to report telemetries.
    - For more information, see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

## Recommendations for ease of use

Here’s a quick summary of recommendations. More details are covered in the rest of this doc.
Use a .bashrc file to define default settings for your environment variables.

This way you don’t have to specify all the environment variables on the command-line each time you run the tests.

If you are only interested in running tests directly related to your changes, set the `FILTER` environment variable from
the command-line.
This will set the `FILTER` for your session until you either unset it or open up a new git bash session.

For example, to run only the IAM tests:

```
 export FILTER="test_iam"
./gradlew test-pg-aurora
./gradlew test-pg-aurora  # FILTER is still set to "test_iam"
unset FILTER  # Done testing the IAM tests, unset FILTER
```

## Running Aurora Integration Tests

1. Set up your environment variables
    1. If you don’t have one already, create a `.bashrc` file at `~/` if you are on Mac or `C:\Users\<your_username>\`
       if you are on PC
    2. Add the environment variables specified in the [Environment Variables section](#environment-variables) to
       your `.bashrc` file to set their default values. The default values can be overridden on the command line
2. Install Poetry
   - Mac/Linux:
       - `curl -sSL https://install.python-poetry.org | python3 -`
   - Windows (Powershell):
       - `(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -`
3. Execute `poetry install` to install the `aws_advanced_python_wrapper` module and the required dependencies
4. Open a git bash terminal in VSCode or Pycharm by opening the terminal window, clicking the dropdown, and then
   selecting “git bash”. The terminal window can be opened using “View → Tool Windows → Terminal” in Pycharm or “View →
   Terminal” in VSCode.
5. Run `source ~/.bashrc` in the new terminal to load your environment variables.
6. In the git bash terminal, navigate to the project root directory and execute a gradle integration test task, e.g.:
   `./gradlew test-pg-aurora`. Other tasks are defined in `tests/integration/host/build.gradle.kts`.

### Existing clusters and leftovers

When `REUSE_RDS_DB` isn't `true` and `RDS_DB_NAME` names a cluster that exists, an Aurora run fails before it adds the rule for the runner's IP or makes a cluster.
Then set `REUSE_RDS_DB=true` to use that cluster, or set `RDS_DB_NAME` to a new name.
An Aurora run adds an all-traffic rule for the runner's IP to the `default` security group, described `Test run at <time>`, unless a rule for that IP exists.
When `REUSE_RDS_DB` isn't `true`, it removes the rule at the end only when it added the rule, so later runs keep a rule they didn't add.
A cancelled or killed run, or one whose setup failed after it added the rule, can leave the rule behind.
Before you remove such a rule, check that the first command below lists it with a time before the oldest run still going started, because in a shared account someone may have added a rule for the same IP by hand.
Remove it only when no test run from that IP is still going, because such runs share the rule.

```bash
aws ec2 describe-security-groups --group-names default --query "SecurityGroups[0].IpPermissions[?IpProtocol=='-1'].IpRanges[] | [?Description && starts_with(Description, 'Test run at')].[CidrIp,Description]" --region <region>
aws ec2 revoke-security-group-ingress --group-name default --protocol -1 --cidr <ip>/32 --region <region>
```

A cancelled or killed run also leaves its cluster, named `RDS_DB_NAME`, or `test-pg-` or `test-mysql-` and a random suffix when that is unset.
Delete each instance that the first command below lists, and then the cluster:

```bash
aws rds describe-db-clusters --db-cluster-identifier <name> --query "DBClusters[0].DBClusterMembers[].DBInstanceIdentifier" --region <region>
aws rds delete-db-instance --db-instance-identifier <instance> --skip-final-snapshot --region <region>
aws rds delete-db-cluster --db-cluster-identifier <name> --skip-final-snapshot --region <region>
```

## Running async integration tests

The wrapper ships an async counterpart for each sync test file at the top level of `tests/integration/container`, except the KMS Encryption Plugin's files, because the plugin is sync only.
The `sqlalchemy/` and `django/` folders have no async counterparts.
Async tests exercise `AsyncAwsWrapperConnection` (raw) and `create_async_engine` with the wrapper's dialects: `postgresql+aws_wrapper_psycopg`, shared with sync through `get_async_dialect_cls`, and `mysql+aws_wrapper_aiomysql`.
They run through their own Gradle tasks, separate from the sync tasks:

| Deployment | Engine | Sync task | Async task |
|---|---|---|---|
| Aurora | PostgreSQL | `:integration-testing:test-pg-aurora` | `:integration-testing:test-pg-aurora-async` |
| Aurora | MySQL | `:integration-testing:test-mysql-aurora` | `:integration-testing:test-mysql-aurora-async` |
| RDS Multi-AZ | PostgreSQL | `:integration-testing:test-pg-multi-az` | `:integration-testing:test-pg-multi-az-async` |
| RDS Multi-AZ | MySQL | `:integration-testing:test-mysql-multi-az` | `:integration-testing:test-mysql-multi-az-async` |

Gradle tasks are invoked from the repository root via the wrapper (`./gradlew :integration-testing:<task-name>`). Environment variables are identical to the sync tasks (see the env-var table above). Sync and async tasks are disjoint — a single Gradle invocation runs only one axis. To cover both, run both tasks sequentially.

Async tests do not introduce any new pytest dependencies — they follow the repo's existing convention of `asyncio.run(inner())` inside sync `def test_...` functions (matching `tests/unit/test_aio_*.py`).

**Aurora Serverless v2 operational notes:**

1. **Security-group allowlist is not managed automatically when `REUSE_RDS_DB=true`.** The harness skips IP authorization in reuse mode. Ensure the test runner's outbound IP is already in the target cluster's security group(s) before invoking any Gradle task; the first failure mode otherwise is connection refused / network unreachable.
2. **Cold-start latency.** Serverless v2 can take 30–60 seconds to scale from 0 ACUs on the first connection. Warm the cluster with a trivial query (`SELECT 1`) before running timing-sensitive tests so that warmup cost isn't charged against a test's 600-second timeout.
3. **Topology-gated tests skip environmentally.** Multi-instance tests (failover, some read/write splitting assertions) need ≥2 instances to execute; they skip on single-writer topologies. Blue/Green and Global Database tests require specific engine versions / cluster configurations. Tests skipped under these conditions are environmental, not regressions — they are equivalent to the sync suite's existing behavior under the same cluster topology.

## Running the KMS encryption tests

The KMS Encryption Plugin's tests are the `test_kms_encryption_*.py` files in `tests/integration/container`, plus `sqlalchemy/test_sqlalchemy_kms_encryption.py` and `django/test_django_kms_encryption.py`.
Each environment has its own Gradle task.

| Task | Databases | KMS | AWS credentials |
|---|---|---|---|
| `test-kms-encryption-docker` | Docker | LocalStack | None |
| `test-kms-encryption-docker-kms` | Docker | A real key | Required, see below |
| `test-kms-encryption-aurora` | Aurora | A real key | As for the Aurora tasks, plus KMS, see below |

The LocalStack task runs every module except `test_kms_encryption_real_kms.py`, which runs only on the real-key Docker task, and `test_kms_encryption_aurora.py`, which runs only on the Aurora task.
The KMS tests skip outside these tasks, and fail inside them when the endpoint or the key is missing.

### The LocalStack task

The LocalStack task needs no AWS account.
The harness gives the test container dummy credentials and passes LocalStack's address to the tests as `KMS_ENDPOINT_URL`.
To run the same tests against local servers and LocalStack without the harness, use `tests/integration/kms_local/run.sh`.

### Using an existing key

To run a real-key task on an existing key, set `KMS_KEY_ARN` to the key's ARN and `KMS_REGION` to the region in that ARN, and for the Docker task set `KMS_DECRYPT_ONLY_ROLE_ARN` to the ARN of a role that may only decrypt with that key.
The harness then looks nothing up and makes nothing, whatever `KMS_ALLOW_KEY_CREATION` is.
The run fails before any environment starts when `KMS_KEY_ARN` isn't a key ARN (for example an alias, an alias ARN or a key ID), when `KMS_REGION` is unset or isn't the region in `KMS_KEY_ARN`, when `KMS_KEY_ALIAS` is also set, or, for the Docker task, when `KMS_DECRYPT_ONLY_ROLE_ARN` is unset.
CI runs this way, with the repository variables `KMS_KEY_ARN`, `KMS_REGION`, `KMS_DECRYPT_ONLY_ROLE_ARN` and `KMS_IT_ROLE_ARN` (the role that the real-key job assumes), which an administrator sets together.
On a push to `main`, the real-key job runs only when all four are set, and the Aurora job only when `KMS_KEY_ARN` and `KMS_REGION` are.

### Letting the harness find or make the key

When `KMS_KEY_ARN` is unset, the harness uses the key that a KMS alias names: the alias in `KMS_KEY_ALIAS`, or `alias/aws-advanced-python-wrapper/integration-tests` when that is unset.
`KMS_KEY_ALIAS` is `alias/` followed by letters, digits, `/`, `_` or `-`, and can't start with `alias/aws/`.
The harness looks up the alias, and makes the key and the alias, in `KMS_REGION`, or else `RDS_DB_REGION`, or else `us-east-2`.
It uses the key only when the key is enabled, is a symmetric encryption key, and has the harness's markers: a description that starts with `aws-advanced-python-wrapper test KMS key` and the tag `aws-advanced-python-wrapper=integration-tests`.
Otherwise the run fails before any environment starts.
Then set `KMS_KEY_ALIAS` to another alias, use an existing key as above with `KMS_KEY_ALIAS` unset, or remove the alias if its owner agrees.

The harness makes a key, an alias or a role only when `KMS_ALLOW_KEY_CREATION` is `1`, `true` or `yes`, in any case and with spaces trimmed, as the Ruby wrapper reads it; any other value, or none, leaves creation off.
With creation on, when the alias doesn't exist, the harness makes the key and then the alias, and keeps both, so later runs share the key.
The Docker task always makes a role that may only decrypt with the key, and deletes the role when the run ends, so without `KMS_KEY_ARN` it needs `KMS_ALLOW_KEY_CREATION` even to reuse the key that the alias names.
The Aurora task makes no role, so it reuses the key that the alias names without `KMS_ALLOW_KEY_CREATION`, and needs the variable only when the alias doesn't exist.
With creation off, the Docker task, and an Aurora task whose alias doesn't exist, fail before any environment starts, with a message that names `KMS_ALLOW_KEY_CREATION`, `KMS_KEY_ALIAS` and `KMS_KEY_ARN`.
In the Ruby wrapper the same variable lets one test make a throwaway key, which the test schedules for deletion at once; here it lets the harness make the kept key, the alias and the per-run role.
For the Docker task, setting `KMS_DECRYPT_ONLY_ROLE_ARN` without `KMS_KEY_ARN` also fails the run before anything is made.
The test modules create keys only in LocalStack; on AWS the key is the harness's or yours.

### Cost and concurrent runs

There is one shared key for each alias and region, and each costs $1 a month, prorated by the hour, until it is scheduled for deletion; an alias costs nothing.
When two runs make the alias at the same time, the one that loses schedules the key it made for deletion with the 7-day minimum and uses the other run's key, or fails and asks to run again when it can't see that key within 30 seconds.
A run that fails after it made its key schedules that key for deletion when KMS refused the alias with an answer other than throttling and the alias doesn't name the key, or the alias names another key; when the run can't tell whether the alias names its key, it keeps the key and logs a warning.
A key pending deletion costs nothing, and AWS deletes it after 7 to 30 days.

### Permissions

To find the key, the harness needs `kms:DescribeKey` and `kms:ListResourceTags` on the key the alias names.
To make the key and the alias, it needs `kms:CreateKey`, `kms:TagResource` with the tag `aws-advanced-python-wrapper=integration-tests`, and `kms:CreateAlias` on the alias and on the new key, and to remove a key it made that no alias names, `kms:ScheduleKeyDeletion` on that key.
For the Docker task it also needs `iam:CreateRole`, `iam:PutRolePolicy`, `iam:DeleteRolePolicy`, `iam:DeleteRole` and `sts:AssumeRole` on roles named `aws-advanced-python-wrapper-test-decrypt-only-*`, and, when the credentials are a role session, `iam:GetRole` on the session's own role.
The harness never deletes or re-points an alias, and never changes a key it didn't make in the same run.
A permission scoped by that tag can take up to five minutes to cover a new key, so the harness's and the test container's calls on a key that new can be refused, on the run that makes the key or loses the race to make it; a run that starts more than five minutes after the key was made uses a key that such a permission covers.
So grant `kms:DescribeKey`, `kms:ListResourceTags`, `kms:TagResource`, `kms:CreateAlias`, `kms:ScheduleKeyDeletion` and the test container's KMS actions on `arn:aws:kms:<region>:<account>:key/*`, `kms:CreateAlias` on `arn:aws:kms:<region>:<account>:alias/aws-advanced-python-wrapper/*` and on any alias you set in `KMS_KEY_ALIAS`, and `kms:CreateKey` on `*`, rather than by tag.
The harness refuses root and federated-user credentials for the Docker task, because they can't assume a role.
The test container gets the same credentials, and uses `kms:GenerateDataKey`, `kms:Decrypt` and `kms:DescribeKey` on the test key and `sts:AssumeRole` on the decrypt-only role; with an existing key, give the Docker task's test credentials only these.
The manual CloudTrail test also needs `cloudtrail:LookupEvents`.
With `iam:PutRolePolicy` and `sts:AssumeRole` on those roles, the caller, and the test container that holds its credentials, can give such a role any permission and use it, so grant them only in an account set aside for testing.
In a shared account, use an existing key and role, as CI does.
Run the Docker task with a principal limited to these actions, and the Aurora task with these KMS actions plus what the Aurora tasks need, not an administrator.

### Clean-up at the end of a run and on cancel

The harness deletes its role, and schedules the deletion of a key it made as [Cost and concurrent runs](#cost-and-concurrent-runs) says, with the credentials it started with, so use credentials that outlast the run.
A cancelled run tries to do this before the Gradle test process exits; a cancel while the harness makes the key or the role first waits until that step ends.
A killed process, an AWS response lost on the way, a key kept because the alias may name it, or a clean-up call that fails, for example because the credentials expired during a long run, can leave a role, or a key that no alias names, behind.
A failed clean-up call, and a run that keeps its key, log a warning that names the role, or the region of the key, never a key ID.

### Finding and removing leftovers

1. List the tagged keys in a region and the decrypt-only roles:

   ```bash
   aws resourcegroupstaggingapi get-resources --resource-type-filters kms:key --tag-filters Key=aws-advanced-python-wrapper,Values=integration-tests --query "ResourceTagMappingList[].ResourceARN" --region <region>
   aws iam list-roles --query "Roles[?starts_with(RoleName, 'aws-advanced-python-wrapper-test-decrypt-only-')].[RoleName,CreateDate]"
   ```

   These searches also need `tag:GetResources`, `iam:ListRoles` and `kms:ListAliases`, and removing the shared key needs `kms:DeleteAlias` on the alias and on the key; the harness's own actions include none of them.
2. For each key, check whether it is already `PendingDeletion`, and read its description, which starts with `aws-advanced-python-wrapper test KMS key` and gives the time it was made:

   ```bash
   aws kms describe-key --key-id <key-arn> --query "KeyMetadata.[KeyState,Description]" --region <region>
   ```

3. For each key that isn't `PendingDeletion`, list the aliases that name it.
   A key that an alias names, such as the shared key, is in use and isn't a leftover.

   ```bash
   aws kms list-aliases --key-id <key-arn> --query "Aliases[].AliasName" --region <region>
   ```

4. Remove only what was made before the oldest run still going in the account started, because a run uses what it made until it ends.
   Delete a role with these commands; the first fails with `NoSuchEntity` if the run stopped before it added the policy:

   ```bash
   aws iam delete-role-policy --role-name <name> --policy-name decrypt-only
   aws iam delete-role --role-name <name>
   ```

5. Schedule each key that isn't `PendingDeletion` and that no alias names for deletion:

   ```bash
   aws kms schedule-key-deletion --key-id <key-arn> --pending-window-in-days 7 --region <region>
   ```

Never schedule the shared key for deletion while a run in the region may use it, because a key scheduled for deletion is unusable at once.
To remove the shared key when no run is going, get its ARN, delete the alias, and then schedule the key for deletion with the command in step 5:

```bash
aws kms describe-key --key-id alias/aws-advanced-python-wrapper/integration-tests --query KeyMetadata.Arn --region <region>
aws kms delete-alias --alias-name alias/aws-advanced-python-wrapper/integration-tests --region <region>
```

Delete the alias first: while it names a key pending deletion, every run that looks it up fails, and once it is gone the next run with `KMS_ALLOW_KEY_CREATION=true` makes a new key and alias.
Repeat these steps in each region where a real-key task ran with `KMS_ALLOW_KEY_CREATION=true`, and for each alias you set in `KMS_KEY_ALIAS` whose key's description starts with `aws-advanced-python-wrapper test KMS key`, with that alias in place of `alias/aws-advanced-python-wrapper/integration-tests`.
The Aurora task can also leave a security group rule and a cluster; see [Existing clusters and leftovers](#existing-clusters-and-leftovers).

### Manual tests and reports

Three tests in `test_kms_encryption_real_kms.py` are manual, because they wait for CloudTrail's delivery delay or a 900-second session, or need an IAM condition.
They run on the real-key Docker task only when `KMS_MANUAL_TESTS=1` is set, which CI never sets, so they never run in CI.
CI uploads the HTML and JUnit reports as artifacts.
The KMS tests print the database password, the application users' passwords and a real key's account ID and key ID as `***` in those reports, but not in the console output, so use throwaway database passwords.

## Debugging Aurora Integration Tests - Pycharm

> [!WARNING]\
> The integration tests can only be debugged in Pycharm if you are using Pycharm Professional. Pycharm
> Professional can be downloaded separately from Pycharm Community. You will either need to get a license or sign up for
> a temporary trial. If you want to debug the integration tests but cannot get a license, you will need to use VSCode,
> which does not require a license.

1. Follow steps 1-5 from [Run Integration Tests](#running-aurora-integration-tests). Make sure you set the DEBUG_ENV
   environment variable to PYCHARM.
2. Create a remote debug configuration.
    1. Open your run/debug configurations by selecting Run -> Edit configurations... from the menu at the top.
    2. Click the + icon and select Python Debug Server.
    3. Give the configuration a name: Debug Python Integration Tests. Set the Port field to 5005. Under the Path
       mappings
       field, click on the folder icon. Click + and add the following mapping:

       Local path: `<path_to_project>/aws-advanced-python-wrapper`

       Remote path: `/app`

   4. Click OK, Apply, OK.
3. At the top right of the Pycharm window, click the dropdown arrow and select your newly created debug configuration
4. Click the bug symbol to start the debug configuration. Pycharm will begin listening for a debug connection
5. Navigate to your Pycharm git bash terminal. In the terminal, navigate to the project root directory. Execute a gradle
   integration test debug task, e.g.: `./gradlew debug-pg-aurora`. Other tasks are defined
   in `tests/integration/host/build.gradle.kts`
6. The logs in the console will print the following message prompting you to open the Pycharm debug window when the
   debugger has attached:
   `Attaching to the debugger - open the Pycharm debug tab and click resume to begin debugging your tests...`
   The debugger pauses at the entry point automatically. Set any desired breakpoints in the integration test code and
   then click resume to begin debugging.

## Debugging Aurora Integration Tests - VSCode

1. Follow steps 1-5 from [Run Integration Tests](#running-aurora-integration-tests). Make sure you set the DEBUG_ENV
   environment variable to VSCODE.
2. If you haven’t already, create a `launch.json` file in the `.vscode` directory. Add an `Attach` configuration as
   the [one provided in this repository](../../.vscode/launch.json).
3. Add any desired breakpoints to the integration test code.
4. Navigate to your Pycharm git bash terminal. In the terminal, navigate to the project root directory. Execute a Gradle
   integration test debug task, e.g.: `./gradlew debug-pg-aurora`. Other tasks are defined
   in `tests/integration/host/build.gradle.kts`.
5. Wait for the following message prompting you to attach the debugger from VSCode:
   `Starting debugpy - you may now attach to the debugger from vscode...`
6. In VSCode, open the Run/Debug window by clicking on the debug/start icon on the left side of the VSCode window. At
   the top, select the drop-down and then select the “Attach” configuration. Click the green start icon to attach the
   debugger.

## Environment Variables

| Environment Variable Name | Required | Description                                                                                                                                                                                                      | Example Value                                |
|---------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `DB_USERNAME`             | Yes      | The username to access the database.                                                                                                                                                                             | `admin`                                      |
| `DB_PASSWORD`             | Yes      | The database cluster password.                                                                                                                                                                                   | `password`                                   |
| `DB_DATABASE_NAME`        | No       | Name of the database that will be used by the tests. The default database name is test.                                                                                                                          | `test_db_name`                               |
| `RDS_DB_NAME`             | Yes      | The database identifier for your Aurora or RDS cluster. Must be a unique value to avoid conflicting with existing clusters.                                                                                      | `db-identifier`                              |
| `RDS_DB_DOMAIN`           | No       | The existing database connection suffix. Use this variable to run against an existing database.                                                                                                                  | `XYZ.us-east-2.rds.amazonaws.com`            |
| `IAM_USER`                | No       | User within the database that is identified with AWSAuthenticationPlugin. This is used for AWS IAM Authentication and is optional                                                                                | `example_user_name`                          |
| `AWS_ACCESS_KEY_ID`       | Aurora and RDS only | An AWS access key associated with an IAM user or role with RDS permissions.                                                                                                                                      | `ASIAIOSFODNN7EXAMPLE`                       |
| `AWS_SECRET_ACCESS_KEY`   | Aurora and RDS only | The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                                                                                                   | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`   |
| `AWS_SESSION_TOKEN`       | No       | AWS Session Token for CLI, SDK, & API access. This value is for MFA credentials only. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | `AQoDYXdzEJr...<remainder of session token>` |                                          |
| `REUSE_RDS_DB`            | Yes      | Set to true if you would like to use an existing cluster for your tests.                                                                                                                                         | `false`                                      |
| `RDS_DB_REGION`           | Yes      | The database region.                                                                                                                                                                                             | `us-east-2`                                  |
| `DEBUG_ENV`               | No       | The IDE you will be using to debug the tests, values are either `PYCHARM` or `VSCODE`                                                                                                                            | `PYCHARM`                                    |
| `KMS_KEY_ARN`             | No       | The ARN of an existing KMS key for the real-key KMS tasks. See [Running the KMS encryption tests](#running-the-kms-encryption-tests).                                                                            | `arn:aws:kms:us-east-2:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab` |
| `KMS_REGION`              | No       | The region of the KMS key: the region in `KMS_KEY_ARN`, or where the harness looks up the alias. See [Running the KMS encryption tests](#running-the-kms-encryption-tests).                                      | `us-east-2`                                  |
| `KMS_DECRYPT_ONLY_ROLE_ARN` | No       | With `KMS_KEY_ARN`, for the real-key Docker task: a role that may only decrypt with the key. See [Running the KMS encryption tests](#running-the-kms-encryption-tests).                                          | `arn:aws:iam::123456789012:role/kms-test-decrypt-only` |
| `KMS_KEY_ALIAS`           | No       | Without `KMS_KEY_ARN`: the alias whose key the harness uses, `alias/aws-advanced-python-wrapper/integration-tests` when unset. See [Running the KMS encryption tests](#running-the-kms-encryption-tests).        | `alias/my-team/kms-tests`                    |
| `KMS_ALLOW_KEY_CREATION`  | No       | Without `KMS_KEY_ARN`: `1`, `true` or `yes`, in any case, lets the harness make the key and the alias when the alias doesn't exist, and the decrypt-only role, which the Docker task always needs. See [Running the KMS encryption tests](#running-the-kms-encryption-tests). | `true`                                       |
| `KMS_MANUAL_TESTS`        | No       | Set to `1` to run the manual tests on the real-key Docker task. See [Running the KMS encryption tests](#running-the-kms-encryption-tests).                                                                       | `1`                                          |

Docker tasks don't read the AWS credential variables, except the KMS task that uses a real key (see [Running the KMS encryption tests](#running-the-kms-encryption-tests)).

## Managing Environment Variables

After following steps 1-3 from the [Run Integration Tests](#running-aurora-integration-tests) section, your default
integration test environment variables will be set. However, you can unset and/or override these variables from the git
bash terminal to quickly change the integration test settings. This is useful to quickly test different integration test
configurations. Some examples are shown below.

**Setting/Overriding an environment variable for the current git bash session**

`export FILTER="test_iam_using"`

**Setting/Overriding an environment variable for a single command**

`FILTER="test_iam_using or test_iam_valid" ./gradlew test-pg-aurora`

**Clearing an environment variable for the current git bash session**

`unset NUM_INSTANCES`

**Reloading defaults from .bashrc**

`source ~/.bashrc`

**Checking the value of an environment variable**

`echo $FILTER`

## Filtering tests

There are two ways to filter which tests you would like to run. The first way is likely faster/easier but the second way
can also be useful.

1. Via test name using the FILTER environment variable. This variable is passed to Pytest’s `-k` command-line flag to
   filter
   out tests. Pytest will run tests containing names matching the given string expression, which can include Python
   operators that use filenames, class names, and function names as variables. Make sure you wrap your string expression
   in
   quotes if it contains space(s). Some examples can be seen below. More info can be
   found [here](https://docs.pytest.org/en/7.1.x/how-to/usage.html#:~:text=run%20tests%20by%20keyword%20expressions).

    ```md
    # Runs tests whose name contains the substring "test_iam_using" or "test_iam_valid"
    
    export FILTER="test_iam_using or test_iam_valid"
    
    # Runs TestMyClass.test_something but not TestMyClass.test_method_simple
    
    export FILTER="MyClass and not method"
    ```

2. Via test group using the `exclude-<*>` system properties. There are two ways you can edit these.

   **Option 1**: Open up `TestEnvironmentProvider.java` and scroll down to the section that parses these properties. You
   can
   change the default passed to `System.getProperty()` to exclude or include these test groups. However, this will not
   work if the system property was already defined by the gradle task you are executing. For example, to disable the
   failover tests:

    ```java
    // Default was originally "false"
    final boolean excludeFailover = Boolean.parseBoolean(System.getProperty("exclude-failover", "true"));
    ```

   **Option 2**: Open up tests/integration/host/build.gradle.kts and edit the system property of the task you are
   executing.
   For example, to disable the failover tests:

    ```
    tasks.register<Test>("test-pg-aurora") {
        group = "verification"
        filter.includeTestsMatching("integration.host.TestRunner.runTests")
        doFirst {
            systemProperty("exclude-docker", "true")
            systemProperty("exclude-performance", "true")
            systemProperty("exclude-mysql-driver", "true")
            systemProperty("exclude-mysql-engine", "true")
            
            // Add this line to disable the failover tests
            systemProperty("exclude-failover", "true")
        }
    }
    ```

## Viewing test results

Each run writes its test report to `tests/integration/container/reports/<environment>.html`, with a JUnit XML report, `<environment>.xml`, beside it.
`<environment>` names the deployment, the engine, the number of instances and the Python version.
The logs are in the console where you ran the tests, and under `show details` for each test in the report.
