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
The LocalStack task needs no AWS account.
The harness gives the test container dummy credentials and passes LocalStack's address to the tests as `KMS_ENDPOINT_URL`.
The KMS tests skip outside these tasks, and fail inside them when the endpoint or the key is missing.
On the real-key tasks, when `KMS_KEY_ARN` is unset, the harness uses the key that a KMS alias names: the alias in `KMS_KEY_ALIAS`, or `alias/aws-advanced-python-wrapper/integration-tests` when that is unset.
For the Docker task it also makes a role that may only decrypt with that key, and deletes the role when the run ends.
When the alias doesn't exist, the harness makes the key and then the alias, and keeps both, so later runs share the key.
When the alias names a key that isn't enabled, isn't a symmetric encryption key, or lacks the harness's markers (a description that starts with `aws-advanced-python-wrapper test KMS key` and the tag `aws-advanced-python-wrapper=integration-tests`), the run fails before anything is made.
Then set `KMS_KEY_ALIAS` to another alias, use an existing key as below with `KMS_KEY_ALIAS` unset, or remove the alias if its owner agrees.
`KMS_KEY_ALIAS` is `alias/` followed by letters, digits, `/`, `_` or `-`, and can't start with `alias/aws/`.
The harness looks up the alias, and makes the key and the alias, in `KMS_REGION`, or else `RDS_DB_REGION`, or else `us-east-2`.
To use an existing key instead, set `KMS_KEY_ARN` and `KMS_REGION`, and for the Docker task the ARN of a role that may only decrypt in `KMS_DECRYPT_ONLY_ROLE_ARN`; the harness then looks nothing up and makes nothing, as in CI.
For the Docker task, setting `KMS_DECRYPT_ONLY_ROLE_ARN` without `KMS_KEY_ARN` fails the run before anything is made.
Setting `KMS_KEY_ALIAS` together with `KMS_KEY_ARN` also fails the run before anything is made.
In `KMS_KEY_ARN`, use a full key ARN, not an alias.
The test modules create keys only in LocalStack; on AWS the key is the harness's or yours.
There is one shared key for each alias and region, and each costs $1 a month, prorated by the hour, until it is scheduled for deletion; an alias costs nothing.
When two runs make the alias at the same time, the one that loses schedules the key it made for deletion with the 7-day minimum and uses the other run's key, or fails and asks to run again when it can't see that key yet.
A run whose setup fails after it made its key and before it made the alias also schedules that key for deletion.
A key pending deletion costs nothing, and AWS deletes it after 7 to 30 days.
To find the key, the harness needs `kms:DescribeKey` and `kms:ListResourceTags` on the key the alias names.
To make the key and the alias, it needs `kms:CreateKey`, `kms:TagResource` with the tag `aws-advanced-python-wrapper=integration-tests`, and `kms:CreateAlias` on the alias and on the new key, and to remove a key it made that no alias names, `kms:ScheduleKeyDeletion` on that key.
For the Docker task it also needs `iam:CreateRole`, `iam:PutRolePolicy`, `iam:DeleteRolePolicy`, `iam:DeleteRole` and `sts:AssumeRole` on roles named `aws-advanced-python-wrapper-test-decrypt-only-*`, and, when the credentials are a role session, `iam:GetRole` on the session's own role.
The harness never deletes or re-points an alias, and never changes a key it didn't make in the same run.
A permission scoped by that tag can take up to five minutes to cover a new key, so the harness's and the test container's calls on a key that new can be refused, on the run that makes the key or loses the race to make it; a run that starts more than five minutes after the key was made uses a key that such a permission covers.
So grant `kms:DescribeKey`, `kms:ListResourceTags`, `kms:CreateAlias`, `kms:ScheduleKeyDeletion` and the test container's KMS actions on `arn:aws:kms:<region>:<account>:key/*`, and `kms:CreateAlias` on `arn:aws:kms:<region>:<account>:alias/aws-advanced-python-wrapper/*` and on any alias you set in `KMS_KEY_ALIAS`, in an account set aside for testing, rather than by tag.
The harness refuses root and federated-user credentials for the Docker task, because they can't assume a role.
The test container gets the same credentials, and uses `kms:GenerateDataKey`, `kms:Decrypt` and `kms:DescribeKey` on the test key and `sts:AssumeRole` on the decrypt-only role.
With `iam:PutRolePolicy` and `sts:AssumeRole` on those roles, the caller, and the test container that holds its credentials, can give such a role any permission and use it, so grant them only in an account set aside for testing.
In a shared account, use an existing key and role, as CI does.
Run the Docker task with a principal limited to these actions, and the Aurora task with these KMS actions plus what the Aurora tasks need, not an administrator.
With an existing key, give the test credentials only `kms:GenerateDataKey`, `kms:Decrypt` and `kms:DescribeKey` on the test key, and `sts:AssumeRole` on the decrypt-only role.
The harness deletes its role, and schedules the deletion of a key it made that no alias names, with the credentials it started with, so use credentials that outlast the run.
A cancelled run tries to remove them before its JVM exits.
A killed JVM, a cancel while the harness is still making them, or a clean-up call that fails, for example because the credentials expired during a long run, can leave a role, or a key that no alias names, behind; a failed call logs a warning that names the key id or the role name.
To find what is left in a region, run `aws resourcegroupstaggingapi get-resources --resource-type-filters kms:key --tag-filters Key=aws-advanced-python-wrapper,Values=integration-tests --query "ResourceTagMappingList[].ResourceARN" --region <region>` and `aws iam list-roles --query "Roles[?starts_with(RoleName, 'aws-advanced-python-wrapper-test-decrypt-only-')].[RoleName,CreateDate]"`.
The searches also need `tag:GetResources`, `iam:ListRoles` and `kms:ListAliases`, and removing the shared key needs `kms:DeleteAlias` on the alias and on the key; the harness's own actions include none of them.
For each key, `aws kms describe-key --key-id <key-arn> --query "KeyMetadata.[KeyState,Description]" --region <region>` shows whether it is already `PendingDeletion`, and its description, which starts with `aws-advanced-python-wrapper test KMS key` and gives the time it was made.
For each key that isn't `PendingDeletion`, `aws kms list-aliases --key-id <key-arn> --query "Aliases[].AliasName" --region <region>` lists the aliases that name it; a key that an alias names, such as the shared key, is in use and isn't a leftover.
Remove only what was made before the oldest run still going in the account started, because a run uses what it made until it ends.
Delete a role with `aws iam delete-role-policy --role-name <name> --policy-name decrypt-only`, which fails with `NoSuchEntity` if the run stopped before it added the policy, and then `aws iam delete-role --role-name <name>`.
Schedule each key that isn't `PendingDeletion` and that no alias names for deletion with `aws kms schedule-key-deletion --key-id <key-arn> --pending-window-in-days 7 --region <region>`.
Never schedule the shared key for deletion while a run in the region may use it, because a key scheduled for deletion is unusable at once.
To remove the shared key when no run is going, get its ARN with `aws kms describe-key --key-id alias/aws-advanced-python-wrapper/integration-tests --query KeyMetadata.Arn --region <region>`, delete the alias with `aws kms delete-alias --alias-name alias/aws-advanced-python-wrapper/integration-tests --region <region>`, and then schedule the key for deletion with the command above.
Delete the alias first: while it names a key pending deletion, every run that looks it up fails, and once it is gone the next run makes a new key and alias.
Repeat these steps in each region where the real-key tasks ran without `KMS_KEY_ARN`, and for each alias you set in `KMS_KEY_ALIAS` whose key's description starts with `aws-advanced-python-wrapper test KMS key`, with that alias in place of `alias/aws-advanced-python-wrapper/integration-tests`.
A cancelled or killed Aurora run, or one whose setup failed (for example because `RDS_DB_NAME` names an existing cluster), can also leave an all-traffic rule for the runner's IP in the `default` security group, described `Test run at <time>`; later runs keep a rule they didn't add, so remove it with `aws ec2 revoke-security-group-ingress --group-name default --protocol -1 --cidr <ip>/32 --region <region>`.
Check first that `aws ec2 describe-security-groups --group-names default --query "SecurityGroups[0].IpPermissions[?IpProtocol=='-1'].IpRanges[] | [?Description && starts_with(Description, 'Test run at')].[CidrIp,Description]" --region <region>` lists the rule with a time before the oldest run still going started, because in a shared account someone may have added a rule for the same IP by hand, and remove it only when no Aurora test run from that IP is still going, because such runs share the rule.
A cancelled or killed Aurora run also leaves its cluster, named `RDS_DB_NAME`, or `test-pg-` or `test-mysql-` and a random suffix when that is unset; delete each instance that `aws rds describe-db-clusters --db-cluster-identifier <name> --query "DBClusters[0].DBClusterMembers[].DBInstanceIdentifier" --region <region>` lists with `aws rds delete-db-instance --db-instance-identifier <instance> --skip-final-snapshot --region <region>`, and then the cluster with `aws rds delete-db-cluster --db-cluster-identifier <name> --skip-final-snapshot --region <region>`.
Reports keep failure output, which can include the database password, so use throwaway database passwords and keep reports private.
To run the same tests against local servers and LocalStack without the harness, use `tests/integration/kms_local/run.sh`.

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
