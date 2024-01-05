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
    - RDS permissions
    - EC2 permissions so integration tests can add the current IP address in the Aurora cluster's EC2 security group.
    - For more information,
      see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

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

    6. Click OK, Apply, OK.
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
| `DB_USER`             | Yes      | The username to access the database.                                                                                                                                                                             | `admin`                                      |
| `DB_PASSWORD`             | Yes      | The database cluster password.                                                                                                                                                                                   | `password`                                   |
| `DB_DATABASE_NAME`        | No       | Name of the database that will be used by the tests. The default database name is test.                                                                                                                          | `test_db_name`                               |
| `AURORA_CLUSTER_NAME`     | Yes      | The database identifier for your Aurora cluster. Must be a unique value to avoid conflicting with existing clusters.                                                                                             | `db-identifier`                              |
| `RDS_CLUSTER_DOMAIN`   | No       | The existing database connection suffix. Use this variable to run against an existing database.                                                                                                                  | `XYZ.us-east-2.rds.amazonaws.com`            |
| `IAM_USER`                | No       | User within the database that is identified with AWSAuthenticationPlugin. This is used for AWS IAM Authentication and is optional                                                                                | `example_user_name`                          |
| `AWS_ACCESS_KEY_ID`       | Yes      | An AWS access key associated with an IAM user or role with RDS permissions.                                                                                                                                      | `ASIAIOSFODNN7EXAMPLE`                       |
| `AWS_SECRET_ACCESS_KEY`   | Yes      | The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                                                                                                   | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`   |
| `AWS_SESSION_TOKEN`       | No       | AWS Session Token for CLI, SDK, & API access. This value is for MFA credentials only. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | `AQoDYXdzEJr...<remainder of session token>` |                                          |
| `REUSE_AURORA_CLUSTER`    | Yes      | Set to true if you would like to use an existing cluster for your tests.                                                                                                                                         | `false`                                      |
| `AURORA_DB_REGION`        | Yes      | The database region.                                                                                                                                                                                             | `us-east-2`                                  |
| `DEBUG_ENV`               | No       | The IDE you will be using to debug the tests, values are either `PYCHARM` or `VSCODE`                                                                                                                            | `PYCHARM`                                    |

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

The test results can be viewed in the test report generated at
`tests/integration/container/reports/integration_tests.html`. The logs can be viewed from the console where you executed
the tests or by clicking `show details` on a given test in the test report.
