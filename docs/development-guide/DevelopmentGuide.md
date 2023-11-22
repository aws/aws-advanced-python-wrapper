# Development Guide

### Setup
Make sure you have Amazon Corretto 8+ or Java 8+ installed.

Clone the AWS Python Driver repository:

```bash
git clone https://github.com/awslabs/aws-advanced-python-wrapper.git
```

You can now make changes in the repository.

### Building the AWS Advanced Python Driver
Navigate to project root:
```bash
cd aws-advanced-python-wrapper
```
To build the AWS Advanced Python Driver without running the tests:
Mac:

```bash
./gradlew build -x test
```

Windows:
```bash
gradlew build -x test
```

Mac:
```bash
./gradlew build
```

Windows:
```bash
gradlew build
```

## Testing Overview

The AWS Python Driver uses the following tests to verify its correctness and performance on both JVM and GraalVM:

| Tests                                         | Description                                                                                                                                              |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| Unit tests                                    | Tests for AWS Python Driver correctness.                                                                                                                   |
| Failover integration tests                    | Driver-specific tests for different reader and writer failover workflows using the Failover Connection Plugin.                                           |
| Enhanced failure monitoring integration tests | Driver-specific tests for the enhanced failure monitoring functionality using the Host Monitoring Connection Plugin.                                     |
| AWS authentication integration tests          | Driver-specific tests for AWS authentication methods with the AWS Secrets Manager Plugin or the AWS IAM Authentication Plugin.                           |
| Connection plugin manager benchmarks          | The [benchmarks](../../benchmarks/README.md) subproject measures the overhead from executing Python method calls with multiple connection plugins enabled. |

### Extra Integration Tests

The AWS Python Driver repository also contains additional integration tests for external tools such as HikariCP, the Spring framework or Hibernate ORM.

The AWS Python Driver has been manually verified to work with database tools such as DBeaver.

### Performance Tests

The Python Wrapper has 2 types of performance tests:
- benchmarks measuring the AWS Python Driver's overhead when executing simple Python methods using the JMH microbenchmark framework
- manually-triggered performance tests measuring the failover and enhanced failure monitoring plugins' performance under different configurations

### Running the Tests

After building the AWS Python Driver you can now run the unit tests.
This will also validate your environment is set up correctly.

Mac:
```bash
./gradlew test
```

Windows:
```bash
./gradlew test
```

#### Integration Tests
For more information on how to run the integration tests, please visit [Integration Tests](../development-guide/IntegrationTests.md).

#### Sample Code
[Connection Test Sample Code](../../docs/examples/PGIamAuthentication.py)

## Architecture
For more information on how the AWS Advanced Python Driver functions and how it is structured, please visit [Architecture](./Architecture.md).