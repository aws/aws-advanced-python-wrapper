# Development Guide

### Setup
Make sure you have Python 3.8+ installed, along with your choice of underlying Python driver (see [minimum requirements](../GettingStarted.md#minimum-requirements)).

Clone the AWS Python Driver repository:

```bash
git clone https://github.com/awslabs/aws-advanced-python-wrapper.git
```

You can now make changes in the repository.

## Testing Overview

The AWS Python Driver uses the following tests to verify its correctness and performance on both JVM and GraalVM:

| Tests                                         | Description                                                                                                                                              |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| Unit tests                                    | Tests for AWS Python Driver correctness.                                                                                                                   |
| Failover integration tests                    | Driver-specific tests for different reader and writer failover workflows using the Failover Connection Plugin.                                           |
| Enhanced failure monitoring integration tests | Driver-specific tests for the enhanced failure monitoring functionality using the Host Monitoring Connection Plugin.                                     |
| AWS authentication integration tests          | Driver-specific tests for AWS authentication methods with the AWS Secrets Manager Plugin or the AWS IAM Authentication Plugin.                           |
| Connection plugin manager benchmarks          | The [benchmarks](../../benchmarks/README.md) subproject measures the overhead from executing Python method calls with multiple connection plugins enabled. |

### Performance Tests

The Python Wrapper has 2 types of performance tests:
- benchmarks measuring the AWS Python Driver's overhead when executing simple Python methods using pytest-benchmark
- manually-triggered performance tests measuring the failover and enhanced failure monitoring plugins' performance under different configurations

### Running the Tests

Running the tests will also validate your environment is set up correctly.

Install Poetry:

Mac:
```bash
brew install poetry
```

Windows:
```bash
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
```
Run the tests:

Mac:
```bash
poetry run python -m pytest ./tests/unit/*
```

Windows:
```bash
poetry run python -m pytest ./tests/unit/*
```

#### Integration Tests
For more information on how to run the integration tests, please visit [Integration Tests](../development-guide/IntegrationTests.md).

#### Sample Code
See the [Examples](../../docs/examples/) directory for sample code.
