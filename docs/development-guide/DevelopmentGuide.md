# Development Guide

### Setup
Make sure you have Python 3.10 - 3.13 (inclusive) installed, along with your choice of underlying Python driver (see [minimum requirements](../GettingStarted.md#minimum-requirements)).

Clone the AWS Advanced Python Driver repository:

```bash
git clone https://github.com/awslabs/aws-advanced-python-wrapper.git
```

You can now make changes in the repository.

## Testing Overview

The AWS Advanced Python Driver uses the following tests to verify its correctness:

| Tests                                         | Description                                                                                                                    |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Unit tests                                    | Tests for AWS Advanced Python Driver correctness.                                                                              |
| Failover integration tests                    | Driver-specific tests for different reader and writer failover workflows using the Failover Connection Plugin.                 |
| Enhanced failure monitoring integration tests | Driver-specific tests for the enhanced failure monitoring functionality using the Host Monitoring Connection Plugin.           |
| AWS authentication integration tests          | Driver-specific tests for AWS authentication methods with the AWS Secrets Manager Plugin or the AWS IAM Authentication Plugin. |

### Performance Tests

The AWS Advanced Python Driver has the following tests to verify its performance:

| Tests                                | Description                                                                                                                                                                                                                                                                                             |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Connection plugin manager benchmarks | The [benchmarks](../../benchmarks/README.md) subproject measures the overhead from executing Python method calls with multiple connection plugins enabled.                                                                                                                                              |
| Manually-triggered performance tests | The [failover plugin performance tests](../../tests/integration/container/test_failover_performance.py) and [enhanced failure monitoring performance tests](../../tests/integration/container/test_read_write_splitting_performance.py) measure the plugins' performance under different configurations |

### Running the Tests

Running the tests will also validate your environment is set up correctly.

Install Poetry:

Mac:
```bash
brew install poetry
```

Windows:
```bash
pipx install poetry
```

Run the tests:

```bash
poetry run python -m pytest ./tests/unit
```

#### Integration Tests
For more information on how to run the integration tests, please visit [Integration Tests](../development-guide/IntegrationTests.md).

#### Sample Code
See the [Examples](../../docs/examples/) directory for sample code.
