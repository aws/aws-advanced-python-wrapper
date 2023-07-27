/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.host;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DatabaseInstances;
import integration.GenericTypedParameterResolver;
import integration.TargetPythonVersion;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class TestEnvironmentProvider implements TestTemplateInvocationContextProvider {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentProvider.class.getName());

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    ArrayList<TestTemplateInvocationContext> resultContextList = new ArrayList<>();

    final String numInstancesVar = System.getenv("NUM_INSTANCES");
    final Integer numInstances = numInstancesVar == null ? null : Integer.parseInt(numInstancesVar);
    final Set<Integer> validNumInstances = new HashSet<>(Arrays.asList(1, 2, 5));
    if (numInstances != null && !validNumInstances.contains(numInstances)) {
      throw new RuntimeException(
          String.format(
              "The NUM_INSTANCES environment variable was set to an invalid value: %d. Valid values are: %s.",
              numInstances, validNumInstances));
    } else if (numInstances != null) {
      System.out.println(String.format("The NUM_INSTANCES environment variable was set to %d. All test configurations for different cluster sizes will be skipped."));
    }

    final boolean noDocker = Boolean.parseBoolean(System.getProperty("test-no-docker", "false"));
    final boolean noAurora = Boolean.parseBoolean(System.getProperty("test-no-aurora", "false"));
    final boolean noPerformance =
        Boolean.parseBoolean(System.getProperty("test-no-performance", "false"));
    final boolean noMysqlEngine =
        Boolean.parseBoolean(System.getProperty("test-no-mysql-engine", "false"));
    final boolean noMysqlDriver =
        Boolean.parseBoolean(System.getProperty("test-no-mysql-driver", "false"));
    final boolean noPgEngine =
        Boolean.parseBoolean(System.getProperty("test-no-pg-engine", "false"));
    final boolean noPgDriver =
        Boolean.parseBoolean(System.getProperty("test-no-pg-driver", "false"));
    final boolean noMariadbEngine =
        Boolean.parseBoolean(System.getProperty("test-no-mariadb-engine", "false"));
    final boolean noMariadbDriver =
        Boolean.parseBoolean(System.getProperty("test-no-mariadb-driver", "false"));
    final boolean noFailover =
        Boolean.parseBoolean(System.getProperty("test-no-failover", "false"));
    final boolean noIam = Boolean.parseBoolean(System.getProperty("test-no-iam", "false"));
    final boolean noSecretsManager =
        Boolean.parseBoolean(System.getProperty("test-no-secrets-manager", "false"));
    final boolean noPython38 = Boolean.parseBoolean(System.getProperty("test-no-python-38", "false"));
    final boolean noPython311 = Boolean.parseBoolean(System.getProperty("test-no-python-311", "false"));

    if (!noDocker) {
      if (numInstances == null || numInstances == 1) {
        if (!noMysqlEngine && !noPython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noPgEngine && !noPython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noMariadbEngine && !noPython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MARIADB,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noMysqlEngine && !noPython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noPgEngine && !noPython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noMariadbEngine && !noPython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MARIADB,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
      }

      // multiple instances
      if (numInstances == null || numInstances == 2) {
        if (!noMysqlEngine && !noPython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noPgEngine && !noPython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noMariadbEngine && !noPython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MARIADB,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noMysqlEngine && !noPython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noPgEngine && !noPython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
        if (!noMariadbEngine && !noPython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MARIADB,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
      }
    }

    if (!noAurora) {
      if (!noMysqlEngine && !noPython311) {
        if (numInstances == null || numInstances == 5) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.MULTI_INSTANCE,
                      5,
                      DatabaseEngineDeployment.AURORA,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      noIam ? null : TestEnvironmentFeatures.IAM,
                      noSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                      noPerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }

        if (numInstances == null || numInstances == 2) {
          // Tests for HIKARI, IAM, SECRETS_MANAGER and PERFORMANCE are covered by
          // cluster configuration above, so it's safe to skip these tests for configurations below.
          // The main goal of the following cluster configurations is to check failover.
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.AURORA,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
      }
      if (!noPgEngine && !noPython311) {
        if (numInstances == null || numInstances == 5) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.MULTI_INSTANCE,
                      5,
                      DatabaseEngineDeployment.AURORA,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      noIam ? null : TestEnvironmentFeatures.IAM,
                      noSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                      noPerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }

        if (numInstances == null || numInstances == 2) {
          // Tests for HIKARI, IAM, SECRETS_MANAGER and PERFORMANCE are covered by
          // cluster configuration above, so it's safe to skip these tests for configurations below.
          // The main goal of the following cluster configurations is to check failover.
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.AURORA,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
        }
      }
    }

    int index = 1;
    for (TestTemplateInvocationContext testTemplateInvocationContext : resultContextList) {
      LOGGER.finest(
          "Added to the test queue: " + testTemplateInvocationContext.getDisplayName(index++));
    }

    return Arrays.stream(resultContextList.toArray(new TestTemplateInvocationContext[0]));
  }

  private TestTemplateInvocationContext getEnvironment(TestEnvironmentRequest info) {
    return new TestTemplateInvocationContext() {
      @Override
      public String getDisplayName(int invocationIndex) {
        return String.format("[%d] - %s", invocationIndex, info.getDisplayName());
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
        return Collections.singletonList(new GenericTypedParameterResolver<>(info));
      }
    };
  }
}
