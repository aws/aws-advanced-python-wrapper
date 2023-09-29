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

import integration.host.DatabaseEngine;
import integration.host.DatabaseEngineDeployment;
import integration.host.DatabaseInstances;
import integration.host.GenericTypedParameterResolver;
import integration.host.TargetPythonVersion;
import integration.host.TestEnvironmentFeatures;
import integration.host.TestEnvironmentRequest;
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
      System.out.println(String.format("The NUM_INSTANCES environment variable was set to %d. All test configurations for different cluster sizes will be skipped.", numInstances));
    }

    final boolean excludeDocker = Boolean.parseBoolean(System.getProperty("exclude-docker", "false"));
    final boolean excludeAurora = Boolean.parseBoolean(System.getProperty("exclude-aurora", "false"));
    final boolean excludePerformance =
        Boolean.parseBoolean(System.getProperty("exclude-performance", "false"));
    final boolean excludeMysqlEngine =
        Boolean.parseBoolean(System.getProperty("exclude-mysql-engine", "false"));
    final boolean excludeMysqlDriver =
        Boolean.parseBoolean(System.getProperty("exclude-mysql-driver", "false"));
    final boolean excludePgEngine =
        Boolean.parseBoolean(System.getProperty("exclude-pg-engine", "false"));
    final boolean excludePgDriver =
        Boolean.parseBoolean(System.getProperty("exclude-pg-driver", "false"));
    final boolean excludeFailover =
        Boolean.parseBoolean(System.getProperty("exclude-failover", "false"));
    final boolean excludeIam = Boolean.parseBoolean(System.getProperty("exclude-iam", "false"));
    final boolean excludeSecretsManager =
        Boolean.parseBoolean(System.getProperty("exclude-secrets-manager", "false"));
    final boolean excludePython38 = Boolean.parseBoolean(System.getProperty("exclude-python-38", "false"));
    final boolean excludePython311 = Boolean.parseBoolean(System.getProperty("exclude-python-311", "false"));
    final boolean testAutoscalingOnly = Boolean.parseBoolean(System.getProperty("test-autoscaling", "false"));

    if (!excludeDocker) {
      if (numInstances == null || numInstances == 1) {
        if (!excludeMysqlEngine && !excludePython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
        if (!excludePgEngine && !excludePython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
        if (!excludeMysqlEngine && !excludePython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
        if (!excludePgEngine && !excludePython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.SINGLE_INSTANCE,
                      1,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
      }

      // multiple instances
      if (numInstances == null || numInstances == 2) {
        if (!excludeMysqlEngine && !excludePython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
        if (!excludePgEngine && !excludePython38) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_8,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
        if (!excludeMysqlEngine && !excludePython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.MYSQL,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
        if (!excludePgEngine && !excludePython311) {
          resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      DatabaseEngine.PG,
                      DatabaseInstances.MULTI_INSTANCE,
                      2,
                      DatabaseEngineDeployment.DOCKER,
                      TargetPythonVersion.PYTHON_3_11,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
      }
    }

    if (!excludeAurora) {
      if (!excludeMysqlEngine && !excludePython311) {
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
                      excludeFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      excludeIam ? null : TestEnvironmentFeatures.IAM,
                      excludeSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                      excludePerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }

        if (numInstances == null || numInstances == 2) {
          // Tests for IAM, SECRETS_MANAGER and PERFORMANCE are covered by
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
                      excludeFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }
      }
      if (!excludePgEngine && !excludePython311) {
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
                      excludeFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      excludeIam ? null : TestEnvironmentFeatures.IAM,
                      excludeSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                      excludePerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
        }

        if (numInstances == null || numInstances == 2) {
          // Tests for IAM, SECRETS_MANAGER and PERFORMANCE are covered by
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
                      excludeFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
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
