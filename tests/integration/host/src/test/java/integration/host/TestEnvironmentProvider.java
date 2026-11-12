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
import integration.util.KmsTestUtility;
import integration.util.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class TestEnvironmentProvider implements TestTemplateInvocationContextProvider {

  static final ArrayList<EnvPreCreateInfo> preCreateInfos = new ArrayList<>();
  static KmsTestUtility kmsTestUtility;
  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentProvider.class.getName());

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {

    preCreateInfos.clear();
    ArrayList<TestTemplateInvocationContext> resultContextList = new ArrayList<>();

    TestEnvironmentConfiguration config = new TestEnvironmentConfiguration();

    final List<Integer> validNumInstances = Arrays.asList(1, 2, 3, 5);
    final List<Integer> instancesToTest;
    if (config.numInstances != null) {
      System.out.printf(
              "The NUM_INSTANCES environment variable was set to %d. All test configurations for different cluster sizes will be skipped.%n",
              config.numInstances);
      if (!validNumInstances.contains(config.numInstances)) {
        throw new RuntimeException(
                String.format(
                        "The NUM_INSTANCES environment variable was set to an invalid value: %d. Valid values are: %s.",
                        config.numInstances, validNumInstances));
      }
      instancesToTest = Arrays.asList(config.numInstances);
    } else {
      instancesToTest =  validNumInstances;
    }

    for (DatabaseEngineDeployment deployment : DatabaseEngineDeployment.values()) {
      if (deployment == DatabaseEngineDeployment.DOCKER && config.excludeDocker) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.AURORA && config.excludeAurora) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS) {
        // Not in use.
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER && config.excludeMultiAzCluster) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE && config.excludeMultiAzInstance) {
        continue;
      }
      final boolean kmsLocalstack = config.testKmsEncryptionOnly
          && deployment == DatabaseEngineDeployment.DOCKER
          && !config.excludeKmsLocalstack;

      for (DatabaseEngine engine : DatabaseEngine.values()) {
        if (engine == DatabaseEngine.PG && config.excludePgEngine) {
          continue;
        }
        if (engine == DatabaseEngine.MYSQL && config.excludeMysqlEngine) {
          continue;
        }

        for (DatabaseInstances instances : DatabaseInstances.values()) {
          if (deployment == DatabaseEngineDeployment.DOCKER
              && instances != DatabaseInstances.SINGLE_INSTANCE) {
            continue;
          }

          for (int numOfInstances : instancesToTest) {
            if (instances == DatabaseInstances.SINGLE_INSTANCE && numOfInstances > 1) {
              continue;
            }
            if (instances == DatabaseInstances.MULTI_INSTANCE && numOfInstances == 1) {
              continue;
            }
            if (numOfInstances == 1 && config.excludeInstances1) {
              continue;
            }
            if (numOfInstances == 2 && config.excludeInstances2) {
              continue;
            }
            if (numOfInstances == 3 && config.excludeInstances3) {
              continue;
            }
            if (numOfInstances == 5 && config.excludeInstances5) {
              continue;
            }
            if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER && numOfInstances != 3) {
              // Multi-AZ clusters supports only 3 instances
              continue;
            }
            if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE && numOfInstances != 1) {
              // Multi-AZ Instances supports only 1 instance
              continue;
            }
           if (deployment == DatabaseEngineDeployment.AURORA && numOfInstances == 3 && config.numInstances == null) {
             // Aurora supports clusters with 3 instances but running such tests is similar
             // to running tests on 5-instance cluster.
             // Let's save some time and skip tests for this configuration
             continue;
           }

            for (TargetPythonVersion targetPythonVersion : TargetPythonVersion.values()) {
              if (targetPythonVersion == TargetPythonVersion.PYTHON_3_10 && config.excludePython310) {
                continue;
              }
              if (targetPythonVersion == TargetPythonVersion.PYTHON_3_11 && config.excludePython311) {
                continue;
              }
              if (targetPythonVersion == TargetPythonVersion.PYTHON_3_12 && config.excludePython312) {
                continue;
              }
              if (targetPythonVersion == TargetPythonVersion.PYTHON_3_13 && config.excludePython313) {
                continue;
              }
              if (targetPythonVersion == TargetPythonVersion.PYTHON_3_14 && config.excludePython314) {
                continue;
              }

              for (boolean withBlueGreenFeature : Arrays.asList(true, false)) {
                if (!withBlueGreenFeature) {
                  if (config.testBlueGreenOnly) {
                    continue;
                  }
                }
                if (withBlueGreenFeature) {
                  if (config.excludeBlueGreen && !config.testBlueGreenOnly) {
                    continue;
                  }
                  // Run BlueGreen test only for MultiAz Instances with 1 node or for Aurora
                  if (deployment != DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE
                      && deployment != DatabaseEngineDeployment.AURORA) {
                    continue;
                  }
                }

                resultContextList.add(
                    getEnvironment(
                        new TestEnvironmentRequest(
                            engine,
                            instances,
                            instances == DatabaseInstances.SINGLE_INSTANCE ? 1 : numOfInstances,
                            deployment,
                            targetPythonVersion,
                            TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                            engine == DatabaseEngine.PG ? TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED : null,
                            // A KMS-only Docker environment without LocalStack calls real KMS, so it needs credentials.
                            deployment == DatabaseEngineDeployment.DOCKER
                                && config.excludeTracesTelemetry
                                && config.excludeMetricsTelemetry
                                && (!config.testKmsEncryptionOnly || kmsLocalstack)
                                ? null
                                : TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                            deployment == DatabaseEngineDeployment.DOCKER || config.excludeFailover
                                ? null
                                : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                            deployment == DatabaseEngineDeployment.DOCKER
                                || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER
                                || config.excludeIam
                                ? null
                                : TestEnvironmentFeatures.IAM,
                            config.excludeSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                            config.excludePerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                            config.excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                            config.excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                            config.excludeSyncDrivers ? TestEnvironmentFeatures.SKIP_SYNC_DRIVER_TESTS : null,
                            config.excludeAsyncDrivers ? TestEnvironmentFeatures.SKIP_ASYNC_DRIVER_TESTS : null,
                            config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null,
                            config.excludeTracesTelemetry ? null : TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED,
                            config.excludeMetricsTelemetry ? null : TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED,
                            withBlueGreenFeature ? TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT : null,
                            config.testKmsEncryptionOnly ? TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY : null,
                            kmsLocalstack ? TestEnvironmentFeatures.KMS_LOCALSTACK : null)));
              }
            }
          }
        }
      }
    }

    // Before any environment starts, so a KMS Aurora run never creates a cluster without a key: a real-key run
    // uses KMS_KEY_ARN, or the harness finds or makes the key (and for Docker makes a decrypt-only role) once per JVM.
    boolean realKey = false;
    boolean dockerRealKey = false;
    for (EnvPreCreateInfo preCreateInfo : preCreateInfos) {
      final Set<TestEnvironmentFeatures> features = preCreateInfo.request.getFeatures();
      if (features.contains(TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY)
          && !features.contains(TestEnvironmentFeatures.KMS_LOCALSTACK)) {
        realKey = true;
        dockerRealKey |= preCreateInfo.request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.DOCKER;
      }
    }
    if (realKey && !StringUtils.isNullOrEmpty(config.kmsKeyArn)) {
      if (StringUtils.isNullOrEmpty(config.kmsRegion)) {
        throw new RuntimeException("Environment variable KMS_REGION is required with KMS_KEY_ARN.");
      }
      if (!StringUtils.isNullOrEmpty(config.kmsKeyAlias)) {
        throw new RuntimeException("KMS_KEY_ALIAS is set with KMS_KEY_ARN: unset one; with KMS_KEY_ARN the harness "
            + "looks nothing up and makes nothing.");
      }
    } else if (realKey) {
      if (dockerRealKey && !StringUtils.isNullOrEmpty(config.kmsDecryptOnlyRoleArn)) {
        throw new RuntimeException("KMS_DECRYPT_ONLY_ROLE_ARN is set without KMS_KEY_ARN: set KMS_KEY_ARN and "
            + "KMS_REGION too, or unset it and the harness finds or makes the key and makes the role.");
      }
      if (!StringUtils.isNullOrEmpty(config.kmsKeyAlias)
          && (!config.kmsKeyAlias.matches("alias/[A-Za-z0-9/_-]+") || config.kmsKeyAlias.startsWith("alias/aws/"))) {
        throw new RuntimeException("KMS_KEY_ALIAS must be alias/ followed by letters, digits, /, _ or -, and not "
            + "start with alias/aws/.");
      }
      if (StringUtils.isNullOrEmpty(config.awsAccessKeyId)) {
        throw new RuntimeException("Environment variable AWS_ACCESS_KEY_ID is required.");
      }
      if (StringUtils.isNullOrEmpty(config.awsSecretAccessKey)) {
        throw new RuntimeException("Environment variable AWS_SECRET_ACCESS_KEY is required.");
      }
      final String region = !StringUtils.isNullOrEmpty(config.kmsRegion)
          ? config.kmsRegion
          : !StringUtils.isNullOrEmpty(config.rdsDbRegion) ? config.rdsDbRegion : "us-east-2";
      final String keyAlias = !StringUtils.isNullOrEmpty(config.kmsKeyAlias)
          ? config.kmsKeyAlias
          : KmsTestUtility.KEY_ALIAS;
      final boolean withDecryptOnlyRole = dockerRealKey;
      kmsTestUtility = context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).getOrComputeIfAbsent(
          KmsTestUtility.class,
          key -> KmsTestUtility.create(
              region, keyAlias, config.awsAccessKeyId, config.awsSecretAccessKey, config.awsSessionToken,
              withDecryptOnlyRole),
          KmsTestUtility.class);
    }

    int index = 1;
    for (TestTemplateInvocationContext testTemplateInvocationContext : resultContextList) {
      LOGGER.finest(
          "Added to the test queue: " + testTemplateInvocationContext.getDisplayName(index++));
    }

    return Arrays.stream(resultContextList.toArray(new TestTemplateInvocationContext[0]));
  }

  private TestTemplateInvocationContext getEnvironment(TestEnvironmentRequest info) {
    return new AwsWrapperTestTemplateInvocationContext(info) {

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

  public abstract static class AwsWrapperTestTemplateInvocationContext
      implements TestTemplateInvocationContext {
    AwsWrapperTestTemplateInvocationContext(final TestEnvironmentRequest info) {
      int index = preCreateInfos.size();
      info.setEnvPreCreateIndex(index);

      EnvPreCreateInfo envPreCreateInfo = new EnvPreCreateInfo();
      envPreCreateInfo.request = info;
      preCreateInfos.add(envPreCreateInfo);
    }

    public abstract String getDisplayName(int invocationIndex);

    public abstract List<Extension> getAdditionalExtensions();
  }

  public static class EnvPreCreateInfo {
    public TestEnvironmentRequest request;
    public Future<Object> envPreCreateFuture; // TestEnvironment or Exception
  }
}
