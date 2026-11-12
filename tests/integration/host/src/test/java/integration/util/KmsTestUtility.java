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

package integration.util;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.Tag;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.StsException;

/**
 * Makes the KMS test resources for a real-key run that has no KMS_KEY_ARN: a key and, for the Docker task, a role
 * that may only decrypt with it. Removes them on {@link #close()}, which runs at the end of the JUnit run and from a
 * shutdown hook, and touches only what this instance made.
 */
public class KmsTestUtility implements ExtensionContext.Store.CloseableResource {

  public static final String KEY_MARKER = "aws-advanced-python-wrapper test KMS key";
  public static final String ROLE_PREFIX = "aws-advanced-python-wrapper-test-decrypt-only-";

  private static final Logger LOGGER = Logger.getLogger(KmsTestUtility.class.getName());
  private static final String TAG_KEY = "aws-advanced-python-wrapper";
  private static final String TAG_VALUE = "integration-tests";
  private static final String ROLE_POLICY_NAME = "decrypt-only";

  private final Region region;
  private final KmsClient kmsClient;
  private final IamClient iamClient;
  private final StsClient stsClient;

  private volatile String keyId;
  private volatile String keyArn;
  private volatile String roleName;
  private volatile String roleArn;
  private volatile boolean rolePolicyPut;
  private boolean closed;

  private KmsTestUtility(Region region, AwsCredentialsProvider credentialsProvider) {
    this.region = region;
    this.kmsClient = KmsClient.builder().region(region).credentialsProvider(credentialsProvider).build();
    this.iamClient = IamClient.builder().region(Region.AWS_GLOBAL).credentialsProvider(credentialsProvider).build();
    this.stsClient = StsClient.builder().region(region).credentialsProvider(credentialsProvider).build();
  }

  /**
   * Makes a key tagged and described with {@link #KEY_MARKER} and, when asked, a role named with
   * {@link #ROLE_PREFIX} that may only decrypt with it and that the caller can assume. A failure removes what was
   * made before it is thrown.
   */
  public static KmsTestUtility create(
      String region,
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String awsSessionToken,
      boolean withDecryptOnlyRole) {
    final KmsTestUtility utility = new KmsTestUtility(
        AuroraTestUtility.getRegionInternal(region),
        StaticCredentialsProvider.create(
            StringUtils.isNullOrEmpty(awsSessionToken)
                ? AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey)
                : AwsSessionCredentials.create(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)));

    String step = "GetCallerIdentity";
    try {
      String principal = null;
      if (withDecryptOnlyRole) {
        // An assumed-role ARN drops the role's path, so the role ARN comes from GetRole.
        final String callerArn = utility.stsClient.getCallerIdentity().arn();
        final String[] arnParts = callerArn.split(":", 6);
        final String[] resource = arnParts[5].split("/");
        if ("sts".equals(arnParts[2]) && "assumed-role".equals(resource[0])) {
          step = "GetRole";
          principal = utility.iamClient.getRole(builder -> builder.roleName(resource[1])).role().arn();
        } else if ("iam".equals(arnParts[2]) && "user".equals(resource[0])) {
          principal = callerArn;
        } else {
          throw new RuntimeException("root and federated-user credentials can't assume a role: use an IAM user or "
              + "role, or set KMS_KEY_ARN, KMS_REGION and KMS_DECRYPT_ONLY_ROLE_ARN");
        }
      }

      Runtime.getRuntime().addShutdownHook(new Thread(utility::close));

      step = "CreateKey";
      final CreateKeyResponse key = utility.kmsClient.createKey(
          builder -> builder
              .keySpec(KeySpec.SYMMETRIC_DEFAULT)
              .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
              .description(KEY_MARKER + " made " + Instant.now())
              .tags(Tag.builder().tagKey(TAG_KEY).tagValue(TAG_VALUE).build()));
      utility.keyId = key.keyMetadata().keyId();
      utility.keyArn = key.keyMetadata().arn();
      LOGGER.finer("Made KMS test key " + utility.keyId + " in " + utility.getRegion());

      if (withDecryptOnlyRole) {
        step = "CreateRole";
        final String newRoleName = ROLE_PREFIX + System.currentTimeMillis();
        final String trustPolicy = String.format(
            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"%s\"},"
                + "\"Action\":\"sts:AssumeRole\"}]}",
            principal);
        utility.roleArn = utility.iamClient.createRole(
            builder -> builder
                .roleName(newRoleName)
                .path("/")
                .assumeRolePolicyDocument(trustPolicy)
                .description("Made by the aws-advanced-python-wrapper integration tests; removed when the run ends"))
            .role().arn();
        utility.roleName = newRoleName;
        LOGGER.finer("Made decrypt-only role " + newRoleName);

        step = "PutRolePolicy";
        final String decryptOnlyPolicy = String.format(
            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"kms:Decrypt\","
                + "\"Resource\":\"%s\"}]}",
            utility.keyArn);
        utility.iamClient.putRolePolicy(
            builder -> builder.roleName(newRoleName).policyName(ROLE_POLICY_NAME).policyDocument(decryptOnlyPolicy));
        utility.rolePolicyPut = true;

        // A new role can refuse AssumeRole for a few seconds; the tests assume it with the caller's credentials.
        step = "AssumeRole";
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (true) {
          try {
            utility.stsClient.assumeRole(
                builder -> builder
                    .roleArn(utility.roleArn)
                    .roleSessionName("harness-readiness-check")
                    .durationSeconds(900));
            break;
          } catch (StsException e) {
            if (!"AccessDenied".equals(e.awsErrorDetails().errorCode())) {
              throw e;
            }
            if (System.nanoTime() > deadline) {
              throw new RuntimeException(
                  "The decrypt-only role " + newRoleName + " couldn't be assumed within 120 seconds.");
            }
          }
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for the decrypt-only role " + newRoleName + ".");
          }
        }
      }
    } catch (AwsServiceException e) {
      // The SDK's message can name the caller's ARN, so only the step and the error code are kept.
      utility.close();
      throw new RuntimeException(
          "Couldn't make the KMS test resources: " + step + ": " + e.awsErrorDetails().errorCode()
              + ". Grant what \"Running the KMS encryption tests\" in docs/development-guide/IntegrationTests.md "
              + "lists, or set KMS_KEY_ARN and KMS_REGION, and for the Docker task KMS_DECRYPT_ONLY_ROLE_ARN, "
              + "to use an existing key.");
    } catch (RuntimeException e) {
      utility.close();
      throw e;
    }
    return utility;
  }

  public String getKeyArn() {
    return this.keyArn;
  }

  public String getRegion() {
    return this.region.id();
  }

  public String getDecryptOnlyRoleArn() {
    return this.roleArn;
  }

  /**
   * Removes the role, then schedules the key for deletion in 7 days. Runs once; later calls do nothing.
   */
  @Override
  public synchronized void close() {
    if (this.closed) {
      return;
    }
    this.closed = true;

    if (this.rolePolicyPut) {
      try {
        this.iamClient.deleteRolePolicy(builder -> builder.roleName(this.roleName).policyName(ROLE_POLICY_NAME));
      } catch (RuntimeException e) {
        warnLeftover("the policy of role " + this.roleName, e);
      }
    }
    if (this.roleName != null) {
      try {
        this.iamClient.deleteRole(builder -> builder.roleName(this.roleName));
        LOGGER.finer("Deleted role " + this.roleName);
      } catch (RuntimeException e) {
        warnLeftover("role " + this.roleName, e);
      }
    }
    if (this.keyId != null) {
      try {
        this.kmsClient.scheduleKeyDeletion(
            builder -> builder.keyId(this.keyId).pendingWindowInDays(7));
        LOGGER.finer("Scheduled KMS test key " + this.keyId + " for deletion in 7 days");
      } catch (RuntimeException e) {
        warnLeftover("KMS test key " + this.keyId, e);
      }
    }
  }

  private static void warnLeftover(String what, RuntimeException e) {
    final String reason = e instanceof AwsServiceException
        ? ((AwsServiceException) e).awsErrorDetails().errorCode()
        : e.getClass().getSimpleName();
    LOGGER.warning(String.format(
        "Couldn't remove %s (%s): remove it as \"Running the KMS encryption tests\" in "
            + "docs/development-guide/IntegrationTests.md says.",
        what, reason));
  }
}
