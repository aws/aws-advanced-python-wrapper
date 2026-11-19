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
import software.amazon.awssdk.services.iam.model.EntityAlreadyExistsException;
import software.amazon.awssdk.services.iam.model.NoSuchEntityException;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.AlreadyExistsException;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.NotFoundException;
import software.amazon.awssdk.services.kms.model.Tag;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.StsException;

/**
 * Finds or makes the KMS test resources for a real-key run that has no KMS_KEY_ARN: the key that a KMS alias names,
 * made together with the alias when the alias is missing and creation is allowed, and kept for later runs, and, for
 * the Docker task, a role that may only decrypt with it. Removes the role on {@link #close()}, which runs at the end
 * of the JUnit run and from a shutdown hook, and touches only what this instance made.
 */
public class KmsTestUtility implements ExtensionContext.Store.CloseableResource {

  public static final String KEY_ALIAS = "alias/aws-advanced-python-wrapper/integration-tests";

  private static final Logger LOGGER = Logger.getLogger(KmsTestUtility.class.getName());
  private static final String KEY_MARKER = "aws-advanced-python-wrapper test KMS key";
  private static final String ROLE_PREFIX = "aws-advanced-python-wrapper-test-decrypt-only-";
  private static final String TAG_KEY = "aws-advanced-python-wrapper";
  private static final String TAG_VALUE = "integration-tests";
  private static final String ROLE_POLICY_NAME = "decrypt-only";

  private final Region region;
  private final String keyAlias;
  private final KmsClient kmsClient;
  private final IamClient iamClient;
  private final StsClient stsClient;

  private String step;
  private volatile String keyArn;
  private volatile String unnamedKeyId;
  private volatile boolean aliasRequested;
  // The SDK may retry a CreateAlias call whose first attempt made the alias, so once any call was sent, close() reads
  // the alias before it schedules the new key for deletion.
  private volatile boolean aliasSent;
  private volatile String roleName;
  private volatile String roleArn;
  private volatile boolean rolePolicyPut;
  private boolean closed;

  private KmsTestUtility(Region region, String keyAlias, AwsCredentialsProvider credentialsProvider) {
    this.region = region;
    this.keyAlias = keyAlias;
    this.kmsClient = KmsClient.builder().region(region).credentialsProvider(credentialsProvider).build();
    this.iamClient = IamClient.builder().region(Region.AWS_GLOBAL).credentialsProvider(credentialsProvider).build();
    this.stsClient = StsClient.builder().region(region).credentialsProvider(credentialsProvider).build();
  }

  /**
   * Uses the key that the alias names when it is enabled, a symmetric encryption key, and tagged and described with
   * {@link #KEY_MARKER}, and refuses any other key; when the alias is missing and creation is allowed, makes such a
   * key and then the alias, and keeps both. When asked, also makes a role named with {@link #ROLE_PREFIX} that may
   * only decrypt with the key and that the caller can assume. A failure removes the role, and a key made here that no
   * alias may name, before it is thrown.
   */
  public static KmsTestUtility create(
      String region,
      String keyAlias,
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String awsSessionToken,
      boolean allowKeyCreation,
      boolean withDecryptOnlyRole) {
    final KmsTestUtility utility = new KmsTestUtility(
        AuroraTestUtility.getRegionInternal(region),
        keyAlias,
        StaticCredentialsProvider.create(
            StringUtils.isNullOrEmpty(awsSessionToken)
                ? AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey)
                : AwsSessionCredentials.create(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)));

    try {
      final String principal = withDecryptOnlyRole ? utility.callerPrincipal() : null;
      Runtime.getRuntime().addShutdownHook(new Thread(utility::close, "kms-test-resources-cleanup"));
      utility.keyArn = utility.findOrMakeKey(allowKeyCreation).arn();
      if (withDecryptOnlyRole) {
        utility.makeDecryptOnlyRole(principal);
      }
    } catch (AwsServiceException e) {
      // The SDK's message can name the caller's ARN, so only the step and the error code are kept.
      utility.close();
      throw new RuntimeException(
          "Couldn't find or make the KMS test resources for " + keyAlias + " in " + utility.getRegion() + ": "
              + utility.step + ": " + e.awsErrorDetails().errorCode()
              + ". Grant what \"Running the KMS encryption tests\" in docs/development-guide/IntegrationTests.md "
              + "lists, or, with KMS_KEY_ALIAS unset, set KMS_KEY_ARN and KMS_REGION, and for the Docker task "
              + "KMS_DECRYPT_ONLY_ROLE_ARN, to use an existing key.");
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
   * Removes the role, then schedules a key made here for deletion in 7 days when no alias may name it. Runs once;
   * later calls do nothing.
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
      } catch (NoSuchEntityException e) {
        // PutRolePolicy failed, so there is no policy to remove.
      } catch (RuntimeException e) {
        warnLeftover("the policy of role " + this.roleName, e);
      }
    }
    if (this.roleName != null) {
      try {
        this.iamClient.deleteRole(builder -> builder.roleName(this.roleName));
        LOGGER.finer("Deleted role " + this.roleName);
      } catch (NoSuchEntityException e) {
        // CreateRole failed, so there is no role to remove.
      } catch (RuntimeException e) {
        warnLeftover("role " + this.roleName, e);
      }
    }
    scheduleUnnamedKeyDeletion();
  }

  /**
   * Returns the ARN of the role or the user that the credentials belong to, for the decrypt-only role's trust policy.
   */
  private String callerPrincipal() {
    this.step = "GetCallerIdentity";
    final String callerArn = this.stsClient.getCallerIdentity().arn();
    final String[] arnParts = callerArn.split(":", 6);
    final String[] resource = arnParts[5].split("/");
    if ("sts".equals(arnParts[2]) && "assumed-role".equals(resource[0])) {
      // An assumed-role ARN drops the role's path, so the role ARN comes from GetRole.
      this.step = "GetRole";
      return this.iamClient.getRole(builder -> builder.roleName(resource[1])).role().arn();
    }
    if ("iam".equals(arnParts[2]) && "user".equals(resource[0])) {
      return callerArn;
    }
    throw new RuntimeException("root and federated-user credentials can't assume a role: use an IAM user or "
        + "role, or set KMS_KEY_ARN, KMS_REGION and KMS_DECRYPT_ONLY_ROLE_ARN");
  }

  /**
   * Returns the key that the alias names when {@link #keyRefusalReason} finds no reason to refuse it. When the alias
   * doesn't exist and creation is allowed, makes a key and the alias and returns that key, or the checked key of a
   * run that made the alias first.
   */
  private KeyMetadata findOrMakeKey(boolean allowKeyCreation) {
    this.step = "DescribeKey";
    KeyMetadata found = describeAlias();
    if (found == null) {
      if (!allowKeyCreation) {
        throw new RuntimeException("KMS alias " + this.keyAlias + " doesn't exist in " + getRegion()
            + ": set KMS_ALLOW_KEY_CREATION=true to let the harness make the key and the alias, set KMS_KEY_ALIAS "
            + "to an alias that names a test key, or set KMS_KEY_ARN and KMS_REGION to use an existing key.");
      }
      // Under the lock close() takes, so a shutdown hook waits for this step and then sees which key the alias names.
      synchronized (this) {
        if (this.closed) {
          throw new RuntimeException("Cancelled before the KMS test key was made.");
        }
        this.step = "CreateKey";
        final KeyMetadata newKey = this.kmsClient.createKey(
            builder -> builder
                .keySpec(KeySpec.SYMMETRIC_DEFAULT)
                .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
                .description(KEY_MARKER + " made " + Instant.now())
                .tags(Tag.builder().tagKey(TAG_KEY).tagValue(TAG_VALUE).build()))
            .keyMetadata();
        this.unnamedKeyId = newKey.keyId();

        found = makeAlias(newKey);
        if (newKey.keyId().equals(found.keyId())) {
          // The alias names this key, also when an SDK retry of CreateAlias found its first attempt's alias.
          this.unnamedKeyId = null;
          LOGGER.finer("Made a KMS test key and the alias " + this.keyAlias + " in " + getRegion());
          return newKey;
        }
        LOGGER.finer("Another run made " + this.keyAlias + " first");
        scheduleUnnamedKeyDeletion();
      }
    }

    final String reason = keyRefusalReason(found);
    if (reason != null) {
      throw new RuntimeException("KMS alias " + this.keyAlias + " in " + getRegion() + " names a key " + reason
          + ": set KMS_KEY_ALIAS to another alias, or, with KMS_KEY_ALIAS unset, KMS_KEY_ARN and KMS_REGION, "
          + "and for the Docker task KMS_DECRYPT_ONLY_ROLE_ARN, to use a key directly, or remove the alias if "
          + "its owner agrees.");
    }
    LOGGER.finer("Using the KMS test key named by " + this.keyAlias + " in " + getRegion());
    return found;
  }

  /**
   * Makes the alias name the new key, and returns the key that the alias then names: the new key, or the key of a
   * run that made the alias first.
   */
  private KeyMetadata makeAlias(KeyMetadata newKey) {
    // KMS can refuse an alias for a key it doesn't find yet.
    this.step = "CreateAlias";
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (true) {
      // Until KMS answers, the alias may name the new key, so close() keeps it unless the alias names another key.
      this.aliasRequested = true;
      this.aliasSent = true;
      try {
        this.kmsClient.createAlias(builder -> builder.aliasName(this.keyAlias).targetKeyId(newKey.keyId()));
        return newKey;
      } catch (NotFoundException e) {
        // KMS refused the alias, so it names no key yet.
        this.aliasRequested = false;
        if (System.nanoTime() > deadline) {
          throw new RuntimeException(
              "KMS didn't find the new key for the alias " + this.keyAlias + " within 30 seconds; run again.");
        }
      } catch (AlreadyExistsException e) {
        return awaitAliasedKey();
      } catch (AwsServiceException e) {
        // A 4xx answer other than throttling refuses the request, so the alias doesn't name the new key.
        if (e.statusCode() >= 400 && e.statusCode() < 500 && !e.isThrottlingException()) {
          this.aliasRequested = false;
        }
        throw e;
      }
      pause("making the alias " + this.keyAlias);
    }
  }

  /**
   * Returns the key that the existing alias names. KMS can take seconds to show the alias, also when it names this
   * run's key.
   */
  private KeyMetadata awaitAliasedKey() {
    this.step = "DescribeKey";
    final long visibleBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    KeyMetadata named = describeAlias();
    while (named == null) {
      if (System.nanoTime() > visibleBy) {
        throw new RuntimeException("KMS alias " + this.keyAlias + " in " + getRegion()
            + " exists but isn't visible yet, or was removed since: run again.");
      }
      pause("making the alias " + this.keyAlias);
      named = describeAlias();
    }
    return named;
  }

  /**
   * Returns why the key that the alias names can't be the test key, or null when it can.
   */
  private String keyRefusalReason(KeyMetadata key) {
    if (key.keyState() != KeyState.ENABLED) {
      return "in state " + key.keyStateAsString();
    }
    if (key.keySpec() != KeySpec.SYMMETRIC_DEFAULT) {
      return "of spec " + key.keySpecAsString() + ", not SYMMETRIC_DEFAULT";
    }
    if (key.keyUsage() != KeyUsageType.ENCRYPT_DECRYPT) {
      return "for " + key.keyUsageAsString() + ", not ENCRYPT_DECRYPT";
    }
    if (key.description() == null || !key.description().startsWith(KEY_MARKER)) {
      return "whose description doesn't start with \"" + KEY_MARKER + "\"";
    }
    this.step = "ListResourceTags";
    for (Tag tag : this.kmsClient.listResourceTags(builder -> builder.keyId(key.keyId())).tags()) {
      if (TAG_KEY.equals(tag.tagKey()) && TAG_VALUE.equals(tag.tagValue())) {
        return null;
      }
    }
    return "without the tag " + TAG_KEY + "=" + TAG_VALUE;
  }

  /**
   * Makes a role that the principal can assume and that may only decrypt with the key, and waits until the role can
   * be assumed.
   */
  private void makeDecryptOnlyRole(String principal) {
    final String newRoleName = ROLE_PREFIX + System.currentTimeMillis();
    final String trustPolicy = String.format(
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"%s\"},"
            + "\"Action\":\"sts:AssumeRole\"}]}",
        principal);
    final String decryptOnlyPolicy = String.format(
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"kms:Decrypt\","
            + "\"Resource\":\"%s\"}]}",
        this.keyArn);

    // Under the lock close() takes, so a shutdown hook either runs before the role exists or removes it.
    synchronized (this) {
      if (this.closed) {
        throw new RuntimeException("Cancelled before the decrypt-only role was made.");
      }
      this.step = "CreateRole";
      // Set before each call, so close() also removes what a call made when its response was lost.
      this.roleName = newRoleName;
      try {
        this.roleArn = this.iamClient.createRole(
            builder -> builder
                .roleName(newRoleName)
                .path("/")
                .assumeRolePolicyDocument(trustPolicy)
                .description("Made by the aws-advanced-python-wrapper integration tests; removed when the run ends"))
            .role().arn();
      } catch (EntityAlreadyExistsException e) {
        // The role of that name may be another run's, so close() leaves it.
        this.roleName = null;
        throw e;
      }
      LOGGER.finer("Made decrypt-only role " + newRoleName);

      this.step = "PutRolePolicy";
      this.rolePolicyPut = true;
      this.iamClient.putRolePolicy(
          builder -> builder.roleName(newRoleName).policyName(ROLE_POLICY_NAME).policyDocument(decryptOnlyPolicy));
    }

    // A new role can refuse AssumeRole for a few seconds; the tests assume it with the caller's credentials.
    this.step = "AssumeRole";
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
    while (true) {
      try {
        this.stsClient.assumeRole(
            builder -> builder
                .roleArn(this.roleArn)
                .roleSessionName("harness-readiness-check")
                .durationSeconds(900));
        return;
      } catch (StsException e) {
        if (!"AccessDenied".equals(e.awsErrorDetails().errorCode())) {
          throw e;
        }
        if (System.nanoTime() > deadline) {
          throw new RuntimeException(
              "The decrypt-only role " + newRoleName + " couldn't be assumed within 120 seconds.");
        }
      }
      pause("waiting for the decrypt-only role " + newRoleName);
    }
  }

  /**
   * Returns the metadata of the key that the alias names, or null when the alias doesn't exist.
   */
  private KeyMetadata describeAlias() {
    try {
      return this.kmsClient.describeKey(builder -> builder.keyId(this.keyAlias)).keyMetadata();
    } catch (NotFoundException e) {
      return null;
    }
  }

  /**
   * Schedules the key made here for deletion in 7 days, unless the alias names it, because a key that the alias names
   * is unusable as soon as its deletion is scheduled. While a CreateAlias call may have made the alias and the alias
   * can't be read, it keeps the key with a warning.
   */
  private void scheduleUnnamedKeyDeletion() {
    final String keyId = this.unnamedKeyId;
    if (keyId == null) {
      return;
    }
    this.unnamedKeyId = null;
    if (this.aliasSent) {
      KeyMetadata named;
      try {
        named = describeAlias();
      } catch (RuntimeException e) {
        named = null;
      }
      if (named != null && keyId.equals(named.keyId())) {
        LOGGER.finer("The alias " + this.keyAlias + " names the KMS test key that this run made, so it is kept");
        return;
      }
      if (named == null && this.aliasRequested) {
        LOGGER.warning("Kept the KMS test key that this run made in " + getRegion() + ", because " + this.keyAlias
            + " may name it: if no alias names it, schedule its deletion as \"Running the KMS encryption tests\" in "
            + "docs/development-guide/IntegrationTests.md says.");
        return;
      }
    }
    try {
      this.kmsClient.scheduleKeyDeletion(builder -> builder.keyId(keyId).pendingWindowInDays(7));
      LOGGER.finer("Scheduled the KMS test key that this run made for deletion in 7 days");
    } catch (RuntimeException e) {
      warnLeftover("the KMS test key that this run made in " + getRegion(), e);
    }
  }

  /**
   * Waits 2 seconds before the next attempt of a step.
   */
  private static void pause(String what) {
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while " + what + ".");
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
