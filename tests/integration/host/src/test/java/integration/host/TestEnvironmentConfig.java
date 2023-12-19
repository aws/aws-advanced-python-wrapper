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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import integration.host.util.RdsTestUtility;
import integration.host.util.ContainerHelper;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import integration.host.util.StringUtils;
import software.amazon.awssdk.services.rds.model.DBCluster;

public class TestEnvironmentConfig implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentConfig.class.getName());

  private static final String DATABASE_CONTAINER_NAME_PREFIX = "database-container-";
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  protected static final int PROXY_CONTROL_PORT = 8474;
  private final TestEnvironmentInfo info =
      new TestEnvironmentInfo(); // only this info is passed to test container

  // The following variables are local to host portion of test environment. They are not shared with a
  // test container.

  private int numOfInstances;
  private boolean reuseDbCluster;
  private String clusterName; // "cluster-mysql"
  private String clusterDomain; // "XYZ.us-west-2.rds.amazonaws.com"

  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private String awsSessionToken;

  private GenericContainer<?> testContainer;
  private final ArrayList<GenericContainer<?>> databaseContainers = new ArrayList<>();
  private ArrayList<ToxiproxyContainer> proxyContainers;

  private String runnerIP;

  private final Network network = Network.newNetwork();

  private RdsTestUtility rdsUtil;

  private TestEnvironmentConfig(TestEnvironmentRequest request) {
    this.info.setRequest(request);
  }

  public static TestEnvironmentConfig build(TestEnvironmentRequest request) {
    TestEnvironmentConfig env = new TestEnvironmentConfig(request);

    switch (request.getDatabaseEngineDeployment()) {
      case DOCKER:
        initDatabaseParams(env);
        createDatabaseContainers(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          throw new UnsupportedOperationException(TestEnvironmentFeatures.IAM.toString());
        }

        if (request.getFeatures().contains(TestEnvironmentFeatures.FAILOVER_SUPPORTED)) {
          throw new UnsupportedOperationException(
              TestEnvironmentFeatures.FAILOVER_SUPPORTED.toString());
        }

        break;
      case AURORA:
        initDatabaseParams(env);
        createDbCluster(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          configureIamAccess(env);
        }

        break;
      case MULTI_AZ:
        initDatabaseParams(env);
        createDbCluster(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          throw new UnsupportedOperationException(TestEnvironmentFeatures.IAM.toString());
        }

        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      createProxyContainers(env);
    }

    createTestContainer(env);

    return env;
  }

  private static void createDatabaseContainers(TestEnvironmentConfig env) {
    ContainerHelper containerHelper = new ContainerHelper();

    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        env.numOfInstances = 1;
        break;
      case MULTI_INSTANCE:
        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.numOfInstances < 1 || env.numOfInstances > 15) {
          LOGGER.warning(
              env.numOfInstances + " instances were requested but the requested number must be "
                  + "between 1 and 15. 5 instances will be used as a default.");
          env.numOfInstances = 5;
        }
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseInstances().toString());
    }

    switch (env.info.getRequest().getDatabaseEngine()) {
      case MYSQL:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createMysqlContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      3306));
        }
        break;

      case PG:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createPostgresContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      5432));
        }
        break;

      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createDbCluster(TestEnvironmentConfig env) {
    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        initAwsCredentials(env);
        env.numOfInstances = 1;
        createDbCluster(env, 1);
        break;
      case MULTI_INSTANCE:
        initAwsCredentials(env);

        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.numOfInstances < 1 || env.numOfInstances > 15) {
          LOGGER.warning(
              env.numOfInstances + " instances were requested but the requested number must be "
                  + "between 1 and 15. 5 instances will be used as a default.");
          env.numOfInstances = 5;
        }

        createDbCluster(env, env.numOfInstances);
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createDbCluster(TestEnvironmentConfig env, int numOfInstances) {
    DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();
    String deploymentStr = deployment.toString();
    env.info.setRegion(
        !StringUtils.isNullOrEmpty(System.getenv(deploymentStr + "_DB_REGION"))
            ? System.getenv(deploymentStr + "_DB_REGION")
            : "us-east-2");

    env.reuseDbCluster =
        !StringUtils.isNullOrEmpty(System.getenv("REUSE_" + deploymentStr + "_CLUSTER"))
            && Boolean.parseBoolean(System.getenv("REUSE_" + deploymentStr + "_CLUSTER"));
    env.clusterName = System.getenv(deploymentStr + "_CLUSTER_NAME"); // "cluster-mysql"
    env.clusterDomain =
        System.getenv("RDS_CLUSTER_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"

    if (StringUtils.isNullOrEmpty(env.clusterDomain)) {
      throw new RuntimeException("Environment variable RDS_CLUSTER_DOMAIN is required.");
    }

    env.rdsUtil =
        new RdsTestUtility(
            env.info.getRegion(),
            env.awsAccessKeyId,
            env.awsSecretAccessKey,
            env.awsSessionToken);

    ArrayList<TestInstanceInfo> instances = new ArrayList<>();

    if (env.reuseDbCluster) {
      if (!env.rdsUtil.doesClusterExist(env.clusterName)) {
        throw new RuntimeException(
            "It's requested to reuse existing DB cluster but it doesn't exist: "
                + env.clusterName
                + "."
                + env.clusterDomain);
      }
      LOGGER.finer(
          "Reuse existing cluster " + env.clusterName + ".cluster-" + env.clusterDomain);

      DBCluster clusterInfo = env.rdsUtil.getClusterInfo(env.clusterName);

      DatabaseEngine existingEngine = env.rdsUtil.getClusterEngine(clusterInfo);
      if (existingEngine != env.info.getRequest().getDatabaseEngine()) {
        throw new RuntimeException(
            "Existing cluster is "
                + existingEngine
                + " cluster. "
                + env.info.getRequest().getDatabaseEngine()
                + " is expected.");
      }

      env.info.setDatabaseEngine(clusterInfo.engine());
      env.info.setDatabaseEngineVersion(clusterInfo.engineVersion());
      instances.addAll(env.rdsUtil.getClusterInstanceIds(env.clusterName));

    } else {
      if (StringUtils.isNullOrEmpty(env.clusterName)) {
        env.clusterName = getRandomName(env.info.getRequest());
        LOGGER.finer("Cluster to create: " + env.clusterName);
      }

      try {
        String engine = getDbEngine(env.info.getRequest());
        String engineVersion = getDbEngineVersion(env.info.getRequest());
        String instanceClass = getDbInstanceClass(env.info.getRequest());

        env.clusterDomain =
            env.rdsUtil.createCluster(
                env.info.getDatabaseInfo().getUsername(),
                env.info.getDatabaseInfo().getPassword(),
                env.info.getDatabaseInfo().getDefaultDbName(),
                env.clusterName,
                deployment,
                engine,
                instanceClass,
                engineVersion,
                numOfInstances,
                instances);
        env.info.setDatabaseEngine(engine);
        env.info.setDatabaseEngineVersion(engineVersion);
        LOGGER.finer(
            "Created a new cluster " + env.clusterName + ".cluster-" + env.clusterDomain);
      } catch (Exception e) {

        LOGGER.finer("Error creating a cluster " + env.clusterName + ". " + e.getMessage());

        // remove cluster and instances
        LOGGER.finer("Deleting cluster " + env.clusterName);
        env.rdsUtil.deleteCluster(env.clusterName);
        LOGGER.finer("Deleted cluster " + env.clusterName);

        throw new RuntimeException(e);
      }
    }

    env.info.setClusterName(env.clusterName);

    int port = getPort(env.info.getRequest());

    env.info
        .getDatabaseInfo()
        .setClusterEndpoint(env.clusterName + ".cluster-" + env.clusterDomain, port);
    env.info
        .getDatabaseInfo()
        .setClusterReadOnlyEndpoint(
            env.clusterName + ".cluster-ro-" + env.clusterDomain, port);
    env.info.getDatabaseInfo().setInstanceEndpointSuffix(env.clusterDomain, port);

    env.info.getDatabaseInfo().getInstances().clear();
    env.info.getDatabaseInfo().getInstances().addAll(instances);

    try {
      env.runnerIP = env.rdsUtil.getPublicIPAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    env.rdsUtil.ec2AuthorizeIP(env.runnerIP);

    DatabaseEngine engine = env.info.getRequest().getDatabaseEngine();
    if (DatabaseEngineDeployment.MULTI_AZ.equals(deployment) && DatabaseEngine.PG.equals(engine)) {
      DriverHelper.registerDriver(engine);

      try (Connection conn = DriverHelper.getDriverConnection(env.info);
           Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE EXTENSION IF NOT EXISTS rds_tools");
      } catch (SQLException e) {
        throw new RuntimeException("An exception occurred while creating the rds_tools extension.", e);
      }
    }
  }

  private static String getRandomName(TestEnvironmentRequest request) {
    DatabaseEngineDeployment deployment = request.getDatabaseEngineDeployment();
    String deploymentStr;
    if (DatabaseEngineDeployment.MULTI_AZ.equals(deployment)) {
      // Underscores are not allowed in DB identifiers, so MULTI_AZ is not valid
      deploymentStr = "multi-az";
    } else {
      deploymentStr = request.getDatabaseEngineDeployment().toString();
    }

    String engine = request.getDatabaseEngine().toString();
    return String.format("test-%s-%s-%d", deploymentStr, engine, System.nanoTime());
  }

  private static String getDbEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
        return getAuroraEngine(request);
      case RDS:
      case MULTI_AZ:
        return getRdsEngine(request);
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }
  }

  private static String getAuroraEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "aurora-mysql";
      case PG:
        return "aurora-postgresql";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getRdsEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "mysql";
      case PG:
        return "postgres";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getDbEngineVersion(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
        return getAuroraEngineVersion(request);
      case RDS:
      case MULTI_AZ:
        return getRdsEngineVersion(request);
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }
  }

  private static String getAuroraEngineVersion(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "8.0.mysql_aurora.3.03.0";
      case PG:
        return "15.2";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getRdsEngineVersion(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "8.0.33";
      case PG:
        return "15.4";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getDbInstanceClass(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
        return "db.r6g.large";
      case RDS:
      case MULTI_AZ:
        return "db.m5d.large";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static int getPort(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return 3306;
      case PG:
        return 5432;
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static void initDatabaseParams(TestEnvironmentConfig env) {
    final String dbUsername;
    final String dbPassword;

    if (DatabaseEngineDeployment.MULTI_AZ.equals(env.info.getRequest().getDatabaseEngineDeployment())) {
      dbUsername = !StringUtils.isNullOrEmpty(System.getenv("MULTI_AZ_USERNAME"))
          ? System.getenv("MULTI_AZ_USERNAME") : "test_user";
      dbPassword = !StringUtils.isNullOrEmpty(System.getenv("MULTI_AZ_PASSWORD"))
          ? System.getenv("MULTI_AZ_PASSWORD") : "secret_password";
    } else {
      dbUsername = !StringUtils.isNullOrEmpty(System.getenv("DB_USERNAME"))
          ? System.getenv("DB_USERNAME") : "test_user";
      dbPassword = !StringUtils.isNullOrEmpty(System.getenv("DB_PASSWORD"))
          ? System.getenv("DB_PASSWORD") : "secret_password";
    }

    final String dbName =
        !StringUtils.isNullOrEmpty(System.getenv("DB_DATABASE_NAME"))
            ? System.getenv("DB_DATABASE_NAME")
            : "test_database";

    env.info.setDatabaseInfo(new TestDatabaseInfo());
    env.info.getDatabaseInfo().setUsername(dbUsername);
    env.info.getDatabaseInfo().setPassword(dbPassword);
    env.info.getDatabaseInfo().setDefaultDbName(dbName);
  }

  private static void initAwsCredentials(TestEnvironmentConfig env) {
    env.awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    env.awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    env.awsSessionToken = System.getenv("AWS_SESSION_TOKEN");

    if (StringUtils.isNullOrEmpty(env.awsAccessKeyId)) {
      throw new RuntimeException("Environment variable AWS_ACCESS_KEY_ID is required.");
    }
    if (StringUtils.isNullOrEmpty(env.awsSecretAccessKey)) {
      throw new RuntimeException("Environment variable AWS_SECRET_ACCESS_KEY is required.");
    }

    if (env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      env.info.setAwsAccessKeyId(env.awsAccessKeyId);
      env.info.setAwsSecretAccessKey(env.awsSecretAccessKey);
      if (!StringUtils.isNullOrEmpty(env.awsSessionToken)) {
        env.info.setAwsSessionToken(env.awsSessionToken);
      }
    }
  }

  private static void createProxyContainers(TestEnvironmentConfig env) {
    ContainerHelper containerHelper = new ContainerHelper();

    int port = getPort(env.info.getRequest());

    env.info.setProxyDatabaseInfo(new TestProxyDatabaseInfo());
    env.info.getProxyDatabaseInfo().setControlPort(PROXY_CONTROL_PORT);
    env.info.getProxyDatabaseInfo().setUsername(env.info.getDatabaseInfo().getUsername());
    env.info.getProxyDatabaseInfo().setPassword(env.info.getDatabaseInfo().getPassword());
    env.info.getProxyDatabaseInfo().setDefaultDbName(env.info.getDatabaseInfo().getDefaultDbName());

    env.proxyContainers = new ArrayList<>();

    int proxyPort = 0;
    for (TestInstanceInfo instance : env.info.getDatabaseInfo().getInstances()) {
      ToxiproxyContainer container =
          containerHelper.createProxyContainer(env.network, instance, PROXIED_DOMAIN_NAME_SUFFIX);

      container.start();
      env.proxyContainers.add(container);

      ToxiproxyContainer.ContainerProxy proxy =
          container.getProxy(instance.getHost(), instance.getPort());

      if (proxyPort != 0 && proxyPort != proxy.getOriginalProxyPort()) {
        throw new RuntimeException("DB cluster proxies should be on the same port.");
      }
      proxyPort = proxy.getOriginalProxyPort();
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-cluster",
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterEndpoint(),
              port,
              proxyPort));

      env.info
          .getProxyDatabaseInfo()
          .setClusterEndpoint(
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterReadOnlyEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-ro-cluster",
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint(),
              port,
              proxyPort));

      env.info
          .getProxyDatabaseInfo()
          .setClusterReadOnlyEndpoint(
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getInstanceEndpointSuffix())) {
      env.info
          .getProxyDatabaseInfo()
          .setInstanceEndpointSuffix(
              env.info.getDatabaseInfo().getInstanceEndpointSuffix() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    for (TestInstanceInfo instanceInfo : env.info.getDatabaseInfo().getInstances()) {
      TestInstanceInfo proxyInstanceInfo =
          new TestInstanceInfo(
              instanceInfo.getInstanceId(),
              instanceInfo.getHost() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
      env.info.getProxyDatabaseInfo().getInstances().add(proxyInstanceInfo);
    }
  }

  private static void createTestContainer(TestEnvironmentConfig env) {
    final ContainerHelper containerHelper = new ContainerHelper();

    env.testContainer = containerHelper.createTestContainer(
        "aws/rds-test-container",
        getContainerBaseImageName(env.info.getRequest()));

    env.testContainer
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(env.network)
        .withEnv("TEST_ENV_INFO_JSON", getEnvironmentInfoAsString(env))
        .withEnv("TEST_ENV_DESCRIPTION", env.info.getRequest().getDisplayName());
    if (env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      env.testContainer
          .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
          .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
          .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken);
    }

    env.testContainer.start();
  }

  private static String getContainerBaseImageName(TestEnvironmentRequest request) {
    switch (request.getTargetPythonVersion()) {
      case PYTHON_3_8:
        return "python:3.8.18";
      case PYTHON_3_11:
        return "python:3.11.5";
      default:
        throw new NotImplementedException(request.getTargetPythonVersion().toString());
    }
  }

  private static void configureIamAccess(TestEnvironmentConfig env) {

    if (env.info.getRequest().getDatabaseEngineDeployment() != DatabaseEngineDeployment.AURORA) {
      throw new UnsupportedOperationException(
          env.info.getRequest().getDatabaseEngineDeployment().toString());
    }

    env.info.setIamUsername(
        !StringUtils.isNullOrEmpty(System.getenv("IAM_USER"))
            ? System.getenv("IAM_USER")
            : "jane_doe");

    if (!env.reuseDbCluster) {
      DriverHelper.registerDriver(env.info.getRequest().getDatabaseEngine());

      try {
        env.rdsUtil.addAuroraAwsIamUser(env.info);
      } catch (SQLException e) {
        throw new RuntimeException("Error configuring IAM access.", e);
      }
    }
  }

  private static String getEnvironmentInfoAsString(TestEnvironmentConfig env) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(env.info);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing environment details.", e);
    }
  }

  public void runTests(String folderName) throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();
    containerHelper.runTest(this.testContainer, folderName, this);
  }

  public void debugTests(String folderName) throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();
    containerHelper.debugTest(this.testContainer, folderName, this);
  }

  @Override
  public void close() {
    LOGGER.info("TEC.close begin");
    if (this.databaseContainers != null) {
      LOGGER.info("TEC database containers not null");
      for (GenericContainer<?> container : this.databaseContainers) {
        try {
          LOGGER.info("TEC stopping container...");
          container.stop();
        } catch (Exception ex) {
          // ignore
        }
      }
      this.databaseContainers.clear();
    }

    if (this.testContainer != null) {
      LOGGER.info("TEC stopping test container...");
      this.testContainer.stop();
      this.testContainer = null;
    }

    if (this.proxyContainers != null) {
      LOGGER.info("TEC proxyContainers not null");
      for (ToxiproxyContainer proxyContainer : this.proxyContainers) {
        LOGGER.info("TEC stopping proxy container...");
        proxyContainer.stop();
      }
      this.proxyContainers = null;
    }

    switch (this.info.getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
      case MULTI_AZ:
        deleteDbCluster();
        break;
      case RDS:
        throw new NotImplementedException(this.info.getRequest().getDatabaseEngineDeployment().toString());
      default:
        LOGGER.info("TEC hit default switch branch");
        // do nothing
    }
  }

  public String getPrimaryInfo() {
    TestEnvironmentRequest request = this.info.getRequest();
    String deployment = request.getDatabaseEngineDeployment().toString();
    String engine = request.getDatabaseEngine().toString();
    String numInstances = Integer.toString(request.getNumOfInstances());
    String pythonVersion = request.getTargetPythonVersion().toString();
    return String.format("%s-%s-%s_instance-%s", deployment, engine, numInstances, pythonVersion);
  }

  private void deleteDbCluster() {
    LOGGER.info("TEC deleteDbCluster begin");
    LOGGER.info("TEC reuseDbCluster value: " + this.reuseDbCluster);
    if (!this.reuseDbCluster && !StringUtils.isNullOrEmpty(this.runnerIP)) {
      LOGGER.info("TEC deauthorizing IP...");
      rdsUtil.ec2DeauthorizesIP(runnerIP);
    }

    if (!this.reuseDbCluster) {
      LOGGER.info("TEC deleting cluster...");
      LOGGER.finest("Deleting cluster " + this.clusterName + ".cluster-" + this.clusterDomain);
      rdsUtil.deleteCluster(this.clusterName);
      LOGGER.finest("Deleted cluster " + this.clusterName + ".cluster-" + this.clusterDomain);
    } else {
      LOGGER.info("TEC hit reuseDbCluster in deleteDbCluster");
    }
  }
}
