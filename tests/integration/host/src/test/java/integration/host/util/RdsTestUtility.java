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

package integration.host.util;

import integration.host.DatabaseEngine;
import integration.host.DatabaseEngineDeployment;
import integration.host.DriverHelper;
import integration.host.TestEnvironmentInfo;
import integration.host.TestInstanceInfo;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Creates and destroys AWS RDS Clusters and Instances. To use this functionality the following environment variables
 * must be defined: - AWS_ACCESS_KEY_ID - AWS_SECRET_ACCESS_KEY
 */
public class RdsTestUtility {

  private static final Logger LOGGER = Logger.getLogger(RdsTestUtility.class.getName());

  // Default values
  private String dbUsername = "my_test_username";
  private String dbPassword = "my_test_password";
  private String dbName = "test";
  private String dbIdentifier = "test-identifier";
  private DatabaseEngineDeployment dbEngineDeployment;
  private String dbEngine = "aurora-postgresql";
  private String dbEngineVersion = "13.9";
  private String dbInstanceClass = "db.r5.large";
  private String storageType = "io1";
  private int allocatedStorage = 100;
  private int iops = 1000;
  private final Region dbRegion;
  private final String dbSecGroup = "default";
  private int numOfInstances = 5;
  private ArrayList<TestInstanceInfo> instances = new ArrayList<>();

  private final RdsClient rdsClient;
  private final Ec2Client ec2Client;
  private static final Random rand = new Random();

  private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

  /**
   * Initializes an AmazonRDS & AmazonEC2 client. RDS client used to create/destroy clusters & instances. EC2 client
   * used to add/remove IP from security group.
   */
  public RdsTestUtility() {
    this(Region.US_EAST_1, DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *               https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   */
  public RdsTestUtility(Region region) {
    this(region, DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *               https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   */
  public RdsTestUtility(String region) {
    this(getRegionInternal(region), DefaultCredentialsProvider.create());
  }

  public RdsTestUtility(
      String region, String awsAccessKeyId, String awsSecretAccessKey, String awsSessionToken) {

    this(
        getRegionInternal(region),
        StaticCredentialsProvider.create(
            StringUtils.isNullOrEmpty(awsSessionToken)
                ? AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey)
                : AwsSessionCredentials.create(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)));
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region              define AWS Regions, refer to
   *                            https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   * @param credentialsProvider Specific AWS credential provider
   */
  public RdsTestUtility(Region region, AwsCredentialsProvider credentialsProvider) {
    dbRegion = region;

    rdsClient =
        RdsClient.builder().region(dbRegion).credentialsProvider(credentialsProvider).build();

    ec2Client =
        Ec2Client.builder().region(dbRegion).credentialsProvider(credentialsProvider).build();
  }

  protected static Region getRegionInternal(String rdsRegion) {
    Optional<Region> regionOptional =
        Region.regions().stream().filter(r -> r.id().equalsIgnoreCase(rdsRegion)).findFirst();

    if (regionOptional.isPresent()) {
      return regionOptional.get();
    }
    throw new IllegalArgumentException(String.format("Unknown AWS region '%s'.", rdsRegion));
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @param username      Master username for access to database
   * @param password      Master password for access to database
   * @param dbName        Database name
   * @param identifier    Database cluster identifier
   * @param engine        Database engine to use, refer to
   *                      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Welcome.html
   * @param instanceClass instance class, refer to
   *                      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
   * @param version       the database engine's version
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster(
      String username,
      String password,
      String dbName,
      String identifier,
      DatabaseEngineDeployment deployment,
      String engine,
      String instanceClass,
      String version,
      int numOfInstances,
      ArrayList<TestInstanceInfo> instances) throws InterruptedException {
    this.dbUsername = username;
    this.dbPassword = password;
    this.dbName = dbName;
    this.dbIdentifier = identifier;
    this.dbEngineDeployment = deployment;
    this.dbEngine = engine;
    this.dbInstanceClass = instanceClass;
    this.dbEngineVersion = version;
    this.numOfInstances = numOfInstances;
    this.instances = instances;

    return createCluster();
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster() throws InterruptedException {
    // Create Cluster
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();
    CreateDbClusterRequest.Builder clusterBuilder =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(dbIdentifier)
            .databaseName(dbName)
            .masterUsername(dbUsername)
            .masterUserPassword(dbPassword)
            .sourceRegion(dbRegion.id())
            .engine(dbEngine)
            .engineVersion(dbEngineVersion)
            .storageEncrypted(true)
            .tags(testRunnerTag);

    if (DatabaseEngineDeployment.AURORA.equals(this.dbEngineDeployment)) {
      clusterBuilder = clusterBuilder.enableIAMDatabaseAuthentication(true);
    } else if (DatabaseEngineDeployment.MULTI_AZ.equals(this.dbEngineDeployment)) {
      clusterBuilder =
          clusterBuilder.allocatedStorage(allocatedStorage)
              .dbClusterInstanceClass(dbInstanceClass)
              .storageType(storageType)
              .iops(iops);
    }

    rdsClient.createDBCluster(clusterBuilder.build());

    // When creating an Aurora deployment, you need to create instances using CreateDbInstance requests.
    // For multi-AZ deployments, this is not the case - the instances are created automatically.
    if (DatabaseEngineDeployment.AURORA.equals(this.dbEngineDeployment)) {
      // Create Instances
      for (int i = 1; i <= numOfInstances; i++) {
        final String instanceName = dbIdentifier + "-" + i;
        rdsClient.createDBInstance(
            CreateDbInstanceRequest.builder()
                .dbClusterIdentifier(dbIdentifier)
                .dbInstanceIdentifier(instanceName)
                .dbInstanceClass(dbInstanceClass)
                .engine(dbEngine)
                .engineVersion(dbEngineVersion)
                .publiclyAccessible(true)
                .tags(testRunnerTag)
                .build());
      }
    }

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(30)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster();
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()));
    final String endpoint = dbInstancesResult.dbInstances().get(0).endpoint().address();
    final String clusterDomainPrefix = endpoint.substring(endpoint.indexOf('.') + 1);

    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      this.instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return clusterDomainPrefix;
  }

  /**
   * Gets public IP.
   *
   * @return public IP of user
   * @throws UnknownHostException when checkip host isn't available
   */
  public String getPublicIPAddress() throws UnknownHostException {
    String ip;
    try {
      URL ipChecker = new URL("http://checkip.amazonaws.com");
      BufferedReader reader = new BufferedReader(new InputStreamReader(ipChecker.openStream()));
      ip = reader.readLine();
    } catch (Exception e) {
      throw new UnknownHostException("Unable to get IP");
    }
    return ip;
  }

  /**
   * Authorizes IP to EC2 Security groups for RDS access.
   */
  public void ec2AuthorizeIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
      return;
    }

    if (ipExists(ipAddress)) {
      return;
    }

    try {
      ec2Client.authorizeSecurityGroupIngress(
          (builder) ->
              builder
                  .groupName(dbSecGroup)
                  .cidrIp(ipAddress + "/32")
                  .ipProtocol("-1") // All protocols
                  .fromPort(0) // For all ports
                  .toPort(65535));
    } catch (Ec2Exception exception) {
      if (!DUPLICATE_IP_ERROR_CODE.equalsIgnoreCase(exception.awsErrorDetails().errorCode())) {
        throw exception;
      }
    }
  }

  private boolean ipExists(String ipAddress) {
    final DescribeSecurityGroupsResponse response =
        ec2Client.describeSecurityGroups(
            (builder) ->
                builder
                    .groupNames(dbSecGroup)
                    .filters(
                        software.amazon.awssdk.services.ec2.model.Filter.builder()
                            .name("ip-permission.cidr")
                            .values(ipAddress + "/32")
                            .build()));

    return response != null && !response.securityGroups().isEmpty();
  }

  /**
   * De-authorizes IP from EC2 Security groups.
   */
  public void ec2DeauthorizesIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
      return;
    }
    try {
      ec2Client.revokeSecurityGroupIngress(
          (builder) ->
              builder
                  .groupName(dbSecGroup)
                  .cidrIp(ipAddress + "/32")
                  .ipProtocol("-1") // All protocols
                  .fromPort(0) // For all ports
                  .toPort(65535));
    } catch (Ec2Exception exception) {
      // Ignore
    }
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   *
   * @param identifier database identifier to delete
   */
  public void deleteCluster(String identifier) {
    dbIdentifier = identifier;
    deleteCluster();
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   */
  public void deleteCluster() {
    // deleteDBinstance requests are not necessary to delete a multi-az cluster.
    if (!this.dbEngineDeployment.equals(DatabaseEngineDeployment.MULTI_AZ)) {
      for (int i = 1; i <= numOfInstances; i++) {
        try {
          rdsClient.deleteDBInstance(
              DeleteDbInstanceRequest.builder()
                  .dbInstanceIdentifier(dbIdentifier + "-" + i)
                  .skipFinalSnapshot(true)
                  .build());
        } catch (Exception ex) {
          LOGGER.finest("Error deleting instance " + dbIdentifier + "-" + i + ". " + ex.getMessage());
          // Ignore this error and continue with other instances
        }
      }
    }

    // Tear down cluster
    rdsClient.deleteDBCluster(
        (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(dbIdentifier)));
  }

  public boolean doesClusterExist(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    try {
      rdsClient.describeDBClusters(request);
    } catch (DbClusterNotFoundException ex) {
      return false;
    }
    return true;
  }

  public DBCluster getClusterInfo(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    final DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
    if (!response.hasDbClusters()) {
      throw new RuntimeException("Cluster " + clusterId + " not found.");
    }

    return response.dbClusters().get(0);
  }

  public DatabaseEngine getClusterEngine(final DBCluster cluster) {
    switch (cluster.engine()) {
      case "aurora-postgresql":
      case "postgres":
        return DatabaseEngine.PG;
      case "aurora-mysql":
      case "mysql":
        return DatabaseEngine.MYSQL;
      default:
        throw new UnsupportedOperationException(cluster.engine());
    }
  }

  public List<TestInstanceInfo> getClusterInstanceIds(final String clusterId) {
    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(Filter.builder().name("db-cluster-id").values(clusterId).build()));

    List<TestInstanceInfo> result = new ArrayList<>();
    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      result.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }
    return result;
  }

  public void addAuroraAwsIamUser(TestEnvironmentInfo info) throws SQLException {
    DatabaseEngine dbEngine = info.getRequest().getDatabaseEngine();
    String dbUser = info.getIamUsername();
    String dbName = info.getDatabaseInfo().getDefaultDbName();

    try (final Connection conn = DriverHelper.getDriverConnection(info);
         final Statement stmt = conn.createStatement()) {

      switch (dbEngine) {
        case MYSQL:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute(
              "CREATE USER " + dbUser + " IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';");
          stmt.execute("GRANT ALL PRIVILEGES ON " + dbName + ".* TO '" + dbUser + "'@'%';");
          break;
        case PG:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute("CREATE USER " + dbUser + ";");
          stmt.execute("GRANT rds_iam TO " + dbUser + ";");
          stmt.execute("GRANT ALL PRIVILEGES ON DATABASE " + dbName + " TO " + dbUser + ";");
          break;
        default:
          throw new UnsupportedOperationException(dbEngine.toString());
      }
    }
  }
}
