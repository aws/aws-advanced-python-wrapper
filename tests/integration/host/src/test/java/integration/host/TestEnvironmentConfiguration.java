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

import integration.DebugEnv;

public class TestEnvironmentConfiguration {

  public boolean excludeDocker =
      Boolean.parseBoolean(System.getProperty("exclude-docker", "false"));
  public boolean excludeAurora =
      Boolean.parseBoolean(System.getProperty("exclude-aurora", "false"));
  public boolean excludeMultiAz =
      Boolean.parseBoolean(System.getProperty("exclude-multi-az", "false"));
  public boolean excludePerformance =
      Boolean.parseBoolean(System.getProperty("exclude-performance", "false"));
  public boolean excludeMysqlEngine =
      Boolean.parseBoolean(System.getProperty("exclude-mysql-engine", "false"));
  public boolean excludeMysqlDriver =
      Boolean.parseBoolean(System.getProperty("exclude-mysql-driver", "false"));
  public boolean excludePgEngine =
      Boolean.parseBoolean(System.getProperty("exclude-pg-engine", "false"));
  public boolean excludePgDriver =
      Boolean.parseBoolean(System.getProperty("exclude-pg-driver", "false"));
  public boolean excludeFailover =
      Boolean.parseBoolean(System.getProperty("exclude-failover", "false"));
  public boolean excludeIam =
      Boolean.parseBoolean(System.getProperty("exclude-iam", "false"));
  public boolean excludeSecretsManager =
      Boolean.parseBoolean(System.getProperty("exclude-secrets-manager", "false"));
  public boolean testAutoscalingOnly =
      Boolean.parseBoolean(System.getProperty("test-autoscaling", "false"));
  public boolean excludeDsql =
      Boolean.parseBoolean(System.getProperty("exclude-dsql", "false"));

  public boolean excludeInstances1 =
      Boolean.parseBoolean(System.getProperty("exclude-instances-1", "false"));
  public boolean excludeInstances2 =
      Boolean.parseBoolean(System.getProperty("exclude-instances-2", "false"));
  public boolean excludeInstances3 =
      Boolean.parseBoolean(System.getProperty("exclude-instances-3", "false"));
  public boolean excludeInstances5 =
      Boolean.parseBoolean(System.getProperty("exclude-instances-5", "false"));

  public boolean excludeTracesTelemetry =
      Boolean.parseBoolean(System.getProperty("exclude-traces-telemetry", "false"));
  public boolean excludeMetricsTelemetry =
      Boolean.parseBoolean(System.getProperty("exclude-metrics-telemetry", "false"));

  public boolean excludePython38 =
      Boolean.parseBoolean(System.getProperty("exclude-python-38", "false"));
  public boolean excludePython311 =
      Boolean.parseBoolean(System.getProperty("exclude-python-311", "false"));

  public String testFilter = System.getProperty("FILTER");

  public String rdsDbRegion = System.getenv("RDS_DB_REGION");

  public boolean reuseRdsCluster = Boolean.parseBoolean(System.getenv("REUSE_RDS_CLUSTER"));
  public String rdsClusterName = System.getenv("RDS_CLUSTER_NAME"); // "cluster-mysql"
  public String rdsClusterDomain =
      System.getenv("RDS_CLUSTER_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"
  public String rdsEndpoint =
          System.getenv("RDS_ENDPOINT"); // "https://rds-int.amazon.com"

  // Expected values: "latest", "lts", or engine version, for example, "15.4"
  // If left as empty, will use LTS version
  public String auroraMySqlDbEngineVersion = System.getenv("AURORA_MYSQL_DB_ENGINE_VERSION");
  public String auroraPgDbEngineVersion = System.getenv("AURORA_PG_ENGINE_VERSION");

  public String dbName = System.getenv("DB_DATABASE_NAME");
  public String dbUsername = System.getenv("DB_USERNAME");
  public String dbPassword = System.getenv("DB_PASSWORD");

  public String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
  public String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
  public String awsSessionToken = System.getenv("AWS_SESSION_TOKEN");

  public String iamUser = System.getenv("IAM_USER");

  public DebugEnv debugEnv = DebugEnv.fromEnv();
}
