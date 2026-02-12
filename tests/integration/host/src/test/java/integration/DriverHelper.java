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

package integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public class DriverHelper {

  private static final Logger LOGGER = Logger.getLogger(DriverHelper.class.getName());

  public static String getDriverProtocol(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return "jdbc:mysql://";
      case PG:
        return "jdbc:postgresql://";
      default:
        throw new UnsupportedOperationException(databaseEngine.toString());
    }
  }

  public static String getDriverClassname(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return getDriverClassname(TestDriver.MYSQL);
      case PG:
        return getDriverClassname(TestDriver.PG);
      default:
        throw new UnsupportedOperationException(databaseEngine.toString());
    }
  }

  public static String getDriverClassname(TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "com.mysql.cj.jdbc.Driver";
      case PG:
        return "org.postgresql.Driver";
      default:
        throw new UnsupportedOperationException(testDriver.toString());
    }
  }

  public static void registerDriver(DatabaseEngine engine) {
    try {
      Class.forName(DriverHelper.getDriverClassname(engine));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Driver not found: "
              + DriverHelper.getDriverClassname(engine),
          e);
    }
  }

  public static Connection getDriverConnection(TestEnvironmentInfo info) throws SQLException {
    String url;
    switch (info.getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
      case RDS_MULTI_AZ_CLUSTER:
        url = String.format(
            "%s%s:%d/%s",
            DriverHelper.getDriverProtocol(info.getRequest().getDatabaseEngine()),
            info.getDatabaseInfo().getClusterEndpoint(),
            info.getDatabaseInfo().getClusterEndpointPort(),
            info.getDatabaseInfo().getDefaultDbName());
        break;
      case RDS_MULTI_AZ_INSTANCE:
        url = String.format(
            "%s%s:%d/%s",
            DriverHelper.getDriverProtocol(info.getRequest().getDatabaseEngine()),
            info.getDatabaseInfo().getInstances().get(0).getHost(),
            info.getDatabaseInfo().getInstances().get(0).getPort(),
            info.getDatabaseInfo().getDefaultDbName());
        break;
      default:
        throw new UnsupportedOperationException(info.getRequest().getDatabaseEngineDeployment().toString());
    }
    return DriverManager.getConnection(url, info.getDatabaseInfo().getUsername(), info.getDatabaseInfo().getPassword());
  }
}
