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

import com.mysql.cj.conf.PropertyKey;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.PGProperty;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

public class DriverHelper {

  private static final Logger LOGGER = Logger.getLogger(DriverHelper.class.getName());

  public static String getDriverProtocol(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return "jdbc:mysql://";
      case PG:
        return "jdbc:postgresql://";
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static Connection getDriverConnection(TestEnvironmentInfo info) throws SQLException {
    final String url =
        String.format(
            "%s%s:%d/%s",
            DriverHelper.getDriverProtocol(info.getRequest().getDatabaseEngine()),
            info.getDatabaseInfo().getClusterEndpoint(),
            info.getDatabaseInfo().getClusterEndpointPort(),
            info.getDatabaseInfo().getDefaultDbName());
    return DriverManager.getConnection(url, info.getDatabaseInfo().getUsername(), info.getDatabaseInfo().getPassword());
  }

  public static String getDriverProtocol(DatabaseEngine databaseEngine, TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "jdbc:mysql://";
      case PG:
        return "jdbc:postgresql://";
      default:
        throw new NotImplementedException(testDriver.toString());
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

  public static String getWrapperDriverProtocol(
      DatabaseEngine databaseEngine, TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "jdbc:aws-wrapper:mysql://";
      case PG:
        return "jdbc:aws-wrapper:postgresql://";
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static String getDriverClassname(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return getDriverClassname(TestDriver.MYSQL);
      case PG:
        return getDriverClassname(TestDriver.PG);
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static String getDriverClassname(TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "com.mysql.cj.jdbc.Driver";
      case PG:
        return "org.postgresql.Driver";
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static String getHostnameSql(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return "SELECT @@hostname";
      case PG:
        return "SELECT inet_server_addr()";
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static void setConnectTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            PropertyKey.connectTimeout.getKeyName(), String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            PGProperty.CONNECT_TIMEOUT.getName(), String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static void setSocketTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            PropertyKey.socketTimeout.getKeyName(), String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            PGProperty.SOCKET_TIMEOUT.getName(), String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static void setTcpKeepAlive(TestDriver testDriver, Properties props, boolean enabled) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), String.valueOf(enabled));
        break;
      case PG:
        props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), String.valueOf(enabled));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static void setMonitoringConnectTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            "monitoring-" + PropertyKey.connectTimeout.getKeyName(),
            String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            "monitoring-" + PGProperty.CONNECT_TIMEOUT.getName(),
            String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static void setMonitoringSocketTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            "monitoring-" + PropertyKey.socketTimeout.getKeyName(),
            String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            "monitoring-" + PGProperty.SOCKET_TIMEOUT.getName(),
            String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }
}
