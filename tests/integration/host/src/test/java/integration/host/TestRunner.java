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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestEnvironmentProvider.class)
public class TestRunner {
  @BeforeAll
  public static void deleteOldReports() {
    File reportFolder = new File("../container/reports");
    if (reportFolder.exists()) {
      deleteFolder(reportFolder);
    }
  }

   @AfterAll
   public static void cleanup() throws IOException, InterruptedException {
     System.out.println("Inside integration test cleanup");

     String currentPath = new java.io.File(".").getCanonicalPath();
     System.out.println("Current dir:" + currentPath);

     File reportFolder = new File("../container/reports");
     System.out.println("Report folder exists: " + reportFolder.exists());

     if (reportFolder.exists()) {
       File mergeReportsScript = new File("./scripts/merge_reports.py");
       ProcessBuilder processBuilder = new ProcessBuilder("python", mergeReportsScript.getAbsolutePath());
       Process process = processBuilder.start();
       int exitCode = process.waitFor();

       System.out.println("Exit code: " + exitCode);
       assertEquals(0, exitCode, "An error occurred while attempting to run merge_reports.py");

       // Delete individual reports in favor of consolidated report
       File[] files = reportFolder.listFiles();
       if (files != null) {
         for (File f: files) {
           if (!f.getName().equals("integration_tests.html") && !f.getName().equals("assets")) {
             f.delete();
           }
         }
       }
       reportFolder.delete();
     }
   }

  @TestTemplate
  public void runTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig config = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      config.runTests("./tests/integration/container");
    }
  }

  @TestTemplate
  public void debugTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    if (System.getenv("DEBUG_ENV") == null) {
      throw new RuntimeException("Environment variable 'DEBUG_ENV' is required to debug the integration tests. " +
                                 "Please set 'DEBUG_ENV' to 'PYCHARM' or 'VSCODE'.");
    }

    try (final TestEnvironmentConfig config = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      config.debugTests("./tests/integration/container");
    }
  }

  private static void deleteFolder(File folder) {
    File[] files = folder.listFiles();
    if (files != null) {
      for (File f: files) {
        if(f.isDirectory()) {
              deleteFolder(f);
          } else {
              f.delete();
          }
      }
    }
    folder.delete();
  }
}
