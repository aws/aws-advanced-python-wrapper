import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    id("java")
}

group = "software.amazon.python.integration.tests"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.checkerframework:checker-qual:3.26.0")
    testImplementation("org.junit.platform:junit-platform-commons:1.9.0")
    testImplementation("org.junit.platform:junit-platform-engine:1.9.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.9.0")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.1")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.9.0")
    testImplementation("org.postgresql:postgresql:42.5.0")
    testImplementation("mysql:mysql-connector-java:8.0.31")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.1.0")
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.4")
    testImplementation("org.mockito:mockito-inline:4.8.0")
    testImplementation("software.amazon.awssdk:rds:2.20.49")
    testImplementation("software.amazon.awssdk:ec2:2.20.61")
    testImplementation("software.amazon.awssdk:secretsmanager:2.20.49")
    testImplementation("org.testcontainers:testcontainers:1.17.4")
    testImplementation("org.testcontainers:mysql:1.18.0")
    testImplementation("org.testcontainers:postgresql:1.17.5")
    testImplementation("org.testcontainers:mariadb:1.17.3")
    testImplementation("org.testcontainers:junit-jupiter:1.17.4")
    testImplementation("org.testcontainers:toxiproxy:1.17.5")
    testImplementation("org.apache.poi:poi-ooxml:5.2.2")
    testImplementation("org.slf4j:slf4j-simple:2.0.3")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
}

tasks.test {
    filter.excludeTestsMatching("integration.*")
}

tasks.withType<Test> {
    useJUnitPlatform()
    outputs.upToDateWhen { false }
    testLogging {
        events(PASSED, FAILED, SKIPPED)
        showStandardStreams = true
        exceptionFormat = FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    reports.junitXml.required.set(true)
    reports.html.required.set(false)

    systemProperty("java.util.logging.config.file", "${project.buildDir}/resources/test/logging-test.properties")
}

tasks.register<Test>("test-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-performance", "true")

        // TODO: Temporary disable Mysql and MariaDb tests. Uncomment when the driver supports them.
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")

        systemProperty("test-no-python-38", "true")
    }
}

tasks.register<Test>("test-all-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-performance", "true")

        // TODO: Temporary disable Mysql and MariaDb tests. Uncomment when the driver supports them.
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")

        systemProperty("test-no-python-38", "true")
    }
}

tasks.register<Test>("test-all-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")

        // TODO: Temporary disable Mysql and MariaDb tests. Uncomment when the driver supports them.
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")

        systemProperty("test-no-python-38", "true")
    }
}

tasks.register<Test>("test-all-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
    }
}

tasks.register<Test>("test-all-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
    }
}

// Debug

tasks.register<Test>("debug-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-performance", "true")

        // TODO: Temporary disable Mysql and MariaDb tests. Uncomment when the driver supports them.
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")

        systemProperty("test-no-python-38", "true")
    }
}

tasks.register<Test>("debug-all-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-performance", "true")

        // TODO: Temporary disable Mysql and MariaDb tests. Uncomment when the driver supports them.
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")

        systemProperty("test-no-python-38", "true")
    }
}

tasks.register<Test>("debug-all-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")

        // TODO: Temporary disable Mysql and MariaDb tests. Uncomment when the driver supports them.
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")

        systemProperty("test-no-python-38", "true")
    }
}

tasks.register<Test>("debug-all-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
    }
}

tasks.register<Test>("debug-all-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
    }
}
