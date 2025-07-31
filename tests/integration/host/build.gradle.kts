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
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.4")
    testImplementation("org.mockito:mockito-inline:4.8.0")
    testImplementation("software.amazon.awssdk:rds:2.20.49")
    testImplementation("software.amazon.awssdk:ec2:2.20.61")
    testImplementation("software.amazon.awssdk:secretsmanager:2.20.49")
    testImplementation("software.amazon.awssdk:dsql:2.29.34")
    // Note: all org.testcontainers dependencies should have the same version
    testImplementation("org.testcontainers:testcontainers:1.21.2")
    testImplementation("org.testcontainers:mysql:1.21.2")
    testImplementation("org.testcontainers:postgresql:1.21.2")
    testImplementation("org.testcontainers:junit-jupiter:1.21.2")
    testImplementation("org.testcontainers:toxiproxy:1.21.2")
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

tasks.register<Test>("test-python-3.11-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
    }
}

tasks.register<Test>("test-python-3.8-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-311", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
    }
}

tasks.register<Test>("test-python-3.11-pg") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
    }
}

tasks.register<Test>("test-python-3.8-pg") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-311", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
    }
}

tasks.register<Test>("test-python-3.11-dsql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-dsql", "false")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-autoscaling", "true")
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-python-3.8-dsql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-dsql", "false")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-python-311", "true")
        systemProperty("exclude-autoscaling", "true")
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
    }
}

tasks.register<Test>("test-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
    }
}

tasks.register<Test>("test-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
    }
}

tasks.register<Test>("test-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
    }
}

tasks.register<Test>("test-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-pg-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-mysql-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-autoscaling") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-autoscaling", "true")
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-pg-aurora-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-iam", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
    }
}

tasks.register<Test>("test-mysql-aurora-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-iam", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
    }
}

tasks.register<Test>("test-all-dsql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-dsql", "false")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-autoscaling", "true")
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("test-bgd-mysql-instance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-2", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

tasks.register<Test>("test-bgd-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-1", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

tasks.register<Test>("test-bgd-pg-instance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-2", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

tasks.register<Test>("test-bgd-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-1", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

// Debug

tasks.register<Test>("debug-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("debug-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
    }
}

tasks.register<Test>("debug-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
    }
}

tasks.register<Test>("debug-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
    }
}

tasks.register<Test>("debug-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
    }
}

tasks.register<Test>("debug-autoscaling") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-autoscaling", "true")
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
    }
}

tasks.register<Test>("debug-pg-aurora-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-iam", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
    }
}

tasks.register<Test>("debug-mysql-aurora-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-iam", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
    }
}

tasks.register<Test>("debug-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("debug-pg-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("debug-mysql-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("debug-all-dsql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-autoscaling", "true")
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-bg", "true")
    }
}

tasks.register<Test>("debug-bgd-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-1", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

tasks.register<Test>("debug-bgd-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-1", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

tasks.register<Test>("debug-bgd-mysql-instance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-2", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}

tasks.register<Test>("debug-bgd-pg-instance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-python-38", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-secrets-manager", "true")
        systemProperty("exclude-instances-2", "true")
        systemProperty("exclude-instances-3", "true")
        systemProperty("exclude-instances-5", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("test-bg-only", "true")
    }
}
