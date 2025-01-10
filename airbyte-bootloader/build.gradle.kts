plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.flyway)
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.jooq)
  implementation(libs.guava)
  implementation(libs.apache.commons.lang)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-persistence:job-persistence"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.jupiter.system.stubs)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

airbyte {
  application {
    mainClass = "io.airbyte.bootloader.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "DATABASE_URL" to "jdbc:postgresql://localhost:5432/airbyte",
      ),
    )
  }

  docker {
    imageName = "bootloader"
  }
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
