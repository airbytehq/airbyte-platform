plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

configurations {
  create("jdbc")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.kotlin.logging)

  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(libs.jooq)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
}

airbyte {
  application {
    name = "airbyte-metrics-reporter"
    mainClass = "io.airbyte.metrics.reporter.ApplicationKt"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
  }
  docker {
    imageName = "metrics-reporter"
  }
}
