plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.openapi)

  implementation(libs.bundles.datadog)
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.temporal)
  implementation(libs.bundles.temporal.telemetry)
  implementation(libs.failsafe)
  implementation(libs.failsafe.okhttp)
  implementation(libs.kubernetes.client)
  implementation(libs.kubernetes.httpclient.okhttp)
  implementation(libs.google.cloud.storage)
  implementation(libs.guava)
  implementation(libs.kotlin.logging)
  implementation(libs.micronaut.cache.caffeine)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.micronaut.jooq)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.okhttp)
  implementation(libs.reactor.core)
  implementation(libs.reactor.kotlin.extensions)
  implementation(libs.slf4j.api)
  implementation(libs.bundles.micronaut.metrics)
  implementation(platform(libs.micronaut.platform))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-commons-with-dependencies"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-micronaut-temporal"))
  implementation(project(":oss:airbyte-worker-models"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.kotlin.reflect)
  runtimeOnly(libs.bundles.bouncycastle)
  runtimeOnly(libs.bundles.logback)

  kspTest((platform(libs.micronaut.platform)))
  kspTest(libs.bundles.micronaut.test.annotation.processor)
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(project(":oss:airbyte-json-validation"))
  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.apache.commons.lang)
  testImplementation(libs.testcontainers.vault)
  testImplementation(libs.jakarta.ws.rs.api)
}

airbyte {
  application {
    mainClass.set("io.airbyte.workload.launcher.ApplicationKt")
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_VERSION" to "dev",
        "DATA_PLANE_ID" to "local",
        "MICRONAUT_ENVIRONMENTS" to "test"
      )
    )
  }
  docker {
    imageName.set("workload-launcher")
  }
}
