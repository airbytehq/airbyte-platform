import java.util.Properties

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
  kotlin("kapt")
  kotlin("jvm")
}

dependencies {
  kapt(platform(libs.micronaut.platform))
  kapt(libs.bundles.micronaut.annotation.processor)
  kapt(libs.micronaut.openapi)

  implementation(libs.bundles.datadog)
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.bundles.log4j)
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
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.micronaut.jooq)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.okhttp)
  implementation(libs.reactor.core)
  implementation(libs.reactor.kotlin.extensions)
  implementation(libs.slf4j.api)
  implementation(libs.bundles.micronaut.metrics)
  implementation(platform(libs.micronaut.platform))
  implementation(project(":airbyte-api"))
  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-micronaut"))
  implementation(project(":airbyte-commons-temporal"))
  implementation(project(":airbyte-commons-temporal-core"))
  implementation(project(":airbyte-commons-with-dependencies"))
  implementation(project(":airbyte-commons-worker"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-config:config-secrets"))
  implementation(project(":airbyte-data"))
  implementation(project(":airbyte-featureflag"))
  implementation(project(":airbyte-metrics:metrics-lib"))
  implementation(project(":airbyte-micronaut-temporal"))
  implementation(project(":airbyte-worker-models"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.kotlin.reflect)
  runtimeOnly(libs.appender.log4j2)
  runtimeOnly(libs.bundles.bouncycastle)

  // Required for secret hydration in OSS
  runtimeOnly(libs.hikaricp)
  runtimeOnly(libs.h2.database)

  kaptTest((platform(libs.micronaut.platform)))
  kaptTest(libs.bundles.micronaut.test.annotation.processor)
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(project(":airbyte-json-validation"))
  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.apache.commons.lang)
  testImplementation(libs.testcontainers.vault)
}

val env = Properties().apply {
  load(rootProject.file(".env.dev").inputStream())
}

airbyte {
  application {
    mainClass.set("io.airbyte.workload.launcher.ApplicationKt")
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    @Suppress("UNCHECKED_CAST")
    localEnvVars.putAll(env.toMutableMap() as Map<String, String>)
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_VERSION" to env["VERSION"].toString(),
        "DATA_PLANE_ID" to "local",
        "MICRONAUT_ENVIRONMENTS" to "test"
      )
    )
  }
  docker {
    imageName.set("workload-launcher")
  }
}

// This is a workaround related to kaptBuild errors. It seems to be because there are no tests in cloud-airbyte-api-server.
// TODO: this should be removed when we move to kotlin 1.9.20
// TODO: we should write tests
afterEvaluate {
  tasks.named("kaptGenerateStubsTestKotlin") {
    enabled = false
  }
}