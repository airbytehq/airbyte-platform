plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.micronaut.http)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.jackson.kotlin)
  implementation(libs.kotlin.logging)
  implementation(libs.slf4j.api)

  implementation(project(":oss:airbyte-api:server-api-client"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.kotlin.reflect)
  runtimeOnly(libs.bundles.logback)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
}

airbyte {
  application {
    mainClass = "io.airbyte.directjobrunner.ApplicationKt"
    defaultJvmArgs = listOf("-XX:MaxRAMPercentage=75.0")

    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_VERSION" to "dev",
        "MICRONAUT_ENVIRONMENTS" to "test",
      ),
    )
  }
  docker {
    imageName = "direct-job-runner"
  }
}
