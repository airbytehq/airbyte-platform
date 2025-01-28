plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.kotlin.logging)
  implementation(libs.temporal.sdk)
  implementation(libs.airbyte.protocol)

  implementation(project(mapOf("path" to ":oss:airbyte-commons-temporal")))
  implementation(libs.okhttp)
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-connector-rollout-shared"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)

  testImplementation(libs.temporal.testing)
  testImplementation(libs.mockk)
  testImplementation(libs.mockito.inline)
}

airbyte {
  application {
    mainClass = "io.airbyte.connector.rollout.worker.ConnectorRolloutWorkerApplication"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "DATA_PLANE_ID" to "local",
        "MICRONAUT_ENVIRONMENTS" to "test",
      ),
    )
  }
  docker {
    imageName = "connector-rollout-worker"
  }
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
