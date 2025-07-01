plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
}

configurations.all {
  exclude(group = "io.micronaut", module = "micronaut-http-server-netty")
  exclude(group = "io.micronaut.openapi")
  exclude(group = "io.micronaut.flyway")
  exclude(group = "io.micronaut.http")
  exclude(group = "io.micronaut.jaxrs")
  exclude(group = "io.micronaut.email")
  exclude(group = "io.micronaut.validation")
  exclude(group = "io.micronaut.cache")
  exclude(group = "io.micronaut.reactor")
  exclude(group = "io.micronaut.kotlin")
  exclude(group = "io.micronaut.acme")
  exclude(group = "io.micronaut.aws")
  exclude(group = "io.micronaut.azure")
  exclude(group = "io.micronaut.cassandra")
  exclude(group = "io.micronaut.chatbots")
  exclude(group = "io.micronaut.coherence")
  exclude(group = "io.micronaut.controlpanel")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.kotlin.logging)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.slf4j.api)
  implementation(libs.micronaut.jooq)
  implementation("org.apache.commons:commons-compress:1.27.1")

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-commons-storage"))

  runtimeOnly(libs.snakeyaml)
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
    mainClass.set("io.airbyte.asyncProfiler.ApplicationKt")
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
    imageName.set("async-profiler")
  }
}

