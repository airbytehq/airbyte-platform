import java.util.Properties

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("org.jetbrains.kotlin.jvm")
  id("org.jetbrains.kotlin.kapt")
  id("io.airbyte.gradle.docker")
}

dependencies {
  kapt(libs.v3.swagger.annotations)
  kapt(platform(libs.micronaut.platform))
  kapt(libs.bundles.micronaut.annotation.processor)
  kapt(libs.micronaut.openapi)

  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  annotationProcessor(libs.micronaut.jaxrs.processor)
  annotationProcessor(libs.micronaut.openapi)

  implementation(libs.bundles.jackson)
  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.micronaut.http)
  implementation(libs.failsafe.okhttp)
  implementation(libs.jakarta.transaction.api)
  implementation(libs.bundles.temporal)
  implementation(libs.bundles.temporal.telemetry)
  implementation(libs.log4j.impl)
  implementation(libs.micronaut.jaxrs.server)
  implementation(libs.micronaut.security)
  implementation(libs.okhttp)
  implementation(libs.v3.swagger.annotations)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.reactor.core)
  implementation(libs.kotlin.logging)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.bundles.datadog)
  implementation(libs.jsoup)

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-temporal-core"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-featureflag"))
  implementation(project(":airbyte-metrics:metrics-lib"))

  compileOnly(libs.v3.swagger.annotations)
  compileOnly(libs.micronaut.openapi.annotations)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.javax.databind)

  kaptTest(platform(libs.micronaut.platform))
  kaptTest(libs.bundles.micronaut.test.annotation.processor)
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(project(":airbyte-test-utils"))
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.reactor.test)
  testImplementation(libs.mockk)
}

kapt {
  correctErrorTypes = true
  arguments {
    arg("micronaut.openapi.project.dir", projectDir.toString())
  }
}

val env = Properties().apply {
  load(rootProject.file(".env.dev").inputStream())
}

airbyte {
  application {
    mainClass = "io.airbyte.workload.server.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    @Suppress("UNCHECKED_CAST")
    localEnvVars.putAll(env.toMap() as Map<String, String>)
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to (System.getenv("AIRBYTE_ROLE") ?: "undefined"),
        "AIRBYTE_VERSION" to env["VERSION"].toString(),
        "MICRONAUT_ENVIRONMENTS" to "control-plane",
        "SERVICE_NAME" to project.name,
        "TRACKING_STRATEGY" to env["TRACKING_STRATEGY"].toString(),
        "WORKLOAD_API_BEARER_TOKEN" to "ItsASecret",
      )
    )
  }
  docker {
    imageName = "workload-api-server"
  }
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "AIRBYTE_VERSION" to env["VERSION"],
      "MICRONAUT_ENVIRONMENTS" to "test",
      "SERVICE_NAME" to project.name,
    )
  )
}

tasks.withType(JavaCompile::class).configureEach {
  options.compilerArgs = listOf("-parameters")
}

// Even though Kotlin is excluded on Spotbugs, this projects)
// still runs into spotbug issues. Working theory is that)
// generated code is being picked up. Disable as a short-term fix.)
tasks.named("spotbugsMain") {
  enabled = false
}
