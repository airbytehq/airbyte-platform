plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
}

dependencies {
  ksp(libs.v3.swagger.annotations)
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.openapi)

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
  implementation(libs.jakarta.transaction.api)
  implementation(libs.micronaut.jaxrs.server)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.micronaut.security)
  implementation(libs.micronaut.security.jwt)
  implementation(libs.okhttp)
  implementation(libs.v3.swagger.annotations)
  implementation(libs.reactor.core)
  implementation(libs.kotlin.logging)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.bundles.datadog)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-workload"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))

  compileOnly(libs.v3.swagger.annotations)
  compileOnly(libs.micronaut.openapi.annotations)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.javax.databind)
  runtimeOnly(libs.bundles.logback)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.reactor.test)
  testImplementation(libs.mockk)
}

airbyte {
  application {
    mainClass = "io.airbyte.workload.server.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "MICRONAUT_ENVIRONMENTS" to "control-plane",
        "SERVICE_NAME" to project.name,
        "TRACKING_STRATEGY" to "logging",
        "WORKLOAD_API_BEARER_TOKEN" to "ItsASecret",
      ),
    )
  }
  docker {
    imageName = "workload-api-server"
  }
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "AIRBYTE_VERSION" to "dev",
      "MICRONAUT_ENVIRONMENTS" to "test",
      "SERVICE_NAME" to project.name,
    ),
  )
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs = listOf("-parameters")
}

// Even though Kotlin is excluded on Spotbugs, this project still runs into SpotBugs issues.
// The working theory is that generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}
