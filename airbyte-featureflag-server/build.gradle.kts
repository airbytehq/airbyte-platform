plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
}

dependencies {
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.v3.swagger.annotations)
  ksp(platform(libs.micronaut.platform))

  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  annotationProcessor(libs.micronaut.jaxrs.processor)
  annotationProcessor(platform(libs.micronaut.platform))

  compileOnly(libs.v3.swagger.annotations)
  compileOnly(libs.micronaut.openapi.annotations)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.micronaut.http)
  implementation(libs.micronaut.security)
  implementation(libs.v3.swagger.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.dataformat.yml)
  implementation(libs.jackson.kotlin)
  implementation(libs.kotlin.logging)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-micronaut"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
}

airbyte {
  application {
    mainClass = "io.airbyte.featureflag.server.ApplicationKt"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "MICRONAUT_ENVIRONMENTS" to "control-plane",
        "SERVICE_NAME" to project.name,
        "TRACKING_STRATEGY" to "logging",
      ),
    )
  }
  docker {
    imageName = "featureflag-server"
  }
}
