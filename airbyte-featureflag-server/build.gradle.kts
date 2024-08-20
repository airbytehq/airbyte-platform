import java.util.Properties

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
  implementation(libs.log4j.impl)
  implementation(libs.micronaut.http)
  implementation(libs.micronaut.jaxrs.server)
  implementation(libs.micronaut.security)
  implementation(libs.v3.swagger.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.dataformat)
  implementation(libs.jackson.kotlin)

  implementation(project(":oss:airbyte-commons"))

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
}

val env = Properties().apply {
  load(rootProject.file(".env.dev").inputStream())
}

airbyte {
  application {
    mainClass = "io.airbyte.featureflag.server.ApplicationKt"
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
      )
    )
  }
  docker {
    imageName = "featureflag-server"
  }
}
