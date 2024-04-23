import java.util.Properties

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.keycloak.client)

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-auth"))
  implementation(project(":airbyte-commons-micronaut"))
  implementation(project(":airbyte-commons-micronaut-security"))
  implementation(project(":airbyte-data"))
  implementation(project(":airbyte-db:db-lib"))
  implementation(project(":airbyte-db:jooq"))

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.jupiter.system.stubs)

  testImplementation(project(":airbyte-test-utils"))
}

val env = Properties().apply {
  load(rootProject.file(".env.dev").inputStream())
}
airbyte {
  application {
    mainClass = "io.airbyte.keycloak.setup.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
  }
  docker {
    imageName = "keycloak-setup"
  }
}
