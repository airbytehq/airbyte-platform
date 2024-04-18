import java.util.Properties

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

configurations.all {
  resolutionStrategy {
    force(libs.flyway.core, libs.jooq)
  }
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.flyway)
  implementation(libs.jooq)
  implementation(libs.guava)

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-micronaut"))
  implementation(project(":airbyte-config:init"))
  implementation(project(":airbyte-config:specs"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-config:config-persistence"))
  implementation(project(":airbyte-config:config-secrets"))
  implementation(project(":airbyte-data"))
  implementation(project(":airbyte-db:db-lib"))
  implementation(project(":airbyte-metrics:metrics-lib"))
  implementation(project(":airbyte-json-validation"))
  implementation(project(":airbyte-featureflag"))
  implementation(libs.airbyte.protocol)
  implementation(project(":airbyte-persistence:job-persistence"))

  runtimeOnly(libs.snakeyaml)

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)    // Lombok must be added BEFORE Micronaut
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.jupiter.system.stubs)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

val env = Properties().apply {
  load(rootProject.file(".env.dev").inputStream())
}

airbyte {
  application {
    mainClass = "io.airbyte.bootloader.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    @Suppress("UNCHECKED_CAST")
    localEnvVars.putAll(env.toMutableMap() as Map<String, String>)
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to (System.getenv("AIRBYTE_ROLE") ?: "undefined"),
        "AIRBYTE_VERSION" to env["VERSION"].toString(),
        "DATABASE_URL" to "jdbc:postgresql://localhost:5432/airbyte",
      )
    )
  }

  docker {
    imageName = "bootloader"
  }
}
