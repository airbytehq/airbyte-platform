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
  implementation(libs.bundles.flyway)
  implementation(libs.jooq)
  implementation(libs.guava)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-persistence:job-persistence"))

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
