import de.undercouch.gradle.tasks.download.Download

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  id("de.undercouch.download") version "5.4.0"
}

dependencies {
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-json-validation"))

  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)
  implementation(libs.kotlin.logging)
  implementation(libs.google.cloud.storage)
  implementation(libs.micronaut.cache.caffeine)
  implementation(libs.airbyte.protocol)
  implementation(libs.okhttp)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.junit.pioneer)
}

airbyte {
  spotless {
    excludes =
      listOf(
        "src/main/resources/seed/oss_registry.json",
        "src/main/resources/seed/local_oss_registry.json",
      )
  }
}

tasks.register<Download>("downloadConnectorRegistry") {
  src("https://connectors.airbyte.com/files/registries/v0/oss_registry.json")
  dest(File(projectDir, "src/main/resources/seed/local_oss_registry.json"))
  overwrite(true)
}
