plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  id("org.jetbrains.kotlin.plugin.serialization") version "2.2.0"
}

dependencies {
  implementation("com.google.devtools.ksp:symbol-processing-api:2.1.21-2.0.1")
  implementation(libs.micronaut.inject)
  implementation(libs.snakeyaml)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
}