plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.micronaut.inject)
  implementation(libs.launchdarkly)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.dataformat.yml)
  implementation(libs.jackson.kotlin)
  implementation(libs.okhttp)
  implementation(project(":oss:airbyte-commons"))
  implementation(libs.kotlin.logging)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(kotlin("test"))
  testImplementation(kotlin("test-junit5"))
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.junit)
}
