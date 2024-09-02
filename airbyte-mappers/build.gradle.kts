plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  implementation(project(":oss:airbyte-config:config-models"))

  implementation(libs.bundles.jackson)
  implementation(libs.kotlin.logging)

  testImplementation(project(":oss:airbyte-commons"))
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockk)
}
