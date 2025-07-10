plugins {
  id("io.airbyte.gradle.jvm.lib")
}

dependencies {
  implementation(libs.jackson.databind)
  implementation(libs.airbyte.protocol)
}
