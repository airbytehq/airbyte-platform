plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  implementation(libs.kotlin.logging)
  implementation("org.jetbrains.kotlinx:dataframe:1.0.0-Beta2")

  testImplementation(libs.mockk)
  testImplementation(libs.slf4j.simple)
}
