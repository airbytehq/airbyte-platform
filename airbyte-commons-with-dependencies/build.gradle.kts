plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-config:config-models"))

  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.mockito.core)
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
}
