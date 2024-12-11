plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.kotlin.logging)
  implementation(libs.bundles.jackson)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-config:config-models"))

  implementation(libs.micronaut.inject)
  implementation(project(":oss:airbyte-commons-storage"))

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockk)
}
