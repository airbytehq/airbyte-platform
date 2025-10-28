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

  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-commons-micronaut"))
  api(project(":oss:airbyte-config:config-models"))

  implementation(libs.micronaut.inject)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockito.inline)
}
