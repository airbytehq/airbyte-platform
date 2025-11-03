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

  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-commons-license"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-domain:models"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-config:config-persistence"))

  implementation(libs.micronaut.inject)
  implementation(libs.micronaut.cache.caffeine)

  // Required for stigg
  implementation(libs.bundles.stigg)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.kotlin)
  testImplementation(libs.mockk)
}
