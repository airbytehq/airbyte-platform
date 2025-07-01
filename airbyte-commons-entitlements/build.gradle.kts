plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)
  api(libs.bundles.micronaut.kotlin)
  api(libs.kotlin.logging)
  api(libs.bundles.jackson)

  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-commons-micronaut"))
  api(project(":oss:airbyte-config:config-models"))

  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-commons-license"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))

  implementation(libs.micronaut.inject)
  implementation(libs.micronaut.cache.caffeine)

  // Required for stigg
  implementation(libs.stigg.api.client)
  implementation(libs.apollo.runtime)
  implementation(libs.apollo.api.jvm)
  implementation(libs.apollo.adapters.jvm)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockk)
}
