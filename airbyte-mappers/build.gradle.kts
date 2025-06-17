plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.micronaut.cache.caffeine)
  implementation(libs.network.json.validator)
  implementation(libs.victools.json.schema.generator)
  implementation(libs.victools.json.schema.jackson.module)
  implementation(project(":oss:airbyte-config:config-models"))

  implementation(libs.bundles.jackson)
  implementation(libs.guava)
  implementation(libs.kotlin.logging)

  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.micronaut.test)
}
