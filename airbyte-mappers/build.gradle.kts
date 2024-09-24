plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))

  implementation(libs.bundles.jackson)
  implementation(libs.guava)
  implementation(libs.kotlin.logging)

  testImplementation(project(":oss:airbyte-commons"))
  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockk)
}
