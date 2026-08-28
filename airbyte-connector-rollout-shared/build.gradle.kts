plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.otel.annotations)
  implementation(libs.bundles.temporal)
  api(project(":oss:airbyte-api:server-api"))
  api(project(":oss:airbyte-api:server-api-client"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))

  testImplementation(libs.mockk)
}
