plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(libs.okhttp)
  implementation("org.apache.httpcomponents:httpclient:4.5.13")
  implementation("org.commonmark:commonmark:0.21.0")

  implementation(libs.guava)
  implementation(libs.bundles.apache)
  implementation(libs.commons.io)
  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)

  testImplementation(libs.mockk)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockwebserver)
}
