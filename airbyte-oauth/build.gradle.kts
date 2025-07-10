plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)
  implementation(libs.guava)
  implementation(libs.google.cloud.storage)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation("com.hubspot.jinjava:jinjava:2.7.4")

  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(libs.airbyte.protocol)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockito.kotlin)
  testImplementation(project(":oss:airbyte-config:config-persistence"))
}
