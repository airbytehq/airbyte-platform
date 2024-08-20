plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
  implementation(libs.bundles.jackson)
  implementation(libs.guava)
  implementation(libs.google.cloud.storage)
  implementation(libs.bundles.apache)
  implementation(libs.appender.log4j2)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)

  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(libs.airbyte.protocol)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(project(":oss:airbyte-config:config-persistence"))
}
