plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.kotlin.logging)
  implementation(libs.azure.storage)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation(libs.s3)
  implementation(libs.google.cloud.storage)
  implementation(libs.slf4j.api)
  implementation(libs.jackson.kotlin)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-commons-server"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-featureflag"))  // Explicitly needed for FeatureFlagClient
  implementation(libs.bundles.micronaut)
  implementation(libs.micronaut.inject)
  implementation(libs.bundles.logback)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.kotlin.coroutines)

  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockk)
}
