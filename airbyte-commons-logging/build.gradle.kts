plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)
  api(libs.bundles.micronaut.metrics)
  api(libs.kotlin.logging)
  api(libs.commons.io)

  // Dependencies for specific cloud clients
  api(libs.google.cloud.storage)
  api(libs.aws.java.sdk.s3)
  api(libs.aws.java.sdk.sts)
  api(libs.s3)

  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-metrics:metrics-lib"))
  api(project(":oss:airbyte-featureflag"))

  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockk)
}