plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)
  api(libs.bundles.micronaut.kotlin)
  api(libs.bundles.micronaut.metrics)
  api(libs.kotlin.logging)
  api(libs.azure.storage)
  api(libs.aws.java.sdk.s3)
  api(libs.aws.java.sdk.sts)
  api(libs.s3)
  api(libs.google.cloud.storage)
  api(libs.guava)
  api(libs.slf4j.api)
  api(libs.jackson.kotlin)

  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-metrics:metrics-lib"))
  api(project(":oss:airbyte-featureflag"))

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
