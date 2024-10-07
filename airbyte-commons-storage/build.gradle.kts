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
  api(libs.commons.io)
  api(libs.azure.storage)
  api(libs.aws.java.sdk.s3)
  api(libs.aws.java.sdk.sts)
  api(libs.s3)
  api(libs.google.cloud.storage)
  api(libs.guava)
  api(libs.slf4j.api)

  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-metrics:metrics-lib"))
  api(project(":oss:airbyte-featureflag"))

  // Dependencies for specific storage clients
  // TODO: This is deprecated, but required to make the real van logging solution happy.
  implementation("com.microsoft.azure:azure-storage:8.6.6")
  implementation(libs.micronaut.inject)
  implementation(libs.bundles.logback)

  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockk)
}
