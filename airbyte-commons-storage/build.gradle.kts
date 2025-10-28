plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  // Keep as api only if storage types appear in public method signatures
  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-commons-micronaut"))

  // Hide implementation details from consumers - they don't need to see storage SDKs
  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.kotlin.logging)
  implementation(libs.azure.storage)          // Hidden: Azure SDK
  implementation(libs.aws.java.sdk.s3)        // Hidden: AWS S3 SDK
  implementation(libs.aws.java.sdk.sts)       // Hidden: AWS STS SDK
  implementation(libs.s3)                     // Hidden: S3 SDK
  implementation(libs.google.cloud.storage)   // Hidden: GCS SDK (still used in StorageClient.kt)
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  implementation(libs.jackson.kotlin)
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-featureflag"))

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
