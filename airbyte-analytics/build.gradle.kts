plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.segment.java.analytics)
  api(libs.micronaut.http)
  api(libs.micronaut.cache.caffeine)
  api(libs.bundles.micronaut.annotation)
  api(libs.bundles.micronaut.kotlin)
  api(libs.kotlin.logging)
  api(project(":oss:airbyte-commons"))
  api(project(":oss:airbyte-config:config-models"))
  api(project(":oss:airbyte-api:server-api"))
  api(project(":oss:airbyte-data"))

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
