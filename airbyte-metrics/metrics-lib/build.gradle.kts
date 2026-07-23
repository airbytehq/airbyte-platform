plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.metrics) // Keep: metric types may be in public API
  implementation(libs.kotlin.logging)
  implementation(libs.micronaut.cache.caffeine)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-db:db-lib"))

  implementation(libs.google.cloud.storage)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.otel.api)
  implementation(libs.otel.semconv)
  implementation(libs.micrometer.otlp)
  implementation(libs.micrometer.statsd)
  implementation(libs.java.dogstatsd.client)

  testImplementation(project(":oss:airbyte-config:config-persistence"))
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.platform.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockk)
  testImplementation(libs.junit.jupiter.system.stubs)
  testImplementation(libs.junit.pioneer)
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "DD_CONSTANT_TAGS" to "a, ,c, d,e ",
    ),
  )
}
