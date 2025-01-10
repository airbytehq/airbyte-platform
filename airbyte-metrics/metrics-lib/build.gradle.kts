plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.metrics)
  api(libs.kotlin.logging)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-db:db-lib"))

  implementation(libs.guava)
  implementation(libs.google.cloud.storage)

  implementation(libs.otel.semconv)
  implementation(libs.otel.sdk)
  implementation(libs.otel.sdk.testing)
  implementation(libs.micrometer.statsd)
  implementation(platform(libs.otel.bom))
  implementation(("io.opentelemetry:opentelemetry-api"))
  implementation(("io.opentelemetry:opentelemetry-sdk"))
  implementation(("io.opentelemetry:opentelemetry-exporter-otlp"))

  implementation(libs.java.dogstatsd.client)
  implementation(libs.bundles.datadog)

  testImplementation(project(":oss:airbyte-config:config-persistence"))
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.platform.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockk)
  testImplementation((variantOf(libs.opentracing.util) { classifier("tests") }))

  testImplementation(libs.junit.pioneer)
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "DD_CONSTANT_TAGS" to "a, ,c, d,e ",
    ),
  )
}
