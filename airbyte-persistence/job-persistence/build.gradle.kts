plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-oauth"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-domain:services"))
  implementation(project(":oss:airbyte-domain:models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(libs.airbyte.protocol)
  implementation(libs.sentry.java)
  implementation(libs.otel.semconv)
  implementation(libs.micrometer.statsd)
  implementation(libs.datadog.statsd.client)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)
  implementation(libs.otel.annotations)

  testImplementation(project(":oss:airbyte-config:config-persistence"))
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.platform.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
}
