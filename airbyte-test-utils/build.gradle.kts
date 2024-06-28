plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

configurations.all {
  exclude(group = "io.micronaut.jaxrs")
  exclude(group = "io.micronaut.sql")
}

dependencies {
  api(libs.junit.jupiter.api)
  api(libs.kotlin.logging)
  api(libs.failsafe)
  api(libs.okhttp)
  api(project(":airbyte-db:db-lib"))
  api(project(":airbyte-db:jooq"))
  api(project(":airbyte-config:config-models"))
  api(project(":airbyte-config:config-persistence"))

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-api"))
  implementation(project(":airbyte-commons-auth"))
  implementation(project(":airbyte-commons-temporal"))
  implementation(project(":airbyte-commons-worker"))
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.bundles.flyway)
  implementation(libs.temporal.sdk)
  implementation(libs.google.cloud.api.client)
  implementation(libs.google.cloud.sqladmin)

  // Mark as compile only(to avoid leaking transitively to connectors
  compileOnly(libs.platform.testcontainers.postgresql)

  testImplementation(libs.platform.testcontainers.postgresql)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
}
