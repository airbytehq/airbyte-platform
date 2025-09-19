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
  api(project(":oss:airbyte-db:db-lib"))
  api(project(":oss:airbyte-db:jooq"))
  api(project(":oss:airbyte-config:config-models"))
  api(project(":oss:airbyte-config:config-persistence"))

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.bundles.flyway)
  implementation(libs.temporal.sdk)
  implementation(libs.google.cloud.api.client)
  implementation(libs.google.cloud.sqladmin)
  implementation(libs.micronaut.security.jwt)

  // Mark as compile only to avoid leaking transitively to connectors
  compileOnly(libs.platform.testcontainers.postgresql)

  testImplementation(libs.platform.testcontainers.postgresql)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
}
