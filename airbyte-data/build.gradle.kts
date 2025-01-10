plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  `java-test-fixtures`
}

dependencies {
  api(libs.bundles.micronaut.annotation)
  api(libs.micronaut.cache.caffeine)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  implementation(libs.bundles.apache)
  implementation(libs.bundles.jackson)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.datadog)
  implementation(libs.guava)
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-license"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(libs.airbyte.protocol)
  // For Keycloak Application Management
  implementation(libs.bundles.keycloak.client)
  implementation(libs.micronaut.security.jwt)

  testImplementation(libs.assertj.core)
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockk)
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.kotest)

  // TODO: flip this import - MockData should live in airbyte-data's testFixtures
  // and be imported in this manner by config-persistence
  // We can move the BaseConfigDatasets to airbyte-data's testFixtures as well.
  testImplementation(testFixtures(project(":oss:airbyte-config:config-persistence")))
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into SpotBugs issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
