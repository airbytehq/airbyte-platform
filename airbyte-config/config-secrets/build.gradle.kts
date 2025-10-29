plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  `java-test-fixtures`
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.kotlin.logging)
  implementation(libs.slf4j.api)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.google.cloud.storage)
  implementation(libs.micronaut.jooq)
  implementation(libs.guava)
  api(libs.bundles.secret.hydration)  // Keep: secret hydration types may be in public API
  api(libs.airbyte.protocol)  // Keep: protocol types in public API
  implementation(libs.jakarta.transaction.api)
  implementation(libs.micronaut.data.tx)
  implementation(libs.aws.java.sdk.sts)
  api(project(":oss:airbyte-commons"))  // Keep: commons types in public API

  /*
   * Marked as "implementation" to avoid leaking these dependencies to services
   * that only use the retrieval side of the secret infrastructure.  The services
   * that do need these dependencies will already have them declared, as they will
   * need to define singletons from these modules in order for everything work.
   */
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-domain:models"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-featureflag"))


  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.testcontainers.vault)
  testImplementation(testFixtures(project(":oss:airbyte-config:config-persistence")))
  testFixturesImplementation(project(":oss:airbyte-config:config-models"))
}
