plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  `java-test-fixtures`
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut

  api(libs.bundles.micronaut.annotation)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  implementation(libs.bundles.apache)
  implementation(libs.bundles.jackson)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.guava)
  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-auth"))
  implementation(project(":airbyte-commons-protocol"))
  implementation(project(":airbyte-commons-license"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-config:config-secrets"))
  implementation(project(":airbyte-db:db-lib"))
  implementation(project(":airbyte-db:jooq"))
  implementation(project(":airbyte-json-validation"))
  implementation(project(":airbyte-featureflag"))
  implementation(libs.airbyte.protocol)
  // For Keycloak Application Management
  implementation(libs.bundles.keycloak.client)
  implementation(libs.micronaut.security.jwt)

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockk)
  testImplementation(project(":airbyte-test-utils"))
  testImplementation(libs.bundles.junit)

  // TODO: flip this import - MockData should live in airbyte-data's testFixtures
  // and be imported in this manner by config-persistence
  // We can move the BaseConfigDatasets to airbyte-data's testFixtures as well.
  testImplementation(testFixtures(project(":airbyte-config:config-persistence")))
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into SpotBugs issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.
// By default, runs all annotation(processors and disables annotation(processing by javac, however).  Once lombok has
// been removed, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
