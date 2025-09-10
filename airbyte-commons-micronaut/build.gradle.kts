plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(project(":oss:airbyte-configuration-processor"))

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.micronaut.security)
  implementation(libs.failsafe)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-api:problems-api"))

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockk)
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Copy>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
