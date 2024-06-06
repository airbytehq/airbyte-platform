plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok) // Lombok must be added BEFORE Micronaut
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.keycloak.client)
  implementation(libs.bundles.micronaut)
  implementation(libs.failsafe.okhttp)
  implementation(libs.kotlin.logging)
  implementation(libs.okhttp)
  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-micronaut"))
  implementation(project(":airbyte-config:config-models"))

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockk)
}

tasks.named<Test>("test") {
  maxHeapSize = "2g"
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.
// Once lombok has been removed, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
