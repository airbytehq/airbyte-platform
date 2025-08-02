plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.temporal)
  implementation(libs.failsafe)
  implementation(libs.kotlin.logging)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-worker-models"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-json-validation"))

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.temporal.testing)
  // Needed to be able to mock final class
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.kotlin)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
}

tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
