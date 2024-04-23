plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.temporal)
  implementation(libs.bundles.apache)
  implementation(libs.failsafe)

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-temporal-core"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-config:config-persistence"))
  implementation(project(":airbyte-featureflag"))
  implementation(project(":airbyte-metrics:metrics-lib"))
  implementation(project(":airbyte-notification"))
  implementation(project(":airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":airbyte-worker-models"))
  implementation(project(":airbyte-api"))
  implementation(project(":airbyte-json-validation"))

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.temporal.testing)
  // Needed to be able to mock final class)
  testImplementation(libs.mockito.inline)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
}
