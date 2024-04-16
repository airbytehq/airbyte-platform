plugins {
  id("io.airbyte.gradle.jvm")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-json-validation"))

  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.airbyte.protocol)
  implementation(libs.guava)
  implementation(libs.bundles.jackson)

  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
