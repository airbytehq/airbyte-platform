plugins {
  id("io.airbyte.gradle.jvm")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-json-validation"))

  implementation(libs.bundles.micronaut.annotation)
  implementation(libs.airbyte.protocol)
  implementation(libs.guava)
  implementation(libs.bundles.jackson)
  implementation(libs.bundles.apache)

  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
