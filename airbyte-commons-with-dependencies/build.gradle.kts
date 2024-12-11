plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-config:config-models"))

  implementation(libs.guava)

  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.mockito.core)
  testImplementation(libs.bundles.micronaut.test)
}
