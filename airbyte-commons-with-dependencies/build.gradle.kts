plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-temporal"))
  implementation(project(":airbyte-config:config-models"))

  implementation(libs.guava)

  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.mockito.core)
  testImplementation(libs.bundles.micronaut.test)
}
