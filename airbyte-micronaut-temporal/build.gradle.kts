plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.bundles.micronaut)
  implementation(libs.byte.buddy)
  implementation(libs.guava)
  implementation(libs.spring.core)
  implementation(libs.kotlin.logging)
  implementation(libs.temporal.sdk) {
    exclude(module = "guava")
  }

  implementation(project(":oss:airbyte-commons-temporal-core"))

  testImplementation(libs.assertj.core)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockito.inline)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
