plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)

  implementation("org.apache.commons:commons-text:1.10.0")

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-mappers"))
  implementation(libs.airbyte.protocol)
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  implementation(libs.bundles.datadog)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
}
