plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)
  api(libs.micronaut.cache.caffeine)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-json-validation"))
  implementation(libs.failsafe.okhttp)
  implementation(libs.guava)
  implementation(libs.okhttp)
  implementation(libs.bundles.jackson)
  implementation(libs.semver4j)
  implementation(libs.kotlin.logging)

  testImplementation(project(":oss:airbyte-test-utils"))
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
}

tasks.processResources {
  from("${(ext["ossRootProject"] as Project).projectDir}/airbyte-connector-builder-resources")
}
