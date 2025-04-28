plugins {
  id("io.airbyte.gradle.jvm.lib")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.openapi)

  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-domain:models"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-api:problems-api")) // TODO remove this once api-problems are refactored into domain-problems
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(libs.openai.java)
  implementation(libs.bundles.datadog)

  testImplementation(libs.mockk)
  testImplementation(libs.bundles.kotest)
}
