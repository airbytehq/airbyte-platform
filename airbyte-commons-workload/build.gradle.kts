plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.openapi)

  implementation(libs.bundles.jackson)
  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.micronaut.http)
  implementation(libs.jakarta.transaction.api)
  implementation(libs.micronaut.jaxrs.server)
  implementation(libs.micronaut.security)
  implementation(libs.micronaut.security.jwt)
  implementation(libs.okhttp)
  implementation(libs.reactor.core)
  implementation(libs.kotlin.logging)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.bundles.datadog)
  
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))

  compileOnly(libs.micronaut.openapi.annotations)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)
  
  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.reactor.test)
  testImplementation(libs.mockk)
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "AIRBYTE_VERSION" to "dev",
      "MICRONAUT_ENVIRONMENTS" to "test",
      "SERVICE_NAME" to project.name,
    ),
  )
}

tasks.withType(JavaCompile::class).configureEach {
  options.compilerArgs = listOf("-parameters")
}

// Even though Kotlin is excluded on Spotbugs, this projects
// still runs into SpotBugs issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}
