plugins {
  id("io.airbyte.gradle.jvm.lib")
}

dependencies {
  annotationProcessor(libs.micronaut.openapi)

  ksp(libs.micronaut.openapi)
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.v3.swagger.annotations)
  ksp(libs.jackson.kotlin)
  ksp(libs.moshi.kotlin)

  api(libs.bundles.micronaut.annotation)
  api(libs.micronaut.http)
  api(libs.failsafe.okhttp)
  api(libs.failsafe.retrofit)
  api(libs.okhttp)
  api(libs.guava)
  api(libs.java.jwt)
  api(libs.google.auth.library.oauth2.http)
  api(libs.kotlin.logging)
  api(libs.jackson.kotlin)
  api(libs.moshi.kotlin)
  api(project(":oss:airbyte-config:config-models"))
  api(project(":oss:airbyte-metrics:metrics-lib"))

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.micronaut.security.oauth2)
  implementation(libs.openapi.jackson.databind.nullable)
  implementation(libs.reactor.core)
  implementation(libs.slf4j.api)
  implementation(libs.swagger.annotations)
  implementation(libs.retrofit)

  implementation(project(":oss:airbyte-commons"))

  compileOnly(libs.v3.swagger.annotations)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs = listOf("-parameters")
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into spotbug issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}
