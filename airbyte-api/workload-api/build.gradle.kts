plugins {
  id("io.airbyte.gradle.jvm.lib")
}

airbyte {
  spotless {
    excludes = listOf("src/main/openapi/workload-openapi.yaml")
  }
}

dependencies {
  annotationProcessor(libs.micronaut.openapi)

  ksp(libs.micronaut.openapi)
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.v3.swagger.annotations)
  ksp(libs.jackson.kotlin)
  ksp(libs.moshi.kotlin)

  api(project(":oss:airbyte-api:server-api"))
  api(project(":oss:airbyte-api:commons"))

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.openapi.jackson.databind.nullable)
  implementation(libs.reactor.core)
  implementation(libs.bundles.retrofit)
  implementation(libs.slf4j.api)
  implementation(libs.swagger.annotations)
  implementation(project(":oss:airbyte-commons"))

  compileOnly(libs.v3.swagger.annotations)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.retrofit.mock)
  testImplementation(libs.kotlin.test.runner.junit5)
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs = listOf("-parameters")
}

// TODO(cole): remove when we upgrade to the latest kotlin
kotlin {
  compilerOptions {
    freeCompilerArgs.add("-Xmulti-dollar-interpolation")
  }
}
