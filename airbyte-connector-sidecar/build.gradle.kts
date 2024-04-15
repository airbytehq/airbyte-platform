import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.Properties
import java.util.zip.ZipFile

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
  kotlin("jvm")
  kotlin("kapt")
}

buildscript {
  repositories {
    mavenCentral()
  }
  dependencies {
    // necessary to convert the well_know_types from yaml to json
    val jacksonVersion = libs.versions.fasterxml.version.get()
    classpath("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
    classpath("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
  }
}

val airbyteProtocol by configurations.creating
configurations.all {
  resolutionStrategy {
    // Ensure that the versions defined in deps.toml are used)
    // instead of versions from transitive dependencies)
    // Force to avoid(updated version brought in transitively from Micronaut 3.8+)
    // that is incompatible with our current Helm setup)
    force(libs.s3, libs.aws.java.sdk.s3)
  }
}

configurations.all {
  exclude(group = "io.micronaut", module = "micronaut-http-server-netty")
  exclude(group = "io.micronaut.openapi")
  exclude(group = "io.micronaut.flyway")
  exclude(group = "io.micronaut.sql")
}

dependencies {
  kapt(platform(libs.micronaut.platform))
  kapt(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.micronaut.light)
  implementation(libs.google.cloud.storage)
  implementation(libs.java.jwt)
  implementation(libs.kotlin.logging)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.slf4j.api)

  implementation(project(":airbyte-api"))
  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-converters"))
  implementation(project(":airbyte-commons-protocol"))
  implementation(project(":airbyte-commons-temporal"))
  implementation(project(":airbyte-commons-worker"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-metrics:metrics-lib")) // necessary for doc store
  implementation(project(":airbyte-worker-models"))
  implementation(libs.airbyte.protocol)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.kotlin.reflect)
  runtimeOnly(libs.appender.log4j2)
  runtimeOnly(libs.bundles.bouncycastle) // cryptography package

  kaptTest(platform(libs.micronaut.platform))
  kaptTest(libs.bundles.micronaut.annotation.processor)
  kaptTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.apache.commons.lang)

  airbyteProtocol(libs.airbyte.protocol) {
    isTransitive = false
  }
}

val env = Properties().apply {
  load(rootProject.file(".env.dev").inputStream())
}

airbyte {
  application {
    mainClass.set("io.airbyte.connectorSidecar.ApplicationKt")
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    @Suppress("UNCHECKED_CAST")
    localEnvVars.putAll(env.toMutableMap() as Map<String, String>)
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_VERSION" to env["VERSION"].toString(),
        "DATA_PLANE_ID" to "local",
        "MICRONAUT_ENVIRONMENTS" to "test"
      )
    )
  }
  docker {
    imageName.set("connector-sidecar")
  }
}

// Duplicated from :airbyte-worker, eventually, this should be handled in :airbyte-protocol)
val generateWellKnownTypes = tasks.register("generateWellKnownTypes") {
  inputs.files(airbyteProtocol) // declaring inputs)
  val targetFile = project.file("build/airbyte/docker/WellKnownTypes.json")
  outputs.file(targetFile) // declaring outputs)

  doLast {
    val wellKnownTypesYamlPath = "airbyte_protocol/well_known_types.yaml"
    airbyteProtocol.files.forEach {
      val zip = ZipFile(it)
      val entry = zip.getEntry(wellKnownTypesYamlPath)

      val wellKnownTypesYaml = zip.getInputStream(entry).bufferedReader().use { reader -> reader.readText() }
      val rawJson = yamlToJson(wellKnownTypesYaml)
      targetFile.getParentFile().mkdirs()
      targetFile.writeText(rawJson)
    }
  }
}

tasks.named("dockerBuildImage") {
  dependsOn(generateWellKnownTypes)
}

fun yamlToJson(rawYaml: String): String {
  val mappedYaml: Any = YAMLMapper().registerKotlinModule().readValue(rawYaml)
  return ObjectMapper().registerKotlinModule().writeValueAsString(mappedYaml)
}

// This is a workaround related to kaptBuild errors. It seems to be because there are no tests in cloud-airbyte-api-server.
// TODO: this should be removed when we move to kotlin 1.9.20
// TODO: we should write tests
afterEvaluate {
  tasks.named("kaptGenerateStubsTestKotlin") {
    enabled = false
  }
}
