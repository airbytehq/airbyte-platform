import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.zip.ZipFile

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
}

buildscript {
  repositories {
    mavenCentral()
  }
  dependencies {
    // necessary to convert the well_know_types from yaml to json
    val jacksonVersion =
      libs.versions.fasterxml.version
        .get()
    classpath("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
    classpath("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
  }
}

val airbyteProtocol: Configuration by configurations.creating

configurations.all {
  exclude(group = "io.micronaut", module = "micronaut-http-server-netty")
  exclude(group = "io.micronaut.openapi")
  exclude(group = "io.micronaut.flyway")
  exclude(group = "io.micronaut.sql")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut.light)
  implementation(libs.google.cloud.storage)
  implementation(libs.java.jwt)
  implementation(libs.kotlin.logging)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.slf4j.api)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.micronaut.http.client)
  implementation(libs.retrofit)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-metrics:metrics-lib")) // necessary for doc store
  implementation(project(":oss:airbyte-worker-models"))
  implementation(libs.airbyte.protocol)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.kotlin.reflect)
  runtimeOnly(libs.bundles.logback)
  runtimeOnly(libs.bundles.bouncycastle) // cryptography package

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.airbyte.protocol)
  testImplementation(libs.retrofit.mock)

  airbyteProtocol(libs.airbyte.protocol) {
    isTransitive = false
  }
}

airbyte {
  application {
    mainClass.set("io.airbyte.connectorSidecar.ApplicationKt")
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_VERSION" to "dev",
        "DATA_PLANE_ID" to "local",
        "MICRONAUT_ENVIRONMENTS" to "test",
      ),
    )
  }
  docker {
    imageName.set("connector-sidecar")
  }
}

// Duplicated from :oss:airbyte-worker, eventually, this should be handled in :oss:airbyte-protocol
val generateWellKnownTypes =
  tasks.register("generateWellKnownTypes") {
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

tasks.named("dockerCopyDistribution") {
  dependsOn(generateWellKnownTypes)
}

fun yamlToJson(rawYaml: String): String {
  val mappedYaml: Any = YAMLMapper().registerKotlinModule().readValue(rawYaml)
  return ObjectMapper().registerKotlinModule().writeValueAsString(mappedYaml)
}
