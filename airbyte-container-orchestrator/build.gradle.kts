import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.zip.ZipFile

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

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

val airbyteProtocol: Configuration by configurations.creating

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.cache)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.guava)
  implementation(libs.s3)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.sts)
  implementation(libs.kubernetes.client)
  implementation(libs.bundles.datadog)
  implementation(libs.kotlin.coroutines)
  implementation(libs.kotlin.coroutines.sl4j)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-micronaut-security"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-with-dependencies"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-mappers"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(project(":oss:airbyte-worker-models"))
  implementation(libs.kotlin.coroutines)

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.mockito.inline)
  testImplementation(libs.bundles.bouncycastle)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.coroutines.test)
  testImplementation(libs.assertj.core)

  airbyteProtocol(libs.airbyte.protocol) {
    isTransitive = false
  }
}

airbyte {
  application {
    mainClass = "io.airbyte.container.orchestrator.ApplicationKt"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
  }
  docker {
    imageName = "container-orchestrator"
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

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
