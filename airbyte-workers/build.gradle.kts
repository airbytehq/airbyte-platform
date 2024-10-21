import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.gradle.api.tasks.testing.logging.TestLogEvent
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
val jdbc: Configuration by configurations.creating

configurations.all {
  // The quartz-scheduler brings in an outdated version(of hikari, we do not want to inherit this version.)
  exclude(group = "com.zaxxer", module = "HikariCP-java7")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok) // Lombok must be added BEFORE Micronaut
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.spotbugs.annotations)
  implementation(platform(libs.micronaut.platform))
  implementation(libs.google.cloud.storage)
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.cache)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.jooq)
  implementation(libs.s3)
  implementation(libs.sts)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation(libs.google.auth.library.oauth2.http)
  implementation(libs.java.jwt)
  implementation(libs.kubernetes.client)
  implementation(libs.guava)
  implementation(libs.temporal.sdk) {
    exclude(module = "guava")
  }
  implementation(libs.apache.ant)
  implementation(libs.apache.commons.lang)
  implementation(libs.apache.commons.text)
  implementation(libs.quartz.scheduler)
  implementation(libs.micrometer.statsd)
  implementation(libs.bundles.datadog)
  implementation(libs.sentry.java)
  implementation(libs.failsafe)

  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-micronaut-security"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-commons-with-dependencies"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-micronaut-temporal"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-worker-models"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.javax.databind)
  runtimeOnly(libs.bundles.logback)

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok) // Lombok must be added BEFORE Micronaut
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.temporal.testing)
  testImplementation(libs.json.path)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockk)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.bundles.bouncycastle)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)

  testRuntimeOnly(libs.junit.jupiter.engine)

  integrationTestAnnotationProcessor(platform(libs.micronaut.platform))
  integrationTestAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
  integrationTestImplementation(libs.bundles.junit)
  integrationTestImplementation(libs.junit.pioneer)
  integrationTestImplementation(libs.bundles.micronaut.test)

  airbyteProtocol(libs.airbyte.protocol) {
    isTransitive = false
  }
}

airbyte {
  application {
    mainClass = "io.airbyte.workers.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "MICRONAUT_ENVIRONMENTS" to "control-plane",
      ),
    )
  }
  docker {
    imageName = "worker"
  }
}

tasks.register<Test>("cloudStorageIntegrationTest") {
  useJUnitPlatform {
    includeTags("cloud-storage")
  }
  testLogging {
    events(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
  }
}

// Duplicated in :oss:airbyte-container-orchestrator, eventually, this should be handled in :oss:airbyte-protocol
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

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.
// By default, runs all annotation(processors and disables annotation(processing by javac, however).  Once lombok has
// been removed, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
