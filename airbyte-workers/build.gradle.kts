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
  implementation(libs.bundles.temporal.telemetry)
  implementation(libs.jooq)
  implementation(libs.s3)
  implementation(libs.sts)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation(libs.google.auth.library.oauth2.http)
  implementation(libs.java.jwt)
  implementation(libs.kotlin.logging)
  implementation(libs.kubernetes.client)
  implementation(libs.guava)
  implementation(libs.retrofit)
  implementation(libs.temporal.sdk) {
    exclude(module = "guava")
  }
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

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.temporal.testing)
  testImplementation(libs.json.path)
  testImplementation(libs.json.smart)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.kotlin)
  testImplementation(libs.mockk)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.bundles.bouncycastle)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.retrofit.mock)
  testImplementation(variantOf(libs.opentracing.util) { classifier("tests") })

  testRuntimeOnly(libs.junit.jupiter.engine)

  kspIntegrationTest(platform(libs.micronaut.platform))
  kspIntegrationTest(libs.bundles.micronaut.test.annotation.processor)

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
    mainClass = "io.airbyte.workers.ApplicationKt"
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

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
