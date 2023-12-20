import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.Properties
import java.util.zip.ZipFile
import org.gradle.api.tasks.testing.logging.TestLogEvent

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        // necessary to convert the well_know_types from yaml to json
        val jacksonVersion = "2.16.0"
        classpath("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
        classpath("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    }
}


plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

val airbyteProtocol by configurations.creating
val jdbc by configurations.creating

configurations.all {
    // The quartz-scheduler brings in a really old version(of hikari, we do not want to inherit this version.)
    exclude(group= "com.zaxxer", module= "HikariCP-java7")
    resolutionStrategy {
        // Ensure that the versions defined in deps.toml are used)
        // instead of versions from transitive dependencies)
        force (libs.flyway.core, libs.jooq, libs.s3, libs.aws.java.sdk.s3, libs.sts, libs.aws.java.sdk.sts)
    }
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    implementation(libs.spotbugs.annotations)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.google.cloud.storage)
    implementation(libs.bundles.micronaut)
    implementation(libs.bundles.micronaut.metrics)
    implementation(libs.micronaut.cache.caffeine)
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
        exclude( module = "guava")
    }
    implementation(libs.apache.ant)
    implementation(libs.apache.commons.lang)
    implementation(libs.apache.commons.text)
    implementation(libs.quartz.scheduler)
    implementation(libs.micrometer.statsd)
    implementation(libs.bundles.datadog)
    implementation(libs.sentry.java)

    implementation(project(":airbyte-analytics"))
    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-converters"))
    implementation(project(":airbyte-commons-micronaut"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-commons-temporal-core"))
    implementation(project(":airbyte-commons-worker"))
    implementation(project(":airbyte-commons-with-dependencies"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-config:config-secrets"))
    implementation(project(":airbyte-config:specs"))
    implementation(project(":airbyte-config:init"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(project(":airbyte-micronaut-temporal"))
    implementation(project(":airbyte-json-validation"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-notification"))
    implementation(project(":airbyte-worker-models"))

    runtimeOnly(libs.javax.databind)

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.temporal.testing)
    testImplementation(libs.json.path)
    testImplementation(libs.mockito.inline)
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers)
    testImplementation(libs.platform.testcontainers.postgresql)
    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.bundles.bouncycastle)
    testImplementation(project(":airbyte-api"))

    integrationTestAnnotationProcessor(platform(libs.micronaut.bom))
    integrationTestAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    integrationTestImplementation(libs.bundles.junit)
    integrationTestImplementation(libs.junit.pioneer)
    integrationTestImplementation(libs.bundles.micronaut.test)

    airbyteProtocol(libs.airbyte.protocol) {
        isTransitive = false
    }
}

val env = Properties().apply {
    load(rootProject.file(".env.dev").inputStream())
}

airbyte {
    application {
        mainClass = "io.airbyte.workers.Application"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
        @Suppress("UNCHECKED_CAST")
        localEnvVars.putAll(env.toMap() as Map<String, String>)
        localEnvVars.putAll(mapOf(
            "AIRBYTE_ROLE"          to (System.getenv("AIRBYTE_ROLE") ?: "undefined"),
            "AIRBYTE_VERSION"       to env["VERSION"].toString(),
            "MICRONAUT_ENVIRONMENTS" to "control-plane",
        ))
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

// Duplicated in :airbyte-container-orchestrator, eventually, this should be handled in :airbyte-protocol)
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
