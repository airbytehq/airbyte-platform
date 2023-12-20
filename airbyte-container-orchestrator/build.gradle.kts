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
        val jacksonVersion = "2.16.0"
        classpath("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
        classpath("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    }
}

plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
    kotlin("jvm")
    kotlin("kapt")
}

val airbyteProtocol by configurations.creating

configurations.all {
    resolutionStrategy {
        // Ensure that the versions defined in deps.toml are used)
        // instead of versions from transitive dependencies)
        // Force to avoid(updated version brought in transitively from Micronaut 3.8+)
        // that is incompatible with our current Helm setup)
        force (libs.s3, libs.aws.java.sdk.s3)
    }
}
dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    annotationProcessor(libs.lombok)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.bundles.micronaut.metrics)
    implementation(libs.guava)
    implementation(libs.s3)
    implementation(libs.aws.java.sdk.s3)
    implementation(libs.sts)
    implementation(libs.kubernetes.client)
    implementation(libs.bundles.datadog)
    implementation(libs.bundles.log4j)
    compileOnly(libs.lombok)

    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-commons-converters"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-commons-micronaut"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-commons-with-dependencies"))
    implementation(project(":airbyte-commons-worker"))
    implementation(project(":airbyte-config:init"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-json-validation"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(project(":airbyte-worker-models"))

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.bundles.mockito.inline)
    testImplementation(libs.bundles.bouncycastle)
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers)
    testImplementation(libs.platform.testcontainers.postgresql)

    airbyteProtocol(libs.airbyte.protocol) {
        isTransitive = false
    }
}

airbyte {
    application {
        mainClass = "io.airbyte.container_orchestrator.Application"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    }
    docker {
        imageName = "container-orchestrator"
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
