import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import org.openapitools.generator.gradle.plugin.tasks.GenerateTask
import java.util.Properties

plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("org.openapi.generator")
    id("io.airbyte.gradle.publish")
}

dependencies {
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.1")
    implementation("com.googlecode.json-simple:json-simple:1.1.1")

    // Cloud service dependencies. These are not strictly necessary yet, but likely needed for any full-fledged cloud service)
    implementation(libs.bundles.datadog)
    // implementation(libs.bundles.temporal  uncomment this when we start using temporal to invoke connector commands)
    implementation(libs.sentry.java)

    implementation(libs.guava)

    // Micronaut dependencies)
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.micronaut.security)

    implementation(project(":airbyte-commons"))

    // OpenAPI code generation(dependencies)
    implementation("io.swagger:swagger-annotations:1.6.2")

    // Internal dependencies)
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-commons-server"))
    implementation(project(":airbyte-commons-worker"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-config:init"))
    implementation(project(":airbyte-metrics:metrics-lib"))

    implementation(libs.airbyte.protocol)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}

val env = Properties().apply {
    load(rootProject.file(".env.dev").inputStream())
}

airbyte {
    application {
        mainClass = "io.airbyte.connector_builder.MicronautConnectorBuilderServerRunner"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
        @Suppress("UNCHECKED_CAST")
        localEnvVars.putAll(env.toMap() as Map<String, String>)
        localEnvVars.putAll(mapOf(
            "AIRBYTE_ROLE"   to (System.getenv("AIRBYTE_ROLE") ?: ""),
        "AIRBYTE_VERSION" to env["VERSION"].toString(),
        // path to CDK virtual environment)
        "CDK_PYTHON"     to (System.getenv("CDK_PYTHON") ?: ""),
        // path to CDK connector builder"s main.py)
        "CDK_ENTRYPOINT" to (System.getenv("CDK_ENTRYPOINT") ?: ""),
        ))
    }
    docker {
        imageName = "connector-builder-server"
    }
}

val generateOpenApiServer = tasks.register<GenerateTask>("generateOpenApiServer") {
    val specFile = "$projectDir/src/main/openapi/openapi.yaml"
    inputs.file(specFile)
    inputSpec = specFile
    outputDir = "$buildDir/generated/api/server"

    generatorName = "jaxrs-spec"
    apiPackage = "io.airbyte.connector_builder.api.generated"
    invokerPackage = "io.airbyte.connector_builder.api.invoker.generated"
    modelPackage = "io.airbyte.connector_builder.api.model.generated"

    schemaMappings.putAll(mapOf(
            "ConnectorConfig"  to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorManifest" to "com.fasterxml.jackson.databind.JsonNode",
    ))

    // Our spec does not have nullable, but if it changes, this would be a gotcha that we would want to avoid)
    configOptions.putAll(mapOf(
            "dateLibrary" to "java8",
            "generatePom"              to "false",
            "interfaceOnly" to "true",
            /*)
            JAX-RS generator does not respect nullable properties defined in the OpenApi Spec.
            It means that if a field is not nullable but not set it is still returning a null value for this field in the serialized json.
            The below Jackson annotation(is made to only(keep non null values in serialized json.
            We are not yet using nullable=true properties in our OpenApi so this is a valid(workaround at the moment to circumvent the default JAX-RS behavior described above.
            Feel free to read the conversation(on https://github.com/airbytehq/airbyte/pull/13370 for more details.
            */
            "additionalModelTypeAnnotations" to "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
    ))
}

tasks.named("compileJava") {
    dependsOn(generateOpenApiServer)
}
//// Ensures that the generated models are compiled during the build step so they are available for use at runtime)

sourceSets {
    main {
        java {
            srcDirs("$buildDir/generated/api/server/src/gen/java")
        }
        resources {
            srcDir("$projectDir/src/main/openapi/")
        }
    }
}

val copyPythonDeps = tasks.register<Copy>("copyPythonDependencies") {
    from("$projectDir/requirements.txt")
    into("$buildDir/airbyte/docker/")
}
//
tasks.named<DockerBuildImage>("dockerBuildImage") {
    // Set build args
    // Current CDK version(used by the Connector Builder and workers running Connector Builder connectors
    val cdkVersion: String = File(project.projectDir.parentFile, "airbyte-connector-builder-resources/CDK_VERSION").readText().trim()
    buildArgs.put("CDK_VERSION", cdkVersion)

    dependsOn(copyPythonDeps, generateOpenApiServer)
}
