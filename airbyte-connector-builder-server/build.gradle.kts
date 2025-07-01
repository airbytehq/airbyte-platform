import io.airbyte.gradle.tasks.DockerBuildxTask
import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("org.openapi.generator")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.kube-reload")
}

dependencies {
  // Micronaut dependencies
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  annotationProcessor(libs.micronaut.jaxrs.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.jaxrs.processor)

  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.openapi.jackson.databind.nullable)

  // Cloud service dependencies. These are not strictly necessary yet, but likely needed for any full-fledged cloud service
  implementation(libs.bundles.datadog)
  // implementation(libs.bundles.temporal)  uncomment this when we start using temporal to invoke connector commands
  implementation(libs.sentry.java)
  implementation(libs.guava)
  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.cache)
  implementation(libs.micronaut.http)
  implementation(libs.micronaut.security)
  implementation(libs.micronaut.security.jwt)
  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.bundles.micronaut.metrics)

  // OpenAPI code generation(dependencies)
  implementation(libs.swagger.annotations)

  // Internal dependencies)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-server"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-api:problems-api"))

  implementation(libs.airbyte.protocol)

  // Third-party dependencies
  implementation("org.kohsuke:github-api:1.327")
  implementation("org.yaml:snakeyaml:2.2")
  implementation("io.pebbletemplates:pebble:3.2.4")

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.bundles.logback)

  testRuntimeOnly(libs.junit.jupiter.engine)

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockk)
  testImplementation(libs.mockito.kotlin)
  testImplementation(libs.junit.pioneer)
}

airbyte {
  application {
    mainClass = "io.airbyte.connectorbuilder.ApplicationKt"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        // path to CDK virtual environment)
        "CDK_PYTHON" to (System.getenv("CDK_PYTHON") ?: ""),
        // path to CDK connector builder's main.py
        "CDK_ENTRYPOINT" to (System.getenv("CDK_ENTRYPOINT") ?: ""),
      ),
    )
  }
  docker {
    imageName = "connector-builder-server"
  }

  kubeReload {
    deployment = "ab-connector-builder-server"
    container = "airbyte-connector-builder-server"
  }
}

val generateOpenApiServer =
  tasks.register<GenerateTask>("generateOpenApiServer") {
    val specFile = "$projectDir/src/main/openapi/openapi.yaml"
    inputs.file(specFile).withPathSensitivity(PathSensitivity.RELATIVE)
    inputSpec.set(specFile)

    outputDir = "${project.layout.buildDirectory.get()}/generated/api/server"

    generatorName = "jaxrs-spec"
    packageName = "io.airbyte.connectorbuilder.api.generated"
    apiPackage = "io.airbyte.connectorbuilder.api.generated"
    invokerPackage = "io.airbyte.connectorbuilder.api.invoker.generated"
    modelPackage = "io.airbyte.connectorbuilder.api.model.generated"

    schemaMappings.putAll(
      mapOf(
        "ConnectorConfig" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorManifest" to "com.fasterxml.jackson.databind.JsonNode",
        "AirbyteStateMessage" to "com.fasterxml.jackson.databind.JsonNode",
      ),
    )

    // Our spec does not have nullable, but if it changes, this would be a gotcha that we would want to avoid
    configOptions.putAll(
      mapOf(
        "dateLibrary" to "java8",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "hideGenerationTimestamp" to "true",
      /*
      JAX-RS generator does not respect nullable properties defined in the OpenApi Spec.
      It means that if a field is not nullable but not set it is still returning a null value for this field in the serialized json.
      The below Jackson annotation is made to only keep non-null values in serialized json.
      We are not yet using nullable=true properties in our OpenApi so this is a valid workaround at the moment to circumvent the default JAX-RS behavior described above.
      Feel free to read the conversation on https://github.com/airbytehq/airbyte/pull/13370 for more details.
       */
        "additionalModelTypeAnnotations" to
          "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
      ),
    )

    doLast {
      // Remove unnecessary invoker classes to avoid Micronaut picking them up and registering them as beans
      delete("${outputDir.get()}/src/gen/java/${invokerPackage.get().replace(".", "/")}")
      // Clean up any javax references
      listOf(apiPackage.get(), modelPackage.get()).forEach { sourceDir ->
        updateToJakartaApi(file("${outputDir.get()}/src/gen/java/${sourceDir.replace(".", "/")}"))
      }
    }
  }

tasks.named("compileJava") {
  dependsOn(generateOpenApiServer)
}

afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(generateOpenApiServer)
  }
}

// Ensures that the generated models are compiled during the build step, so they are available for use at runtime

sourceSets {
  main {
    java {
      srcDirs("${project.layout.buildDirectory.get()}/generated/api/server/src/gen/java")
    }
    resources {
      srcDir("$projectDir/src/main/openapi/")
    }
  }
}

val copyPythonDeps =
  tasks.register<Copy>("copyPythonDependencies") {
    from("$projectDir/requirements.txt")
    into("${project.layout.buildDirectory.get()}/airbyte/docker/")
  }
//
tasks.named<DockerBuildxTask>("dockerBuildImage") {
  // Set build args
  // Current CDK version(used by the Connector Builder and workers running Connector Builder connectors
  val cdkVersion: String = File((ext["ossRootProject"] as Project).projectDir, "airbyte-connector-builder-resources/CDK_VERSION").readText().trim()
  buildArgs.put("CDK_VERSION", cdkVersion)
}

tasks.named("dockerCopyDistribution") {
  dependsOn(copyPythonDeps, generateOpenApiServer)
}

tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

private fun updateToJakartaApi(srcDir: File) {
  srcDir.walk().forEach { file ->
    if (file.isFile) {
      var contents = file.readText()
      contents =
        contents
          .replace("javax.ws.rs", "jakarta.ws.rs")
          .replace("javax.validation", "jakarta.validation")
          .replace("javax.annotation.processing", "jakarta.annotation")
          .replace("javax.annotation", "jakarta.annotation")
          .replace("jakarta.annotation.processing", "jakarta.annotation")
          .replace("List<@Valid ", "List<")
      file.writeText(contents)
    }
  }
}
