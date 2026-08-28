import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  id("io.airbyte.gradle.jvm.lib")
}

airbyte {
  spotless {
    excludes =
      listOf(
        "src/main/openapi/api.yaml",
        "src/main/openapi/api_sdk.yaml",
        "src/main/openapi/api_terraform.yaml",
        "src/main/openapi/api_documentation_connections.yaml",
        "src/main/openapi/api_documentation_sources.yaml",
        "src/main/openapi/api_documentation_destinations.yaml",
        "src/main/openapi/api_documentation_streams.yaml",
        "src/main/openapi/api_documentation_jobs.yaml",
        "src/main/openapi/api_documentation_workspaces.yaml",
      )
  }
}

// The OpenAPI-generated sources contain no Micronaut beans, so keep KSP from walking them.
// KSP matches excludedSources against source roots, so list the exact generated roots (a fileTree would be ignored).
ksp {
  excludedSources.from(
    layout.buildDirectory.dir("generated/api/server/src/gen/java"),
    layout.buildDirectory.dir("generated/api/server2/src/main/kotlin"),
    layout.buildDirectory.dir("generated/api/scim-server/src/main/kotlin"),
  )
}

dependencies {

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.v3.swagger.annotations)
  ksp(libs.jackson.kotlin)
  ksp(libs.moshi.kotlin)

  api(project(":oss:airbyte-api:commons"))

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.openapi.jackson.databind.nullable)
  implementation(libs.reactor.core)
  implementation(libs.slf4j.api)
  implementation(libs.swagger.annotations)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))

  compileOnly(libs.v3.swagger.annotations)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
}

val specFile = "$projectDir/src/main/openapi/config.yaml"
val scimSpecFile = "$projectDir/src/main/openapi/scim.yaml"

val genApiServer =
  tasks.register<GenerateTask>("generateApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/server"

    inputs.file(specFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "jaxrs-spec"
    inputSpec = specFile
    outputDir = serverOutputDir
    templateDir.set("$projectDir/src/main/resources/templates/jaxrs-spec")

    apiPackage = "io.airbyte.api.generated"
    invokerPackage = "io.airbyte.api.invoker.generated"
    modelPackage = "io.airbyte.api.model.generated"

    // Keep the schemaMappings here, in genApiServer2 and generateApiDocs below, and in
    // server-api-client/build.gradle.kts (genApiClient) in sync — the client is generated
    // from this module's spec in a separate module.
    schemaMappings =
      mapOf(
        "OAuthConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "StreamJsonSchema" to "com.fasterxml.jackson.databind.JsonNode",
        "StateBlob" to "com.fasterxml.jackson.databind.JsonNode",
        "FieldSchema" to "com.fasterxml.jackson.databind.JsonNode",
        "MapperConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        // Polymorphic on `type`; generated as opaque JsonNode and parsed into the
        // domain sealed class via Jackson @JsonTypeInfo. See config.yaml note —
        // an OpenAPI discriminator here breaks kotlin-server/jaxrs-spec codegen.
        "PrivateLinkServiceConfig" to "com.fasterxml.jackson.databind.JsonNode",
        "DeclarativeManifest" to "com.fasterxml.jackson.databind.JsonNode",
        "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
        "AuditLogDetails" to "com.fasterxml.jackson.databind.JsonNode",
      )

    generateApiDocumentation = false

    configOptions =
      mapOf(
        "dateLibrary" to "java8",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "hideGenerationTimestamp" to "true",
            /*
            JAX-RS generator does not respect nullable properties defined in the OpenApi Spec.
            It means that if a field is not nullable but not set it is still returning a null value for this field in the serialized json.
            The below Jackson annotation is made to only keep non null values in serialized json.
            We are not yet using nullable=true properties in our OpenApi so this is a valid workaround at the moment to circumvent the default JAX-RS behavior described above.
            Feel free to read the conversation on https://github.com/airbytehq/airbyte/pull/13370 for more details.
             */
        "additionalModelTypeAnnotations" to
          "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
        // Generate separate classes for each endpoint "domain"
        "useTags" to "true",
        "useJakartaEe" to "true",
      )

    doLast {
      // Remove unnecessary invoker classes to avoid Micronaut picking them up and registering them as beans
      delete("${outputDir.get()}/src/gen/java/${invokerPackage.get().replace(".", "/").replace("-","_")}")
    }
  }

val genApiServer2 =
  tasks.register<GenerateTask>("genApiServer2") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/server2"

    inputs.file(specFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = specFile
    outputDir = serverOutputDir
    templateDir.set("$projectDir/src/main/resources/templates/kotlin-server")

    packageName = "io.airbyte.api.server.generated"

    generateApiDocumentation = false

    configOptions =
      mapOf(
        "dateLibrary" to "java8",
        "enumPropertyNaming" to "UPPERCASE",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "library" to "jaxrs-spec",
        "returnResponse" to "false",
        "additionalModelTypeAnnotations" to
          "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
        "useTags" to "true",
        "useJakartaEe" to "true",
      )

    schemaMappings =
      mapOf(
        "OAuthConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "StreamJsonSchema" to "com.fasterxml.jackson.databind.JsonNode",
        "StateBlob" to "com.fasterxml.jackson.databind.JsonNode",
        "FieldSchema" to "com.fasterxml.jackson.databind.JsonNode",
        "MapperConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        // Polymorphic on `type`; generated as opaque JsonNode and parsed into the
        // domain sealed class via Jackson @JsonTypeInfo. See config.yaml note —
        // an OpenAPI discriminator here breaks kotlin-server/jaxrs-spec codegen.
        "PrivateLinkServiceConfig" to "com.fasterxml.jackson.databind.JsonNode",
        "DeclarativeManifest" to "com.fasterxml.jackson.databind.JsonNode",
        "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
        "AuditLogDetails" to "com.fasterxml.jackson.databind.JsonNode",
      )
  }

val genScimApiServer =
  tasks.register<GenerateTask>("genScimApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/scim-server"

    inputs.file(scimSpecFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = scimSpecFile
    outputDir = serverOutputDir
    templateDir.set("$projectDir/src/main/resources/templates/kotlin-server")

    doFirst {
      delete(serverOutputDir)
    }

    packageName = "io.airbyte.api.scim.generated"

    schemaMappings =
      mapOf(
        "ScimUserRequest" to "io.airbyte.api.scim.ScimUserRequest",
        "ScimGroupRequest" to "io.airbyte.api.scim.ScimGroupRequest",
        "ScimPatchRequest" to "io.airbyte.api.scim.ScimPatchRequest",
      )

    generateApiDocumentation = false

    configOptions =
      mapOf(
        "dateLibrary" to "java8",
        "enumPropertyNaming" to "UPPERCASE",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "library" to "jaxrs-spec",
        "returnResponse" to "true",
        "additionalModelTypeAnnotations" to
          "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
        "useTags" to "true",
        "useJakartaEe" to "true",
      )
  }


val genApiDocs =
  tasks.register<GenerateTask>("generateApiDocs") {
    val docsOutputDir = "${getLayout().buildDirectory.get()}/generated/api/docs"

    generatorName = "html"
    inputSpec = specFile
    outputDir = docsOutputDir

    apiPackage = "io.airbyte.api.client.generated"
    invokerPackage = "io.airbyte.api.client.invoker.generated"
    modelPackage = "io.airbyte.api.client.model.generated"

    schemaMappings =
      mapOf(
        "OAuthConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "StreamJsonSchema" to "com.fasterxml.jackson.databind.JsonNode",
        "StateBlob" to "com.fasterxml.jackson.databind.JsonNode",
        "FieldSchema" to "com.fasterxml.jackson.databind.JsonNode",
        "MapperConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        // Polymorphic on `type`; generated as opaque JsonNode and parsed into the
        // domain sealed class via Jackson @JsonTypeInfo. See config.yaml note —
        // an OpenAPI discriminator here breaks kotlin-server/jaxrs-spec codegen.
        "PrivateLinkServiceConfig" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
        "AuditLogDetails" to "com.fasterxml.jackson.databind.JsonNode",
      )

    generateApiDocumentation = false

    configOptions =
      mapOf(
        "dateLibrary" to "java8",
        "generatePom" to "false",
        "interfaceOnly" to "true",
      )
  }

sourceSets {
  main {
    java {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/server/src/gen/java",
        "$projectDir/src/main/java",
      )
    }
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/server2/src/main/kotlin",
        "${project.layout.buildDirectory.get()}/generated/api/scim-server/src/main/kotlin",
        "$projectDir/src/main/kotlin",
      )
    }
    resources {
      srcDir("$projectDir/src/main/openapi/")
    }
  }
}

tasks.named("compileJava") {
  dependsOn(genApiDocs, genApiServer)
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs = listOf("-parameters")
}

tasks.named("compileKotlin") {
  dependsOn(genApiServer2, genScimApiServer)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genApiDocs, genApiServer, genApiServer2, genScimApiServer)
  }
}

