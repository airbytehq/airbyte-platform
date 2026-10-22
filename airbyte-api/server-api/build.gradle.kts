import org.openapitools.generator.gradle.plugin.tasks.GenerateTask
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.resolver.Resolver

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
    layout.buildDirectory.dir("generated/api/server/src/main/kotlin"),
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
  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.reactor.core)
  implementation(libs.slf4j.api)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))

  compileOnly(libs.v3.swagger.annotations)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
  testImplementation(libs.network.json.validator)
}

val specFile = "$projectDir/src/main/openapi/config.yaml"
val scimSpecFile = "$projectDir/src/main/openapi/scim.yaml"

// The kotlin-server generator copies an operation into the API interface of every tag it carries.
// Generate from a copy of the spec that keeps only the first tag, so each operation belongs to one
// interface and a controller doesn't inherit operations it does not serve.
val firstTagSpecFile = layout.buildDirectory.file("openapi/config-first-tag.yaml")
val genFirstTagSpec =
  tasks.register("genFirstTagSpec") {
    inputs.file(specFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.file(firstTagSpecFile)

    doLast {
      // leave unquoted dates (in examples) as strings, so the copy differs from the spec only in tags
      val resolver =
        object : Resolver() {
          override fun addImplicitResolvers() {
            addImplicitResolver(Tag.BOOL, BOOL, "yYnNtTfFoO")
            addImplicitResolver(Tag.INT, INT, "-+0123456789")
            addImplicitResolver(Tag.FLOAT, FLOAT, "-+0123456789.")
            addImplicitResolver(Tag.MERGE, MERGE, "<")
            addImplicitResolver(Tag.NULL, NULL, "~nN\u0000")
            addImplicitResolver(Tag.NULL, EMPTY, null)
            addImplicitResolver(Tag.YAML, YAML, "!&*")
          }
        }
      val yaml = Yaml(Constructor(), Representer(), DumperOptions().apply { defaultFlowStyle = DumperOptions.FlowStyle.BLOCK }, resolver)
      val spec = file(specFile).reader().use { yaml.load<MutableMap<String, Any?>>(it) }
      @Suppress("UNCHECKED_CAST")
      val paths = spec["paths"] as Map<String, Map<String, Any?>>
      paths.values.flatMap { it.values }.filterIsInstance<MutableMap<String, Any?>>().forEach { operation ->
        (operation["tags"] as? List<*>)?.let { tags -> operation["tags"] = tags.take(1) }
      }
      firstTagSpecFile.get().asFile.writer().use { yaml.dump(spec, it) }
    }
  }

val genApiServer =
  tasks.register<GenerateTask>("generateApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/server"

    dependsOn(genFirstTagSpec)
    inputs.file(firstTagSpecFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = firstTagSpecFile.get().asFile.path
    outputDir = serverOutputDir
    // the generator never deletes files, so interfaces dropped from the spec would otherwise linger
    cleanupOutput = true
    // templates/kotlin-server is shared with the SCIM generator. This flag turns on the template
    // behavior the Config API wire format relies on (empty collection defaults, enum toString), which
    // SCIM does not use.
    additionalProperties = mapOf("javaModelParity" to true)
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

    // Keep the schemaMappings here, in generateApiDocs below, and in
    // server-api-client/build.gradle.kts (genApiClient) in sync — the client is generated
    // from this module's spec in a separate module.
    schemaMappings =
      mapOf(
        "OAuthConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteData" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteData" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "FusionSearchResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "FusionSearchStatusResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "FusionUsageWindow" to "io.airbyte.api.model.FusionUsageWindow",
        "DestinationExecuteResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "SkillDocsResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteParams" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteParams" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteMeta" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteMeta" to "com.fasterxml.jackson.databind.JsonNode",
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

    // Document FusionUsageWindow from the schema; only JVM generators map its handwritten model.
    schemaMappings =
      mapOf(
        "OAuthConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteData" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteData" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "FusionSearchResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "FusionSearchStatusResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "SkillDocsResponse" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteParams" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteParams" to "com.fasterxml.jackson.databind.JsonNode",
        "SourceExecuteMeta" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationExecuteMeta" to "com.fasterxml.jackson.databind.JsonNode",
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
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/server/src/main/kotlin",
        "${project.layout.buildDirectory.get()}/generated/api/scim-server/src/main/kotlin",
        "$projectDir/src/main/kotlin",
      )
    }
    resources {
      srcDir("$projectDir/src/main/openapi/")
    }
  }
}

tasks.named("compileKotlin") {
  dependsOn(genApiDocs, genApiServer, genScimApiServer)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genApiDocs, genApiServer, genScimApiServer)
  }
}


// Every Kotlin source here is generated; the models annotate constructor parameters (@JsonProperty,
// @field:Valid, ...). Keep the current parameter-only annotation target and drop the K2 warning that
// the default will change, which nothing hand-written can act on in this module.
kotlin {
  compilerOptions {
    freeCompilerArgs.add("-Xannotation-default-target=first-only")
  }
}
