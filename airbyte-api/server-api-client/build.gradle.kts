import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  id("io.airbyte.gradle.jvm.lib")
}

// The OpenAPI-generated sources contain no Micronaut beans, so keep KSP from walking them.
// KSP matches excludedSources against source roots, so list the exact generated root (a fileTree would be ignored).
ksp {
  excludedSources.from(
    layout.buildDirectory.dir("generated/api/client/src/main/kotlin"),
  )
}

dependencies {

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(project(":oss:airbyte-api:commons"))

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.micronaut.security.jwt)
  implementation(libs.openapi.jackson.databind.nullable)
  implementation(libs.slf4j.api)
  implementation(project(":oss:airbyte-commons-micronaut"))

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.kotlin.test.runner.junit5)
  testImplementation(project(":oss:airbyte-commons-auth"))
}

// The spec lives in server-api; only the file is read — there is no project dependency in either direction.
val specFile = project(":oss:airbyte-api:server-api").file("src/main/openapi/config.yaml").path

val genApiClient =
  tasks.register<GenerateTask>("genApiClient") {
    val clientOutputDir = "${getLayout().buildDirectory.get()}/generated/api/client"

    inputs.file(specFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = specFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.api.client.generated"
    invokerPackage = "io.airbyte.api.client.invoker.generated"
    modelPackage = "io.airbyte.api.client.model.generated"

    // Keep in sync with the schemaMappings blocks in server-api/build.gradle.kts (generateApiServer,
    // genApiServer2, generateApiDocs). An entry missing here still compiles but generates a typed
    // client model where the server sees JsonNode — a wire mismatch that only surfaces at runtime.
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
        "enumPropertyNaming" to "UPPERCASE",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "serializationLibrary" to "jackson",
      )

    doLast {
      val apiClientPath = "${outputDir.get()}/src/main/kotlin/org/openapitools/client/infrastructure/ApiClient.kt"
      updateApiClientWithFailsafe(apiClientPath)
      updateDomainClientsWithFailsafe("${outputDir.get()}/src/main/kotlin/io/airbyte/api/client/generated")
      configureApiSerializer("${outputDir.get()}/src/main/kotlin/org/openapitools/client/infrastructure/Serializer.kt")
    }
  }

sourceSets {
  main {
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/client/src/main/kotlin",
        "$projectDir/src/main/kotlin",
      )
    }
  }
}

tasks.named("compileKotlin") {
  dependsOn(genApiClient)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genApiClient)
  }
}

private fun updateApiClientWithFailsafe(clientPath: String) {
  /*
   * UPDATE ApiClient.kt to use Failsafe.
   */
  val apiClientFile = file(clientPath)
  var apiClientFileText =
    apiClientFile
      .readText()
      // replace class declaration
      .replace(
        "open class ApiClient(val baseUrl: String, val client: Call.Factory = defaultClient) {",
        "open class ApiClient(val baseUrl: String, val client: Call.Factory = defaultClient, val policy : RetryPolicy<Response> = RetryPolicy.ofDefaults()) {",
      )
      // replace execute call
      .replace(
        "val response = client.newCall(request).execute()",
        """val call = client.newCall(request)
        val failsafeCall = FailsafeCall.with(policy).compose(call)
        val response: Response = failsafeCall.execute()""",
      )

  // add imports if not exist
  if (!apiClientFileText.contains("import dev.failsafe.RetryPolicy")) {
    val newImports = """import dev.failsafe.RetryPolicy
import dev.failsafe.okhttp.FailsafeCall"""
    apiClientFileText = apiClientFileText.replaceFirst("import ", "$newImports\nimport ")
  }
  apiClientFile.writeText(apiClientFileText)
}

private fun updateDomainClientsWithFailsafe(clientPath: String) {
  /*
   * UPDATE domain clients to use Failsafe.
   */
  val dir = file(clientPath)
  dir.walk().forEach { domainClient ->
    if (domainClient.name.endsWith(".kt")) {
      var domainClientFileText = domainClient.readText()
      val defaultRetryPolicy =
        if (domainClient.name == "ScimConfigApi.kt") {
          "RetryPolicy.builder<okhttp3.Response>().withMaxRetries(0).build()"
        } else {
          "RetryPolicy.ofDefaults()"
        }
      val defaultClient =
        if (domainClient.name == "ScimConfigApi.kt") {
          "ApiClient.defaultClient.newBuilder().retryOnConnectionFailure(false).build()"
        } else {
          "ApiClient.defaultClient"
        }

      // replace class declaration
      domainClientFileText =
        domainClientFileText.replace(
          "class (\\S+)\\(basePath: kotlin.String = defaultBasePath, client: Call.Factory = ApiClient.defaultClient\\) : ApiClient\\(basePath, client\\)"
            .toRegex(),
          "class $1(basePath: kotlin.String = defaultBasePath, client: Call.Factory = $defaultClient, policy : RetryPolicy<okhttp3.Response> = $defaultRetryPolicy) : ApiClient(basePath, client, policy)",
        )

      // add imports if not exist
      if (!domainClientFileText.contains("import dev.failsafe.RetryPolicy")) {
        val newImports = "import dev.failsafe.RetryPolicy"
        domainClientFileText = domainClientFileText.replaceFirst("import ", "$newImports\nimport ")
      }

      domainClient.writeText(domainClientFileText)
    }
  }
}

private fun configureApiSerializer(serializerPath: String) {
  /*
   * UPDATE Serializer to match the Java generator's version
   * Also configure StreamReadConstraints to allow large JSON strings (e.g., large HTTP response bodies).
   */
  val serializerFile = file(serializerPath)

  val imports =
    listOf(
      "import com.fasterxml.jackson.annotation.JsonInclude",
      "import com.fasterxml.jackson.core.StreamReadConstraints",
      "import com.fasterxml.jackson.databind.ObjectMapper",
      "import com.fasterxml.jackson.databind.DeserializationFeature",
      "import com.fasterxml.jackson.databind.SerializationFeature",
      "import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule",
      "import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper",
      "import org.openapitools.jackson.nullable.JsonNullableModule",
    )

  val body =
    """
object Serializer {
    @JvmStatic
    val jacksonObjectMapper: ObjectMapper = jacksonObjectMapper()
        .findAndRegisterModules()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
        .enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING)
        .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
        .registerModule(JavaTimeModule())
        .registerModule(JsonNullableModule())
        .also {
            // Raise the max string length to allow large HTTP response bodies (e.g., >20MB)
            // We've seen errors when calling the manifest-server, which can return large responses.
            // 100MB seems like a reasonable limit for now.
            it.factory.setStreamReadConstraints(
                StreamReadConstraints.builder()
                    .maxStringLength(100_000_000)
                    .build()
            )
        }
}
    """.trimIndent()

  serializerFile.writeText(
    """
package org.openapitools.client.infrastructure

${imports.joinToString("\n")}

$body
    """.trimIndent(),
  )
}
