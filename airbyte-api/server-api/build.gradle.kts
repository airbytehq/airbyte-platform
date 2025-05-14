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

dependencies {
  annotationProcessor(libs.micronaut.openapi)

  ksp(libs.micronaut.openapi)
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
  implementation(libs.micronaut.security.oauth2)
  implementation(libs.openapi.jackson.databind.nullable)
  implementation(libs.reactor.core)
  implementation(libs.slf4j.api)
  implementation(libs.swagger.annotations)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))

  compileOnly(libs.v3.swagger.annotations)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
}

val specFile = "$projectDir/src/main/openapi/config.yaml"

val genApiServer =
  tasks.register<GenerateTask>("generateApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/server"

    inputs.file(specFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "jaxrs-spec"
    inputSpec = specFile
    outputDir = serverOutputDir
    templateDir = "$projectDir/src/main/resources/templates/jaxrs-spec"

    apiPackage = "io.airbyte.api.generated"
    invokerPackage = "io.airbyte.api.invoker.generated"
    modelPackage = "io.airbyte.api.model.generated"

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
        "DeclarativeManifest" to "com.fasterxml.jackson.databind.JsonNode",
        "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
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
    templateDir = "$projectDir/src/main/resources/templates/kotlin-server"

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
        "DeclarativeManifest" to "com.fasterxml.jackson.databind.JsonNode",
        "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
      )
  }

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
        "DeclarativeManifest" to "com.fasterxml.jackson.databind.JsonNode",
        "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
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
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
        "BillingEvent" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorIPCOptions" to "com.fasterxml.jackson.databind.JsonNode",
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
        "${project.layout.buildDirectory.get()}/generated/api/client/src/main/kotlin",
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
  dependsOn(genApiClient, genApiServer2)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genApiDocs, genApiClient, genApiServer, genApiServer2)
  }
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into spotbug issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
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

      // replace class declaration
      domainClientFileText =
        domainClientFileText.replace(
          "class (\\S+)\\(basePath: kotlin.String = defaultBasePath, client: Call.Factory = ApiClient.defaultClient\\) : ApiClient\\(basePath, client\\)"
            .toRegex(),
          "class $1(basePath: kotlin.String = defaultBasePath, client: Call.Factory = ApiClient.defaultClient, policy : RetryPolicy<okhttp3.Response> = RetryPolicy.ofDefaults()) : ApiClient(basePath, client, policy)",
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

private fun updateDomainClientsToIncludeHttpResponseBodyOnClientException(clientPath: String) {
  val dir = file(clientPath)
  dir.walk().forEach { domainClient ->
    if (domainClient.name.endsWith(".kt")) {
      val domainClientFileText =
        domainClient.readText().replace(
          "throw ClientException(\"Client error : \${localVarError.statusCode} \${localVarError.message.orEmpty()}\", localVarError.statusCode, localVarResponse)",
          "throw ClientException(\"Client error : \${localVarError.statusCode} \${localVarError.message.orEmpty()} \${localVarError.body ?: \"\"}\", localVarError.statusCode, localVarResponse)",
        )

      domainClient.writeText(domainClientFileText)
    }
  }
}

private fun configureApiSerializer(serializerPath: String) {
  /*
   * UPDATE Serializer to match the Java generator's version
   */
  val serializerFile = file(serializerPath)

  val imports =
    listOf(
      "import com.fasterxml.jackson.annotation.JsonInclude",
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
