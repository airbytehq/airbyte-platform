import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  id("io.airbyte.gradle.jvm.lib")
}

airbyte {
  spotless {
    excludes = listOf("src/main/openapi/workload-openapi.yaml")
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

  api(libs.bundles.micronaut.annotation)
  api(libs.micronaut.http)
  api(libs.bundles.micronaut.metrics)
  api(libs.failsafe.okhttp)
  api(libs.okhttp)
  api(libs.guava)
  api(libs.java.jwt)
  api(libs.google.auth.library.oauth2.http)
  api(libs.kotlin.logging)
  api(libs.jackson.kotlin)
  api(libs.moshi.kotlin)
  api(project(":airbyte-config:config-models"))

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.commons.io)
  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jackson.datatype)
  implementation(libs.jackson.databind)
  implementation(libs.openapi.jackson.databind.nullable)
  implementation(libs.reactor.core)
  implementation(libs.slf4j.api)
  implementation(libs.swagger.annotations)
    
  implementation(project(":airbyte-commons"))

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
val airbyteApiSpecFile = "$projectDir/src/main/openapi/api.yaml"
val airbyteApiProblemsSpecFile = "$projectDir/src/main/openapi/api-problems.yaml"
val airbyteApiSpecTemplateDirApi = "$projectDir/src/main/resources/templates/jaxrs-spec-api"
val publicApiSpecTemplateDirApi = "$projectDir/src/main/resources/templates/jaxrs-spec-api/public_api"
val workloadSpecFile = "$projectDir/src/main/openapi/workload-openapi.yaml"
val connectorBuilderServerSpecFile = project(":airbyte-connector-builder-server").file("src/main/openapi/openapi.yaml").getPath()

val genApiServer = tasks.register<GenerateTask>("generateApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/server"

    inputs.file(specFile)
    outputs.dir(serverOutputDir)

    generatorName = "jaxrs-spec"
    inputSpec = specFile
    outputDir = serverOutputDir

    apiPackage = "io.airbyte.api.generated"
    invokerPackage = "io.airbyte.api.invoker.generated"
    modelPackage = "io.airbyte.api.model.generated"

    schemaMappings = mapOf(
            "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
            "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
            "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
            "DeclarativeManifest"               to "com.fasterxml.jackson.databind.JsonNode",
            "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
  )

    generateApiDocumentation = false

    configOptions = mapOf(
            "dateLibrary"                   to "java8",
            "generatePom"                   to "false",
            "interfaceOnly"                 to "true",
            /*
            JAX-RS generator does not respect nullable properties defined in the OpenApi Spec.
            It means that if a field is not nullable but not set it is still returning a null value for this field in the serialized json.
            The below Jackson annotation is made to only keep non null values in serialized json.
            We are not yet using nullable=true properties in our OpenApi so this is a valid workaround at the moment to circumvent the default JAX-RS behavior described above.
            Feel free to read the conversation on https://github.com/airbytehq/airbyte/pull/13370 for more details.
            */
            "additionalModelTypeAnnotations" to "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",

            // Generate separate classes for each endpoint "domain"
            "useTags"                       to "true",
            "useJakartaEe"                  to "true",
  )

  doLast {
    // Remove unnecessary invoker classes to avoid Micronaut picking them up and registering them as beans
    delete("${outputDir.get()}/src/gen/java/${invokerPackage.get().replace(".", "/").replace("-","_")}")
  }
}

val genApiServer2 = tasks.register<GenerateTask>("genApiServer2") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/server2"

    inputs.file(specFile)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = specFile
    outputDir = serverOutputDir
    templateDir = "$projectDir/src/main/resources/templates/kotlin-server/public-api"

    packageName = "io.airbyte.api.server.generated"

    generateApiDocumentation = false

    configOptions = mapOf(
      "dateLibrary"                   to "java8",
      "enumPropertyNaming"            to "UPPERCASE",
      "generatePom"                   to "false",
      "interfaceOnly"                 to "true",
      "library"                       to "jaxrs-spec",
      "returnResponse"                to "false",
      "additionalModelTypeAnnotations" to "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
      "useTags"                       to "true",
      "useJakartaEe"                  to "true",
  )

    schemaMappings = mapOf(
            "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
            "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
            "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
            "DeclarativeManifest"               to "com.fasterxml.jackson.databind.JsonNode",
            "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )
}

val genApiClient = tasks.register<GenerateTask>("genApiClient") {
    val clientOutputDir = "${getLayout().buildDirectory.get()}/generated/api/client"

    inputs.file(specFile)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = specFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.api.client.generated"
    invokerPackage = "io.airbyte.api.client.invoker.generated"
    modelPackage = "io.airbyte.api.client.model.generated"

    schemaMappings = mapOf(
      "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
      "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
      "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
      "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
      "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
      "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
      "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
      "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
      "DeclarativeManifest"               to "com.fasterxml.jackson.databind.JsonNode",
      "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
      "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )

    generateApiDocumentation = false

    configOptions = mapOf(
            "enumPropertyNaming" to "UPPERCASE",
            "generatePom"        to "false",
            "interfaceOnly"      to "true",
            "serializationLibrary" to "jackson",
    )

    doLast {
        val apiClientPath = "${outputDir.get()}/src/main/kotlin/org/openapitools/client/infrastructure/ApiClient.kt"
        updateApiClientWithFailsafe(apiClientPath)
        updateDomainClientsWithFailsafe("${outputDir.get()}/src/main/kotlin/io/airbyte/api/client/generated")
        configureApiSerializer("${outputDir.get()}/src/main/kotlin/org/openapitools/client/infrastructure/Serializer.kt")
    }
}

val genApiDocs = tasks.register<GenerateTask>("generateApiDocs") {
    val docsOutputDir = "${getLayout().buildDirectory.get()}/generated/api/docs"

    generatorName = "html"
    inputSpec = specFile
    outputDir = docsOutputDir

    apiPackage = "io.airbyte.api.client.generated"
    invokerPackage = "io.airbyte.api.client.invoker.generated"
    modelPackage = "io.airbyte.api.client.model.generated"

    schemaMappings = mapOf(
            "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
            "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
            "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
  )

    generateApiDocumentation = false

    configOptions = mapOf(
            "dateLibrary"  to "java8",
            "generatePom" to  "false",
            "interfaceOnly" to "true",
  )
}

val genPublicApiServer = tasks.register<GenerateTask>("generatePublicApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/public_api/server"

    inputs.file(specFile)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = specFile
    outputDir = serverOutputDir
    templateDir = "$projectDir/src/main/resources/templates/kotlin-server/public-api"

    packageName = "io.airbyte.publicApi.server.generated"

    generateApiDocumentation = false

    configOptions = mapOf(
            "dateLibrary"                   to "java8",
            "enumPropertyNaming"            to "UPPERCASE",
            "generatePom"                   to "false",
            "interfaceOnly"                 to "true",
            "library"                       to "jaxrs-spec",
            "returnResponse"                to "true",
            "useBeanValidation"             to "true",
            "performBeanValidation"         to "true",
            "additionalModelTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
            "additionalEnumTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
            "useTags"                       to "true",
            "useJakartaEe"                  to "true",
    )

    schemaMappings = mapOf(
            "SourceConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "OAuthInputConfiguration"      to "com.fasterxml.jackson.databind.JsonNode",
            "OAuthCredentialsConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"     to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )
}

val genAirbyteApiServer =tasks.register<GenerateTask>("generateAirbyteApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/airbyte_api/server"

    inputs.file(airbyteApiSpecFile)
    outputs.dir(serverOutputDir)

    generatorName = "jaxrs-spec"
    inputSpec = airbyteApiSpecFile
    outputDir = serverOutputDir
    templateDir = airbyteApiSpecTemplateDirApi

    apiPackage = "io.airbyte.airbyte-api.generated"
    invokerPackage = "io.airbyte.airbyte-api.invoker.generated"
    modelPackage = "io.airbyte.airbyte-api.model.generated"

    generateApiDocumentation = false

    configOptions = mapOf(
      "dateLibrary"                   to "java8",
      "generatePom"                   to "false",
      "interfaceOnly"                 to "true",
      "returnResponse"                to "true",
      "useBeanValidation"             to "true",
      "performBeanValidation"         to "true",
      "additionalModelTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
      "additionalEnumTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
      "useJakartaEe"                  to "true",
    )

    schemaMappings = mapOf(
      "SourceConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
      "OAuthInputConfiguration"      to "com.fasterxml.jackson.databind.JsonNode",
      "OAuthCredentialsConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
      "DestinationConfiguration"     to "com.fasterxml.jackson.databind.JsonNode",
      "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )

    doLast {
      // Remove unnecessary invoker classes to avoid Micronaut picking them up and registering them as beans
      delete("${outputDir.get()}/src/gen/java/${invokerPackage.get().replace(".", "/").replace("-","_")}")
    }
}

val genAirbyteApiServer2 = tasks.register<GenerateTask>("generateAirbyteApiServer2") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/airbyte_api/server2"

    inputs.file(airbyteApiSpecFile)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = specFile
    outputDir = serverOutputDir
    templateDir = "$projectDir/src/main/resources/templates/kotlin-server"

    packageName = "io.airbyte.airbyteApi.server.generated"

    generateApiDocumentation = false

    configOptions = mapOf(
      "dateLibrary"                   to "java8",
      "enumPropertyNaming"            to "UPPERCASE",
      "generatePom"                   to "false",
      "interfaceOnly"                 to "true",
      "library"                       to "jaxrs-spec",
      "returnResponse"                to "true",
      "useBeanValidation"             to "true",
      "performBeanValidation"         to "true",
      "additionalModelTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
      "additionalEnumTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
      "useTags"                       to "true",
      "useJakartaEe"                  to "true",
    )

    schemaMappings = mapOf(
      "SourceConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
      "OAuthInputConfiguration"      to "com.fasterxml.jackson.databind.JsonNode",
      "OAuthCredentialsConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
      "DestinationConfiguration"     to "com.fasterxml.jackson.databind.JsonNode",
      "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )
}

val genAirbyteApiProblems = tasks.register<GenerateTask>("genAirbyteApiProblems") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/problems"

    inputs.file(airbyteApiProblemsSpecFile)
    outputs.dir(serverOutputDir)

    generatorName = "jaxrs-spec"
    inputSpec = airbyteApiProblemsSpecFile
    outputDir = serverOutputDir
    templateDir = publicApiSpecTemplateDirApi

    packageName = "io.airbyte.api.problems"
    invokerPackage = "io.airbyte.api.problems.invoker.generated"
    modelPackage = "io.airbyte.api.problems.model.generated"

    generateApiDocumentation = false

    configOptions = mapOf(
        "enumPropertyNaming"  to "UPPERCASE",
        "generatePom"         to "false",
        "interfaceOnly"       to "true",
        "useJakartaEe"                  to "true",
    )

    doLast {
        // Remove unnecessary invoker classes to avoid Micronaut picking them up and registering them as beans
        delete("${outputDir.get()}/src/gen/java/${invokerPackage.get().replace(".", "/").replace("-","_")}")

        val generatedModelPath = "${outputDir.get()}/src/gen/java/${modelPackage.get().replace(".", "/").replace("-", "_")}"
        generateProblemThrowables(generatedModelPath)
    }
}

// TODO: Linked to document okhhtp
val genWorkloadApiClient = tasks.register<GenerateTask>("genWorkloadApiClient") {
    val clientOutputDir = "${getLayout().buildDirectory.get()}/generated/workloadapi/client"

    inputs.file(workloadSpecFile)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = workloadSpecFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.workload.api.client.generated"
    packageName = "io.airbyte.workload.api.client.generated"
    modelPackage = "io.airbyte.workload.api.client.model.generated"

    schemaMappings = mapOf(
            "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
            "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
            "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )

    generateApiDocumentation = false

    configOptions = mapOf(
      "enumPropertyNaming"  to "UPPERCASE",
      "generatePom"         to "false",
      "interfaceOnly"       to "true",
    )

    doLast {
        val apiClientPath = "${outputDir.get()}/src/main/kotlin/io/airbyte/workload/api/client/generated/infrastructure/ApiClient.kt"
        updateApiClientWithFailsafe(apiClientPath)
        val generatedDomainClientsPath = "${outputDir.get()}/src/main/kotlin/io/airbyte/workload/api/client/generated"
        updateDomainClientsWithFailsafe(generatedDomainClientsPath)
        // the kotlin client (as opposed to the java client) doesn't include the response body in the exception message.
        updateDomainClientsToIncludeHttpResponseBodyOnClientException(generatedDomainClientsPath)
    }

    dependsOn(":airbyte-workload-api-server:compileKotlin", "genApiClient")
}

val genConnectorBuilderServerApiClient = tasks.register<GenerateTask>("genConnectorBuilderServerApiClient") {
    val clientOutputDir = "${getLayout().buildDirectory.get()}/generated/connectorbuilderserverapi/client"

    inputs.file(connectorBuilderServerSpecFile)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = connectorBuilderServerSpecFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.connectorbuilderserver.api.client.generated"
    invokerPackage = "io.airbyte.connectorbuilderserver.api.client.invoker.generated"
    modelPackage = "io.airbyte.connectorbuilderserver.api.client.model.generated"

    schemaMappings = mapOf(
            "ConnectorConfig"   to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorManifest" to "com.fasterxml.jackson.databind.JsonNode",
            "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
    )

    generateApiDocumentation = false

    configOptions = mapOf(
      "enumPropertyNaming"  to "UPPERCASE",
      "generatePom"         to "false",
      "interfaceOnly"       to "true",
      "serializationLibrary" to "jackson",
    )

    doLast {
        // Delete file generated by the client task
        delete(file("${outputDir.get()}/src/main/kotlin/org"))

        val generatedDomainClientsPath = "${outputDir.get()}/src/main/kotlin/io/airbyte/connectorbuilderserver/api/client/generated"
        updateDomainClientsWithFailsafe(generatedDomainClientsPath)
        // the kotlin client (as opposed to the java client) doesn't include the response body in the exception message.
        updateDomainClientsToIncludeHttpResponseBodyOnClientException(generatedDomainClientsPath)
    }

    dependsOn("genApiClient")
}

sourceSets {
  main {
    java {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/server/src/gen/java",
        "${project.layout.buildDirectory.get()}/generated/airbyte_api/server/src/gen/java",
        "${project.layout.buildDirectory.get()}/generated/api/problems/src/gen/kotlin",
        "${project.layout.buildDirectory.get()}/generated/api/problems/src/gen/java",
        "$projectDir/src/main/java",
      )
    }
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/workloadapi/client/src/main/kotlin",
        "${project.layout.buildDirectory.get()}/generated/api/server2/src/main/kotlin",
        "${project.layout.buildDirectory.get()}/generated/airbyte_api/server2/src/main/kotlin",
        "${project.layout.buildDirectory.get()}/generated/public_api/server/src/main/kotlin",
        "${project.layout.buildDirectory.get()}/generated/connectorbuilderserverapi/client/src/main/kotlin",
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
  dependsOn(genApiDocs, genApiServer, genAirbyteApiServer)
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs = listOf("-parameters")
}

tasks.named("compileKotlin") {
    dependsOn(genApiClient, genWorkloadApiClient, genConnectorBuilderServerApiClient, genAirbyteApiProblems,
            genApiServer2, genAirbyteApiServer2, genPublicApiServer)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genApiDocs, genApiClient, genApiServer, genApiServer2, genAirbyteApiServer, genAirbyteApiServer2,
    genPublicApiServer, genWorkloadApiClient, genConnectorBuilderServerApiClient,
    genAirbyteApiProblems)
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
    var apiClientFileText = apiClientFile.readText()
        // replace class declaration
        .replace(
            "open class ApiClient(val baseUrl: String, val client: OkHttpClient = defaultClient) {",
            "open class ApiClient(val baseUrl: String, val client: OkHttpClient = defaultClient, val policy : RetryPolicy<Response> = RetryPolicy.ofDefaults()) {")
        // replace execute call
        .replace("val response = client.newCall(request).execute()",
            """val call = client.newCall(request)
        val failsafeCall = FailsafeCall.with(policy).compose(call)
        val response: Response = failsafeCall.execute()

        return response.use { processResponse(response) }
        }

        protected inline fun <reified T: Any?> processResponse(response: Response): ApiResponse<T?> {""")

    // add imports if not exist
    if (!apiClientFileText.contains("import dev.failsafe.RetryPolicy")) {
        val newImports = """import dev.failsafe.RetryPolicy
import dev.failsafe.okhttp.FailsafeCall"""
        apiClientFileText = apiClientFileText.replaceFirst("import ", newImports + "\nimport ")

    }
    apiClientFile.writeText(apiClientFileText)
}

private fun generateProblemThrowables(problemsOutputDir: String) {
    val dir = file(problemsOutputDir)

    val throwableDir = File("${getLayout().buildDirectory.get()}/generated/api/problems/src/gen/kotlin/throwable")
    if (!throwableDir.exists()) {
        throwableDir.mkdirs()
    }

    dir.walk().forEach { errorFile ->
        if (errorFile.name.endsWith("ProblemResponse.java")) {
            val errorFileText = errorFile.readText()
            val problemName: String = "public class (\\S+)ProblemResponse ".toRegex().find(errorFileText)!!.destructured.component1()
            var dataFieldType: String = "private (@Valid )?\n(\\S+) data;".toRegex().find(errorFileText)!!.destructured.component2()
            var dataFieldImport = "import io.airbyte.api.problems.model.generated.$dataFieldType"

            if (dataFieldType == "Object") {
                dataFieldType = "Any"
                dataFieldImport = ""
            }

            val responseClassName = "${problemName}ProblemResponse"
            val throwableClassName = "${problemName}Problem"

            val template = File("$projectDir/src/main/resources/templates/ThrowableProblem.kt.txt")
            val throwableText = template.readText()
                .replace("<problem-class-name>", responseClassName)
                .replace("<problem-throwable-class-name>", throwableClassName)
                .replace("<problem-data-class-import>", dataFieldImport)
                .replace("<problem-data-class-name>", dataFieldType)

            val throwableFile = File(throwableDir, "$throwableClassName.kt")
            throwableFile.writeText(throwableText)
        }
    }
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
      domainClientFileText = domainClientFileText.replace(
        "class (\\S+)\\(basePath: kotlin.String = defaultBasePath, client: OkHttpClient = ApiClient.defaultClient\\) : ApiClient\\(basePath, client\\)".toRegex(),
        "class $1(basePath: kotlin.String = defaultBasePath, client: OkHttpClient = ApiClient.defaultClient, policy : RetryPolicy<okhttp3.Response> = RetryPolicy.ofDefaults()) : ApiClient(basePath, client, policy)"
      )

      // add imports if not exist
      if(!domainClientFileText.contains("import dev.failsafe.RetryPolicy")) {
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
            val domainClientFileText = domainClient.readText().replace(
                    "throw ClientException(\"Client error : \${localVarError.statusCode} \${localVarError.message.orEmpty()}\", localVarError.statusCode, localVarResponse)",
                    "throw ClientException(\"Client error : \${localVarError.statusCode} \${localVarError.message.orEmpty()} \${localVarError.body ?: \"\"}\", localVarError.statusCode, localVarResponse)")

            domainClient.writeText(domainClientFileText)
        }
    }
}

private fun configureApiSerializer(serializerPath: String) {
  /*
   * UPDATE Serializer to match the Java generator's version
   */
  val serializerFile = file(serializerPath)

  val imports = listOf(
    "import com.fasterxml.jackson.annotation.JsonInclude",
    "import com.fasterxml.jackson.databind.ObjectMapper",
    "import com.fasterxml.jackson.databind.DeserializationFeature",
    "import com.fasterxml.jackson.databind.SerializationFeature",
    "import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule",
    "import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper",
    "import org.openapitools.jackson.nullable.JsonNullableModule"
  )

  val body = """
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

  serializerFile.writeText("""
package org.openapitools.client.infrastructure
    
${imports.joinToString("\n")}
    
$body
  """.trimIndent())
}
