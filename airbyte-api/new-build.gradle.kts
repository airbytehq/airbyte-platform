import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("org.openapi.generator")
  kotlin("jvm")
  kotlin("kapt")
}

val specFile = "$projectDir/src/main/openapi/config.yaml"
val airbyteApiSpecFile = "$projectDir/src/main/openapi/api.yaml"
val airbyteApiSpecTemplateDirApi = "$projectDir/src/main/resources/templates/jaxrs-spec-api"
val workloadSpecFile = "$projectDir/src/main/openapi/workload-openapi.yaml"

val genApiServer = tasks.register<GenerateTask>("generateApiServer") {
    val serverOutputDir = "$buildDir/generated/api/server"

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
            "DestinationDefinitionSpecification" to  "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
            "DeclarativeManifest"               to "com.fasterxml.jackson.databind.JsonNode",
            "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
    )

    generateApiDocumentation = false

    configOptions = mapOf(
            "dateLibrary"                   to "java8",
            "generatePom"                   to "false",
            "interfaceOnly"                 to "true",
            /*)
            JAX-RS generator does not respect nullable properties defined in the OpenApi Spec.
            It means that if a field is not nullable but not set it is still returning a null value for this field in the serialized json.
            The below Jackson annotation(is made to only(keep non null values in serialized json.
            We are not yet using nullable=true properties in our OpenApi so this is a valid(workaround at the moment to circumvent the default JAX-RS behavior described above.
            Feel free to read the conversation(on https://github.com/airbytehq/airbyte/pull/13370 for more details.
            */
            "additionalModelTypeAnnotations" to "\n@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",

            // Generate separate classes for each endpoint "domain")
            "useTags"                       to "true",
    )
}

val genApiClient = tasks.register<GenerateTask>("generateApiClient") {
    val clientOutputDir = "$buildDir/generated/api/client"

    inputs.file(specFile)
    outputs.dir(clientOutputDir)

    generatorName = "java"
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
            "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
    )

    library = "native"

    generateApiDocumentation = false

    configOptions = mapOf(
            "dateLibrary"  to "java8",
            "generatePom"  to "false",
            "interfaceOnly" to "true",
    )
}

val genApiClient2 = tasks.register<GenerateTask>("genApiClient2") {
    val clientOutputDir = "$buildDir/generated/api/client2"

    inputs.file(specFile)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = specFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.api.client2.generated"
    invokerPackage = "io.airbyte.api.client2.invoker.generated"
    modelPackage = "io.airbyte.api.client2.model.generated"

    schemaMappings = mapOf(
            "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
            "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
            "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationDefinitionSpecification" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
            "SecretPersistenceConfigurationJson" to "com.fasterxml.jackson.databind.JsonNode",
    )

    generateApiDocumentation = false

    configOptions = mapOf(
            "generatePom"         to "false",
            "interfaceOnly"       to "true",
    )

    doLast {
        /*)
         * UPDATE ApiClient.kt to use Failsafe.
         */
      var apiClientFile = file("build/generated/api/client2/src/main/kotlin/org/openapitools/client/infrastructure/ApiClient.kt")
        var apiClientFileText = apiClientFile.readText()

        // replace class declaration)
        apiClientFileText = apiClientFileText.replace(
                "open class ApiClient(val baseUrl: String, val client: OkHttpClient = defaultClient) {",
                "open class ApiClient(val baseUrl: String, val client: OkHttpClient = defaultClient, val policy: RetryPolicy<Response> = RetryPolicy.ofDefaults()) {")

        // replace execute call)
        apiClientFileText = apiClientFileText.replace(
                "val response = client.newCall(request).execute()",
                """val call = client.newCall(request)
        val failsafeCall = FailsafeCall.with(policy).compose(call)
        val response: Response = failsafeCall.execute()""")

        // add imports if not exist)
        if (!apiClientFileText.contains("import dev.failsafe.RetryPolicy")) {
            val newImports = """import dev.failsafe.RetryPolicy
import dev.failsafe.okhttp.FailsafeCall"""
            apiClientFileText = apiClientFileText.replaceFirst("import ", "$newImports\nimport ")

        }
        apiClientFile.writeText(apiClientFileText)

        // Update domain clients to use Failesafe
        updateDomainClientsWithFailsafe("build/generated/api/client2/src/main/kotlin/io/airbyte/api/client2/generated")
    }
}

val genApiDocs = tasks.register<GenerateTask>("generateApiDocs") {
    val docsOutputDir = "$buildDir/generated/api/docs"

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
    )

    generateApiDocumentation = false

    configOptions = mapOf(
            "dateLibrary"  to "java8",
            "generatePom"  to "false",
            "interfaceOnly" to "true",
    )
}

val genAirbyteApiServer = tasks.register<GenerateTask>("generateAirbyteApiServer") {
    val serverOutputDir = "$buildDir/generated/airbyte_api/server"

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
            "interfaceOnly"                to "true",
            "returnResponse"                to "true",
            "useBeanValidation"            to "true",
            "performBeanValidation"        to "true",
            "additionalModelTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
            "additionalEnumTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
    )

    schemaMappings = mapOf(
            "SourceConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "OAuthInputConfiguration"      to "com.fasterxml.jackson.databind.JsonNode",
            "OAuthCredentialsConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"     to "com.fasterxml.jackson.databind.JsonNode",
    )
}

// TODO: Linked to document okhhtp
val genWorkloadApiClient = tasks.register<GenerateTask>("genWorkloadApiClient") {
    val clientOutputDir = "$buildDir/generated/workloadapi/client"

    inputs.file(workloadSpecFile)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = workloadSpecFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.workload.api.client.generated"
    invokerPackage = "io.airbyte.workload.api.client.invoker.generated"
    modelPackage = "io.airbyte.workload.api.client.model.generated"

    schemaMappings = mapOf(
            "OAuthConfiguration"                to "com.fasterxml.jackson.databind.JsonNode",
            "SourceDefinitionSpecification"     to "com.fasterxml.jackson.databind.JsonNode",
            "SourceConfiguration"               to "com.fasterxml.jackson.databind.JsonNode",
            "DestinationDefinitionSpecification" to  "com.fasterxml.jackson.databind.JsonNode",
            "DestinationConfiguration"          to "com.fasterxml.jackson.databind.JsonNode",
            "StreamJsonSchema"                  to "com.fasterxml.jackson.databind.JsonNode",
            "StateBlob"                         to "com.fasterxml.jackson.databind.JsonNode",
            "FieldSchema"                       to "com.fasterxml.jackson.databind.JsonNode",
  )

    generateApiDocumentation = false

    configOptions = mapOf(
            "enumPropertyNaming"  to "UPPERCASE",
            "generatePom"         to "false",
            "interfaceOnly"       to "true",
    )

    doLast {
        // Delete file generated by the client2 task)
        file("build/generated/workloadapi/client/src/main/kotlin/org").delete()
        // Update domain clients to use Failsafe
        updateDomainClientsWithFailsafe("build/generated/workloadapi/client/src/main/kotlin/io/airbyte/workload/api/client/generated")
    }

    dependsOn(":airbyte-workload-api-server:compileKotlin", genApiClient2)
}


tasks.named("compileJava") {
  dependsOn(genApiDocs, genApiClient, genApiServer, genAirbyteApiServer)
}

kapt {
    correctErrorTypes = true
}

// uses afterEvaluate because at configuration(time, the kaptGenerateStubsKotlin task does not exist.)
afterEvaluate {
    tasks.named("kaptGenerateStubsKotlin").configure {
        mustRunAfter(genApiDocs, genApiClient, genApiClient2, genApiServer, genAirbyteApiServer, genWorkloadApiClient)
    }
}

tasks.named("compileKotlin") {
    dependsOn(genApiClient2, tasks.named("genWorkloadApiClient"))
}

dependencies {
    annotationProcessor(libs.micronaut.openapi)
    kapt(libs.micronaut.openapi)

    compileOnly(libs.v3.swagger.annotations)
    kapt(libs.v3.swagger.annotations)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.commons.io)
    implementation(libs.failsafe.okhttp)
    implementation(libs.guava)
    implementation(libs.javax.annotation.api)
    implementation(libs.javax.ws.rs.api)
    implementation(libs.javax.validation.api)
    implementation(libs.jackson.datatype)
    implementation(libs.moshi.kotlin)
    implementation(libs.okhttp)
    implementation(libs.openapi.jackson.databind.nullable)
    implementation(libs.reactor.core)
    implementation(libs.slf4j.api)
    implementation(libs.swagger.annotations)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
    testImplementation(libs.mockk)
    testImplementation(libs.kotlin.test.runner.junit5)
}

sourceSets["main"].java {
//      java {
          srcDirs("$buildDir/generated/api/server/src/gen/java",
          "$buildDir/generated/airbyte_api/server/src/gen/java",
          "$buildDir/generated/api/client/src/main/java",
          "$buildDir/generated/api/client2/src/main/kotlin",
          "$buildDir/generated/workloadapi/client/src/main/kotlin",
          "$projectDir/src/main/java",
              )
        }
//        resources {
//            srcDir("$projectDir/src/main/openapi/")
//        }
//    }
//}
sourceSets["main"].resources {
  srcDir("$projectDir/src/main/openapi/")
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs.add("-parameters")
}

airbyte {
    spotless {
        excludes = listOf(
          "src/main/openapi/workload-openapi.yaml",
          "$buildDir/generated/**",
        )
    }
}

fun updateDomainClientsWithFailsafe(clientPath:String) {
    /*
     * UPDATE domain clients to use Failsafe.
     */
    val dir = file(clientPath)
    dir.walk().forEach { domainClient ->
//      println("looking at file $domainClient")
        if (domainClient.name.endsWith(".kt")) {
            var domainClientFileText = domainClient.readText()

            // replace class declaration
            domainClientFileText = domainClientFileText.replace(
                    """class (\S+)\(basePath: kotlin.String = defaultBasePath, client: OkHttpClient = ApiClient.defaultClient\) : ApiClient\(basePath, client\)""".toRegex(),
                    "class $1(basePath: kotlin.String = defaultBasePath, client: OkHttpClient = ApiClient.defaultClient, policy: RetryPolicy<okhttp3.Response> = RetryPolicy.ofDefaults()) : ApiClient(basePath, client, policy)"
            )

            // add imports if not exist)
            if(!domainClientFileText.contains("import dev.failsafe.RetryPolicy")) {
                val newImports = "import dev.failsafe.RetryPolicy"
                domainClientFileText = domainClientFileText.replaceFirst("import ", "$newImports\nimport ")
            }
            domainClient.writeText(domainClientFileText)
        }
    }
}
