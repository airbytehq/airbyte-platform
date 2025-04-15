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

  api(project(":oss:airbyte-api:server-api"))
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

  compileOnly(libs.v3.swagger.annotations)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.kotlin.test.runner.junit5)
}

val workloadSpecFile = "$projectDir/src/main/openapi/workload-openapi.yaml"

val genWorkloadApiClient =
  tasks.register<GenerateTask>("genWorkloadApiClient") {
    val clientOutputDir = "${getLayout().buildDirectory.get()}/generated/workloadapi/client"

    inputs.file(workloadSpecFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(clientOutputDir)

    generatorName = "kotlin"
    inputSpec = workloadSpecFile
    outputDir = clientOutputDir

    apiPackage = "io.airbyte.workload.api.client.generated"
    packageName = "io.airbyte.workload.api.client.generated"
    modelPackage = "io.airbyte.workload.api.client.model.generated"

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
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
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
      val apiClientPath = "${outputDir.get()}/src/main/kotlin/io/airbyte/workload/api/client/generated/infrastructure/ApiClient.kt"
      updateApiClientWithFailsafe(apiClientPath)
      val generatedDomainClientsPath = "${outputDir.get()}/src/main/kotlin/io/airbyte/workload/api/client/generated"
      updateDomainClientsWithFailsafe(generatedDomainClientsPath)
      // the kotlin client (as opposed to the java client) doesn't include the response body in the exception message.
      updateDomainClientsToIncludeHttpResponseBodyOnClientException(generatedDomainClientsPath)
    }

    dependsOn(":oss:airbyte-workload-api-server:compileKotlin", ":oss:airbyte-api:server-api:genApiClient")
  }

sourceSets {
  main {
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/workloadapi/client/src/main/kotlin",
        "$projectDir/src/main/kotlin",
      )
    }
    resources {
      srcDir("$projectDir/src/main/openapi/")
    }
  }
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs = listOf("-parameters")
}

tasks.named("compileKotlin") {
  dependsOn(genWorkloadApiClient)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genWorkloadApiClient)
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
      val throwableText =
        template
          .readText()
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
