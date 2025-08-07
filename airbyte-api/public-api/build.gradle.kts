import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  id("io.airbyte.gradle.jvm.lib")
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
  api(project(":oss:airbyte-api:problems-api"))

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

val internalApiSpecFile = project(":oss:airbyte-api:server-api").file("src/main/openapi/config.yaml").path

val genPublicApiServer =
  tasks.register<GenerateTask>("generatePublicApiServer") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/public_api/server"

    inputs.file(internalApiSpecFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "kotlin-server"
    inputSpec = internalApiSpecFile
    outputDir = serverOutputDir
    templateDir.set("$projectDir/src/main/resources/templates/kotlin-server/public-api")

    packageName = "io.airbyte.publicApi.server.generated"

    generateApiDocumentation = false

    configOptions =
      mapOf(
        "dateLibrary" to "java8",
        "enumPropertyNaming" to "UPPERCASE",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "library" to "jaxrs-spec",
        "returnResponse" to "true",
        "useBeanValidation" to "true",
        "performBeanValidation" to "true",
        "additionalModelTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
        "additionalEnumTypeAnnotations" to "@io.micronaut.core.annotation.Introspected",
        "useTags" to "true",
        "useJakartaEe" to "true",
      )

    schemaMappings =
      mapOf(
        "SourceConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "OAuthInputConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "OAuthCredentialsConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "DestinationConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "MapperConfiguration" to "com.fasterxml.jackson.databind.JsonNode",
        "ConnectorBuilderProjectTestingValues" to "com.fasterxml.jackson.databind.JsonNode",
      )
  }

sourceSets {
  main {
    java {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/public_api/server/src/gen/java",
        "$projectDir/src/main/java",
      )
    }
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/public_api/server/src/main/kotlin",
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
  dependsOn(genPublicApiServer)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genPublicApiServer)
  }
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into spotbug issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}
