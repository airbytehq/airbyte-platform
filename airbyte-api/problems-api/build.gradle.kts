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
  api(project(":oss:airbyte-config:config-models"))

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

val airbyteApiProblemsSpecFile = "$projectDir/src/main/openapi/api-problems.yaml"

val genAirbyteApiProblems =
  tasks.register<GenerateTask>("genAirbyteApiProblems") {
    val serverOutputDir = "${getLayout().buildDirectory.get()}/generated/api/problems"

    inputs.file(airbyteApiProblemsSpecFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(serverOutputDir)

    generatorName = "jaxrs-spec"
    inputSpec = airbyteApiProblemsSpecFile
    outputDir = serverOutputDir
    templateDir = "$projectDir/src/main/resources/templates/jaxrs-spec-api/public_api"

    packageName = "io.airbyte.api.problems"
    invokerPackage = "io.airbyte.api.problems.invoker.generated"
    modelPackage = "io.airbyte.api.problems.model.generated"

    generateApiDocumentation = false

    configOptions =
      mapOf(
        "enumPropertyNaming" to "UPPERCASE",
        "generatePom" to "false",
        "interfaceOnly" to "true",
        "useJakartaEe" to "true",
        "hideGenerationTimestamp" to "true",
      )

    doLast {
      // Remove unnecessary invoker classes to avoid Micronaut picking them up and registering them as beans
      delete("${outputDir.get()}/src/gen/java/${invokerPackage.get().replace(".", "/").replace("-","_")}")

      val generatedModelPath = "${outputDir.get()}/src/gen/java/${modelPackage.get().replace(".", "/").replace("-", "_")}"
      generateProblemThrowables(generatedModelPath)
    }
  }

sourceSets {
  main {
    java {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/problems/src/gen/java",
        "$projectDir/src/main/java",
      )
    }
    kotlin {
      srcDirs(
        "${project.layout.buildDirectory.get()}/generated/api/problems/src/gen/kotlin",
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
  dependsOn(genAirbyteApiProblems)
}

// uses afterEvaluate because at configuration time, the kspKotlin task does not exist.
afterEvaluate {
  tasks.named("kspKotlin").configure {
    mustRunAfter(genAirbyteApiProblems)
  }
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into spotbug issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
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
