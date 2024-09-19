import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jsonschema2pojo.SourceType

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  id("com.github.eirnym.js2p")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)

  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-featureflag"))

  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.datadog)
  implementation(libs.bundles.jackson)
  implementation(libs.spotbugs.annotations)
  implementation(libs.guava)
  implementation(libs.micronaut.kotlin.extension.functions)
  implementation(libs.bundles.apache)
  implementation(libs.airbyte.protocol)
  implementation(libs.commons.io)
  implementation(libs.kotlin.logging)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.bundles.micronaut.test)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

jsonSchema2Pojo {
  setSourceType(SourceType.YAMLSCHEMA.name)
  setSource(files("${sourceSets["main"].output.resourcesDir}/types"))
  targetDirectory = file("${project.layout.buildDirectory.get()}/generated/src/gen/java/")

  targetPackage = "io.airbyte.config"
  useLongIntegers = true

  removeOldOutput = true

  generateBuilders = true
  includeConstructors = false
  includeSetters = true
  serializable = true
}

tasks.named<Test>("test") {
  useJUnitPlatform {
    excludeTags("log4j2-config", "logger-client")
  }
}

tasks.named("compileKotlin") {
  dependsOn(tasks.named("generateJsonSchema2Pojo"))
}

tasks.register<Test>("log4j2IntegrationTest") {
  useJUnitPlatform {
    includeTags("log4j2-config")
  }
  testLogging {
    events = setOf(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
  }
}

tasks.register<Test>("logClientsIntegrationTest") {
  useJUnitPlatform {
    includeTags("logger-client")
  }
  testLogging {
    events = setOf(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
  }
}
