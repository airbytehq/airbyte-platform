import org.jsonschema2pojo.SourceType

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  id("com.github.eirnym.js2p")
}

dependencies {
  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(libs.airbyte.protocol)
}

jsonSchema2Pojo {
  setSourceType(SourceType.YAMLSCHEMA.name)
  setSource(files("${sourceSets["main"].output.resourcesDir}/workers_models"))
  targetDirectory = file("${project.layout.buildDirectory.get()}/generated/src/gen/java/")
  removeOldOutput = true

  targetPackage = "io.airbyte.persistence.job.models"

  useLongIntegers = true
  generateBuilders = true
  includeConstructors = false
  includeSetters = true
}

tasks.named("compileKotlin") {
  dependsOn(tasks.named("generateJsonSchema2Pojo"))
}
