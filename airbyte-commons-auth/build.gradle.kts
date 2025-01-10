import org.yaml.snakeyaml.Yaml
import java.io.FileReader

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.keycloak.client)
  implementation(libs.bundles.micronaut)
  implementation(libs.micronaut.security)
  implementation(libs.failsafe.okhttp)
  implementation(libs.kotlin.logging)
  implementation(libs.okhttp)
  implementation(libs.reactor.core)
  implementation(libs.snakeyaml)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-config:config-models"))

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockk)
  testImplementation(libs.reactor.test)
}

tasks.named<Test>("test") {
  maxHeapSize = "2g"
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val generateIntents =
  tasks.register("generateIntents") {

    doLast {
      // Load YAML data
      val intentsYaml = file("src/main/resources/intents.yaml")
      val yaml = Yaml()
      val data = yaml.load<Map<String, Any>>(FileReader(intentsYaml))
      val intentsData = data["intents"] as? Map<*, *>

      // Generate the Intent enum class as a string
      val enumEntries =
        intentsData
          ?.map { (key, value) ->
            // Safely cast value and extract roles
            val details = value as? Map<*, *>
            val roles = (details?.get("roles") as? List<*>)?.filterIsInstance<String>() ?: emptyList()
            val rolesString = roles.joinToString(", ") { "\"$it\"" }
            "$key(setOf($rolesString))"
          }?.joinToString(",\n  ") ?: ""

      // read the intent-template.txt and replace <enum-entries> with the generated enum entries
      val intentClassContent = file("src/main/resources/intent-class-template.txt").readText().replace("<enum-entries>", enumEntries)

      val buildDirPath =
        layout.buildDirectory.asFile
          .get()
          .absolutePath
      val outputDir = File(buildDirPath, "generated/intents/io/airbyte-commons-auth/generated")
      outputDir.mkdirs()
      File(outputDir, "Intent.kt").writeText(intentClassContent)
    }
  }

tasks.named("compileKotlin") {
  dependsOn(generateIntents)
}

kotlin {
  sourceSets["main"].apply {
    kotlin.srcDir(
      "${project.layout.buildDirectory.get()}/generated/intents/io/airbyte-commons-auth/generated",
    )
  }
}
