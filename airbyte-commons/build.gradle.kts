import de.undercouch.gradle.tasks.download.Download

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  alias(libs.plugins.de.undercouch.download)
}

dependencies {
  api(libs.bundles.micronaut.annotation)

  implementation(libs.bundles.jackson)
  implementation(libs.guava)
  implementation(libs.bundles.slf4j)
  implementation(libs.google.cloud.storage)
  implementation(libs.airbyte.protocol)

  // this dependency is an exception to the above rule because it is only used INTERNALLY to the Commons library.
  implementation(libs.json.path)
  implementation(libs.json.smart)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.bundles.logback)
}

airbyte {
  spotless {
    excludes = listOf("src/main/resources/seed/specs_secrets_mask.yaml")
  }
}

val downloadSpecSecretMask =
  tasks.register<Download>("downloadSpecSecretMask") {
    src("https://connectors.airbyte.com/files/registries/v0/specs_secrets_mask.yaml")
    dest(File(projectDir, "src/main/resources/seed/specs_secrets_mask.yaml"))
    overwrite(true)
    onlyIfModified(true)
  }

tasks.named("processResources") {
  dependsOn(downloadSpecSecretMask)
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "Z_TESTING_PURPOSES_ONLY_1" to "value-defined",
      "Z_TESTING_PURPOSES_ONLY_2" to "  ",
    ),
  )
}
