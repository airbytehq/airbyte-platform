plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.picocli.codegen)

  implementation(libs.picocli)
  implementation(libs.micronaut.picocli)
  implementation(libs.kotlin.logging)
  implementation(libs.jackson.datatype)
  implementation(libs.temporal.sdk)
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-connector-rollout-shared"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(libs.airbyte.protocol)
  testImplementation(libs.mockk)

  runtimeOnly(libs.bundles.logback)
}

application {
  // Default to running ConnectorRolloutCLI
  mainClass.set("io.airbyte.connector.rollout.client.ConnectorRolloutCLI")
}

val runConnectorRolloutCLI by tasks.registering(JavaExec::class) {
  group = "application"
  description = "Run the ConnectorRolloutCLI with specified arguments"
  classpath = sourceSets["main"].runtimeClasspath
  mainClass.set("io.airbyte.connector.rollout.client.ConnectorRolloutCLI")
  args = listOf("") // Set default CLI command and options here if needed
}

tasks.withType<JavaCompile> {
  options.encoding = "UTF-8"
  options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-Xlint:deprecation"))
}

airbyte {
  application {
    mainClass.set("io.airbyte.connector.rollout.client.ConnectorRolloutCLI")
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "DATA_PLANE_ID" to "local",
        "MICRONAUT_ENVIRONMENTS" to "test",
      ),
    )
  }
  docker {
    imageName.set("connector-rollout-client")
  }
}

tasks.withType<Jar> {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
