plugins {
  id("io.micronaut.application") version "4.4.2"
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
  application
}

group = "io.airbyte.connector.rollout.client"

java {
  sourceCompatibility = JavaVersion.VERSION_21
  targetCompatibility = JavaVersion.VERSION_21
}

repositories {
  mavenCentral()
}

configurations.all {
  exclude(group="org.apache.logging.log4j")
}

dependencies {
  // TODO: remove the deps not being used
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut

  implementation("info.picocli:picocli:4.7.4")
  implementation("io.micronaut.picocli:micronaut-picocli")
  implementation("io.temporal:temporal-sdk:1.25.0")
  annotationProcessor("info.picocli:picocli-codegen:4.7.4")
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-connector-rollout-shared"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(libs.airbyte.protocol)

  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.12.5")

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

tasks.jar {
  manifest {
    attributes(
      "Main-Class" to "io.airbyte.connector.rollout.client.ConnectorRolloutCLI"
    )
  }

  archiveBaseName.set("run-connector-rollout-cli")
  archiveVersion.set("") // Remove the version from the JAR file name
}

tasks.withType<JavaCompile> {
  options.encoding = "UTF-8"
  options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-Xlint:deprecation"))
}

micronaut {
  runtime("netty")
  testRuntime("junit5")
  processing {
    incremental(true)
    annotations("io.airbyte.connector.rollout.client.*")
  }
}


tasks.withType<Jar> {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
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
        "MICRONAUT_ENVIRONMENTS" to "test"
      )
    )
  }
  docker {
    imageName.set("connector-rollout-client")
  }
}
