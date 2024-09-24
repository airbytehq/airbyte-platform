plugins {
  id("io.micronaut.application") version "4.4.2"
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.docker")
  application
}

group = "io.airbyte.connector.rollout.worker"

java {
  sourceCompatibility = JavaVersion.VERSION_21
  targetCompatibility = JavaVersion.VERSION_21
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(project(mapOf("path" to ":oss:airbyte-commons-temporal")))
  // TODO: remove the deps not being used
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut

  implementation("io.temporal:temporal-sdk:1.25.0")
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-connector-rollout-shared"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(libs.airbyte.protocol)
}

application {
  // Default to running ConnectorRolloutWorker
  mainClass.set("io.airbyte.connector.rollout.worker.ConnectorRolloutWorkerApplication")
}

tasks.jar {
  manifest {
    attributes(
      "Main-Class" to "io.airbyte.connector.rollout.worker.ConnectorRolloutWorkerApplication"
    )
  }

  archiveBaseName.set("run-connector-rollout-worker")
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
    annotations("io.airbyte.connector.rollout.worker.*")
  }
}


tasks.withType<Jar> {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

airbyte {
  application {
    mainClass.set("io.airbyte.connector.rollout.worker.ConnectorRolloutWorkerApplication")
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
    imageName.set("connector-rollout-worker")
  }
}

