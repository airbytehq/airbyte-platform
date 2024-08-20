import java.util.Properties

plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
  kotlin("kapt")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok) // Lombok must be added BEFORE Micronaut
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  kapt(platform(libs.micronaut.platform))
  kapt(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.cache)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.bundles.temporal)
  implementation(libs.bundles.datadog)
  implementation(libs.failsafe)
  implementation(libs.failsafe.okhttp)
  implementation(libs.java.jwt)
  implementation(libs.kotlin.logging)
  implementation(libs.okhttp)
  implementation(libs.sentry.java)
  implementation(libs.lombok)
  implementation(libs.commons.io)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))

  runtimeOnly(libs.snakeyaml)

  kaptTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.micronaut.test)
}

val env =
  Properties().apply {
    load(rootProject.file(".env.dev").inputStream())
  }

airbyte {
  application {
    mainClass = "io.airbyte.cron.MicronautCronRunner"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    @Suppress("UNCHECKED_CAST")
    localEnvVars.putAll(env.toMap() as Map<String, String>)
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to (System.getenv("AIRBYTE_ROLE") ?: "undefined"),
        "AIRBYTE_VERSION" to env["VERSION"].toString(),
      ),
    )
  }

  docker {
    imageName = "cron"
  }
}

kapt {
  keepJavacAnnotationProcessors = true
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.
// Once lombok has been removed, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Copies the connector <> platform compatibility JSON file for use in tests
tasks.register<Copy>("copyPlatformCompatibilityMatrix") {
  val platformCompatibilityFile = project.rootProject.layout.projectDirectory.file("tools/connectors/platform-compatibility/platform-compatibility.json")
  if(file(platformCompatibilityFile).exists()) {
    from(platformCompatibilityFile)
    into(project.layout.projectDirectory.dir("src/test/resources"))
  }
}

tasks.named("processTestResources") {
  dependsOn("copyPlatformCompatibilityMatrix")
}

afterEvaluate {
  tasks.named("spotlessStyling") {
    dependsOn("copyPlatformCompatibilityMatrix")
  }
}