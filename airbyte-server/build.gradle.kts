plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
  id("io.airbyte.gradle.kube-reload")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  annotationProcessor(libs.micronaut.jaxrs.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.jaxrs.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.cache)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.micronaut.jaxrs.server)
  implementation(libs.micronaut.http)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.micronaut.security)
  implementation(libs.micronaut.security.jwt)
  implementation(libs.bundles.flyway)
  implementation(libs.s3)
  implementation(libs.sts)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation(libs.reactor.core)
  implementation(libs.temporal.sdk)
  implementation(libs.bundles.datadog)
  implementation(libs.sentry.java)
  implementation(libs.swagger.annotations)
  implementation(libs.google.cloud.storage)
  implementation(libs.cron.utils)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.kubernetes.client)

  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-api:public-api"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-audit-logging"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-license"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-micronaut-security"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-commons-server"))
  implementation(project(":oss:airbyte-commons-with-dependencies"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-mappers"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-db:jooq"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-mappers"))
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-oauth"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-persistence:job-persistence"))

  runtimeOnly(libs.snakeyaml)
  runtimeOnly(libs.javax.databind)
  runtimeOnly(libs.bundles.logback)

  // Required for local database secret hydration)
  runtimeOnly(libs.hikaricp)
  runtimeOnly(libs.h2.database)

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.micronaut.jaxrs.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.micronaut.jaxrs.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.reactor.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.bundles.kotest)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.micronaut.http.client)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

// we want to be able to access the generated db files from config/init when we build the server docker image.)
val copySeed =
  tasks.register<Copy>("copySeed") {
    from("${project(":oss:airbyte-config:init").layout.buildDirectory.get()}/resources/main/config")
    into("${project.layout.buildDirectory.get()}/config_init/resources/main/config")
    dependsOn(project(":oss:airbyte-config:init").tasks.named("processResources"))
  }

// need to make sure that the files are in the resource directory before copying.)
// tests require the seed to exist.)
tasks.named("test") {
  dependsOn(copySeed)
}
tasks.named("assemble") {
  dependsOn(copySeed)
}

airbyte {
  application {
    mainClass = "io.airbyte.server.Application"
    defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    localEnvVars.putAll(
      mapOf(
        "AIRBYTE_ROLE" to "undefined",
        "AIRBYTE_VERSION" to "dev",
        "DATABASE_USER" to "docker",
        "DATABASE_PASSWORD" to "docker",
        "CONFIG_DATABASE_USER" to "docker",
        "CONFIG_DATABASE_PASSWORD" to "docker",
        // we map the docker pg db to port 5433 so it does not conflict with other pg instances.
        "DATABASE_URL" to "jdbc:postgresql://localhost:5433/airbyte",
        "CONFIG_DATABASE_URL" to "jdbc:postgresql://localhost:5433/airbyte",
        "RUN_DATABASE_MIGRATION_ON_STARTUP" to "true",
        "WORKSPACE_ROOT" to "/tmp/workspace",
        "CONFIG_ROOT" to "/tmp/airbyte_config",
        "TRACKING_STRATEGY" to "logging",
        "TEMPORAL_HOST" to "localhost:7233",
        "MICRONAUT_ENVIRONMENTS" to "control-plane",
      ),
    )
  }

  docker {
    imageName = "server"
  }

  kubeReload {
    deployment = "ab-server"
    container = "airbyte-server-container"
  }

  spotbugs {
    excludes =
      listOf(
        "  <Match>\n" +
          "    <Package name=\"io.airbyte.server.repositories.domain.*\" />\n" +
          "    <!-- All args constructor used by builders trigger this error -->\n" +
          "    <Bug pattern=\"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE\" />\n" +
          "  </Match>",
      )
  }
}

tasks.named<Test>("test") {
  environment(
    mapOf(
      "AIRBYTE_VERSION" to "dev",
      "MICRONAUT_ENVIRONMENTS" to "test",
      "SERVICE_NAME" to project.name,
    ),
  )
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
