import java.util.Properties

plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

configurations.all {
    resolutionStrategy {
        // Ensure that the versions defined in deps.toml are used)
        // instead of versions from transitive dependencies)
        // Force to avoid updated version(brought in transitively from Micronaut 3.8+)
        // that is incompatible with our current Helm setup)
        force (libs.flyway.core, libs.s3, libs.aws.java.sdk.s3, libs.sts, libs.aws.java.sdk.sts)
    }
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.lombok)
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    annotationProcessor(libs.micronaut.jaxrs.processor)

    implementation(libs.reactor.core)
    compileOnly(libs.lombok)

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.bundles.micronaut.data.jdbc)
    implementation(libs.bundles.micronaut.metrics)
    implementation(libs.micronaut.jaxrs.server)
    implementation(libs.micronaut.security)
    implementation(libs.flyway.core)
    implementation(libs.s3)
    implementation(libs.sts)
    implementation(libs.aws.java.sdk.s3)
    implementation(libs.aws.java.sdk.sts)

    implementation(project(":airbyte-analytics"))
    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-auth"))
    implementation(project(":airbyte-commons-converters"))
    implementation(project(":airbyte-commons-license"))
    implementation(project(":airbyte-commons-micronaut"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-commons-temporal-core"))
    implementation(project(":airbyte-commons-server"))
    implementation(project(":airbyte-commons-with-dependencies"))
    implementation(project(":airbyte-config:init"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-config:config-secrets"))
    implementation(project(":airbyte-config:specs"))
    implementation(project(":airbyte-data"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-notification"))
    implementation(project(":airbyte-oauth"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-persistence:job-persistence"))

    implementation(libs.slugify)
    implementation(libs.temporal.sdk)
    implementation(libs.bundles.datadog)
    implementation(libs.sentry.java)
    implementation(libs.swagger.annotations)
    implementation(libs.google.cloud.storage)

    runtimeOnly(libs.javax.databind)

    // Required for local database secret hydration)
    runtimeOnly(libs.hikaricp)
    runtimeOnly(libs.h2.database)

    testImplementation(libs.bundles.micronaut.test)
    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers.postgresql)
    testImplementation(libs.mockwebserver)
    testImplementation(libs.mockito.inline)

    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}

// we want to be able to access the generated db files from config/init when we build the server docker image.)
val copySeed = tasks.register<Copy>("copySeed") {
    from ("${project(":airbyte-config:init").buildDir}/resources/main/config")
    into ("$buildDir/config_init/resources/main/config")
    dependsOn(project(":airbyte-config:init").tasks.named("processResources"))
}

// need to make sure that the files are in the resource directory before copying.)
// tests require the seed to exist.)
tasks.named("test") {
    dependsOn(copySeed)
}
tasks.named("assemble") {
    dependsOn(copySeed)
}

val env = Properties().apply {
    load(rootProject.file(".env.dev").inputStream())
}

airbyte {
    application {
        mainClass = "io.airbyte.server.Application"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
            @Suppress("UNCHECKED_CAST")
            localEnvVars.putAll(env.toMap() as Map<String, String>)
    localEnvVars.putAll(mapOf(
                "AIRBYTE_ROLE"                     to (System.getenv("AIRBYTE_ROLE") ?: "undefined"),
                "AIRBYTE_VERSION"                  to env["VERSION"].toString(),
                "DATABASE_USER"                    to env["DATABASE_USER"].toString(),
                "DATABASE_PASSWORD"                to env["DATABASE_PASSWORD"].toString(),
                "CONFIG_DATABASE_USER"             to (env["CONFIG_DATABASE_USER"]?.toString() ?: ""),
                "CONFIG_DATABASE_PASSWORD"         to (env["CONFIG_DATABASE_PASSWORD"]?.toString() ?: ""),
                // we map the docker pg db to port 5433 so it does not conflict with other pg instances.
                "DATABASE_URL"                     to "jdbc:postgresql://localhost:5433/${env["DATABASE_DB"]}",
                "CONFIG_DATABASE_URL"              to "jdbc:postgresql://localhost:5433/${env["CONFIG_DATABASE_DB"]}",
                "RUN_DATABASE_MIGRATION_ON_STARTUP" to "true",
                "WORKSPACE_ROOT"                   to env["WORKSPACE_ROOT"].toString(),
                "CONFIG_ROOT"                      to "/tmp/airbyte_config",
                "TRACKING_STRATEGY"                to env["TRACKING_STRATEGY"].toString(),
                "TEMPORAL_HOST"                    to "localhost:7233",
                "MICRONAUT_ENVIRONMENTS"           to "control-plane",
    ))
    }

    docker {
        imageName = "server"
    }

    spotbugs {
        excludes = listOf("  <Match>\n" +
                            "    <Package name=\"io.airbyte.server.repositories.domain.*\" />\n" +
                            "    <!-- All args constructor used by builders trigger this error -->\n" +
                            "    <Bug pattern=\"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE\" />\n" +
                            "  </Match>")
    }
}

tasks.named<Test>("test") {
    environment(mapOf(
 "AIRBYTE_VERSION" to env["VERSION"],
 "MICRONAUT_ENVIRONMENTS" to  "test",
 "SERVICE_NAME" to project.name,
    ))
}
